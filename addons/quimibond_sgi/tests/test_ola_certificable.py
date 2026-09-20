# -*- coding: utf-8 -*-
"""Tests de la ola «certificable» (segunda auditoría, 2026-08): requisitos
legales, partes interesadas, plan de acción de objetivos (+B.18), MOC,
recalcular medición (B.16), acuses de la política (D.28), certificado de
calibración, entradas 11-12 de la RxD y correo crítico."""
from datetime import date

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestOlaCertificable(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.manager = cls.env['res.users'].create({
            'name': 'MAST certificable', 'login': 'sgi_cert_mgr',
            'email': 'mast@example.com',
            'group_ids': [(6, 0, [cls.env.ref('quimibond_sgi.group_sgi_manager').id])]})

    # ------------------------------------------------------------------
    # Requisitos legales (6.1.3 / 9.1.2)
    # ------------------------------------------------------------------
    def test_legal_no_cumple_creates_nc(self):
        req = self.env['sgi.legal.requirement'].create({
            'name': 'Registro de residuos peligrosos',
            'reference': 'NOM-052-SEMARNAT-2005', 'system': 'ambiental'})
        req.action_mark_no_cumple()
        self.assertEqual(req.compliance_state, 'no_cumple')
        self.assertTrue(req.last_eval_date)
        self.assertTrue(req.alert_id)
        self.assertEqual(req.alert_id.sgi_classification, 'mayor')
        self.assertEqual(req.alert_id.sgi_source_id.code,
                         'requisito_legal_incumplido')
        # Idempotente mientras la NC siga abierta.
        first = req.alert_id
        req.action_mark_parcial()
        self.assertEqual(req.alert_id, first)
        # Cumplir no borra la trazabilidad de la NC.
        req.action_mark_cumple()
        self.assertEqual(req.compliance_state, 'cumple')
        self.assertTrue(req.next_eval_date)

    def test_legal_source_off_no_nc(self):
        source = self.env.ref('quimibond_sgi.sgi_alert_source_legal')
        source.enabled = False
        self.addCleanup(setattr, source, 'enabled', True)
        req = self.env['sgi.legal.requirement'].create({
            'name': 'Permiso de descarga', 'system': 'ambiental'})
        # Fuente manual apagada: el botón AVISA en vez de callar.
        with self.assertRaises(UserError):
            req.action_mark_no_cumple()

    # ------------------------------------------------------------------
    # Partes interesadas (4.2)
    # ------------------------------------------------------------------
    def test_interested_party_review(self):
        party = self.env['sgi.interested.party'].create({
            'name': 'STPS', 'party_type': 'externa', 'category': 'autoridad',
            'needs': 'Cumplimiento de NOMs de seguridad'})
        self.assertTrue(party.next_review_date)  # sin revisar: debe ya
        party.action_mark_reviewed()
        self.assertEqual(party.last_review_date, date.today())
        self.assertGreater(party.next_review_date, date.today())

    # ------------------------------------------------------------------
    # Objetivos: plan de acción (6.2.2) y salud (B.18)
    # ------------------------------------------------------------------
    def test_objective_action_plan_and_health(self):
        objective = self.env['sgi.objective'].create(
            {'name': 'Reducir desperdicio', 'target_year': 2030})
        line = self.env['sgi.action.line'].create({
            'objective_id': objective.id, 'name': 'Programa de mermas',
            'responsible_id': self.manager.id, 'date_commit': date.today()})
        self.assertTrue(line.activity_id)
        self.assertEqual(line.activity_id.res_id, objective.id)
        # XOR: un origen exactamente.
        risk = self.env['sgi.risk'].create({'name': 'Riesgo doble origen'})
        with self.assertRaises(ValidationError):
            line.write({'risk_id': risk.id})
        # B.18: un KPI sin proceso con último semáforo rojo pinta el objetivo.
        indicator = self.env['sgi.indicator'].create({
            'code': 'ZOBJ', 'name': 'KPI objetivo', 'calc_mode': 'manual',
            'objective_id': objective.id, 'direction': 'higher_better',
            'target_objective': 90.0, 'target_acceptable': 80.0})
        self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id, 'period_date': date(2026, 6, 1),
            'value': 10.0, 'state': 'validado'})
        self.assertEqual(objective.health, 'rojo')

    # ------------------------------------------------------------------
    # B.16: recalcular el valor de una medición
    # ------------------------------------------------------------------
    def test_measure_recompute(self):
        indicator = self.env['sgi.indicator'].create({
            'code': 'ZREC', 'name': 'KPI recompute', 'calc_mode': 'manual'})
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id, 'period_date': date(2026, 6, 1),
            'value': 5.0, 'state': 'capturado'})
        with self.assertRaises(UserError):
            measure.action_recompute_value()  # manual: nada que recalcular
        indicator.calc_mode = 'reproceso'  # devuelve None (sin fuente)
        measure.action_recompute_value()
        self.assertEqual(measure.state, 'pendiente')

    # ------------------------------------------------------------------
    # D.28: acuses de la política
    # ------------------------------------------------------------------
    def test_policy_acks(self):
        policy = self.env['sgi.policy'].create({'name': 'Política integral'})
        with self.assertRaises(UserError):
            policy.action_generate_acks()  # sin documento ligado
        job = self.env['hr.job'].create({'name': 'Puesto política'})
        self.env['hr.employee'].create(
            {'name': 'Empleado política', 'job_id': job.id})
        # Clave única de prueba (R-*): en una BD copia de producción ya existe
        # un MIID vigente y el índice único lo rechazaría.
        doc = self.env['documents.document'].create({
            'name': 'Reglamento política test', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'reglamento',
            'sgi_code': 'R-POLITICA-TEST', 'sgi_state': 'vigente'})
        policy.document_id = doc
        with self.assertRaises(UserError):
            policy.action_generate_acks()  # sin puestos asignados
        doc.sgi_job_ids = [(6, 0, [job.id])]
        action = policy.action_generate_acks()
        self.assertTrue(doc.sgi_ack_ids)
        self.assertEqual(action['res_model'], 'sgi.document.ack')

    # ------------------------------------------------------------------
    # Certificado de calibración (7.1.5)
    # ------------------------------------------------------------------
    def test_calibration_external_requires_certificate(self):
        equipment = self.env['maintenance.equipment'].create(
            {'name': 'Vernier Z', 'sgi_is_measuring': True})
        with self.assertRaises(ValidationError):
            self.env['sgi.calibration'].create({
                'equipment_id': equipment.id, 'calibration_type': 'externa',
                'result': 'conforme'})
        cal = self.env['sgi.calibration'].create({
            'equipment_id': equipment.id, 'calibration_type': 'externa',
            'result': 'conforme', 'certificate_ref': 'CERT-001'})
        self.assertTrue(cal.next_date)

    # ------------------------------------------------------------------
    # RxD: entradas 11 y 12
    # ------------------------------------------------------------------
    def test_rxd_new_entries(self):
        review = self.env['sgi.management.review'].create({
            'period_from': date(2026, 1, 1), 'period_to': date(2026, 6, 30)})
        review.action_load_inputs()
        self.assertTrue(review.legal_summary)
        self.assertTrue(review.participation_summary)

    # ------------------------------------------------------------------
    # MOC (6.3 / 8.1.3)
    # ------------------------------------------------------------------
    def test_moc_gate(self):
        category = self.env.ref('quimibond_sgi.sgi_approval_category_moc')
        request = self.env['approval.request'].create({
            'name': 'Cambio de layout tejido', 'category_id': category.id,
            'request_owner_id': self.manager.id})
        self.assertTrue(request.sgi_is_moc)
        with self.assertRaises(UserError):
            request.action_approve()
        request.write({
            'sgi_reason': 'Reubicar la línea 2',
            'sgi_affected_process_ids': [(0, 0, {
                'code': 'ZMOC', 'name': 'Proceso MOC'})],
            'sgi_moc_risk_note': 'Riesgos evaluados: ruido y tránsito.',
        })
        request._sgi_check_moc_ready()  # ya no truena

    # ------------------------------------------------------------------
    # Correo crítico
    # ------------------------------------------------------------------
    def test_critical_mail_on_grave_incident(self):
        before = self.env['mail.mail'].search_count([])
        self.env['sgi.incident'].create({
            'name': 'Caída con lesión', 'incident_type': 'lesion',
            'severity': 'grave'})
        after = self.env['mail.mail'].search_count([])
        self.assertGreater(
            after, before,
            "El incidente grave debe generar al menos un correo crítico.")
