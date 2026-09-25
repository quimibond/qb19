# -*- coding: utf-8 -*-
"""PR 3 del plan (19.0.50.0.0): auditoría lista para octubre.
AU-1 checklist generado del proceso, AU-2 independencia del auditor por su
puesto, AU-3 informe F-P-G03-07 archivado al cerrar."""
from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestAuditPr3(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.job_exec = cls.env['hr.job'].create({'name': 'PLANEADOR AUD PRUEBA'})
        cls.job_other = cls.env['hr.job'].create({'name': 'CONTADOR AUD PRUEBA'})
        cls.family = cls.env['sgi.job.family'].create({
            'code': 'FAM-AUD', 'name': 'Familia AUD', 'job_ids': [(6, 0, cls.job_exec.ids)]})
        cls.process = cls.env['sgi.process'].create({'code': 'XAU1', 'name': 'Proceso auditado AU'})
        cls.other_process = cls.env['sgi.process'].create({'code': 'XAU2', 'name': 'Otro proceso AU'})
        Activity = cls.env['sgi.process.activity']
        cls.deliverable = cls.env['sgi.deliverable'].create({
            'name': 'Programa semanal AU',
            'odoo_model_id': cls.env['ir.model']._get_id('res.partner')})
        cls.act1 = Activity.create({
            'process_id': cls.process.id, 'name': 'Programar la semana',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_exec.id})],
            'output_deliverable_ids': [(6, 0, cls.deliverable.ids)]})
        cls.act2 = Activity.create({
            'process_id': cls.process.id, 'name': 'Aprobar el programa',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_other.id}),
                         (0, 0, {'role': 'aprueba', 'target_type': 'family', 'family_id': cls.family.id})]})
        cls.act_other = Activity.create({
            'process_id': cls.other_process.id, 'name': 'Conciliar bancos',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_other.id})]})
        cls.auditor_user = new_test_user(
            cls.env, login='au3_auditor', groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.env['hr.employee'].create({
            'name': 'Auditor AU', 'job_id': cls.job_other.id, 'user_id': cls.auditor_user.id})
        cls.exec_user = new_test_user(
            cls.env, login='au3_exec', groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.env['hr.employee'].create({
            'name': 'Planeador AU', 'job_id': cls.job_exec.id, 'user_id': cls.exec_user.id})

    def _audit(self, **vals):
        return self.env['sgi.audit'].create(dict({
            'audit_type': 'interna', 'process_ids': [(6, 0, self.process.ids)],
            'lead_auditor_id': self.auditor_user.id}, **vals))

    def test_01_checklist_generado_del_proceso(self):
        audit = self._audit()
        self.assertFalse(audit.checklist_line_ids)
        audit.action_plan()
        self.assertEqual(audit.state, 'planificada')
        self.assertEqual(audit.checklist_line_ids.mapped('activity_id'), self.act1 | self.act2)
        line = audit.checklist_line_ids.filtered(lambda l: l.activity_id == self.act1)
        self.assertIn('¿Se cumple XAU1', line.question)
        self.assertIn('Programar la semana', line.question)
        self.assertIn('en plazo y con evidencia', line.question)
        self.assertEqual(line.executor, 'PLANEADOR AUD PRUEBA')
        self.assertEqual(line.deliverables, 'Programa semanal AU')
        self.assertTrue(line.can_open_records)
        action = line.action_open_records()
        self.assertEqual(action['res_model'], 'res.partner')
        # Idempotente y con las actividades nuevas.
        audit.action_generate_checklist()
        self.assertEqual(len(audit.checklist_line_ids), 2)
        act3 = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Publicar el programa',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_other.id})]})
        audit.action_generate_checklist()
        self.assertIn(act3, audit.checklist_line_ids.mapped('activity_id'))
        self.assertEqual(audit.checklist_count, 3)

    def test_02_respuesta_no_conforme_crea_su_hallazgo(self):
        audit = self._audit()
        audit.action_plan()
        audit.action_start()
        line = audit.checklist_line_ids.filtered(lambda l: l.activity_id == self.act1)
        line.write({'answer': 'nc_menor', 'evidence': 'Sin programa las semanas 30 y 31'})
        self.assertTrue(line.finding_id)
        self.assertEqual(line.finding_id.finding_type, 'nc_menor')
        self.assertEqual(line.finding_id.process_id, self.process)
        self.assertIn('semanas 30 y 31', line.finding_id.description)
        self.assertEqual(line.finding_id.checklist_line_id, line)
        line.answer = 'nc_mayor'
        self.assertEqual(line.finding_id.finding_type, 'nc_mayor')
        self.assertEqual(audit.checklist_nonconforming_count, 1)
        # Conforme retira el hallazgo mientras no tenga NC.
        finding = line.finding_id
        line.answer = 'conforme'
        self.assertFalse(finding.exists())
        self.assertFalse(line.finding_id)
        self.assertEqual(audit.checklist_answered_count, 1)
        line.answer = 'observacion'
        self.assertEqual(line.finding_id.finding_type, 'observacion')

    def test_03_independencia_por_puesto(self):
        # El planeador ejecuta una actividad del proceso: no puede auditarlo.
        with self.assertRaises(ValidationError):
            self._audit(lead_auditor_id=self.exec_user.id)
        with self.assertRaises(ValidationError):
            self._audit(auditor_ids=[(6, 0, self.exec_user.ids)])
        # Aprobar por la familia de su puesto también cuenta.
        approve_process = self.env['sgi.process'].create({'code': 'XAU3', 'name': 'Proceso aprobado AU'})
        self.env['sgi.process.activity'].create({
            'process_id': approve_process.id, 'name': 'Liberar el lote',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_other.id}),
                         (0, 0, {'role': 'aprueba', 'target_type': 'family', 'family_id': self.family.id})]})
        with self.assertRaises(ValidationError):
            self._audit(lead_auditor_id=self.exec_user.id, process_ids=[(6, 0, approve_process.ids)])
        # En otro proceso sí puede.
        audit = self._audit(lead_auditor_id=self.exec_user.id,
                            process_ids=[(6, 0, self.other_process.ids)])
        self.assertTrue(audit.folio)
        # El contador ejecuta en XAU2 pero no en XAU1: audita XAU1.
        self.assertTrue(self._audit().folio)

    def test_04_informe_archivado_al_cerrar(self):
        audit = self._audit()
        audit.action_plan()
        audit.action_start()
        audit.write({'opening_minutes': 'Alcance confirmado', 'closing_minutes': 'Dos observaciones',
                     'conclusion': 'El proceso opera conforme.'})
        line = audit.checklist_line_ids[:1]
        line.write({'answer': 'observacion', 'evidence': 'Registro tardío'})
        line.finding_id.write({'disposition': 'sin_accion', 'reason_no_action': 'Aislado'})
        audit.action_report()
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_audit_report_document', audit.ids)[0].decode()
        for text in ('F-P-G03-07', 'Alcance confirmado', 'Dos observaciones', 'Checklist por actividad',
                     'Registro tardío', 'Observaciones', 'El proceso opera conforme', 'Firmas'):
            self.assertIn(text, html)
        with self.assertRaises(UserError):
            audit.action_open_report_document()
        audit.action_close()
        self.assertEqual(audit.state, 'cerrada')
        self.assertTrue(audit.report_document_id, "Al cerrar queda el PDF en la auditoría.")
        self.assertEqual(audit.report_document_id.mimetype, 'application/pdf')
        self.assertIn(audit.folio, audit.report_document_id.name)
        self.assertTrue(any('F-P-G03-07' in (m.body or '') for m in audit.message_ids))
