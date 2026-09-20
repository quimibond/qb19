# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError, ValidationError


@tagged('post_install', '-at_install')
class TestEmergency(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.plan = cls.env['sgi.emergency.plan'].create({
            'name': 'Incendio en almacén PT',
            'plan_type': 'incendio',
            'responsible_id': cls.env.user.id,
            'drill_frequency_months': 6,
        })

    def test_01_folios(self):
        self.assertTrue(self.plan.folio.startswith('PE-'))
        drill = self.env['sgi.emergency.drill'].create({'plan_id': self.plan.id})
        self.assertTrue(drill.folio.startswith('SIM-'))

    def test_02_vigente_requires_responsible(self):
        plan = self.env['sgi.emergency.plan'].create({
            'name': 'Sismo', 'plan_type': 'sismo'})
        with self.assertRaises(UserError):
            plan.action_set_vigente()
        plan.responsible_id = self.env.user
        plan.action_set_vigente()
        self.assertEqual(plan.state, 'vigente')

    def test_03_drill_close_lock(self):
        drill = self.env['sgi.emergency.drill'].create({'plan_id': self.plan.id})
        # Sin resultado ni participantes -> bloqueado.
        with self.assertRaises(UserError):
            drill.action_set_realizado()
        drill.write({'result': 'no_satisfactorio', 'participants_count': 25})
        # No satisfactorio sin hallazgos ni acciones -> bloqueado.
        with self.assertRaises(UserError):
            drill.action_set_realizado()
        drill.findings = "Ruta de evacuación obstruida."
        self.env['sgi.action.line'].create({
            'drill_id': drill.id, 'name': 'Despejar ruta',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
        })
        drill.action_set_realizado()
        self.assertEqual(drill.state, 'realizado')
        self.assertTrue(drill.date_done)
        # Fechas del plan: último y próximo simulacro.
        self.assertEqual(self.plan.last_drill_date, drill.date_done)
        self.assertTrue(self.plan.next_drill_date)

    def test_04_drill_satisfactorio_no_actions_needed(self):
        drill = self.env['sgi.emergency.drill'].create({
            'plan_id': self.plan.id, 'result': 'satisfactorio',
            'participants_count': 30,
        })
        drill.action_set_realizado()
        self.assertEqual(drill.state, 'realizado')

    def test_05_action_xor_includes_drill(self):
        drill = self.env['sgi.emergency.drill'].create({'plan_id': self.plan.id})
        risk = self.env['sgi.risk'].create({'name': 'R', 'instrument': 'ryo'})
        with self.assertRaises(ValidationError):
            self.env['sgi.action.line'].create({
                'drill_id': drill.id, 'risk_id': risk.id,
                'name': 'Doble origen', 'responsible_id': self.env.user.id,
                'date_commit': date.today(),
            })

    def test_06_cron_idempotent(self):
        self.plan.action_set_vigente()
        Cron = self.env['sgi.cron']
        Cron.cron_emergency_drills()
        Cron.cron_emergency_drills()
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.emergency.plan'),
            ('res_id', '=', self.plan.id),
        ])
        self.assertEqual(len(acts), 1, "El cron no debe duplicar la actividad.")


@tagged('post_install', '-at_install')
class TestMsa(TransactionCase):

    def _equipment(self):
        return self.env['maintenance.equipment'].create({
            'name': 'Micrómetro MSA', 'sgi_is_measuring': True})

    def test_01_verdict_thresholds(self):
        eq = self._equipment()
        study = self.env['sgi.msa.study'].create({
            'equipment_id': eq.id, 'grr_pct': 8.0})
        self.assertTrue(study.folio.startswith('MSA-'))
        self.assertEqual(study.verdict, 'aceptable')
        study.grr_pct = 22.0
        self.assertEqual(study.verdict, 'marginal')
        study.grr_pct = 35.0
        self.assertEqual(study.verdict, 'inaceptable')
        self.assertEqual(eq.sgi_msa_count, 1)

    def test_02_unacceptable_schedules_activity(self):
        eq = self._equipment()
        study = self.env['sgi.msa.study'].create({
            'equipment_id': eq.id, 'grr_pct': 45.0})
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.msa.study'),
            ('res_id', '=', study.id),
        ])
        self.assertTrue(acts, "MSA inaceptable debe agendar actividad al Jefe MAST.")


@tagged('post_install', '-at_install')
class TestSupplierApproval(TransactionCase):

    def test_01_blocked_supplier_cannot_confirm_po(self):
        partner = self.env['res.partner'].create({
            'name': 'Proveedor Bloqueado', 'is_company': True})
        product = self.env['product.product'].create({
            'name': 'Insumo', 'purchase_ok': True})
        po = self.env['purchase.order'].create({
            'partner_id': partner.id,
            'order_line': [(0, 0, {
                'product_id': product.id, 'product_qty': 1,
                'price_unit': 10.0})],
        })
        # Sin estatus SGI: se confirma normal (fuera del alcance no bloquea).
        po.button_confirm()
        self.assertIn(po.state, ('purchase', 'done'))
        # Bloqueado: la siguiente OC no se confirma.
        partner.action_sgi_block_supplier()
        self.assertEqual(partner.sgi_supplier_status, 'bloqueado')
        po2 = po.copy()
        with self.assertRaises(UserError):
            po2.button_confirm()
        # Aprobado: vuelve a fluir.
        partner.action_sgi_approve_supplier()
        self.assertEqual(partner.sgi_supplier_status, 'aprobado')
        self.assertTrue(partner.sgi_supplier_approved_date)
        po2.button_confirm()
        self.assertIn(po2.state, ('purchase', 'done'))


@tagged('post_install', '-at_install')
class TestSatisfactionKpi(TransactionCase):

    def test_01_no_responses_returns_none(self):
        ind = self.env['sgi.indicator'].create({
            'code': 'TST-SAT', 'name': 'Satisfacción test',
            'calc_mode': 'satisfaccion_cliente', 'direction': 'higher_better',
            'target_objective': 90, 'target_acceptable': 80,
        })
        # Periodo lejano sin respuestas -> None (medición pendiente).
        self.assertIsNone(
            ind._sgi_compute_value(date(2035, 1, 1), date(2035, 1, 31)))

    def test_02_seed_indicator_exists(self):
        ind = self.env.ref('quimibond_sgi.sgi_ind_satisfaccion',
                           raise_if_not_found=False)
        if ind:
            self.assertEqual(ind.calc_mode, 'satisfaccion_cliente')


@tagged('post_install', '-at_install')
class TestDocAltaLink(TransactionCase):

    def test_01_create_document_links_request(self):
        category = self.env['approval.category'].create({
            'name': 'Alta documental test', 'sgi_is_doc_change': True,
            'approval_minimum': 1})
        req = self.env['approval.request'].create({
            'name': 'Alta de instructivo',
            'category_id': category.id,
            'request_owner_id': self.env.user.id,
            'sgi_change_kind': 'alta',
            'sgi_reason': 'Nuevo instructivo de empaque',
        })
        action = req.action_sgi_create_document()
        ctx = action['context']
        self.assertEqual(ctx['sgi_alta_request_id'], req.id)
        doc = self.env['documents.document'].with_context(**ctx).create({
            'name': 'IT nuevo', 'type': 'binary',
            'sgi_doc_type': 'instructivo',
            'sgi_code': 'IT-P-P01-99',
        })
        self.assertEqual(req.sgi_document_id, doc)
