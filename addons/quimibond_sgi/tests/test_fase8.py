# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestAuditChecklist(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.survey = cls.env['survey.survey'].create({
            'title': 'Checklist test', 'survey_type': 'custom'})
        cls.question = cls.env['survey.question'].create({
            'survey_id': cls.survey.id, 'title': '8. Operación',
            'question_type': 'simple_choice', 'sequence': 10})
        cls.ans_ok = cls.env['survey.question.answer'].create({
            'question_id': cls.question.id, 'value': 'Conforme', 'sequence': 1})
        cls.ans_nc = cls.env['survey.question.answer'].create({
            'question_id': cls.question.id, 'value': 'No conforme', 'sequence': 2})
        cls.ans_obs = cls.env['survey.question.answer'].create({
            'question_id': cls.question.id, 'value': 'Observación', 'sequence': 3})

    def _answered_input(self, answer):
        user_input = self.env['survey.user_input'].create({
            'survey_id': self.survey.id})
        self.env['survey.user_input.line'].create({
            'user_input_id': user_input.id,
            'question_id': self.question.id,
            'answer_type': 'suggestion',
            'suggested_answer_id': answer.id,
        })
        return user_input

    def test_00_answer_checklist_links_input(self):
        audit = self.env['sgi.audit'].create({'survey_id': self.survey.id})
        action = audit.action_answer_checklist()
        self.assertEqual(action['type'], 'ir.actions.act_url')
        self.assertIn('/survey/', action['url'])
        self.assertEqual(len(audit.survey_input_ids), 1)
        # Sin encuesta asignada -> error claro.
        bare = self.env['sgi.audit'].create({})
        with self.assertRaises(UserError):
            bare.action_answer_checklist()

    def test_01_checklist_generates_findings(self):
        audit = self.env['sgi.audit'].create({'survey_id': self.survey.id})
        # Sin respuestas ligadas -> error claro.
        with self.assertRaises(UserError):
            audit.action_generate_findings_from_checklist()
        nc_input = self._answered_input(self.ans_nc)
        obs_input = self._answered_input(self.ans_obs)
        ok_input = self._answered_input(self.ans_ok)
        audit.survey_input_ids = [(6, 0, (nc_input | obs_input | ok_input).ids)]
        audit.action_generate_findings_from_checklist()
        # No conforme -> NC menor; Observación -> observación; Conforme -> nada.
        self.assertEqual(len(audit.finding_ids), 2)
        types = set(audit.finding_ids.mapped('finding_type'))
        self.assertEqual(types, {'nc_menor', 'observacion'})
        self.assertIn('8. Operación', audit.finding_ids[0].description)
        # Idempotente: reprocesar no duplica.
        audit.action_generate_findings_from_checklist()
        self.assertEqual(len(audit.finding_ids), 2)


@tagged('post_install', '-at_install')
class TestCustomerReturnNc(TransactionCase):

    def test_01_customer_return_creates_nc(self):
        warehouse = self.env['stock.warehouse'].search(
            [('company_id', '=', self.env.company.id)], limit=1)
        customer_loc = self.env.ref('stock.stock_location_customers')
        partner = self.env['res.partner'].create({
            'name': 'Cliente Devolución', 'is_company': True})
        product = self.env['product.product'].create({
            'name': 'Tela devuelta', 'is_storable': False})
        # Entrega a cliente validada.
        out = self.env['stock.picking'].create({
            'partner_id': partner.id,
            'picking_type_id': warehouse.out_type_id.id,
            'location_id': warehouse.lot_stock_id.id,
            'location_dest_id': customer_loc.id,
            'move_ids': [(0, 0, {
                'product_id': product.id,
                'product_uom_qty': 5,
                'location_id': warehouse.lot_stock_id.id,
                'location_dest_id': customer_loc.id,
            })],
        })
        out.action_confirm()
        out.move_ids.write({'quantity': 5, 'picked': True})
        out._action_done()
        self.assertEqual(out.state, 'done')
        self.assertFalse(out.sgi_return_alert_id,
                         "Una entrega normal no genera NC.")
        # Devolución del cliente (recepción cuyos movimientos retornan la entrega).
        ret = self.env['stock.picking'].create({
            'partner_id': partner.id,
            'picking_type_id': warehouse.in_type_id.id,
            'location_id': customer_loc.id,
            'location_dest_id': warehouse.lot_stock_id.id,
            'move_ids': [(0, 0, {
                'product_id': product.id,
                'product_uom_qty': 2,
                'location_id': customer_loc.id,
                'location_dest_id': warehouse.lot_stock_id.id,
                'origin_returned_move_id': out.move_ids[0].id,
            })],
        })
        ret.action_confirm()
        ret.move_ids.write({'quantity': 2, 'picked': True})
        ret._action_done()
        self.assertEqual(ret.state, 'done')
        alert = ret.sgi_return_alert_id
        self.assertTrue(alert, "La devolución de cliente debe levantar una NC.")
        self.assertEqual(alert.sgi_origin_type, 'reclamacion')
        self.assertEqual(alert.partner_id, partner)
        self.assertEqual(alert.sgi_source_id.code, 'devolucion_cliente')
        # Idempotente: revalidar no duplica.
        ret._sgi_create_return_alert()
        self.assertEqual(ret.sgi_return_alert_id, alert)


@tagged('post_install', '-at_install')
class TestOperationalSignals(TransactionCase):

    def test_01_repetitive_failure_schedules_activity(self):
        equipment = self.env['maintenance.equipment'].create({
            'name': 'Telar señal'})
        for i in range(3):
            self.env['maintenance.request'].create({
                'name': 'Falla %d' % i,
                'equipment_id': equipment.id,
                'maintenance_type': 'corrective',
            })
        Cron = self.env['sgi.cron']
        Cron.cron_operational_signals()
        Cron.cron_operational_signals()
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'maintenance.equipment'),
            ('res_id', '=', equipment.id),
            ('summary', 'like', 'Falla repetitiva%'),
        ])
        self.assertEqual(len(acts), 1,
                         "3 correctivas en 90 días → una sola actividad (sin duplicar).")

    def test_02_two_failures_no_activity(self):
        equipment = self.env['maintenance.equipment'].create({
            'name': 'Rama señal'})
        for i in range(2):
            self.env['maintenance.request'].create({
                'name': 'Falla %d' % i,
                'equipment_id': equipment.id,
                'maintenance_type': 'corrective',
            })
        self.env['sgi.cron'].cron_operational_signals()
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'maintenance.equipment'),
            ('res_id', '=', equipment.id),
        ])
        self.assertFalse(acts, "Con menos de 3 correctivas no se escala.")
