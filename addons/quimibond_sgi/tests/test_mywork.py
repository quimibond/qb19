# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestMyWork(TransactionCase):
    """El ciclo del empleado en «Mi trabajo»: ver qué me toca, entender de
    dónde viene, hacerlo y marcarlo terminado."""

    def test_01_action_line_done_and_origin(self):
        risk = self.env['sgi.risk'].create({
            'name': 'Riesgo mi trabajo', 'instrument': 'ryo',
            'eval_probability': '2', 'eval_impact': '2'})
        line = self.env['sgi.action.line'].create({
            'risk_id': risk.id, 'name': 'Tratar',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
        })
        # El origen se muestra en la lista sin abrir nada.
        self.assertIn(risk.folio, line.origin_display)
        # «Abrir origen» navega al registro que explica la acción.
        action = line.action_open_origin()
        self.assertEqual(action['res_model'], 'sgi.risk')
        self.assertEqual(action['res_id'], risk.id)
        # «Terminar» sella fecha y avance en un click.
        line.action_mark_done()
        self.assertEqual(line.date_done, date.today())
        self.assertEqual(line.progress, '100')
        self.assertEqual(line.state, 'terminada')
        # Idempotente: re-terminar no cambia la fecha original.
        line.action_mark_done()
        self.assertEqual(line.date_done, date.today())

    def test_02_ack_read_flow(self):
        doc = self.env['documents.document'].create({
            'name': 'Doc acuse mi trabajo', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento',
            'sgi_code': 'P-A03', 'sgi_state': 'vigente',
        })
        employee = self.env['hr.employee'].create({
            'name': 'Empleado acuse', 'user_id': self.env.user.id})
        ack = self.env['sgi.document.ack'].create({
            'document_id': doc.id, 'employee_id': employee.id})
        # Sin archivo ni URL el botón de lectura explica el problema.
        with self.assertRaises(UserError):
            ack.action_view_file()
        # Firmar el acuse propio funciona y sella fecha/hora.
        ack.action_mark_read()
        self.assertEqual(ack.state, 'leido')
        self.assertTrue(ack.ack_date)

    def test_03_indicator_trend_action(self):
        ind = self.env['sgi.indicator'].create({
            'code': 'TST-TREND', 'name': 'KPI tendencia', 'calc_mode': 'manual'})
        action = ind.action_view_trend()
        self.assertEqual(action['res_model'], 'sgi.indicator.measure')
        self.assertIn('graph', action['view_mode'])
        self.assertEqual(action['domain'], [('indicator_id', '=', ind.id)])
