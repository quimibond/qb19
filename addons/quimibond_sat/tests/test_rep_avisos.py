# -*- coding: utf-8 -*-
"""Avisos de complementos de pago (1.32.0): actividad por proveedor con sus
facturas pagadas sin complemento, correo listo para pedirlos, y aviso de
cobros a clientes sin complemento timbrado."""
from datetime import date

from odoo.tests import tagged

from .common import RFC_CLIENTE, RFC_QUIMIBOND, SatCommon, syntage_invoice

UUID_PROV = 'cdcdcdcd-0000-4000-8000-000000000001'
UUID_CLI = 'cdcdcdcd-0000-4000-8000-000000000002'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatRepAvisos(SatCommon):

    def setUp(self):
        super().setUp()
        ICP = self.env['ir.config_parameter'].sudo()
        ICP.set_param('quimibond_sat.rep_responsable', self.env.user.login)
        ICP.set_param('quimibond_sat.alert_email', 'conta@example.com')
        self.proveedor.email = 'cxc@papelera.example.com'

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _pay(self, move, day='2026-04-30'):
        wizard = self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=move.ids).create({'payment_date': day})
        wizard.action_create_payments()
        move.invalidate_recordset()

    def _pagada_sin_rep(self, partner, uuid, sid, move_type, **cfdi):
        move = self._invoice(partner, 1000.0, move_type=move_type, day='2026-04-10')
        sat = self._upsert(syntage_invoice(uuid, id=sid, total=1000.0, paymentType='PPD',
                                           issuedAt='2026-04-10 12:00:00', **cfdi))
        sat.write({'move_id': move.id})
        self._pay(move)
        self.env.flush_all()
        self.env.invalidate_all()
        return move, self.env['sat.pago.compare'].browse(sat.id)

    def _proveedor_sin_rep(self):
        return self._pagada_sin_rep(self.proveedor, UUID_PROV, 'prov', 'in_invoice')

    def _cliente_sin_rep(self):
        return self._pagada_sin_rep(
            self.cliente, UUID_CLI, 'cli', 'out_invoice', isIssuer=True, isReceiver=False,
            issuer={'rfc': RFC_QUIMIBOND, 'name': 'PRODUCTORA DE NO TEJIDOS QUIMIBOND', 'blacklistStatus': None},
            receiver={'rfc': RFC_CLIENTE, 'name': 'EUROTECNICA TEXTIL', 'blacklistStatus': None})

    def test_actividad_por_proveedor_sin_duplicar(self):
        bill, row = self._proveedor_sin_rep()
        self.assertEqual(row.issue, 'sin_complemento')
        acts = self.env['sat.pago.compare']._aviso_rep_proveedores()
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts.res_model, 'res.partner')
        self.assertEqual(acts.res_id, self.proveedor.id)
        self.assertEqual(acts.user_id, self.env.user)
        self.assertIn('Pedir complementos de pago', acts.summary)
        self.assertIn(UUID_PROV, acts.note)
        # Otra corrida actualiza la misma actividad
        again = self.env['sat.pago.compare']._aviso_rep_proveedores()
        self.assertEqual(again, acts)
        self.assertEqual(self.env['mail.activity'].search_count([
            ('res_model', '=', 'res.partner'), ('res_id', '=', self.proveedor.id)]), 1)

    def test_correo_listo_para_el_proveedor(self):
        self._proveedor_sin_rep()
        action = self.proveedor._sat_action_pedir_rep()
        self.assertEqual(action['res_model'], 'mail.compose.message')
        template = self.env.ref('quimibond_sat.mail_template_pedir_rep')
        self.assertEqual(action['context']['default_template_id'], template.id)
        body = template._render_field('body_html', self.proveedor.ids)[self.proveedor.id]
        self.assertIn(UUID_PROV, body)
        self.assertIn('1,000.00', body)

    def test_sin_responsable_no_hay_actividad(self):
        self._proveedor_sin_rep()
        ICP = self.env['ir.config_parameter'].sudo()
        ICP.set_param('quimibond_sat.rep_responsable', 'nadie@example.com')
        self.assertFalse(self.env['sat.pago.compare']._aviso_rep_proveedores())

    def test_aviso_de_cobros_sin_complemento(self):
        inv, row = self._cliente_sin_rep()
        self.assertEqual(row.direction, 'issued')
        self.assertEqual(row.issue, 'sin_complemento')
        mail = self.env['sat.pago.compare']._aviso_rep_clientes(today=date(2026, 12, 1))
        self.assertTrue(mail)
        self.assertIn(inv.name, mail.body_html)
        self.assertIn('EUROTECNICA', mail.body_html)
        self.assertIn('1 sin timbrar', mail.subject)

    def test_cron_solo_en_su_dia(self):
        self._proveedor_sin_rep()
        Compare = self.env['sat.pago.compare']
        self.assertEqual(Compare._cron_avisos_rep(today=date(2026, 12, 15)), {})
        hecho = Compare._cron_avisos_rep(today=date(2026, 12, 10))
        self.assertIn('proveedores', hecho)
        self.assertNotIn('clientes', hecho)
