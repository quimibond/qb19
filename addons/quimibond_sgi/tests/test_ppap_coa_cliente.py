# -*- coding: utf-8 -*-
"""57.100.0 (N-14): casillas del cliente automotriz («Exige PPAP ante
cambios», «Exige plan de contingencia») y aviso al validar una salida sin CoA
a un cliente que lo exige (el bloqueo sigue apagado)."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from ..models.sgi_my_pending import NOTICE_MODELS
from .common_users import sgi_set_mast


@tagged('post_install', '-at_install')
class TestPpapCoaCliente(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.coa_block_validation', 'False')
        cls.mast = sgi_set_mast(cls.env, login='sgi_mast_n14')
        cls.wh = cls.env['stock.warehouse'].search([('company_id', '=', cls.env.company.id)], limit=1)
        cls.customers_loc = cls.env.ref('stock.stock_location_customers')
        cls.product = cls.env['product.product'].create(
            {'name': 'Tela N14', 'default_code': 'ZN14', 'type': 'consu'})
        cls.customer = cls.env['res.partner'].create({'name': 'Cliente N14', 'is_company': True})
        cls.customer.sgi_requires_coa = True

    def _picking(self, partner=None):
        """Mismo armado que tests/test_coa.py::_picking."""
        picking = self.env['stock.picking'].create({
            'picking_type_id': self.wh.out_type_id.id,
            'partner_id': (partner or self.customer).id,
            'location_id': self.wh.lot_stock_id.id,
            'location_dest_id': self.customers_loc.id,
            'move_ids': [(0, 0, {
                'product_id': self.product.id, 'product_uom_qty': 3.0,
                'product_uom': self.product.uom_id.id,
                'location_id': self.wh.lot_stock_id.id,
                'location_dest_id': self.customers_loc.id})],
        })
        picking.action_confirm()
        picking.move_ids.write({'quantity': 3.0, 'picked': True})
        return picking

    def _notice(self, picking):
        return self.env['mail.activity'].sudo().with_context(active_test=False).search(
            [('res_model', '=', 'stock.picking'), ('res_id', '=', picking.id),
             ('sgi_cron_key', '=', 'coa_sin_adjuntar:%d' % picking.id)])

    def test_01_casillas_solo_sgi_o_calidad(self):
        plain = new_test_user(self.env, login='z14_ventas', groups='base.group_user')
        with self.assertRaises(UserError):
            self.customer.with_user(plain).write({'sgi_requires_ppap': True})
        with self.assertRaises(UserError):
            self.customer.with_user(plain).write({'sgi_requires_contingency': True})
        self.customer.write({'sgi_requires_ppap': True, 'sgi_requires_contingency': True})
        self.assertTrue(self.customer.sgi_requires_ppap)
        self.assertTrue(self.customer.sgi_requires_contingency)

    def test_02_por_compania(self):
        other = self.env['res.company'].create({'name': 'Z14 otra'})
        self.customer.sgi_requires_ppap = True
        self.assertFalse(self.customer.with_company(other).sgi_requires_ppap)

    def test_03_validar_sin_coa_deja_nota_y_aviso(self):
        picking = self._picking()
        self.assertEqual(picking.sgi_coa_status, 'pendiente')
        picking.button_validate()
        self.assertEqual(picking.state, 'done')
        act = self._notice(picking)
        self.assertEqual(len(act), 1)
        self.assertTrue(act.active)
        self.assertTrue(any('sin CoA' in (body or '') for body in picking.message_ids.mapped('body')))

    def test_04_adjuntar_cierra_el_aviso(self):
        picking = self._picking()
        picking.button_validate()
        att = self.env['ir.attachment'].create(
            {'name': 'ZN14 INV-2046-01-0001.pdf', 'raw': b'%PDF-1.4', 'mimetype': 'application/pdf'})
        picking._sgi_coa_register(att)
        act = self._notice(picking)
        self.assertTrue(act)
        self.assertTrue(all(a.sgi_episode_closed for a in act))
        self.assertFalse(act.filtered('active'))

    def test_05_cliente_sin_coa_no_avisa(self):
        self.customer.sgi_requires_coa = False
        picking = self._picking()
        picking.button_validate()
        self.assertEqual(picking.state, 'done')
        self.assertFalse(self._notice(picking))

    def test_06_aviso_llega_a_mis_pendientes(self):
        self.assertIn('stock.picking', NOTICE_MODELS)

    def test_07_excepcion_del_jefe_de_calidad_no_avisa(self):
        """I-8: si el Jefe de Calidad valida la excepción con motivo, la salida
        sin CoA ya tiene responsable y motivo en el chatter: no hay aviso."""
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.coa_block_validation', 'True')
        job = self.env['hr.job'].create({'name': 'JEFE DE CALIDAD N14'})
        Param.set_param('quimibond_sgi.coa_exception_job_id', str(job.id))
        head = self.env['res.users'].create({
            'name': 'Jefe Calidad N14', 'login': 'z14_jefe_calidad',
            'group_ids': [(6, 0, [self.env.ref('stock.group_stock_manager').id,
                                  self.env.ref('quality.group_quality_manager').id])]})
        self.env['hr.employee'].create({'name': 'Jefe Calidad N14', 'user_id': head.id,
                                        'job_id': job.id})
        picking = self._picking()
        self.env['sgi.coa.exception.wizard'].with_user(head).create({
            'picking_ids': [(6, 0, picking.ids)], 'reason': 'Cliente autorizó por correo'}
        ).action_confirm()
        self.assertEqual(picking.state, 'done')
        self.assertFalse(self._notice(picking))
