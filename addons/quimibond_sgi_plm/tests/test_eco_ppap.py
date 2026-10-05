# -*- coding: utf-8 -*-
"""quimibond_sgi_plm 3.2.0 (SGI 57.100.0, N-14): «Requiere PPAP» se marca solo
por los clientes del producto que lo exigen (nunca se desmarca) y al aplicar
el ECO se genera un PPAP por cliente.

Cada prueba crea su producto, sus clientes y su tipo de ECO (nombres ZPPAP):
la base del build es copia de producción."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestEcoPpap(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        mast = env['res.users'].create({
            'name': 'ZPPAP MAST', 'login': 'zppap_mast',
            'group_ids': [(6, 0, [env.ref('base.group_user').id,
                                  env.ref('quimibond_sgi.group_sgi_manager').id])]})
        env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mast_user_id', str(mast.id))
        cls.tmpl = env['product.template'].create(
            {'name': 'ZPPAP tela', 'type': 'consu', 'sale_ok': True, 'list_price': 10.0})
        cls.product = cls.tmpl.product_variant_id
        Partner = env['res.partner']
        cls.auto = Partner.create({'name': 'ZPPAP automotriz', 'is_company': True})
        cls.auto2 = Partner.create({'name': 'ZPPAP automotriz 2', 'is_company': True})
        cls.plain = Partner.create({'name': 'ZPPAP normal', 'is_company': True})
        (cls.auto | cls.auto2).write({'sgi_requires_ppap': True})
        cls.eco_type = env['mrp.eco.type'].create({'name': 'ZPPAP tipo'})
        cls.stage = env['mrp.eco.stage'].create(
            {'name': 'ZPPAP nueva', 'type_ids': [(4, cls.eco_type.id)]})
        selection = env['mrp.eco']._fields['type'].get_values(env)
        cls.eco_kind = 'product' if 'product' in selection else selection[0]

    def _sell(self, partner, product=None):
        order = self.env['sale.order'].create({
            'partner_id': partner.id,
            'order_line': [(0, 0, {'product_id': (product or self.product).id,
                                   'product_uom_qty': 1})]})
        order.action_confirm()
        return order

    def _eco(self, **kw):
        vals = {'name': 'ZPPAP ECO', 'type_id': self.eco_type.id, 'type': self.eco_kind,
                'product_tmpl_id': self.tmpl.id, 'stage_id': self.stage.id}
        vals.update(kw)
        return self.env['mrp.eco'].create(vals)

    def test_01_cliente_que_exige_marca_ppap(self):
        self._sell(self.auto)
        eco = self._eco()
        self.assertEqual(eco.sgi_ppap_customer_ids, self.auto)
        self.assertTrue(eco.sgi_requires_ppap)
        self.assertTrue(eco.sgi_ppap_auto)
        self.assertEqual(eco.sgi_customer_id, self.auto)
        self.assertTrue(any('ZPPAP automotriz' in (b or '') for b in eco.message_ids.mapped('body')))

    def test_02_cliente_normal_no(self):
        self._sell(self.plain)
        eco = self._eco()
        self.assertFalse(eco.sgi_ppap_customer_ids)
        self.assertFalse(eco.sgi_requires_ppap)

    def test_03_nunca_desmarca(self):
        eco = self._eco(sgi_requires_ppap=True, sgi_customer_id=self.plain.id)
        other = self.env['product.template'].create({'name': 'ZPPAP otra', 'type': 'consu'})
        eco.write({'product_tmpl_id': other.id})
        self.assertTrue(eco.sgi_requires_ppap)
        self.assertFalse(eco.sgi_ppap_auto)
        self.assertEqual(eco.sgi_customer_id, self.plain)

    def test_04_aplicado_no_se_toca(self):
        eco = self._eco()
        self.assertFalse(eco.sgi_requires_ppap)
        eco.write({'state': 'done'})
        self._sell(self.auto)
        eco.invalidate_recordset(['sgi_ppap_customer_ids'])
        eco._sgi_apply_ppap_rule()
        self.assertFalse(eco.sgi_requires_ppap)

    def test_05_un_ppap_por_cliente_e_idempotente(self):
        self._sell(self.auto)
        self._sell(self.auto2)
        eco = self._eco()
        self.assertTrue(eco.sgi_requires_ppap)
        self.assertFalse(eco.sgi_customer_id, "Dos clientes: no elige uno.")
        eco._sgi_handle_ppap()
        eco._sgi_handle_ppap()
        self.assertEqual(eco.sgi_ppap_ids.partner_id, self.auto | self.auto2)
        self.assertEqual(len(eco.sgi_ppap_ids), 2)
        self.assertEqual(eco.sgi_ppap_id, eco.sgi_ppap_ids.sorted('id')[:1])
        self.assertEqual(set(eco.sgi_ppap_ids.mapped('reason')), {'cambio_ingenieria'})

    def test_06_ppap_previo_del_producto_cuenta(self):
        self.env['sgi.ppap'].create({'partner_id': self.auto.id, 'product_tmpl_id': self.tmpl.id,
                                     'reason': 'nuevo_producto'})
        self.assertEqual(self._eco().sgi_ppap_customer_ids, self.auto)

    def test_07_marcado_en_otra_compania_no_cuenta(self):
        other = self.env['res.company'].create({'name': 'ZPPAP otra cía'})
        only_other = self.env['res.partner'].create({'name': 'ZPPAP solo otra', 'is_company': True})
        only_other.with_company(other).sgi_requires_ppap = True
        self.assertFalse(only_other.sgi_requires_ppap)
        self._sell(only_other)
        eco = self._eco()
        self.assertFalse(eco.sgi_ppap_customer_ids)
        self.assertFalse(eco.sgi_requires_ppap)

    def test_08_sin_cliente_ni_ppap_avisa_a_mast(self):
        """Marcado a mano sin cliente: no hay PPAP y queda el aviso al Jefe MAST."""
        eco = self._eco(sgi_requires_ppap=True)
        eco._sgi_handle_ppap()
        self.assertFalse(eco.sgi_ppap_ids)
        self.assertTrue(eco.activity_ids.filtered(lambda a: 'Crear PPAP' in (a.summary or '')))
