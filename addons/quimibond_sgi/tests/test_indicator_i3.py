# -*- coding: utf-8 -*-
"""I-3: desperdicio en kg, margen EBITDA contable y compras de materia prima."""
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestIndicatorI3(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.Indicator = env['sgi.indicator']
        cls.Param = env['ir.config_parameter'].sudo()
        cls.company = env.company
        cls.kg = env.ref('uom.product_uom_kgm')
        cls.uom_unit = env.ref('uom.product_uom_unit')
        cls.period = date(2044, 6, 1)
        cls.period_end = date(2044, 6, 30)

    def _account(self, account_type):
        return self.env['account.account'].search(
            [('account_type', '=', account_type), ('company_ids', 'in', self.company.ids)], limit=1) \
            or self.env['account.account'].search([('account_type', '=', account_type)], limit=1)

    def _invoice(self, move_type, partner, amount, when, account, product=None):
        line = {'name': 'linea', 'quantity': 1.0, 'price_unit': amount,
                'account_id': account.id, 'tax_ids': [(6, 0, [])]}
        if product:
            line['product_id'] = product.id
        move = self.env['account.move'].create({
            'move_type': move_type, 'partner_id': partner.id, 'invoice_date': when,
            'invoice_line_ids': [(0, 0, line)]})
        move.action_post()
        return move

    def test_01_desperdicio_kg(self):
        env = self.env
        stock = env['stock.location'].search([('usage', '=', 'internal')], limit=1)
        waste = env['stock.location'].create({
            'name': 'Desperdicio prueba', 'location_id': stock.id, 'usage': 'internal'})
        prodloc = env['stock.location'].search([('usage', '=', 'production')], limit=1)
        categ = env['product.category'].create({'name': 'Hilo prueba I-3'})
        self.Param.set_param('quimibond_sgi.waste_location_ids', str(waste.id))
        self.Param.set_param('quimibond_sgi.waste_input_categ_ids', str(categ.id))
        hilo = env['product.product'].create({
            'name': 'Hilo I-3', 'type': 'consu', 'is_storable': True,
            'categ_id': categ.id, 'uom_id': self.kg.id})
        tela = env['product.product'].create({
            'name': 'Tela I-3', 'type': 'consu', 'is_storable': True, 'uom_id': self.kg.id})
        mo = env['mrp.production'].create({'product_id': tela.id, 'product_qty': 100.0,
                                           'product_uom_id': self.kg.id})
        when = '2044-05-20 10:00:00'
        consumed = env['stock.move'].create({
            'product_id': hilo.id, 'product_uom_qty': 200.0, 'product_uom': self.kg.id,
            'location_id': stock.id, 'location_dest_id': prodloc.id,
            'raw_material_production_id': mo.id, 'state': 'done', 'date': when})
        consumed.write({'quantity': 200.0, 'picked': True, 'date': when})
        # 30 kg a desperdicio por ajuste de inventario y 2 piezas (no
        # cuentan: no son kg).
        pieza = env['product.product'].create({
            'name': 'Pieza I-3', 'type': 'consu', 'is_storable': True, 'uom_id': self.uom_unit.id})
        Quant = env['stock.quant'].with_context(inventory_mode=True)
        for product, qty in ((tela, 30.0), (pieza, 2.0)):
            Quant.create({'product_id': product.id, 'location_id': waste.id,
                          'inventory_quantity': qty}).action_apply_inventory()
        env['stock.move.line'].search([('product_id', 'in', (tela | pieza).ids),
                                       ('location_dest_id', '=', waste.id)]).write({'date': when})
        ind = self.Indicator.new({'calc_mode': 'desperdicio_kg'})
        detail = ind._detail_desperdicio_kg(self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (30.0, 200.0))
        self.assertEqual(detail['value'], 15.0)
        self.assertEqual(detail['model'], 'stock.move.line')
        self.assertEqual(len(detail['ids']), 1, "Solo la línea en kg.")

    def test_02_margen_ebitda(self):
        customer = self.env['res.partner'].create({'name': 'Cliente EBITDA'})
        supplier = self.env['res.partner'].create({'name': 'Proveedor EBITDA'})
        from .common_accounts import sgi_test_payable
        sgi_test_payable(self.env, supplier)
        ind = self.Indicator.new({'calc_mode': 'margen_ebitda'})
        base = ind._detail_margen_ebitda(self.period, self.period_end)
        self._invoice('out_invoice', customer, 1000.0, self.period, self._account('income'))
        self._invoice('in_invoice', supplier, 600.0, self.period, self._account('expense_direct_cost'))
        self._invoice('in_invoice', supplier, 250.0, self.period, self._account('expense'))
        self._invoice('in_invoice', supplier, 40.0, self.period, self._account('expense_depreciation'))
        self._invoice('in_invoice', supplier, 30.0, self.period, self._account('expense_other'))
        detail = ind._detail_margen_ebitda(self.period, self.period_end)
        self.assertAlmostEqual(detail['denominator'] - base['denominator'], 1000.0, places=2)
        self.assertAlmostEqual(detail['numerator'] - base['numerator'], 150.0, places=2,
                               msg="1000 − 600 − 250; depreciación y financieros fuera.")
        # Ventana de 12 meses: una factura del año anterior no entra.
        self._invoice('out_invoice', customer, 500.0, date(2043, 5, 1), self._account('income'))
        after = ind._detail_margen_ebitda(self.period, self.period_end)
        self.assertAlmostEqual(after['denominator'], detail['denominator'], places=2)
        # La póliza de cierre anual (mes 13) dentro de la ventana no cuenta.
        if 'l10n_mx_closing_move' in self.env['account.move']._fields:
            income = self._account('income')
            equity = self._account('equity')
            closing = self.env['account.move'].create({
                'move_type': 'entry', 'date': date(2043, 12, 31), 'ref': 'Cierre prueba',
                'l10n_mx_closing_move': True,
                'line_ids': [(0, 0, {'name': 'cierre', 'account_id': income.id, 'debit': 9000.0}),
                             (0, 0, {'name': 'cierre', 'account_id': equity.id, 'credit': 9000.0})]})
            closing.action_post()
            closed = ind._detail_margen_ebitda(self.period, self.period_end)
            self.assertAlmostEqual(closed['denominator'], detail['denominator'], places=2,
                                   msg="El cierre anual no resta ingresos.")

    def test_03_compras_mp_vs_ventas(self):
        customer = self.env['res.partner'].create({'name': 'Cliente MP'})
        supplier = self.env['res.partner'].create({'name': 'Proveedor MP'})
        from .common_accounts import sgi_test_payable
        sgi_test_payable(self.env, supplier)
        categ = self.env['product.category'].create({'name': 'Materia Prima prueba I-3'})
        sub = self.env['product.category'].create({'name': 'Hilo I-3', 'parent_id': categ.id})
        self.Param.set_param('quimibond_sgi.raw_material_categ_id', str(categ.id))
        mp = self.env['product.product'].create({'name': 'Hilo MP', 'type': 'consu', 'categ_id': sub.id})
        otro = self.env['product.product'].create({'name': 'Papelería', 'type': 'consu'})
        ind = self.Indicator.new({'calc_mode': 'compras_mp_vs_ventas'})
        base = ind._detail_compras_mp_vs_ventas(self.period, self.period_end)
        self._invoice('out_invoice', customer, 2000.0, self.period, self._account('income'))
        expense = self._account('expense_direct_cost')
        self._invoice('in_invoice', supplier, 800.0, date(2044, 4, 15), expense, product=mp)
        self._invoice('in_refund', supplier, 100.0, self.period, expense, product=mp)
        self._invoice('in_invoice', supplier, 300.0, self.period, expense, product=otro)
        detail = ind._detail_compras_mp_vs_ventas(self.period, self.period_end)
        self.assertAlmostEqual(detail['numerator'] - base['numerator'], 700.0, places=2,
                               msg="800 de abril (ventana de 3 meses) − 100 de nota; papelería fuera.")
        self.assertAlmostEqual(detail['denominator'] - base['denominator'], 2000.0, places=2)
        self.assertEqual(detail['model'], 'account.move')
