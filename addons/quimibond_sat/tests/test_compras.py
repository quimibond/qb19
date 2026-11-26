# -*- coding: utf-8 -*-
"""Facturas de proveedor desde el SAT (1.31.0): desde la orden de compra o con
la regla del proveedor, con su semáforo.

El decodificador de CFDI es de l10n_mx_edi (Enterprise): se simula como en
test_auto_bill."""
from datetime import timedelta
from unittest.mock import patch

from odoo import Command, fields
from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_A = 'eeeeeeee-0000-4000-8000-000000000001'
UUID_B = 'eeeeeeee-0000-4000-8000-000000000002'
XML = b'<?xml version="1.0"?><cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Total="3287.86"/>'
CLAVE = '14111500'


def item(qty, price, description='CONCEPTO', clave=CLAVE):
    return {'quantity': qty, 'unitAmount': price, 'totalAmount': qty * price, 'discountAmount': 0,
            'description': description, 'productIdentification': clave, 'identificationNumber': ''}


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatCompras(SatCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.journal = cls.company_data['default_journal_purchase']
        cls.company.sat_auto_bill_journal_id = cls.journal
        cls.expense = cls.company_data['default_account_expense']
        cls.other_expense = cls.expense.copy({'code': '601.99.99', 'name': 'Gasto de prueba SAT'})

    # ── ayudas ──────────────────────────────────────────────────────

    def _days_ago(self, days):
        return fields.Datetime.to_string(fields.Datetime.now() - timedelta(days=days))

    def _cfdi(self, uuid=UUID_A, **overrides):
        overrides.setdefault('issuedAt', self._days_ago(5))
        overrides.setdefault('items', [item(1, 3287.86, 'PAPELERIA')])
        return self.env['sat.cfdi']._upsert_from_syntage(syntage_invoice(uuid, **overrides), self.company,
                                                         event_type='pull')

    def _fake_decoder(self, amount=3287.86):
        test = self

        def decode(move, file_data, new):
            move.write({
                'partner_id': test.proveedor.id,
                'invoice_date': fields.Date.today(),
                'invoice_line_ids': [Command.create({
                    'name': 'PAPELERIA', 'quantity': 1.0, 'price_unit': amount,
                    'account_id': test.expense.id, 'tax_ids': [Command.clear()],
                })],
            })

        def get_decoder(self_move, file_data, new=False):
            if file_data.get('name', '').endswith('.xml'):
                return {'decoder': decode, 'priority': 20}

        return patch.object(type(self.env['account.move']), '_get_edi_decoder', get_decoder)

    def _patch_xml(self):
        return patch.object(type(self.env['sat.cfdi']), '_fetch_xml', lambda rec: XML)

    def _rule(self, **vals):
        vals = dict({'partner_id': self.proveedor.id, 'account_id': self.other_expense.id,
                     'company_id': self.company.id}, **vals)
        return self.env['sat.bill.rule'].create(vals)

    def _bill(self, cfdi, amount=3287.86):
        with self._fake_decoder(amount), self._patch_xml():
            action = cfdi.action_create_bill()
        return self.env['account.move'].browse(action['res_id'])

    # ── sin orden de compra: regla del proveedor ────────────────────

    def test_confirmed_rule_gives_green(self):
        rule = self._rule(state='confirmada')
        move = self._bill(self._cfdi())
        line = move.invoice_line_ids
        self.assertEqual(line.account_id, self.other_expense)   # la regla manda sobre la del importador
        self.assertEqual(line.sat_bill_rule_id, rule)
        self.assertEqual(line.sat_clave, CLAVE)
        self.assertTrue(move.sat_auto_bill)
        self.assertEqual(move.sat_semaforo, 'verde', move.sat_semaforo_motivo)
        self.assertEqual(move.state, 'draft')

    def test_without_rule_is_yellow(self):
        move = self._bill(self._cfdi())
        self.assertEqual(move.invoice_line_ids.account_id, self.expense)
        self.assertEqual(move.sat_semaforo, 'amarillo')
        self.assertIn('sin regla', move.sat_semaforo_motivo)

    def test_proposed_rule_is_yellow(self):
        self._rule()
        move = self._bill(self._cfdi())
        self.assertEqual(move.invoice_line_ids.account_id, self.other_expense)
        self.assertEqual(move.sat_semaforo, 'amarillo')
        self.assertIn('por confirmar', move.sat_semaforo_motivo)

    def test_rule_by_clave_wins(self):
        self._rule(state='confirmada')
        specific = self._rule(state='confirmada', clave_sat=CLAVE, account_id=self.expense.id)
        move = self._bill(self._cfdi())
        self.assertEqual(move.invoice_line_ids.sat_bill_rule_id, specific)
        self.assertEqual(move.invoice_line_ids.account_id, self.expense)

    def test_total_mismatch_is_red_and_not_posted(self):
        self._rule(state='confirmada')
        move = self._bill(self._cfdi(), amount=3000.0)
        self.assertEqual(move.sat_semaforo, 'rojo')
        self.assertIn('No cuadra con el SAT', move.sat_semaforo_motivo)
        move.action_sat_post_green()
        self.assertEqual(move.state, 'draft')

    def test_amount_out_of_range_is_yellow(self):
        self._rule(state='confirmada', monto_min=100.0, monto_max=200.0)
        move = self._bill(self._cfdi())
        self.assertEqual(move.sat_semaforo, 'amarillo')
        self.assertIn('fuera de lo normal', move.sat_semaforo_motivo)

    def test_post_green_and_learn(self):
        self._rule(state='confirmada')
        green = self._bill(self._cfdi())
        self.assertEqual(green.sat_semaforo, 'verde', green.sat_semaforo_motivo)
        green.action_sat_post_green()
        self.assertEqual(green.state, 'posted')
        self.assertFalse(green.sat_semaforo)

    def test_posting_teaches_a_proposed_rule(self):
        move = self._bill(self._cfdi())
        self.assertEqual(move.sat_semaforo, 'amarillo')
        move.invoice_line_ids.account_id = self.other_expense    # Contabilidad elige la cuenta
        move.action_post()
        rule = self.env['sat.bill.rule'].search([('partner_id', '=', self.proveedor.id)])
        self.assertEqual(len(rule), 1)
        self.assertEqual(rule.state, 'propuesta')
        self.assertEqual(rule.account_id, self.other_expense)
        self.assertEqual(rule.clave_sat, CLAVE)

    # ── propuestas desde el historial ───────────────────────────────

    def test_seed_from_history(self):
        for amount in (1000.0, 1200.0, 900.0):
            bill = self._invoice(self.proveedor, amount, day=fields.Date.to_string(fields.Date.today()))
            self.assertEqual(bill.invoice_line_ids.account_id, self.expense)
        created = self.env['sat.bill.rule']._seed_from_history()
        self.assertEqual(created, 1)
        rule = self.env['sat.bill.rule'].search([('partner_id', '=', self.proveedor.id)])
        self.assertEqual(rule.account_id, self.expense)
        self.assertEqual(rule.state, 'propuesta')
        self.assertFalse(rule.clave_sat)
        self.assertEqual((rule.monto_min, rule.monto_max), (900.0, 1200.0))
        self.assertEqual(rule.facturas, 3)
        # No duplica
        self.assertEqual(self.env['sat.bill.rule']._seed_from_history(), 0)

    def test_seed_needs_three_bills(self):
        for amount in (1000.0, 1200.0):
            self._invoice(self.proveedor, amount, day=fields.Date.to_string(fields.Date.today()))
        self.assertEqual(self.env['sat.bill.rule']._seed_from_history(), 0)

    # ── con orden de compra ─────────────────────────────────────────

    def _purchase(self, qty, price=100.0):
        if 'purchase.order' not in self.env:
            self.skipTest('purchase no está instalado')
        product = self.env['product.product'].create({
            'name': 'Tela de prueba SAT', 'type': 'service', 'purchase_method': 'purchase',
            'property_account_expense_id': self.other_expense.id,
        })
        order = self.env['purchase.order'].create({
            'partner_id': self.proveedor.id, 'currency_id': self.mxn.id, 'partner_ref': 'S-999-26',
            'order_line': [Command.create({
                'product_id': product.id, 'product_qty': qty, 'price_unit': price, 'tax_ids': [Command.clear()],
            })],
        })
        order.button_confirm()
        return order

    def _po_cfdi(self, qty, price=100.0):
        amount = qty * price
        return self._cfdi(subtotal=amount, total=amount, transferredTaxes={'total': 0},
                          items=[item(qty, price, 'TELA DE PRUEBA')])

    def test_bill_from_purchase_order(self):
        order = self._purchase(10)
        cfdi = self._po_cfdi(10)
        with self._patch_xml():
            action = cfdi.action_create_bill()
        move = self.env['account.move'].browse(action['res_id'])
        self.assertEqual(move.invoice_line_ids.purchase_line_id, order.order_line)
        self.assertEqual(move.invoice_line_ids.account_id, self.other_expense)   # la de la categoría/producto
        self.assertEqual(move.invoice_line_ids.sat_clave, CLAVE)
        self.assertEqual(move.invoice_origin, order.name)
        self.assertIn('S-999-26', move.ref)
        self.assertIn(move, order.invoice_ids)
        self.assertEqual(cfdi.move_id, move)
        self.assertEqual(move.sat_semaforo, 'verde', move.sat_semaforo_motivo)
        self.assertIn(order.name, move.sat_origen)

    def test_partial_delivery_line_by_line(self):
        order = self._purchase(20)
        cfdi = self._po_cfdi(10)
        with self._patch_xml():
            action = cfdi.action_create_bill()
        move = self.env['account.move'].browse(action['res_id'])
        self.assertEqual(move.invoice_line_ids.purchase_line_id, order.order_line)
        self.assertEqual(move.invoice_line_ids.quantity, 10)
        self.assertEqual(move.sat_semaforo, 'verde', move.sat_semaforo_motivo)
        self.assertEqual(order.order_line.qty_to_invoice, 10)

    def test_price_not_in_order_goes_by_xml(self):
        self._purchase(10, price=100.0)
        cfdi = self._cfdi()     # 1 x 3287.86: no se parece a la orden
        self.assertFalse(cfdi._match_purchase()['lines'])

    def test_open_advance_is_yellow(self):
        self._rule(state='confirmada')
        advance_account = self.env['account.account'].create({
            'code': '120.01.01', 'name': 'Anticipo a proveedores', 'account_type': 'asset_current',
            'reconcile': True})
        entry = self.env['account.move'].create({
            'move_type': 'entry', 'date': fields.Date.today(),
            'line_ids': [
                Command.create({'account_id': advance_account.id, 'partner_id': self.proveedor.id,
                                'debit': 500.0, 'name': 'Anticipo'}),
                Command.create({'account_id': self.company_data['default_journal_bank'].default_account_id.id,
                                'credit': 500.0, 'name': 'Anticipo'}),
            ],
        })
        entry.action_post()
        move = self._bill(self._cfdi())
        self.assertEqual(move.sat_semaforo, 'amarillo')
        self.assertIn('Anticipo abierto', move.sat_semaforo_motivo)

    # ── implantación por etapas (1.31.2) ────────────────────────────

    def test_cron_only_with_order_by_default(self):
        self._rule(state='confirmada')
        cfdi = self._cfdi()
        with self._fake_decoder(), self._patch_xml():
            self.assertEqual(self.env['sat.cfdi']._cron_auto_bills(), 0)
        self.assertFalse(cfdi.move_id)
        self.assertFalse(cfdi.auto_bill_state)            # se vuelve a buscar mañana
        self.assertIn('sin orden de compra', cfdi.auto_bill_note)
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.auto_bill_solo_con_orden', '0')
        with self._fake_decoder(), self._patch_xml():
            self.assertEqual(self.env['sat.cfdi']._cron_auto_bills(), 1)
        self.assertTrue(cfdi.move_id)

    def test_cron_creates_with_order(self):
        order = self._purchase(10)
        cfdi = self._po_cfdi(10)
        with self._patch_xml():
            self.assertEqual(self.env['sat.cfdi']._cron_auto_bills(), 1)
        self.assertEqual(cfdi.move_id.invoice_line_ids.purchase_line_id, order.order_line)

    def test_supplier_with_orders_is_yellow_without_order(self):
        self._rule(state='confirmada')
        order = self._purchase(10)             # otra compra: no cuadra con este CFDI
        move = self._bill(self._cfdi())
        self.assertFalse(move.invoice_line_ids.purchase_line_id)
        self.assertEqual(move.sat_semaforo, 'amarillo')
        self.assertIn(order.name, move.sat_semaforo_motivo)

    def test_storable_without_order_is_yellow(self):
        if 'is_storable' not in self.env['product.product']._fields:
            self.skipTest('stock no está instalado')
        self._rule(state='confirmada')
        product = self.env['product.product'].create({'name': 'Etiqueta de prueba', 'is_storable': True})
        move = self._bill(self._cfdi())
        move.invoice_line_ids.write({'product_id': product.id, 'price_unit': 3287.86,
                                     'tax_ids': [Command.clear()], 'account_id': self.other_expense.id})
        move.action_sat_semaforo()
        self.assertEqual(move.sat_semaforo, 'amarillo')
        self.assertIn('producto de inventario', move.sat_semaforo_motivo)

    def test_check_again_rebuilds_from_late_order(self):
        self._rule(state='confirmada')
        move = self._bill(self._cfdi())
        self.assertEqual(move.sat_semaforo, 'verde', move.sat_semaforo_motivo)
        self.assertEqual(move.invoice_line_ids.sat_cuenta_propuesta_id, self.other_expense)
        order = self._purchase(1, price=3287.86)   # Compras hizo la orden después
        move.action_sat_semaforo()
        self.assertEqual(move.invoice_line_ids.purchase_line_id, order.order_line)
        self.assertEqual(len(move.invoice_line_ids), 1)
        self.assertIn(order.name, move.sat_origen)
        self.assertEqual(move.invoice_origin, order.name)
        self.assertEqual(move.sat_semaforo, 'verde', move.sat_semaforo_motivo)

    def test_order_matches_net_price_after_discount(self):
        order = self._purchase(8, price=112.52)
        cfdi = self._cfdi(subtotal=117.26, discount=4.74, total=112.52, transferredTaxes={'total': 0},
                          items=[dict(item(1, 117.26, 'ETIQUETA'), discountAmount=4.74)])
        self.assertEqual(cfdi._match_purchase()['lines'], order.order_line)

    def test_same_amount_other_cfdi_is_not_a_duplicate(self):
        self._rule(state='confirmada')
        self._bill(self._cfdi())
        other = self._cfdi(UUID_B, id='b')
        self.assertFalse(other._auto_bill_blocker())

    def test_discount_goes_in_its_column(self):
        # Euroeléctrica (26-nov): la orden trae el neto redondeado (0.57) y el
        # CFDI 0.59 con 3 % de descuento (neto 0.5723). Sin la columna de
        # descuento la factura daba 57.00 contra 57.23 del CFDI.
        order = self._purchase(100, price=0.57)
        cfdi = self._cfdi(subtotal=59.0, discount=1.77, total=57.23, transferredTaxes={'total': 0},
                          items=[dict(item(100, 0.59, 'COLLARIN'), discountAmount=1.77)])
        with self._patch_xml():
            action = cfdi.action_create_bill()
        move = self.env['account.move'].browse(action['res_id'])
        line = move.invoice_line_ids
        self.assertEqual(line.purchase_line_id, order.order_line)
        self.assertEqual((line.price_unit, line.discount), (0.59, 3.0))
        self.assertAlmostEqual(move.amount_total, 57.23, places=2)
        self.assertEqual(move.sat_semaforo, 'verde', move.sat_semaforo_motivo)

    def test_bill_gets_the_pdf(self):
        # Aurelio (26-nov): cada factura desde el SAT trae también el PDF.
        self._purchase(10)
        cfdi = self._po_cfdi(10)
        Cfdi = type(self.env['sat.cfdi'])
        with self._patch_xml(), patch.object(Cfdi, '_fetch_pdf', lambda rec: b'%PDF-1.4 prueba'):
            move = self.env['account.move'].browse(cfdi.action_create_bill()['res_id'])
            name = '%s.pdf' % cfdi.uuid.upper()
            pdfs = self.env['ir.attachment'].search([('res_model', '=', 'account.move'), ('res_id', '=', move.id),
                                                     ('name', '=', name)])
            self.assertEqual(len(pdfs), 1)
            self.assertEqual(pdfs.mimetype, 'application/pdf')
            move.action_sat_attach_pdf()          # no lo repite
            self.assertEqual(self.env['ir.attachment'].search_count([
                ('res_model', '=', 'account.move'), ('res_id', '=', move.id), ('name', '=', name)]), 1)

    def test_bill_without_pdf_still_created(self):
        self._purchase(10)
        cfdi = self._po_cfdi(10)
        Cfdi = type(self.env['sat.cfdi'])

        def boom(rec):
            raise UserError('Syntage respondió 404')
        with self._patch_xml(), patch.object(Cfdi, '_fetch_pdf', boom):
            move = self.env['account.move'].browse(cfdi.action_create_bill()['res_id'])
        self.assertTrue(move.invoice_line_ids)
