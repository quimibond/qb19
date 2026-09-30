# -*- coding: utf-8 -*-
"""Releases de clientes (E1a): leer, confirmar la parte, aplicar al
pronóstico repartido por año y recalcular el MPS sumando clientes.

Release inventado con el layout de Lear (sin datos reales)."""
import base64
from datetime import date

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

LEAR_TEXT = "\n".join([
    "                     SUPPLIER SCHEDULE / MATERIAL RELEASE",
    "   Supplier: 9ZZZ0001 Ship-To: 9900",
    "    Release ID: 000901 Release Date: 12/17/40",
    "Purchase Order: 7000001 Buyer: ZZ01",
    "   Item Number: LX00000001AA UM: MT In Transit Qty: 0",
    "               BACK SCRIM PES/100        Receipt Date: 12/10/40 10:00",
    "               IH LAMINATED 62\"          Receipt Qty: 100",
    "                                               Cum Received: 1,000",
    "                                       Packing Slip/Shipper: INV/2040/12/0001",
    "Interval Date Time Reference Q Req Qty Cum Req Qty Net Req Qty",
    "         Prior 0 1,000",
    "Weekly 12/17/40 P 100 1,100 100",
    "         12/24/40 P 200 1,300 200",
    "         12/31/40 P 300 1,600 300",
    "         01/07/41 P 400 2,000 400",
    "Fab Authorization Cum Qty: 1,300 Thru: 12/24/40",
    "Raw Authorization Cum Qty: 2,000 Thru: 01/07/41",
])


@tagged('post_install', '-at_install')
class TestRelease(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.team = cls.env['crm.team'].create({'name': 'Industrial release test'})
        cls.customer = cls.env['res.partner'].create(
            {'name': 'Cliente Release E1a', 'is_company': True})
        cls.product = cls.env['product.product'].create({
            'name': 'Tela release', 'default_code': 'ZZR045Q22JNT160',
            'type': 'consu', 'uom_id': cls.uom_m.id, 'sale_ok': True})
        cls.profile = cls.env['qb.release.profile'].create({
            'partner_id': cls.customer.id, 'team_id': cls.team.id,
            'reader': 'lear_aiag', 'firm_rule': 'fab_auth',
        })

    def _release(self, text=LEAR_TEXT):
        return self.env['qb.release'].create({
            'profile_id': self.profile.id,
            'file': base64.b64encode(text.encode()),
            'filename': 'QUIMIBOND.txt',
        })

    def _confirm_part(self):
        part = self.env['qb.customer.part'].search([
            ('partner_id', '=', self.customer.id),
            ('customer_part', '=', 'LX00000001AA')])
        part.write({'product_id': self.product.id})
        part.action_confirm()
        return part

    def test_01_read_creates_parts_lines_and_catalog(self):
        release = self._release()
        release.action_read()
        self.assertEqual(release.state, 'leido')
        self.assertEqual(release.release_ref, '000901')
        self.assertEqual(release.release_date, date(2040, 12, 17))
        self.assertEqual(release.version, 1)
        self.assertEqual(len(release.part_ids), 1)
        self.assertEqual(len(release.line_ids), 4)
        part = release.part_ids
        self.assertEqual(part.cum_received, 1000)
        self.assertEqual(part.fab_auth_date, date(2040, 12, 24))
        # La parte nueva entra al catálogo sin confirmar; detiene la aplicación.
        self.assertEqual(release.unmapped_count, 1)
        zones = release.line_ids.sorted('date_customer').mapped('zone')
        self.assertEqual(zones, ['firme', 'firme', 'materia_prima', 'materia_prima'])

    def test_02_unreadable_goes_to_review(self):
        release = self._release("esto no es un release")
        release.action_read()
        self.assertEqual(release.state, 'por_revisar')
        self.assertTrue(release.error_reason)
        self.assertFalse(release.part_ids)

    def test_03_apply_needs_review_and_confirmed_part(self):
        release = self._release()
        release.action_read()
        with self.assertRaises(UserError):
            release.action_apply()  # no revisado
        release.action_mark_reviewed()
        with self.assertRaises(UserError):
            release.action_apply()  # parte sin confirmar
        self._confirm_part()
        release.action_apply()
        self.assertEqual(release.state, 'aplicado')

    def test_04_apply_splits_years_and_replaces_previous(self):
        first = self._release()
        first.action_read()
        self._confirm_part()
        first.action_mark_reviewed()
        first.action_apply()
        Budget = self.env['sgi.sales.budget']
        fc40 = Budget.search([('kind', '=', 'pronostico'), ('partner_id', '=', self.customer.id),
                              ('year', '=', 2040)])
        fc41 = Budget.search([('kind', '=', 'pronostico'), ('partner_id', '=', self.customer.id),
                              ('year', '=', 2041)])
        self.assertTrue(fc40 and fc41, "El release cruza de año: un pronóstico por año.")
        self.assertEqual(fc40.state, 'revisado')
        qty40 = {ln.date: ln.qty_budget for ln in fc40.line_ids}
        self.assertEqual(qty40, {date(2040, 12, 17): 100, date(2040, 12, 24): 200,
                                 date(2040, 12, 31): 300})
        # 07/01/41 cae en lunes 7-ene-2041.
        self.assertEqual(fc41.line_ids.qty_budget, 400)
        self.assertEqual(fc41.line_ids.release_id, first)
        self.assertEqual(fc41.line_ids.forecast_source, 'release')

        # Segundo release: ya no trae la semana del 31-dic y cambia otra.
        second_text = LEAR_TEXT.replace("Release ID: 000901", "Release ID: 000902") \
            .replace("12/31/40 P 300 1,600 300", "") \
            .replace("12/24/40 P 200 1,300 200", "12/24/40 P 250 1,350 250")
        second = self._release(second_text)
        second.action_read()
        self.assertEqual(second.version, 2)
        self.assertEqual(second.previous_id, first)
        second.action_mark_reviewed()
        second.action_apply()
        self.assertEqual(first.state, 'reemplazado')
        qty40 = {ln.date: ln.qty_budget for ln in fc40.line_ids}
        self.assertEqual(qty40[date(2040, 12, 24)], 250)
        self.assertEqual(qty40[date(2040, 12, 31)], 0, "La semana que desaparece queda en 0.")

    def test_05_delivery_basis_moves_week(self):
        self.profile.write({'date_basis': 'entrega', 'transit_days': 3})
        release = self._release()
        release.action_read()
        line = release.line_ids.filtered(lambda ln: ln.date_customer == date(2040, 12, 17))
        self.assertEqual(line.date_ship, date(2040, 12, 14))
        self.assertEqual(line.week, date(2040, 12, 10))


@tagged('post_install', '-at_install')
class TestMpsTotals(TransactionCase):
    """N1/N2: la celda del MPS es la suma de todos los clientes, y el
    presupuesto solo se omite para el cliente y mes que el pronóstico cubre."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.team = cls.env['crm.team'].create({'name': 'Industrial mps test'})
        cls.product = cls.env['product.product'].create({
            'name': 'Tela mps', 'default_code': 'ZZMPS', 'type': 'consu',
            'uom_id': cls.uom_m.id})
        cls.a = cls.env['res.partner'].create({'name': 'Cliente A mps', 'is_company': True})
        cls.b = cls.env['res.partner'].create({'name': 'Cliente B mps', 'is_company': True})
        cls.monday = date(2040, 6, 4)

    def _forecast(self, partner, qty):
        fc = self.Budget.create({'year': 2040, 'team_id': self.team.id,
                                 'kind': 'pronostico', 'partner_id': partner.id})
        self.Line.create({'budget_id': fc.id, 'product_id': self.product.id,
                          'date': self.monday, 'uom_id': self.uom_m.id,
                          'qty_budget': qty})
        fc.state = 'revisado'
        return fc

    def test_01_two_customers_add_up(self):
        self._forecast(self.a, 100)
        self._forecast(self.b, 50)
        demand, _omitted = self.Budget._qb_mps_totals(self.product, self.env.company)
        self.assertEqual(demand[(self.product, self.monday)], 150)

    def test_02_budget_omitted_only_for_covered_customer(self):
        self._forecast(self.a, 100)
        budget = self.Budget.create({'year': 2040, 'team_id': self.team.id})
        for partner, qty in ((self.a, 400), (self.b, 80)):
            self.Line.create({'budget_id': budget.id, 'product_id': self.product.id,
                              'partner_id': partner.id, 'date': date(2040, 6, 1),
                              'uom_id': self.uom_m.id, 'qty_budget': qty})
        budget.state = 'aprobado'
        demand, omitted = self.Budget._qb_mps_totals(self.product, self.env.company)
        self.assertEqual(omitted.partner_id, self.a)
        total = sum(demand.values())
        self.assertAlmostEqual(total, 100 + 80, msg="Pronóstico de A + presupuesto de B.")

    def test_03_weekly_mps_splits_budget_month(self):
        if 'manufacturing_period' not in self.env.company._fields:
            self.skipTest("Sin mrp_mps: no hay periodo de MPS.")
        self.env.company.manufacturing_period = 'week'
        split = self.Budget._qb_mps_split_month(date(2040, 6, 1), 100, self.env.company)
        # Junio 2040: lunes 4, 11, 18 y 25.
        self.assertEqual(sorted(split), [date(2040, 6, 4), date(2040, 6, 11),
                                         date(2040, 6, 18), date(2040, 6, 25)])
        self.assertAlmostEqual(sum(split.values()), 100)
