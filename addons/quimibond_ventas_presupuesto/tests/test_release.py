# -*- coding: utf-8 -*-
"""Releases de clientes: leer, confirmar la parte y aplicar al pronóstico vigente
(qb.forecast.line, 3.0.0). El release ya no escribe en el Programa Maestro.

Release inventado con el layout de Lear (sin datos reales)."""
import base64
from datetime import date

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger

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


    def _forecast(self, **domain):
        dom = [('partner_id', '=', self.customer.id)] + [(k, '=', v) for k, v in domain.items()]
        return self.env['qb.forecast.line'].search(dom)

    def test_04_apply_writes_forecast_lines_and_replaces_previous(self):
        first = self._release()
        first.action_read()
        self._confirm_part()
        first.action_mark_reviewed()
        first.action_apply()
        lines = self._forecast()
        self.assertEqual({ln.week: ln.qty_venta for ln in lines},
                         {date(2040, 12, 17): 100, date(2040, 12, 24): 200,
                          date(2040, 12, 31): 300, date(2041, 1, 7): 400},
                         "Una línea por semana, sin partir por año.")
        self.assertEqual(set(lines.mapped('origen')), {'release'})
        self.assertEqual(lines.release_id, first)
        self.assertEqual(set(lines.mapped('customer_part')), {'LX00000001AA'})
        self.assertFalse(self.env['sgi.sales.budget'].search([
            ('kind', '=', 'pronostico'), ('partner_id', '=', self.customer.id)]),
            "Ya no crea documentos de pronóstico por año.")
        self.assertEqual(len(self.env['qb.forecast.change'].search([
            ('release_id', '=', first.id)])), 4, "Cada semana nueva queda en la bitácora.")

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
        lines = self._forecast()
        self.assertEqual({ln.week: ln.qty_venta for ln in lines},
                         {date(2040, 12, 17): 100, date(2040, 12, 24): 250,
                          date(2041, 1, 7): 400}, "La semana que ya no viene se quita.")
        self.assertEqual(lines.release_id, second)
        removed = self.env['qb.forecast.change'].search([
            ('partner_id', '=', self.customer.id), ('week', '=', date(2040, 12, 31)),
            ('qty_after', '=', 0)])
        self.assertTrue(removed, "La semana quitada queda en la bitácora.")

    def test_06_apply_keeps_weeks_before_the_release_and_other_plants(self):
        Line = self.env['qb.forecast.line']
        old = Line.create({'company_id': self.env.company.id, 'partner_id': self.customer.id,
                           'product_id': self.product.id, 'week': date(2040, 12, 10),
                           'qty_origen': 50, 'origen': 'captura'})
        other_plant = Line.create({'company_id': self.env.company.id, 'partner_id': self.customer.id,
                                   'plant': 'Otra planta', 'product_id': self.product.id,
                                   'week': date(2040, 12, 31), 'qty_origen': 70, 'origen': 'excel'})
        release = self._release()
        release.action_read()
        self._confirm_part()
        release.action_mark_reviewed()
        release.action_apply()
        self.assertTrue(old.exists(), "Las semanas anteriores al release no se tocan.")
        self.assertEqual(other_plant.qty_venta, 70, "Otra planta del cliente no se toca.")
        self.assertEqual(len(self._forecast(plant=False)), 5)

    def test_07_profile_per_plant(self):
        self.profile.plant = 'Silao'
        other = self.env['qb.release.profile'].create({
            'partner_id': self.customer.id, 'plant': 'Tennessee',
            'reader': 'lear_aiag', 'firm_rule': 'fab_auth'})
        self.assertIn('Tennessee', other.name)
        with self.assertRaises(Exception), mute_logger('odoo.sql_db'):
            with self.env.cr.savepoint():
                self.env['qb.release.profile'].create({
                    'partner_id': self.customer.id, 'plant': 'Tennessee',
                    'reader': 'lear_aiag', 'firm_rule': 'fab_auth'})
        release = self._release()
        release.action_read()
        self._confirm_part()
        release.action_mark_reviewed()
        release.action_apply()
        self.assertEqual(set(self._forecast().mapped('plant')), {'Silao'})

    def test_05_delivery_basis_moves_week(self):
        self.profile.write({'date_basis': 'entrega', 'transit_days': 3})
        release = self._release()
        release.action_read()
        line = release.line_ids.filtered(lambda ln: ln.date_customer == date(2040, 12, 17))
        self.assertEqual(line.date_ship, date(2040, 12, 14))
        self.assertEqual(line.week, date(2040, 12, 10))
