# -*- coding: utf-8 -*-
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestSupplierOtdByDay(TransactionCase):
    """Reproduce el caso encontrado en producción: recepciones del MISMO día
    contaban como tarde por comparar datetime al segundo, tirando el OTD a
    2-30% y mandando 84/87 proveedores a «Baja»."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.supplier_otd_tolerance_days', '1')
        cls.partner = cls.env['res.partner'].create({
            'name': 'Proveedor OTD', 'is_company': True})
        cls.product = cls.env['product.product'].create({
            'name': 'Insumo OTD', 'is_storable': False})
        cls.warehouse = cls.env['stock.warehouse'].search(
            [('company_id', '=', cls.env.company.id)], limit=1)
        cls.supplier_loc = cls.env.ref('stock.stock_location_suppliers')

    def _reception(self, deadline, done):
        pick = self.env['stock.picking'].create({
            'partner_id': self.partner.id,
            'picking_type_id': self.warehouse.in_type_id.id,
            'location_id': self.supplier_loc.id,
            'location_dest_id': self.warehouse.lot_stock_id.id,
            'move_ids': [(0, 0, {
                'product_id': self.product.id,
                'product_uom_qty': 1,
                'location_id': self.supplier_loc.id,
                'location_dest_id': self.warehouse.lot_stock_id.id,
            })],
        })
        pick.action_confirm()
        pick.move_ids.write({'quantity': 1, 'picked': True})
        pick._action_done()
        # Fechas controladas para el escenario (se escriben tras validar).
        pick.write({'date_deadline': deadline, 'date_done': done})
        return pick

    def test_01_same_day_counts_on_time(self):
        now = fields.Datetime.now()
        morning = now.replace(hour=8, minute=0, second=0)
        evening = now.replace(hour=20, minute=0, second=0)
        # Caso producción: compromiso 08:00, recibido 20:00 del MISMO día.
        self._reception(morning, evening)
        # Caso realmente tarde: compromiso hace 5 días, recibido hoy.
        self._reception(now - timedelta(days=5), evening)
        ev = self.env['sgi.supplier.eval'].create({
            'partner_id': self.partner.id,
            'date_from': (now - timedelta(days=10)).date(),
            'date_to': (now + timedelta(days=1)).date(),
        })
        self.assertEqual(ev.otd_pct, 50.0,
                         "Mismo día = a tiempo; 5 días tarde = incumplida.")
        # Con tolerancia amplia, la tardía también entra: recálculo masivo.
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.supplier_otd_tolerance_days', '10')
        ev.action_recompute()
        self.assertEqual(ev.otd_pct, 100.0)


@tagged('post_install', '-at_install')
class TestDiagnostic(TransactionCase):

    def test_01_diagnostic_builds_report(self):
        wizard = self.env['sgi.diagnostic'].create({})
        self.assertTrue(wizard.result)
        for fragment in ('Diagnóstico', 'Procesos', 'Indicadores y mediciones',
                         'Documental', 'Mejora continua', 'Ajustes clave'):
            self.assertIn(fragment, wizard.result)

    def test_02_detects_indicator_without_responsible(self):
        self.env['sgi.indicator'].create({
            'code': 'TST-DIAG', 'name': 'KPI sin responsable',
            'calc_mode': 'manual'})
        wizard = self.env['sgi.diagnostic'].create({})
        self.assertIn('sin responsable', wizard.result)
        self.assertIn('sin proceso', wizard.result)
