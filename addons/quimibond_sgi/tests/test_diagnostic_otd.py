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
        self.assertTrue(wizard.line_ids, "Al crearse corre el diagnóstico.")
        self.assertIn('Diagnóstico', wizard.summary)
        for fragment in ('Procesos', 'Indicadores y mediciones', 'Documental',
                         'Calidad preventiva y piso', 'Mejora continua', 'Ajustes clave'):
            self.assertIn(fragment, wizard.line_ids.mapped('section'))
        self.assertTrue(set(wizard.line_ids.mapped('level')) <= {'ok', 'warn', 'bad'})
        self.assertEqual(wizard.bad_count + wizard.warn_count + wizard.ok_count, len(wizard.line_ids))
        # El menú corre el diagnóstico y abre la lista nativa de hallazgos
        # acotada a esa corrida (sin HTML, con migas de pan).
        action = self.env['sgi.diagnostic'].action_run()
        self.assertEqual(action['res_model'], 'sgi.diagnostic.line')
        diag_id = action['domain'][0][2]
        lines = self.env['sgi.diagnostic.line'].search(action['domain'])
        self.assertTrue(lines)
        self.assertTrue(all(l.diagnostic_id.id == diag_id for l in lines))
        self.assertEqual(action['context']['search_default_group_section'], 1)
        # Actualizar rehace las filas de la misma corrida.
        before = wizard.line_ids.ids
        wizard.action_refresh()
        self.assertTrue(wizard.line_ids)
        self.assertFalse(set(before) & set(wizard.line_ids.ids))

    def test_02b_detects_empty_control_plan_and_orphan_points(self):
        plan = self.env['sgi.control.plan'].create({'name': 'Plan vacío diag'})
        plan.state = 'vigente'  # data seed bypass: el candado solo corre en el botón
        self.env['quality.point'].create({
            'title': 'Punto suelto diag',
            'picking_type_ids': [(4, self.env.ref('stock.picking_type_in').id)],
        })
        wizard = self.env['sgi.diagnostic'].create({})
        self.assertIn('0 puntos', wizard.summary)
        self.assertIn('sin plan de control', wizard.summary)
        self.assertTrue(wizard.line_ids.filtered(
            lambda l: l.level == 'bad' and '0 puntos' in l.text
            and l.section == 'Calidad preventiva y piso'))
        # El botón del plan abre los puntos sueltos con el plan como default.
        action = plan.action_open_orphan_points()
        self.assertEqual(action['res_model'], 'quality.point')
        self.assertEqual(
            action['context']['default_sgi_control_plan_id'], plan.id)

    def test_02_detects_indicator_without_responsible(self):
        self.env['sgi.indicator'].create({
            'code': 'TST-DIAG', 'name': 'KPI sin responsable',
            'calc_mode': 'manual'})
        wizard = self.env['sgi.diagnostic'].create({})
        self.assertIn('sin responsable', wizard.summary)
        self.assertIn('sin proceso', wizard.summary)
