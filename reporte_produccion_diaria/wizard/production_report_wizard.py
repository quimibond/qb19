from odoo import models, fields, api

class ProductionReportWizard(models.TransientModel):
    _name = 'production.report.wizard'
    _description = 'Asistente de Reporte de Producción'

    date_start = fields.Date(string='Fecha Inicio', required=True, default=fields.Date.context_today)
    date_end = fields.Date(string='Fecha Fin', required=True, default=fields.Date.context_today)

    def action_print_report(self):
        # Filtro corregido: Buscamos a través de workorder_ids
        production_orders = self.env['mrp.production'].search([
            ('date_start', '>=', self.date_start),
            ('date_start', '<=', self.date_end),
            ('state', 'not in', ('draft', 'cancel')),
            ('workorder_ids.workcenter_id.name', 'ilike', 'CIRCULAR%')
        ], order='date_start asc')

        data = {
            'form': self.read()[0],
            'production_orders': production_orders.ids,
        }
        return self.env.ref('reporte_produccion_diaria.action_report_production_daily').report_action(self, data=data)