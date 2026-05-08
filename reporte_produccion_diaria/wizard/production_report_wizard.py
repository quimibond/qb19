from odoo import models, fields, api

class ProductionReportWizard(models.TransientModel):
    _name = 'production.report.wizard'
    _description = 'Asistente de Reporte de Producción'

    date_start = fields.Date(string='Fecha Inicio', required=True, default=fields.Date.context_today)
    date_end = fields.Date(string='Fecha Fin', required=True, default=fields.Date.context_today)

    def action_print_report(self):
        # 1. Primero obtenemos los IDs de los centros de trabajo que coinciden con CIRCULAR
        # Esto hace la búsqueda mucho más rápida y precisa.
        workcenter_ids = self.env['mrp.workcenter'].search([
            ('name', 'ilike', 'CIRCULAR%')
        ]).ids

        # 2. Buscamos las órdenes de trabajo (workorders) que pertenecen a esos centros
        # y que NO estén canceladas ni en borrador.
        # Filtramos por date_start de la orden de producción.
        production_orders = self.env['mrp.production'].search([
            ('date_start', '>=', self.date_start),
            ('date_start', '<=', self.date_end),
            ('state', 'in', ['to_close', 'done']), # ESTRICTO: Solo terminadas o por cerrar
            ('qty_producing', '>', 0),              # Solo si hay producción real
            ('workorder_ids.workcenter_id', 'in', workcenter_ids)
        ], order='date_start asc')

        # 3. Verificación de seguridad: filtramos manualmente por si acaso 
        # (algunas versiones de Odoo 19 tienen comportamientos de caché en search)
        production_orders = production_orders.filtered(lambda x: x.state in ['to_close', 'done'])

        if not production_orders:
            raise models.ValidationError("No se encontraron órdenes finalizadas para los centros CIRCULAR en las fechas seleccionadas.")

        data = {
            'form': self.read()[0],
            'production_orders': production_orders.ids,
        }
        return self.env.ref('reporte_produccion_diaria.action_report_production_daily').report_action(self, data=data)