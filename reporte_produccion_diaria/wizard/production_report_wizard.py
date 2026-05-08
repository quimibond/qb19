from odoo import models, fields, api, _
from odoo.exceptions import ValidationError  # Importación correcta de la excepción

class ProductionReportWizard(models.TransientModel):
    _name = 'production.report.wizard'
    _description = 'Asistente de Reporte de Producción Tejido'

    date_start = fields.Date(
        string='Fecha Inicio', 
        required=True, 
        default=fields.Date.context_today
    )
    date_end = fields.Date(
        string='Fecha Fin', 
        required=True, 
        default=fields.Date.context_today
    )

    def action_print_report(self):
        # 1. Buscamos primero los centros de trabajo para evitar errores de SQL con campos no almacenados
        workcenter_ids = self.env['mrp.workcenter'].search([
            ('name', 'ilike', 'CIRCULAR%')
        ]).ids

        # 2. Buscamos las órdenes. 
        # Importante: Usamos 'state' en ('to_close', 'done') para asegurar que SOLO sean terminadas.
        # Esto excluye automáticamente 'cancel' y 'draft'.
        production_orders = self.env['mrp.production'].search([
            ('date_start', '>=', self.date_start),
            ('date_start', '<=', self.date_end),
            ('state', 'in', ['to_close', 'done']),
            ('qty_producing', '>', 0),
            ('workorder_ids.workcenter_id', 'in', workcenter_ids)
        ], order='date_start asc')

        # 3. Doble verificación de seguridad en Python para excluir canceladas
        production_orders = production_orders.filtered(lambda x: x.state in ['to_close', 'done'])

        if not production_orders:
            # Uso correcto de ValidationError
            raise ValidationError("No se encontraron órdenes finalizadas para los centros CIRCULAR en las fechas seleccionadas.")

        data = {
            'form': self.read()[0],
            'production_orders': production_orders.ids,
        }
        
        return self.env.ref('reporte_produccion_diaria.action_report_production_daily').report_action(self, data=data)