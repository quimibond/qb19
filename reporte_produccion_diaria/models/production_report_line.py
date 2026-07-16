from odoo import models, fields


class ProductionReportLine(models.TransientModel):
    """Línea de resultado del Reporte de Producción Tejido.

    Se genera bajo demanda desde production.report.wizard.action_view_report()
    y se muestra en una vista de lista en lugar de un PDF. Al ser un modelo
    transitorio, Odoo limpia los registros viejos automáticamente (vacuum),
    por lo que no crece indefinidamente.
    """
    _name = 'production.report.line'
    _description = 'Línea de Reporte de Producción Tejido'
    _order = 'date_finished asc, production_id asc'

    date_finished = fields.Date(string='Fecha de Terminación')
    production_id = fields.Many2one('mrp.production', string='Orden de Fabricación')
    product_id = fields.Many2one('product.product', string='Producto')
    workcenter_names = fields.Char(string='Centro de Trabajo')

    qty_planned = fields.Float(string='Cantidad Planeada', digits='Product Unit of Measure')
    qty_produced = fields.Float(string='Cantidad Producida', digits='Product Unit of Measure')
    qty_scrap = fields.Float(string='Cantidad Merma (Scrap)', digits='Product Unit of Measure')

    byproduct_id = fields.Many2one('product.product', string='SubProducto')
    qty_byproduct = fields.Float(string='Cantidad Producida Subproducto', digits='Product Unit of Measure')

    uom_id = fields.Many2one('uom.uom', string='Unidad de Medida')

    currency_id = fields.Many2one(
        'res.currency', string='Moneda',
        default=lambda self: self.env.company.currency_id
    )

    cost_components_total = fields.Monetary(string='Costo Total Componentes')
    cost_operations_total = fields.Monetary(string='Costo Total Operaciones')
    cost_production_total = fields.Monetary(string='Costo Total de la Producción')

    cost_components_avg = fields.Monetary(string='Costo Promedio Componentes por Unidad')
    cost_operations_avg = fields.Monetary(string='Costo Promedio Operaciones por Unidad')
    cost_total_avg = fields.Monetary(string='Costo Total Promedio por Unidad')
