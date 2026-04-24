from odoo import models, fields, api

class MrpTara(models.Model):
    _name = 'mrp.tara'
    _description = 'Tabla de Taras por Producto y Centro de Trabajo'

    product_id = fields.Many2one('product.product', string="Producto", required=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string="Centro de Trabajo")
    tara = fields.Float(string="Tara (kg)", digits=(12, 3), required=True)

    _sql_constraints = [
        ('unique_product_wc', 'UNIQUE(product_id, workcenter_id)', 
         'Ya existe una tara configurada para este producto y centro de trabajo.')
    ]