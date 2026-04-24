from odoo import models, fields, api

class MrpTara(models.Model):
    _name = 'mrp.tara'
    _description = 'Tabla de Taras por Producto y Centro de Trabajo'

    product_id = fields.Many2one('product.product', string="Producto", required=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string="Centro de Trabajo")
    tara = fields.Float(string="Tara (kg)", digits=(12, 3), required=True)

    @api.constrains('product_id', 'workcenter_id')
    def _check_unique_tara(self):
        for reg in self:
            domain = [
                ('product_id', '=', reg.product_id.id),
                ('workcenter_id', '=', reg.workcenter_id.id),
                ('id', '!=', reg.id)
            ]
            if self.search_count(domain) > 0:
                raise ValidationError(_('Ya existe una tara configurada para este producto y centro de trabajo.'))