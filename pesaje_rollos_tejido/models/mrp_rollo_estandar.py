from odoo import models, fields, api, _
from odoo.exceptions import ValidationError

class MrpRolloEstandar(models.Model):
    _name = 'mrp.rollo.estandar'
    _description = 'Tamaño de Rollo Estandar por Articulo'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', string="Producto", required=True)
    rollo_teorico = fields.Float(string="Rollo Teórico (kg)", digits=(12, 3), required=True)

    @api.constrains('product_id')
    def _check_unique_product(self):
        for record in self:
            # Buscamos si ya existe otro registro con el mismo producto
            exists = self.search([
                ('product_id', '=', record.product_id.id),
                ('id', '!=', record.id)
            ])
            if exists:
                raise ValidationError(_("Ya existe una configuración de tamaño de rollo para el producto: %s") % record.product_id.display_name)