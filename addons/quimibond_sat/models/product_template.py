# -*- coding: utf-8 -*-
from odoo import fields, models


class ProductTemplate(models.Model):
    _inherit = 'product.template'

    sat_pago_terceros = fields.Boolean(
        string='Pago a terceros (no va en el CFDI)',
        help='Lo que se paga por cuenta de Quimibond y el proveedor no factura en su CFDI '
             '(impuestos de pedimento en la factura del agente aduanal, por ejemplo). Si la '
             'diferencia entre el SAT y la factura de Odoo son exactamente estas líneas, la '
             'comparación SAT vs Odoo la da por explicada.')

    def write(self, vals):
        res = super().write(vals)
        if 'sat_pago_terceros' in vals:
            # El hallazgo del CFDI es almacenado y no depende de la marca del producto.
            Cfdi = self.env['sat.cfdi'].sudo()
            cfdis = Cfdi.search([('move_id.invoice_line_ids.product_id.product_tmpl_id', 'in', self.ids)])
            self.env.add_to_compute(Cfdi._fields['issue'], cfdis)
        return res
