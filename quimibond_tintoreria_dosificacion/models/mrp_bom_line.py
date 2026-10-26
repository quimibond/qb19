# -*- coding: utf-8 -*-
from odoo import _, api, fields, models
from odoo.exceptions import ValidationError, UserError
from odoo.tools import float_compare

from .dosificacion_mixin import TIPOS_COMPONENTE

CAMPOS_TEORICA = {'gramos_por_litro', 'porcentaje_colorante', 'product_id', 'operation_id'}


class MrpBomLine(models.Model):
    _name = 'mrp.bom.line'
    _inherit = ['mrp.bom.line', 'tintoreria.dosificacion.mixin']

    tipo_componente = fields.Selection(
        TIPOS_COMPONENTE, 'Tipo de componente', compute='_compute_tipo_componente',
        store=True, readonly=True,
        help="Se deriva de la categoría del producto: Químico (g/L) o Colorante (% sobre kg).")
    gramos_por_litro = fields.Float('g/L', digits=(16, 4),
                                    help="Dosificación en gramos por litro de baño (solo químicos).")
    porcentaje_colorante = fields.Float('% sobre kg', digits=(16, 5),
                                        help="Dosificación en % sobre los kg de la orden (solo colorantes).")
    dosificado = fields.Boolean('Dosificado', compute='_compute_dosificado', store=True,
                                help="La cantidad se calcula con g/L o % en la orden de fabricación.")
    formula_line_id = fields.Many2one('tintoreria.formula.line', 'Línea de fórmula', copy=False,
                                      ondelete='set null', index=True)

    @api.depends('product_id.categ_id.complete_name')
    def _compute_tipo_componente(self):
        for linea in self:
            linea.tipo_componente = self._tipo_desde_producto(linea.product_id)

    @api.depends('tipo_componente', 'gramos_por_litro', 'porcentaje_colorante')
    def _compute_dosificado(self):
        for linea in self:
            linea.dosificado = bool(
                (linea.tipo_componente == 'quimico' and linea.gramos_por_litro > 0)
                or (linea.tipo_componente == 'colorante' and linea.porcentaje_colorante > 0))

    @api.constrains('tipo_componente', 'gramos_por_litro', 'porcentaje_colorante')
    def _check_dosificacion_tipo(self):
        for linea in self:
            nombre = linea.product_id.display_name
            if linea.tipo_componente == 'quimico' and linea.porcentaje_colorante:
                raise ValidationError(_("El químico %s se dosifica en g/L, no en %%.", nombre))
            if linea.tipo_componente == 'colorante' and linea.gramos_por_litro:
                raise ValidationError(_("El colorante %s se dosifica en %%, no en g/L.", nombre))
            if linea.tipo_componente not in ('quimico', 'colorante') and (
                    linea.gramos_por_litro or linea.porcentaje_colorante):
                raise ValidationError(_(
                    "El producto %s no es químico ni colorante; no puede llevar g/L ni %%.", nombre))

    # ------------------------------------------------------------------
    # Cantidad teórica de la BoM (referencia / costeo)
    # ------------------------------------------------------------------
    def _dosificacion_qty_teorica(self):
        """Cantidad teórica por lote de la BoM, con la relación de baño del work center
        de la operación. La cantidad real se calcula en la orden de fabricación."""
        self.ensure_one()
        bom = self.bom_id
        kg = self._kg_uom()
        try:
            kg_lote = bom.product_uom_id._compute_quantity(bom.product_qty, kg)
        except UserError:
            kg_lote = bom.product_qty
        operacion = self.operation_id or bom._operacion_tintoreria()
        rb = self._rb_workcenter(
            operacion.workcenter_id, bom.product_id or bom.product_tmpl_id.product_variant_id)
        dosis_kg = self._calcular_dosis_kg(
            self.tipo_componente, self.gramos_por_litro, self.porcentaje_colorante, kg_lote, rb)
        return kg._compute_quantity(dosis_kg, self.product_uom_id)

    def _actualizar_qty_teorica(self):
        for linea in self.filtered('dosificado'):
            qty = linea._dosificacion_qty_teorica()
            if float_compare(qty, linea.product_qty, precision_digits=6) != 0:
                linea.with_context(skip_dosificacion=True).product_qty = qty

    @api.onchange('gramos_por_litro', 'porcentaje_colorante', 'product_id', 'operation_id', 'product_uom_id')
    def _onchange_dosificacion(self):
        for linea in self:
            if linea.dosificado and linea.bom_id:
                linea.product_qty = linea._dosificacion_qty_teorica()

    @api.model_create_multi
    def create(self, vals_list):
        lineas = super().create(vals_list)
        lineas._actualizar_qty_teorica()
        return lineas

    def write(self, vals):
        res = super().write(vals)
        if not self.env.context.get('skip_dosificacion') and CAMPOS_TEORICA & set(vals):
            self._actualizar_qty_teorica()
        return res
