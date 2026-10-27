# -*- coding: utf-8 -*-
from odoo import _, api, fields, models
from odoo.exceptions import ValidationError

from .dosificacion_mixin import TIPOS_COMPONENTE


class TintoreriaFormula(models.Model):
    """Catálogo de fórmulas de tintorería (una por color / proceso, p. ej. 'NATURAL 005')."""
    _name = 'tintoreria.formula'
    _description = 'Fórmula de tintorería'
    _order = 'name'

    name = fields.Char('Fórmula', required=True, index=True,
                       help="Nombre de la fórmula o color, tal como aparece en el catálogo (p. ej. NATURAL 005).")
    active = fields.Boolean(default=True)
    product_id = fields.Many2one(
        'product.product', 'Producto de la fórmula',
        help="Producto que representaba esta fórmula en las BoMs (p. ej. NATURAL005). "
             "Es informativo: ya no se consume como componente. Si se deja vacío, "
             "al crear la fórmula se busca un producto con código igual al nombre sin espacios.")
    notas = fields.Text('Notas')
    company_id = fields.Many2one('res.company', 'Compañía', default=lambda self: self.env.company)
    line_ids = fields.One2many('tintoreria.formula.line', 'formula_id', 'Ingredientes', copy=True)
    bom_ids = fields.One2many('mrp.bom', 'formula_tintoreria_id', 'BoMs que la usan')
    bom_count = fields.Integer('BoMs', compute='_compute_bom_count')
    line_count = fields.Integer('Ingredientes', compute='_compute_line_count')  # 1.3.0: columna de la lista

    _name_uniq = models.Constraint(
        'UNIQUE (name, company_id)',
        'Ya existe una fórmula de tintorería con ese nombre.',
    )

    @api.depends('bom_ids')
    def _compute_bom_count(self):
        for formula in self:
            formula.bom_count = len(formula.bom_ids)

    @api.depends('line_ids')
    def _compute_line_count(self):
        for formula in self:
            formula.line_count = len(formula.line_ids)

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get('product_id') and vals.get('name'):
                codigo = ''.join(vals['name'].split())
                producto = self.env['product.product'].search(
                    [('default_code', '=ilike', codigo)], limit=1)
                if producto:
                    vals['product_id'] = producto.id
        return super().create(vals_list)

    def action_ver_boms(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _('BoMs de %s', self.name),
            'res_model': 'mrp.bom',
            'view_mode': 'list,form',
            'domain': [('formula_tintoreria_id', '=', self.id)],
        }


class TintoreriaFormulaLine(models.Model):
    _name = 'tintoreria.formula.line'
    _inherit = ['tintoreria.dosificacion.mixin']
    _description = 'Ingrediente de fórmula de tintorería'
    _order = 'formula_id, sequence, id'

    formula_id = fields.Many2one('tintoreria.formula', 'Fórmula', required=True,
                                 ondelete='cascade', index=True)
    sequence = fields.Integer('Secuencia', default=10)
    product_id = fields.Many2one('product.product', 'Producto', required=True,
                                 domain="[('categ_id.complete_name', 'ilike', 'Materia Prima')]")
    tipo_componente = fields.Selection(
        TIPOS_COMPONENTE, 'Tipo', compute='_compute_tipo_componente', store=True, readonly=True,
        help="Se deriva de la categoría del producto: Químico (g/L) o Colorante (% sobre kg).")
    gramos_por_litro = fields.Float('g/L', digits=(16, 4),
                                    help="Dosificación en gramos por litro de baño (solo químicos).")
    porcentaje_colorante = fields.Float('% sobre kg', digits=(16, 5),
                                        help="Dosificación en % sobre los kg de la orden (solo colorantes).")
    nota = fields.Char('Observaciones')

    @api.depends('product_id.categ_id.complete_name')
    def _compute_tipo_componente(self):
        for linea in self:
            linea.tipo_componente = self._tipo_desde_producto(linea.product_id)

    @api.constrains('product_id', 'tipo_componente', 'gramos_por_litro', 'porcentaje_colorante')
    def _check_dosificacion(self):
        for linea in self:
            nombre = linea.product_id.display_name
            if not linea.tipo_componente:
                raise ValidationError(_(
                    "El producto %s no es un químico ni un colorante (revisa su categoría).", nombre))
            if linea.tipo_componente == 'quimico':
                if linea.gramos_por_litro <= 0:
                    raise ValidationError(_("El químico %s requiere g/L mayor a 0.", nombre))
                if linea.porcentaje_colorante:
                    raise ValidationError(_("El químico %s se dosifica en g/L, no en %%.", nombre))
            else:
                if linea.porcentaje_colorante <= 0:
                    raise ValidationError(_("El colorante %s requiere un %% mayor a 0.", nombre))
                if linea.gramos_por_litro:
                    raise ValidationError(_("El colorante %s se dosifica en %%, no en g/L.", nombre))
