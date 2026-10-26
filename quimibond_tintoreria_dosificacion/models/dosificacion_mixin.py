# -*- coding: utf-8 -*-
from odoo import api, models

TIPOS_COMPONENTE = [
    ('quimico', 'Químico'),
    ('colorante', 'Colorante'),
]

PARAM_QUIMICO = 'quimibond_tintoreria_dosificacion.categoria_quimico'
PARAM_COLORANTE = 'quimibond_tintoreria_dosificacion.categoria_colorante'


class TintoreriaDosificacionMixin(models.AbstractModel):
    """Utilidades compartidas de dosificación (tipo de componente, relación de baño y cálculo)."""
    _name = 'tintoreria.dosificacion.mixin'
    _description = 'Utilidades de dosificación de tintorería'

    @api.model
    def _tipo_desde_producto(self, product):
        """Devuelve 'quimico', 'colorante' o False según la categoría del producto.

        Las categorías se leen de parámetros del sistema (por defecto
        'Materia Prima / Químico' y 'Materia Prima / Colorante'); las subcategorías
        también cuentan.
        """
        if not product:
            return False
        params = self.env['ir.config_parameter'].sudo()
        cat_quimico = params.get_param(PARAM_QUIMICO, 'Materia Prima / Químico')
        cat_colorante = params.get_param(PARAM_COLORANTE, 'Materia Prima / Colorante')
        nombre = product.categ_id.complete_name or ''
        if nombre == cat_colorante or nombre.startswith(cat_colorante + ' / '):
            return 'colorante'
        if nombre == cat_quimico or nombre.startswith(cat_quimico + ' / '):
            return 'quimico'
        return False

    @api.model
    def _kg_uom(self):
        return self.env.ref('uom.product_uom_kgm')

    @api.model
    def _rb_workcenter(self, workcenter, producto=None):
        """Relación de baño (L/kg) para teñir ``producto`` en el work center: la del artículo en el jet
        y, si no la tiene, la de la máquina (fichas técnicas: ``_ft_relacion_bano``; no leer el texto)."""
        if not workcenter:
            return 0.0
        relacion, _motivo = workcenter._ft_relacion_bano(producto)
        return relacion or 0.0

    @api.model
    def _calcular_dosis_kg(self, tipo, gramos_por_litro, porcentaje, kg_orden, relacion_bano):
        """Cantidad a surtir en kg.

        * Químico:   (g/L x litros de baño) / 1000, con litros = RB (L/kg) x kg de la orden.
        * Colorante: (% / 100) x kg de la orden (no depende de la relación de baño).
        """
        if tipo == 'quimico':
            return (gramos_por_litro or 0.0) * (relacion_bano or 0.0) * kg_orden / 1000.0
        if tipo == 'colorante':
            return (porcentaje or 0.0) / 100.0 * kg_orden
        return 0.0
