# -*- coding: utf-8 -*-
"""Artículo comercial y unidades del producto (E1, diseño v3 sección 2.2).

Nada se inventa aquí: todo sale de Fichas técnicas
(``quimibond_ficha_tecnica_tela``), que ya contesta para Producción las tres
preguntas de unidades, y Ventas usa las mismas respuestas:

- **Artículo** (``qb_articulo_id``): la tela en metros que junta sus códigos
  (W… en metros, IW… en kilos, …M2 en m²). Orden: ficha de acabado
  (``ft_producto_m_id``) → regla de código (Dirección 2026-11-04: con «I» es
  kilos, sin «I» es metros) → desarrollo que creó el producto → él mismo.
- **Rendimiento** (``qb_m_por_kg``): el de la ficha (un solo rendimiento,
  fichas 2.24.0, el que usa la lista en metros) → la especificación de la
  ficha (peso × ancho) → el código → sin dato (no se convierte).
- **Kilos a producir**: metros ÷ rendimiento ÷ primera para planear
  (``primera_para_planear`` de Fichas), la misma cuenta que la lista en metros.

Los campos de Fichas (``ft_*``) son calculados y no se guardan; por eso estos
se guardan y los recalcula el cron nocturno (``cron_qb_articulo``), no un
``@api.depends`` sobre campos sin guardar.
"""
import logging

from odoo import api, fields, models

from ..release import articulo as art

_logger = logging.getLogger(__name__)

CONV_ORIGINS = [
    ('ficha_real', "Ficha (rendimiento real)"),
    ('ficha_especificacion', "Ficha (especificación)"),
    ('codigo', "Código del artículo"),
    ('sin_dato', "Sin dato"),
]
_UOM_REFS = (
    ('uom.product_uom_meter', 'm'),
    ('uom.product_uom_kgm', 'kg'),
)


class ProductProductArticulo(models.Model):
    _inherit = 'product.product'

    qb_articulo_id = fields.Many2one(
        'product.product', string="Artículo (ventas)", index=True, copy=False,
        help="La tela en metros que junta los códigos de este producto (en metros, en kilos con «I» al "
             "frente y en m² con «M2» al final) para medir pronóstico, pedidos y venta. Sale de la ficha de "
             "acabado o de la regla de código; lo recalcula el cron nocturno.")
    qb_m_por_kg = fields.Float(
        string="Metros por kilo (ventas)", digits=(12, 4), copy=False,
        help="Rendimiento con que se convierte la venta entre metros y kilos: el de la ficha de acabado; "
             "si no hay, la especificación de la ficha (peso × ancho); si no, el código.")
    qb_ancho_m = fields.Float(
        string="Ancho de venta (m)", digits=(12, 3), copy=False,
        help="Ancho de venta de la ficha o del código, para convertir m².")
    qb_conv_origen = fields.Selection(CONV_ORIGINS, string="Origen de la conversión", copy=False,
                                      default='sin_dato')

    # --- Recalcular --------------------------------------------------------
    def _qb_sale_unit(self):
        """'m', 'kg' o '' según la unidad de venta del producto."""
        self.ensure_one()
        for xmlid, code in _UOM_REFS:
            ref = self.env.ref(xmlid, raise_if_not_found=False)
            if ref and self.uom_id and self.uom_id._has_common_reference(ref):
                return code
        return ''

    def _qb_articulo_vals(self, by_code):
        """Valores de artículo y conversión de un producto. ``by_code``:
        {default_code: product} de los productos que podrían ser su artículo."""
        self.ensure_one()
        tmpl = self.product_tmpl_id
        articulo = self.env['product.product']
        ficha_m = tmpl.ft_producto_m_id if 'ft_producto_m_id' in tmpl._fields else articulo
        if ficha_m:
            articulo = ficha_m
        if not articulo:
            metros = art.codigo_metros(self.default_code)
            if metros and metros != art.clean(self.default_code):
                articulo = by_code.get(metros, articulo)
        if not articulo and 'sgi_dev_project_id' in tmpl._fields and tmpl.sgi_dev_project_id:
            dev = tmpl.sgi_dev_project_id
            proceso = dev.sgi_dev_product_proceso_id if 'sgi_dev_product_proceso_id' in dev._fields else None
            if proceso and proceso == self and dev.sgi_dev_product_id:
                articulo = dev.sgi_dev_product_id
        articulo = articulo or self
        a_tmpl = articulo.product_tmpl_id
        rend = a_tmpl.ft_rendimiento if 'ft_rendimiento' in a_tmpl._fields else 0.0
        peso = a_tmpl.ft_peso if 'ft_peso' in a_tmpl._fields else 0.0
        ancho_cm = a_tmpl.ft_ancho if 'ft_ancho' in a_tmpl._fields else 0.0
        code_g, code_w = art.gramaje_ancho(articulo.default_code or self.default_code)
        if rend:
            m_por_kg, origen = rend, 'ficha_real'
        elif peso and ancho_cm:
            m_por_kg, origen = art.rendimiento(peso, ancho_cm), 'ficha_especificacion'
        elif code_g and code_w:
            m_por_kg, origen = art.rendimiento(code_g, code_w), 'codigo'
        else:
            m_por_kg, origen = 0.0, 'sin_dato'
        ancho_m = (ancho_cm or code_w or 0.0) / 100.0
        return {'qb_articulo_id': articulo.id, 'qb_m_por_kg': round(m_por_kg, 4),
                'qb_ancho_m': round(ancho_m, 3), 'qb_conv_origen': origen}

    def action_qb_actualizar_articulo(self):
        """Recalcula artículo y conversión de estos productos (solo escribe lo que cambia)."""
        codes = set()
        for product in self:
            metros = art.codigo_metros(product.default_code)
            if metros:
                codes.add(metros)
        by_code = {}
        if codes:
            for candidate in self.with_context(active_test=False).search([('default_code', 'in', list(codes))]):
                code = art.clean(candidate.default_code)
                # Un activo gana sobre un archivado con el mismo código.
                if code not in by_code or (candidate.active and not by_code[code].active):
                    by_code[code] = candidate
        changed = 0
        for product in self:
            vals = product._qb_articulo_vals(by_code)
            current = {'qb_articulo_id': product.qb_articulo_id.id, 'qb_m_por_kg': product.qb_m_por_kg,
                       'qb_ancho_m': product.qb_ancho_m, 'qb_conv_origen': product.qb_conv_origen}
            if vals != current:
                product.write(vals)
                changed += 1
        return changed

    @api.model
    def _qb_articulo_scope(self):
        """Productos que hace falta recalcular: los vendibles con código de tela, los del pronóstico y
        los vendidos en los últimos dos años."""
        products = self.search([('sale_ok', '=', True), ('default_code', '!=', False)])
        products = products.filtered(lambda p: art.es_codigo_tela(art.codigo_metros(p.default_code))
                                     or art.es_codigo_kg(p.default_code))
        if 'qb.forecast.line' in self.env:
            products |= self.env['qb.forecast.line'].sudo().search([]).product_id
        return products

    @api.model
    def cron_qb_articulo(self):
        """Cron nocturno: artículo y conversión de los productos en alcance, por compañía activa."""
        products = self._qb_articulo_scope()
        changed = 0
        for start in range(0, len(products), 500):
            changed += products[start:start + 500].action_qb_actualizar_articulo()
        _logger.info("qb_articulo: %d productos revisados, %d cambiaron", len(products), changed)
        return changed

    # --- Convertir ---------------------------------------------------------
    def qb_convertir(self, qty, from_unit, to_unit):
        """Convierte ``qty`` de este producto entre 'm', 'yd', 'kg', 'm2' y 'venta' (su unidad de
        venta). None si falta el rendimiento o el ancho: nunca se inventa un factor.

        Usa el rendimiento del artículo (el mismo para el código en metros y el de kilos)."""
        self.ensure_one()
        sale_unit = self._qb_sale_unit()
        from_unit = sale_unit if from_unit == 'venta' else (from_unit or '').lower()
        to_unit = sale_unit if to_unit == 'venta' else (to_unit or '').lower()
        if not from_unit or not to_unit:
            return None
        source = self.qb_articulo_id or self
        m_por_kg = self.qb_m_por_kg or source.qb_m_por_kg
        ancho_m = self.qb_ancho_m or source.qb_ancho_m
        return art.convertir(qty, from_unit, to_unit, m_por_kg=m_por_kg, ancho_m=ancho_m)

    def qb_kg_producir(self, qty_venta):
        """Kilos a producir para entregar ``qty_venta`` (en la unidad de venta): metros ÷ rendimiento
        ÷ primera para planear, como la lista en metros de Fichas; en kilos, kilos ÷ primera."""
        self.ensure_one()
        try:
            from odoo.addons.quimibond_ficha_tecnica_tela.models.capacidad_conversion import (
                primera_para_planear)
            plan, _fuente = primera_para_planear(self.env, self.qb_articulo_id or self)
        except ImportError:
            plan = 1.0
        kg = self.qb_convertir(qty_venta, 'venta', 'kg')
        if kg is None:
            return None
        return kg / (plan or 1.0)
