# -*- coding: utf-8 -*-
"""2.24.0 (Dirección General 2026-11-03): un solo rendimiento de tela acabada, calculado con lo fabricado.

Rendimiento = metros de la tela acabada fabricados ÷ kilos de tela consumidos (crudo, teñido o acabado en kg; sin
engomado ni químicos) en sus órdenes terminadas de los últimos 12 meses. Ese número es el
``rendimiento_tela_acabada`` de la ficha: lo usan la conversión kg → m de la lista en metros y los tiempos de rama e
inspección. Sin órdenes, se queda el que tiene la ficha. Se recalcula cada noche antes de la revisión de listas; nunca
toca una lista de materiales.
"""
from datetime import timedelta

from odoo import api, fields, models

DIAS_REAL = 365
POSICION_PROCESO = ('H', 'I', 'J')  # crudo, teñido, acabado (DAT P-D02-01, posición 9)


def es_tela_kg(code):
    """Crudo, teñido o tela (H, I o J en la posición 9), o su código en kg con «I» al frente. Lo demás
    (engomado, colorantes, auxiliares) no es tela."""
    code = (code or '').strip().upper()
    if len(code) in (15, 17) and code.startswith('I'):
        code = code[1:]
    return len(code) in (14, 16) and code[8:9] in POSICION_PROCESO


def ancho_en_m(ancho):
    """El ancho de la ficha en metros (se captura en cm: 160; si ya viene en m: 1.6)."""
    return ancho / 100.0 if ancho > 10 else ancho


class FichaTecnicaAcabadoRendimientoReal(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    rendimiento_real_ordenes = fields.Integer(
        string='Órdenes del rendimiento', readonly=True,
        help='Órdenes terminadas de los últimos 12 meses con las que se calculó el rendimiento.')
    peso_real = fields.Float(
        string='Peso real (g/m²)', digits=(12, 1), compute='_compute_peso_real',
        help='El peso que da lo fabricado al ancho de venta: 1000 ÷ (rendimiento × ancho de venta en m).')

    @api.depends('rendimiento_tela_acabada', 'rendimiento_real_ordenes', 'ancho_acabado')
    def _compute_peso_real(self):
        for ficha in self:
            ancho = ancho_en_m(ficha.ancho_acabado)
            rend = ficha.rendimiento_tela_acabada if ficha.rendimiento_real_ordenes else 0.0
            ficha.peso_real = 1000.0 / (rend * ancho) if rend and ancho else 0.0

    @api.model
    def _ft_fabricado(self, products, desde):
        """{producto: (metros, kilos de tela, órdenes)} de las órdenes terminadas desde ``desde`` que fabricaron
        esos productos."""
        Move = self.env['stock.move'].sudo()
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if not products or not metro or not kg:
            return {}
        metros, ordenes, kilos = {}, {}, {}
        for product, uom, qty, n in Move._read_group(
                [('state', '=', 'done'), ('product_id', 'in', products.ids), ('production_id.state', '=', 'done'),
                 ('production_id.date_finished', '>=', desde)],
                ['product_id', 'product_uom'], ['quantity:sum', 'production_id:count_distinct']):
            if uom == metro or uom._has_common_reference(metro):
                metros[product] = metros.get(product, 0.0) + uom._compute_quantity(qty, metro, round=False)
                ordenes[product] = ordenes.get(product, 0) + n
        for product, componente, uom, qty in Move._read_group(
                [('state', '=', 'done'), ('raw_material_production_id.product_id', 'in', products.ids),
                 ('raw_material_production_id.state', '=', 'done'),
                 ('raw_material_production_id.date_finished', '>=', desde)],
                ['raw_material_production_id.product_id', 'product_id', 'product_uom'], ['quantity:sum']):
            if not es_tela_kg(componente.default_code) or not (uom == kg or uom._has_common_reference(kg)):
                continue
            kilos[product] = kilos.get(product, 0.0) + uom._compute_quantity(qty, kg, round=False)
        return {p: (metros[p], kilos.get(p, 0.0), ordenes.get(p, 0)) for p in metros}

    def _ft_actualizar_rendimiento_real(self):
        """El rendimiento de estas fichas (todas si vacío) con lo fabricado; sin órdenes se queda el de la ficha."""
        fichas = self or self.search([('product_acabado_id', '!=', False)])
        desde = fields.Date.context_today(self) - timedelta(days=DIAS_REAL)
        datos = self._ft_fabricado(fichas.product_acabado_id, desde)
        for ficha in fichas:
            m, kilos, n = datos.get(ficha.product_acabado_id, (0.0, 0.0, 0))
            if m and kilos:
                ficha.write({'rendimiento_tela_acabada': round(m / kilos, 4), 'rendimiento_real_ordenes': n})
            elif ficha.rendimiento_real_ordenes:
                ficha.rendimiento_real_ordenes = 0
        return True


class FichaTecnicaRevisionRendimiento(models.Model):
    _inherit = 'ficha.tecnica.revision'

    @api.model
    def _cron_ft_actualizar(self):
        # 2.24.0: primero el rendimiento (la revisión compara las listas contra él).
        self.env['ficha.tecnica.acabado']._ft_actualizar_rendimiento_real()
        return super()._cron_ft_actualizar()
