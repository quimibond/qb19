# -*- coding: utf-8 -*-
"""P-21 y diagnóstico de los automáticos sin dato (2026-09-24): tres modos
que devolvían None pasan a medirse desde Odoo con detalle.

- MA-04 ``reproceso``: kg producidos por las órdenes cuyo tipo de operación
  está en ``quimibond_sgi.rework_picking_type_ids`` (P-21: un solo criterio
  de reproceso en vez de tres tipos sueltos; hoy Re-proceso Tintorería 106 y
  Re-proceso Acabado 107) ÷ kg de hilo y fibra consumidos en el periodo (la
  misma base que MA-05). Solo líneas en kg: una orden de reproceso en metros
  no entra hasta que se defina su equivalencia.
- AL-01 ``inventario_diferencia``: valor de los ajustes de inventario del
  periodo (``stock.move.value``, que en Odoo 19 es positivo tanto en
  entradas como en salidas) ÷ valor actual de las existencias en
  ubicaciones internas (``stock.quant.value``), en moneda de la compañía.
  Odoo 19 ya no tiene capas de valuación (``stock.valuation.layer``), así
  que no hay valor «al cierre»: el denominador es la foto del día en que se
  calcula. Mensual. Sin ``stock_account`` devuelve None.
- TR-03 ``consumo_energia``: facturado del periodo por el proveedor de energía
  (facturas menos notas de crédito, sin impuestos) ÷ toneladas de hilo y
  fibra consumidas en órdenes. Pesos por tonelada procesada; antes era el
  total en pesos.

Los tres guardan numerador, denominador y registros en la medición
(``_sgi_measure_vals``), como los de I-1/I-3.
"""
from odoo import models

from .sgi_indicator_i3 import _param_ids

REWORK_TYPES_PARAM = 'quimibond_sgi.rework_picking_type_ids'


class SgiIndicatorP21(models.Model):
    _inherit = 'sgi.indicator'

    # ---- MA-04 -----------------------------------------------------------
    def _sgi_rework_picking_types(self):
        return self.env['stock.picking.type'].browse(
            _param_ids(self.env, REWORK_TYPES_PARAM)).exists()

    def _detail_reproceso(self, date_from, date_to):
        env = self.env
        kg = env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        types = self._sgi_rework_picking_types()
        if not kg or not types:
            return {'value': None}
        company = self._sgi_kpi_company()
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        moves = env['stock.move'].search([
            ('state', '=', 'done'), ('company_id', '=', company.id),
            ('production_id', '!=', False),
            ('production_id.picking_type_id', 'in', types.ids),
            ('product_uom', '=', kg.id),
            ('date', '>=', dt_from), ('date', '<', dt_to)])
        # Solo el producto terminado de la orden (los subproductos no son reproceso).
        moves = moves.filtered(lambda m: m.product_id == m.production_id.product_id)
        rework = sum(moves.mapped('quantity'))
        consumed = self._sgi_kg_consumed(dt_from, dt_to, company)
        return {
            'value': round(rework / consumed * 100.0, 2) if consumed else None,
            'numerator': rework, 'denominator': consumed,
            'model': 'mrp.production', 'ids': moves.production_id.ids,
        }

    def _calc_reproceso(self, date_from, date_to):
        return self._detail_reproceso(date_from, date_to)['value']

    def _note_reproceso(self, date_from, date_to):
        if not self._sgi_rework_picking_types():
            return ("Configure los tipos de operación de reproceso "
                    "(parámetro quimibond_sgi.rework_picking_type_ids).")
        return ''

    # ---- AL-01 -----------------------------------------------------------
    def _sgi_has_valuation(self):
        return 'value' in self.env['stock.move']._fields and 'value' in self.env['stock.quant']._fields

    def _detail_inventario_diferencia(self, date_from, date_to):
        env = self.env
        if not self._sgi_has_valuation():
            return {'value': None}
        company = self._sgi_kpi_company()
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        moves = env['stock.move'].sudo().search([
            ('state', '=', 'done'), ('company_id', '=', company.id),
            ('is_inventory', '=', True),
            ('date', '>=', dt_from), ('date', '<', dt_to)])
        adjusted = sum(abs(v) for v in moves.mapped('value'))
        quants = env['stock.quant'].sudo().search([
            ('company_id', '=', company.id), ('location_id.usage', '=', 'internal')])
        stock_value = sum(quants.mapped('value'))
        return {
            'value': round(adjusted / stock_value * 100.0, 2) if stock_value > 0 else None,
            'numerator': adjusted, 'denominator': stock_value,
            'model': 'stock.move', 'ids': moves.ids,
        }

    def _calc_inventario_diferencia(self, date_from, date_to):
        return self._detail_inventario_diferencia(date_from, date_to)['value']

    def _note_inventario_diferencia(self, date_from, date_to):
        if not self._sgi_has_valuation():
            return "Requiere la valuación de inventario (stock_account)."
        return ''

    # ---- TR-03 -----------------------------------------------------------
    def _detail_consumo_energia(self, date_from, date_to):
        partner = self._sgi_energy_partner()
        if not partner:
            return {'value': None}
        company = self._sgi_kpi_company()
        moves = self.env['account.move'].search([
            ('move_type', 'in', ('in_invoice', 'in_refund')),
            ('state', '=', 'posted'),
            ('company_id', '=', company.id),
            ('partner_id', 'child_of', partner.id),
            ('invoice_date', '>=', date_from), ('invoice_date', '<=', date_to)])
        total = 0.0
        for move in moves:
            total += move.amount_untaxed if move.move_type == 'in_invoice' \
                else -move.amount_untaxed
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        tonnes = self._sgi_kg_consumed(dt_from, dt_to, company) / 1000.0
        return {
            'value': round(total / tonnes, 2) if tonnes else None,
            'numerator': round(total, 2), 'denominator': tonnes,
            'model': 'account.move', 'ids': moves.ids,
        }

    def _calc_consumo_energia(self, date_from, date_to):
        return self._detail_consumo_energia(date_from, date_to)['value']
