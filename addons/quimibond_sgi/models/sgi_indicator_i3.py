# -*- coding: utf-8 -*-
"""I-3 (2026-09-24): fórmulas corregidas de tres indicadores, con las
definiciones aprobadas en «Lógica de indicadores del SGI».

- MA-05 ``desperdicio_kg``: kg que entran a las ubicaciones de desperdicio
  (13 PP Desperdicio y 17 PT Desperdicio; desde órdenes y desde ajustes) ÷ kg
  de hilo y fibra consumidos en órdenes de producción, en 3 meses móviles.
  Solo líneas en kg. Antes contaba un subproducto y daba 0.03 %; real ≈ 16 %.
- EX-01 ``margen_ebitda``: (ingresos − costo de ventas − gastos de operación)
  ÷ ingresos, 12 meses móviles, por tipo de cuenta: ``income`` menos
  ``expense_direct_cost`` menos ``expense``. Fuera: depreciación
  (``expense_depreciation``), otros ingresos (``income_other``) y el resultado
  integral de financiamiento (``expense_other``, cuentas 701). Antes medía el
  margen de los pedidos (42.7 %) contra una meta de margen neto.
- EX-02 ``compras_mp_vs_ventas``: líneas de factura de proveedor con producto
  de la categoría Materia Prima (menos notas de crédito, moneda de la
  compañía) ÷ ingresos (cuentas ``income``), 3 meses móviles. Antes sumaba
  todas las facturas de proveedor, incluidas transferencias e IVA mal
  registrados (144 %); real ≈ 35 %.

Parámetros: ``quimibond_sgi.waste_location_ids`` (39,43),
``quimibond_sgi.waste_input_categ_ids`` (350 Fibra, 356 Hilo),
``quimibond_sgi.raw_material_categ_id`` (318 Materia Prima).
"""
from dateutil.relativedelta import relativedelta

from odoo import models

WASTE_LOCATIONS_PARAM = 'quimibond_sgi.waste_location_ids'
WASTE_INPUT_CATEGS_PARAM = 'quimibond_sgi.waste_input_categ_ids'
RAW_MATERIAL_CATEG_PARAM = 'quimibond_sgi.raw_material_categ_id'


def _param_ids(env, key):
    raw = env['ir.config_parameter'].sudo().get_param(key) or ''
    return [int(part) for part in raw.replace(';', ',').split(',') if part.strip().isdigit()]


class SgiIndicatorI3(models.Model):
    _inherit = 'sgi.indicator'

    # ---- MA-05 -----------------------------------------------------------
    def _detail_desperdicio_kg(self, date_from, date_to):
        env = self.env
        start = date_to - relativedelta(months=3) + relativedelta(days=1)
        dt_from, dt_to = self._sgi_dt_bounds(start, date_to)
        kg = env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        locations = env['stock.location'].browse(_param_ids(env, WASTE_LOCATIONS_PARAM)).exists()
        categs = env['product.category'].browse(_param_ids(env, WASTE_INPUT_CATEGS_PARAM)).exists()
        if not kg or not locations or not categs:
            return {'value': None}
        company = self._sgi_kpi_company()
        lines = env['stock.move.line'].search([
            ('state', '=', 'done'), ('company_id', '=', company.id),
            ('location_dest_id', 'child_of', locations.ids),
            ('location_id', 'not child_of', locations.ids),
            ('product_uom_id', '=', kg.id),
            ('date', '>=', dt_from), ('date', '<', dt_to)])
        waste = sum(lines.mapped('quantity'))
        consumed = env['stock.move']._read_group([
            ('state', '=', 'done'), ('company_id', '=', company.id),
            ('raw_material_production_id', '!=', False),
            ('product_id.categ_id', 'child_of', categs.ids),
            ('product_uom', '=', kg.id),
            ('date', '>=', dt_from), ('date', '<', dt_to)], [], ['quantity:sum'])
        consumed = consumed[0][0] if consumed else 0.0
        return {
            'value': round(waste / consumed * 100.0, 2) if consumed else None,
            'numerator': waste, 'denominator': consumed,
            'model': 'stock.move.line', 'ids': lines.ids,
        }

    def _calc_desperdicio_kg(self, date_from, date_to):
        return self._detail_desperdicio_kg(date_from, date_to)['value']

    # ---- EX-01 -----------------------------------------------------------
    def _sgi_balance_by_type(self, date_from, date_to, account_types):
        """Saldo (debe − haber) de las cuentas de esos tipos en el rango, de
        la compañía del KPI, asientos publicados."""
        groups = self.env['account.move.line']._read_group([
            ('company_id', '=', self._sgi_kpi_company().id),
            ('parent_state', '=', 'posted'),
            ('account_id.account_type', 'in', list(account_types)),
            ('date', '>=', date_from), ('date', '<=', date_to)], [], ['balance:sum'])
        return groups[0][0] if groups else 0.0

    def _detail_margen_ebitda(self, date_from, date_to):
        start = date_to - relativedelta(months=12) + relativedelta(days=1)
        income = -self._sgi_balance_by_type(start, date_to, ('income',))
        cogs = self._sgi_balance_by_type(start, date_to, ('expense_direct_cost',))
        opex = self._sgi_balance_by_type(start, date_to, ('expense',))
        ebitda = income - cogs - opex
        return {
            'value': round(ebitda / income * 100.0, 2) if income > 0 else None,
            'numerator': ebitda, 'denominator': income,
        }

    def _calc_margen_ebitda(self, date_from, date_to):
        return self._detail_margen_ebitda(date_from, date_to)['value']

    # ---- EX-02 -----------------------------------------------------------
    def _detail_compras_mp_vs_ventas(self, date_from, date_to):
        env = self.env
        start = date_to - relativedelta(months=3) + relativedelta(days=1)
        categ = env['product.category'].browse(
            _param_ids(env, RAW_MATERIAL_CATEG_PARAM)[:1]).exists()
        if not categ:
            return {'value': None}
        company = self._sgi_kpi_company()
        lines = env['account.move.line'].search([
            ('company_id', '=', company.id), ('parent_state', '=', 'posted'),
            ('move_id.move_type', 'in', ('in_invoice', 'in_refund')),
            ('display_type', '=', 'product'),
            ('product_id.categ_id', 'child_of', categ.ids),
            ('move_id.invoice_date', '>=', start), ('move_id.invoice_date', '<=', date_to)])
        purchases = sum(lines.mapped('balance'))   # facturas +, notas de crédito −
        income = -self._sgi_balance_by_type(start, date_to, ('income',))
        return {
            'value': round(purchases / income * 100.0, 2) if income > 0 else None,
            'numerator': purchases, 'denominator': income,
            'model': 'account.move', 'ids': lines.move_id.ids,
        }

    def _calc_compras_mp_vs_ventas(self, date_from, date_to):
        return self._detail_compras_mp_vs_ventas(date_from, date_to)['value']
