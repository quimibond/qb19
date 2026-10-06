# -*- coding: utf-8 -*-
"""Campos que faltaban para que los indicadores capturados a mano pasen a
fórmula configurable (55.0.0, 2026-09-25); guardados en la base para poder
filtrarlos desde un término (``sgi.indicator.term``). A-022 (auditoría
2026-09): antes vivían todos en ``sgi_kpi_fields.py``; ahora un archivo por
tema (``sgi_kpi_sales``, ``sgi_kpi_quality``, ``sgi_kpi_account``,
``sgi_kpi_review``, ``sgi_kpi_hr``). Mover código entre archivos no toca la
base.

Ventas y logística:

- C1-04 ``product.template.sgi_first_sale_date``: fecha del primer pedido
  confirmado del producto (se fija al confirmar el pedido).
- C2-05 ``stock.picking.sgi_export_crossing_datetime`` y
  ``sgi_export_file_closed_date``: cruce y cierre de expediente de la entrega
  de exportación (los captura Logística).
- C5-03 ``stock.picking.sgi_quality_approved_at`` / ``sgi_release_hours``: hora
  de la última aprobación de Calidad del traslado y horas desde que Inspección
  lo entregó (creación del traslado).
"""
from odoo import api, fields, models


# ---- C1-04 ----------------------------------------------------------------
class ProductTemplateFirstSale(models.Model):
    _inherit = 'product.template'

    sgi_first_sale_date = fields.Date(
        string="Primer pedido confirmado", readonly=True, copy=False, index=True,
        help="Fecha del primer pedido de venta confirmado con este producto (C1-04).")


class SaleOrderFirstSale(models.Model):
    _inherit = 'sale.order'

    def action_confirm(self):
        res = super().action_confirm()
        for order in self:
            day = fields.Date.context_today(order, order.date_order) if order.date_order \
                else fields.Date.context_today(order)
            templates = order.order_line.product_id.product_tmpl_id.sudo().filtered(
                lambda t: not t.sgi_first_sale_date or t.sgi_first_sale_date > day)
            templates.write({'sgi_first_sale_date': day})
        return res


# ---- C2-05 / C5-03 --------------------------------------------------------
class StockPickingKpi(models.Model):
    _inherit = 'stock.picking'

    sgi_export_crossing_datetime = fields.Datetime(
        string="Cruce (exportación)", copy=False,
        help="Fecha y hora en que la mercancía cruzó la frontera (C2-05).")
    sgi_export_file_closed_date = fields.Date(
        string="Expediente cerrado", copy=False,
        help="Fecha en que quedó completo el expediente de exportación (C2-05).")
    sgi_quality_approved_at = fields.Datetime(
        string="Aprobado por Calidad", compute='_compute_sgi_quality_approved', store=True,
        help="Última aprobación de un control de calidad de este traslado (C5-03).")
    sgi_release_hours = fields.Float(
        string="Horas hasta la aprobación", compute='_compute_sgi_quality_approved', store=True,
        digits=(16, 2),
        help="Horas entre la entrega de Inspección (creación del traslado) y la "
             "aprobación de Calidad (C5-03).")

    @api.depends('check_ids.quality_state', 'check_ids.control_date', 'create_date')
    def _compute_sgi_quality_approved(self):
        for picking in self:
            passed = picking.check_ids.filtered(lambda c: c.quality_state == 'pass' and c.control_date)
            approved = max(passed.mapped('control_date')) if passed else False
            picking.sgi_quality_approved_at = approved
            if approved and picking.create_date:
                picking.sgi_release_hours = round(
                    max((approved - picking.create_date).total_seconds(), 0.0) / 3600.0, 2)
            else:
                picking.sgi_release_hours = 0.0
