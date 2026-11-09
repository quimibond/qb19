# -*- coding: utf-8 -*-
"""Demanda por cliente para Planeación (E1, diseño v3 sección 2.4).

Une, por cliente + artículo + semana, los **pedidos** confirmados pendientes de
entregar y el **pronóstico sin pedido** (``qb.forecast.line.qty_neta``), en la
unidad de venta, en metros, en kilos de venta y en **kilos a producir** (la
cuenta de la lista en metros de Fichas: metros ÷ rendimiento ÷ primera para
planear). Planeación la consulta sin poder editarla.

Es una tabla que se reconstruye (cron diario, al importar el forecast y con el
botón «Actualizar»), no una vista SQL: la conversión de unidades y la primera
para planear viven en Python (Fichas) y no se pueden repetir en SQL sin
duplicar la regla. Desviación consciente del diseño (sección 2.4 decía vista).

Cortes de Planeación (correo semanal «Progr. de Cantidades por Tejer»):
- **Acabado: 3 semanas** — la semana en curso y las dos siguientes, en metros;
- **Tejido: próximo mes** — de hoy a un mes, ventana móvil, en kilos.
"""
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

from .qb_forecast import DEV_PRODUCT_STATES, MARKETS, ZONES, dev_state_of, market_of_partner, monday_of

FINISHING_WEEKS = 3
# Pedidos atrasados que aún cuentan como demanda (más viejos son pedidos sin cerrar).
PAST_WEEKS = 26


class QbDemandLine(models.Model):
    _name = 'qb.demand.line'
    _description = "Demanda por cliente (Planeación)"
    _order = 'week, articulo_id, partner_id'
    _rec_name = 'articulo_id'

    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True, readonly=True)
    tipo = fields.Selection([('pedido', "Pedido"), ('pronostico', "Pronóstico sin pedido")],
                            string="Tipo", required=True, readonly=True)
    partner_id = fields.Many2one('res.partner', string="Cliente", index=True, readonly=True)
    plant = fields.Char(string="Planta", readonly=True)
    mercado = fields.Selection(MARKETS, string="Mercado", readonly=True)
    product_id = fields.Many2one('product.product', string="Producto", index=True, readonly=True)
    articulo_id = fields.Many2one('product.product', string="Artículo", index=True, readonly=True)
    week = fields.Date(string="Semana", index=True, readonly=True)
    month = fields.Date(string="Mes", readonly=True)
    zona = fields.Selection(ZONES, string="Zona", readonly=True)
    qty_venta = fields.Float(string="Cantidad (unidad de venta)", digits='Product Unit', readonly=True,
                             aggregator='sum')
    uom_venta = fields.Char(string="Unidad de venta", readonly=True)
    qty_m = fields.Float(string="Metros", digits=(16, 2), readonly=True, aggregator='sum')
    qty_kg = fields.Float(string="Kilos de venta", digits=(16, 2), readonly=True, aggregator='sum')
    qty_kg_producir = fields.Float(string="Kilos a producir", digits=(16, 2), readonly=True, aggregator='sum',
                                   help="Metros ÷ rendimiento de la ficha ÷ primera para planear: la misma cuenta "
                                        "de la lista en metros.")
    conv_ok = fields.Boolean(string="Convertida", readonly=True)
    corte_acabado = fields.Boolean(string="Corte de acabado (3 semanas)", readonly=True)
    corte_tejido = fields.Boolean(string="Corte de tejido (próximo mes)", readonly=True)
    atrasado = fields.Boolean(string="Pedido atrasado", readonly=True,
                              help="Pedido con compromiso antes de la semana en curso que no se ha entregado.")
    en_mps = fields.Boolean(string="Tiene programa en el MPS", readonly=True)
    sale_line_id = fields.Many2one('sale.order.line', string="Línea de pedido", readonly=True)
    order_id = fields.Many2one('sale.order', string="Pedido", readonly=True)
    forecast_line_id = fields.Many2one('qb.forecast.line', string="Renglón del pronóstico", readonly=True)
    customer_po = fields.Char(string="OC del cliente", readonly=True)
    dev_state = fields.Selection(DEV_PRODUCT_STATES, string="Estado del artículo", readonly=True,
                                 help="Estado de desarrollo del artículo. En desarrollo o archivado no se "
                                      "vende todavía; en pilotaje, sí, con aviso.")

    @api.model
    def _qb_cuts(self, week, today):
        current = monday_of(today)
        acabado = current <= week < current + timedelta(weeks=FINISHING_WEEKS)
        # Ventana móvil de un mes: la semana toca el intervalo [hoy, hoy + 1 mes].
        tejido = week <= today + relativedelta(months=1) and week + timedelta(days=6) >= today
        return acabado, tejido

    @api.model
    def _qb_row(self, company, tipo, partner, plant, product, week, qty_venta, today, mps_products,
                markets=None, **extra):
        acabado, tejido = self._qb_cuts(week, today) if week else (False, False)
        qty_m = product.qb_convertir(qty_venta, 'venta', 'm')
        qty_kg = product.qb_convertir(qty_venta, 'venta', 'kg')
        kg_producir = product.qb_kg_producir(qty_venta)
        articulo = product.qb_articulo_id or product
        vals = {
            'company_id': company.id, 'tipo': tipo, 'partner_id': partner.id or False, 'plant': plant or False,
            'mercado': market_of_partner(partner, markets) if partner else 'confeccion',
            'product_id': product.id, 'articulo_id': articulo.id, 'week': week,
            'month': week.replace(day=1) if week else False,
            'qty_venta': qty_venta, 'uom_venta': product.uom_id.name,
            'qty_m': qty_m or 0.0, 'qty_kg': qty_kg or 0.0, 'qty_kg_producir': kg_producir or 0.0,
            'conv_ok': qty_m is not None and qty_kg is not None,
            'corte_acabado': acabado, 'corte_tejido': tejido,
            'en_mps': product.id in mps_products or articulo.id in mps_products,
            'dev_state': dev_state_of(product),
        }
        vals.update(extra)
        return vals

    @api.model
    def action_qb_rebuild(self):
        """Reconstruye la demanda de todas las compañías con pronóstico o pedidos abiertos."""
        Forecast = self.env['qb.forecast.line'].sudo()
        today = fields.Date.context_today(self)
        current = monday_of(today)
        mps_products = set()
        if 'mrp.production.schedule' in self.env:
            mps_products = set(self.env['mrp.production.schedule'].sudo().search([]).product_id.ids)
        rows = []
        markets = {}
        oldest = current - timedelta(weeks=PAST_WEEKS)
        for company in self.env['res.company'].sudo().search([]):
            for sol in Forecast._qb_open_sale_lines(company):
                week = Forecast._qb_sol_week(sol)
                if week and week < oldest:
                    continue  # pedido viejo sin cerrar: no es demanda para planear
                pending = Forecast._qb_sol_pending(sol, sol.product_id) or 0.0
                partner = sol.order_id.partner_id.commercial_partner_id
                rows.append(self._qb_row(
                    company, 'pedido', partner, sol.order_id.partner_shipping_id != partner
                    and sol.order_id.partner_shipping_id.name or False, sol.product_id,
                    week, pending, today, mps_products, markets=markets,
                    zona=Forecast._qb_zone_for(week, today) if week else False,
                    atrasado=bool(week and week < current), sale_line_id=sol.id, order_id=sol.order_id.id,
                    customer_po=sol.order_id.client_order_ref or False))
            lines = Forecast.search([('company_id', '=', company.id), ('week', '>=', current),
                                     ('qty_neta', '>', 0)])
            for line in lines:
                rows.append(self._qb_row(
                    company, 'pronostico', line.partner_id, line.plant or line.ship_to_id.name,
                    line.product_id, line.week, line.qty_neta, today, mps_products,
                    zona=line.zona, mercado=line.mercado, forecast_line_id=line.id, customer_po=line.customer_po or False))
        self.sudo().search([]).unlink()
        if rows:
            self.sudo().create(rows)
        return True

    def action_qb_rebuild_button(self):
        """Botón «Actualizar» de la lista (3.14.2): el cliente manda los ids de la lista, que
        ``action_qb_rebuild`` (@api.model, la llaman crons y migraciones) no recibe."""
        self.env['qb.demand.line'].action_qb_rebuild()
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def action_qb_open(self):
        """Abre la Demanda por cliente (después de reconstruirla)."""
        self.action_qb_rebuild()
        return self.env['ir.actions.act_window']._for_xml_id(
            'quimibond_ventas_presupuesto.qb_demand_line_action')
