# -*- coding: utf-8 -*-
"""Proyeccion de flujo de efectivo (13 semanas).

Parte del saldo contable de efectivo (misma definicion que el reporte NIF
B-2) y proyecta, semana por semana:

* cobros: cuentas por cobrar abiertas por fecha de vencimiento, corregidas con
  el atraso promedio real de cada cliente (cobros conciliados de los ultimos
  12 meses), y pedidos de venta confirmados sin facturar;
* pagos: cuentas por pagar abiertas por vencimiento y ordenes de compra
  confirmadas sin factura;
* compromisos que Odoo no conoce (``cash.flow.forecast.item``): nomina,
  impuestos, prestamos, arrendamientos, rentas... con recurrencia. Un boton
  los siembra con promedios del historial clasificado por el metodo directo.

Todo en moneda de la compania (``amount_residual`` / ``balance``).
"""
import calendar
from collections import defaultdict
from datetime import date, timedelta

from odoo import _, api, fields, models

from .cash_flow_engine import add_months, month_end, month_start

FORECAST_ROWS = [
    # (key, label, kind)  kind: 'flow' | 'balance'
    ('opening', 'Saldo inicial de efectivo', 'balance'),
    ('r_overdue', 'Cobros vencidos (por cobrar, repartidos)', 'flow'),
    ('r_due', 'Cobros a clientes por vencimiento (con atraso histórico)', 'flow'),
    ('r_orders', 'Pedidos de venta confirmados sin facturar', 'flow'),
    ('p_overdue', 'Pagos vencidos a proveedores', 'flow'),
    ('p_due', 'Pagos a proveedores por vencimiento', 'flow'),
    ('p_orders', 'Órdenes de compra confirmadas sin factura', 'flow'),
    ('i_payroll', 'Nómina y cuotas (compromisos)', 'flow'),
    ('i_taxes', 'Impuestos SAT (compromisos)', 'flow'),
    ('i_loans', 'Préstamos (compromisos)', 'flow'),
    ('i_lease', 'Arrendamientos y rentas (compromisos)', 'flow'),
    ('i_interest', 'Intereses y comisiones (compromisos)', 'flow'),
    ('i_assets', 'Activo fijo (compromisos)', 'flow'),
    ('i_related', 'Partes relacionadas (compromisos)', 'flow'),
    ('i_other', 'Otros compromisos', 'flow'),
    ('net', 'Flujo neto de la semana', 'balance'),
    ('closing', 'Saldo final de efectivo', 'balance'),
]
FORECAST_ROW_LABELS = {key: label for key, label, _kind in FORECAST_ROWS}
FLOW_ROWS = [key for key, _label, kind in FORECAST_ROWS if kind == 'flow']

ITEM_CATEGORIES = [
    ('payroll', 'Nómina y cuotas'),
    ('taxes', 'Impuestos SAT'),
    ('loans', 'Préstamos'),
    ('lease', 'Arrendamientos y rentas'),
    ('interest', 'Intereses y comisiones'),
    ('assets', 'Activo fijo'),
    ('related', 'Partes relacionadas'),
    ('other', 'Otros'),
]

# Linea del metodo directo -> categoria de compromiso, para sembrar desde el historial.
HISTORY_TO_CATEGORY = {
    'd_payroll': 'payroll',
    'd_taxes': 'taxes',
    'd_loans_paid': 'loans',
    'd_lease': 'lease',
    'd_interest': 'interest',
    'd_bank_fees': 'interest',
    'd_related': 'related',
}


def clamp_day(year, month, day):
    return date(year, month, min(day, calendar.monthrange(year, month)[1]))


class CashFlowForecastItem(models.Model):
    _name = 'cash.flow.forecast.item'
    _description = 'Compromiso de la proyección de flujo de efectivo'
    _order = 'date_start, id'
    _check_company_auto = True

    config_id = fields.Many2one('cash.flow.config', required=True, ondelete='cascade', index=True)
    company_id = fields.Many2one(related='config_id.company_id', store=True, index=True)
    currency_id = fields.Many2one(related='company_id.currency_id')
    name = fields.Char(required=True, string='Concepto')
    category = fields.Selection(ITEM_CATEGORIES, required=True, default='other', string='Categoría')
    amount = fields.Monetary(
        required=True, string='Importe',
        help='Efecto en efectivo: negativo = salida (pago), positivo = entrada.')
    date_start = fields.Date(required=True, string='Primera fecha', default=fields.Date.context_today)
    date_end = fields.Date(string='Última fecha', help='Vacío = sin fin.')
    recurrence = fields.Selection([
        ('once', 'Una sola vez'),
        ('weekly', 'Semanal'),
        ('biweekly', 'Cada 14 días'),
        ('monthly', 'Mensual (mismo día)'),
    ], required=True, default='monthly')
    partner_id = fields.Many2one('res.partner', string='Contacto')
    auto = fields.Boolean(string='Sembrado del historial', readonly=True)
    active = fields.Boolean(default=True)
    note = fields.Char(string='Nota')

    def _occurrences(self, date_from, date_to):
        """Fechas e importes del compromiso entre ``date_from`` y ``date_to``."""
        self.ensure_one()
        result = []
        end = min(date_to, self.date_end) if self.date_end else date_to
        cur = self.date_start
        guard = 0
        while cur <= end and guard < 1000:
            guard += 1
            if cur >= date_from:
                result.append((cur, self.amount))
            if self.recurrence == 'once':
                break
            if self.recurrence == 'weekly':
                cur = cur + timedelta(days=7)
            elif self.recurrence == 'biweekly':
                cur = cur + timedelta(days=14)
            else:
                nxt = add_months(month_start(cur), 1)
                cur = clamp_day(nxt.year, nxt.month, self.date_start.day)
        return result


class CashFlowConfigForecast(models.Model):
    _inherit = 'cash.flow.config'

    forecast_item_ids = fields.One2many('cash.flow.forecast.item', 'config_id', string='Compromisos')
    forecast_weeks = fields.Integer(string='Semanas a proyectar', default=13)
    forecast_min_cash = fields.Monetary(
        string='Saldo mínimo de efectivo', currency_field='currency_id',
        help='Se resalta cualquier semana cuyo saldo final proyectado quede por debajo.')
    forecast_overdue_weeks = fields.Integer(
        string='Semanas para cobrar lo vencido', default=4,
        help='Los cobros ya vencidos se reparten por igual en las primeras N semanas.')
    forecast_include_orders = fields.Boolean(string='Incluir pedidos confirmados sin facturar', default=True)
    forecast_order_days = fields.Integer(
        string='Días de pedido a cobro/pago', default=30,
        help='Plazo que se suma a la fecha de entrega de un pedido sin facturar para estimar su cobro o pago.')
    forecast_max_delay = fields.Integer(string='Atraso máximo por cliente (días)', default=90)
    currency_id = fields.Many2one(related='company_id.currency_id')

    def compute_forecast(self, date_from=None):
        """Proyeccion serializable (JSON) a partir de ``date_from`` (hoy por default).
        Usable via ``/json/2/cash.flow.config/compute_forecast``."""
        self.ensure_one()
        date_from = fields.Date.to_date(date_from) if date_from else fields.Date.context_today(self)
        return self.env['cash.flow.forecast.engine'].to_summary(
            self.env['cash.flow.forecast.engine'].compute(self, date_from))

    def action_load_forecast_items_from_history(self):
        """Siembra compromisos mensuales con el promedio de los ultimos 3 meses
        completos del metodo directo (nomina, impuestos, prestamos,
        arrendamiento, intereses, partes relacionadas). Reemplaza los
        sembrados antes; los capturados a mano se conservan."""
        self.ensure_one()
        today = fields.Date.context_today(self)
        last_month_end = month_start(today) - timedelta(days=1)
        first = add_months(month_start(last_month_end), -2)
        result = self.env['cash.flow.engine'].compute(self, first, last_month_end)
        sliced = self.env['cash.flow.engine'].slice(result, first, last_month_end)
        self.forecast_item_ids.filtered('auto').unlink()
        values = []
        next_month = add_months(month_start(today), 1) if today.day > 17 else month_start(today)
        for line_key, category in HISTORY_TO_CATEGORY.items():
            monthly = sliced['lines'].get(line_key, 0.0) / 3.0
            if self.currency_id.is_zero(monthly):
                continue
            label = _('%s (promedio %s–%s)', dict(ITEM_CATEGORIES)[category],
                      fields.Date.to_string(first), fields.Date.to_string(last_month_end))
            if category == 'payroll':
                # Dos quincenas: 15 y fin de mes.
                for day in (15, 31):
                    values.append(self._forecast_item_vals(label, category, monthly / 2.0,
                                                           clamp_day(next_month.year, next_month.month, day)))
            elif category == 'taxes':
                values.append(self._forecast_item_vals(label, category, monthly,
                                                       clamp_day(next_month.year, next_month.month, 17)))
            else:
                values.append(self._forecast_item_vals(label, category, monthly, month_end(next_month)))
        items = self.env['cash.flow.forecast.item'].create(values)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {'title': _('Compromisos desde el historial'),
                       'message': _('Se sembraron %s compromisos mensuales.', len(items)),
                       'type': 'success', 'next': {'type': 'ir.actions.act_window_close'}},
        }

    def _forecast_item_vals(self, name, category, amount, first_date):
        return {
            'config_id': self.id, 'name': name, 'category': category, 'amount': amount,
            'date_start': first_date, 'recurrence': 'monthly', 'auto': True,
            'note': _('Promedio mensual del método directo; ajusta o captura el dato real.'),
        }

    def action_open_forecast_report(self):
        self.ensure_one()
        action = self.env['ir.actions.actions']._for_xml_id('quimibond_cash_flow.action_cash_flow_forecast_report')
        action['context'] = dict(self.env.context, report_id=self.env.ref('quimibond_cash_flow.cash_flow_forecast_report').id)
        return action


class CashFlowForecastEngine(models.AbstractModel):
    _name = 'cash.flow.forecast.engine'
    _description = 'Motor de la proyección de flujo de efectivo'

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------
    @api.model
    def compute(self, config, date_from):
        """Proyeccion semanal desde ``date_from``.

        Devuelve ``{'weeks': [(start, end)], 'rows': {row: {week_idx: amount}},
        'details': {row: {week_idx: [detalle]}}, 'opening_cash', 'closing':
        [por semana], 'min_cash', 'below_min': [idx]}``. Cada detalle es un
        dict con ``partner_id``, ``label``, ``amount``, ``model`` e ``ids``."""
        config.ensure_one()
        company = config.company_id
        n_weeks = max(config.forecast_weeks or 13, 1)
        weeks = [(date_from + timedelta(days=7 * i), date_from + timedelta(days=7 * i + 6)) for i in range(n_weeks)]
        horizon_end = weeks[-1][1]
        rows = {key: defaultdict(float) for key in FLOW_ROWS}
        details = {key: defaultdict(list) for key in FLOW_ROWS}

        def week_of(day):
            if day < date_from:
                return 0
            idx = (day - date_from).days // 7
            return idx if idx < n_weeks else None

        def add(row, idx, amount, partner_id, label, model, ids):
            rows[row][idx] += amount
            details[row][idx].append({'partner_id': partner_id, 'label': label, 'amount': amount, 'model': model, 'ids': ids})

        cash_ids = set(config.get_cash_account_ids())
        opening = self.env['cash.flow.engine']._query_cash_balance(company, date_from, cash_ids)

        # ---- cuentas por cobrar --------------------------------------
        delays = self._partner_delays(company, date_from, config.forecast_max_delay or 90)
        overdue_weeks = max(min(config.forecast_overdue_weeks or 1, n_weeks), 1)
        for line_id, partner_id, partner_name, due, residual in self._open_items(company, 'asset_receivable'):
            expected = due + timedelta(days=delays.get(partner_id, 0))
            if expected < date_from:
                share = residual / overdue_weeks
                for idx in range(overdue_weeks):
                    add('r_overdue', idx, share, partner_id, partner_name, 'account.move.line', [line_id])
                continue
            idx = week_of(expected)
            if idx is not None:
                add('r_due', idx, residual, partner_id, partner_name, 'account.move.line', [line_id])

        # ---- cuentas por pagar ---------------------------------------
        for line_id, partner_id, partner_name, due, residual in self._open_items(company, 'liability_payable'):
            if due < date_from:
                add('p_overdue', 0, residual, partner_id, partner_name, 'account.move.line', [line_id])
                continue
            idx = week_of(due)
            if idx is not None:
                add('p_due', idx, residual, partner_id, partner_name, 'account.move.line', [line_id])

        # ---- pedidos confirmados sin facturar ------------------------
        if config.forecast_include_orders:
            term = timedelta(days=config.forecast_order_days or 0)
            for order_id, partner_id, partner_name, when, amount in self._open_sale_orders(company):
                expected = max(when, date_from) + term + timedelta(days=delays.get(partner_id, 0))
                idx = week_of(expected)
                if idx is not None:
                    add('r_orders', idx, amount, partner_id, partner_name, 'sale.order', [order_id])
            for order_id, partner_id, partner_name, when, amount in self._open_purchase_orders(company):
                expected = max(when, date_from) + term
                idx = week_of(expected)
                if idx is not None:
                    add('p_orders', idx, -amount, partner_id, partner_name, 'purchase.order', [order_id])

        # ---- compromisos ---------------------------------------------
        for item in config.forecast_item_ids:
            row = 'i_' + item.category
            for when, amount in item._occurrences(date_from, horizon_end):
                idx = week_of(when)
                if idx is not None:
                    add(row, idx, amount, item.partner_id.id, item.name, 'cash.flow.forecast.item', [item.id])

        # ---- saldos --------------------------------------------------
        net, closing, opening_per_week = [], [], []
        balance = opening
        for idx in range(n_weeks):
            opening_per_week.append(balance)
            week_net = sum(rows[key].get(idx, 0.0) for key in FLOW_ROWS)
            balance += week_net
            net.append(week_net)
            closing.append(balance)
        min_cash = config.forecast_min_cash or 0.0
        return {
            'company_id': company.id,
            'date_from': date_from,
            'weeks': weeks,
            'rows': {key: dict(vals) for key, vals in rows.items()},
            'details': {key: dict(vals) for key, vals in details.items()},
            'opening_cash': opening,
            'opening': opening_per_week,
            'net': net,
            'closing': closing,
            'min_cash': min_cash,
            'below_min': [idx for idx, value in enumerate(closing) if config.forecast_min_cash and value < min_cash],
            'delays': delays,
        }

    @api.model
    def to_summary(self, result):
        currency = self.env['res.company'].browse(result['company_id']).currency_id

        def r(value):
            return currency.round(value)

        return {
            'company_id': result['company_id'],
            'date_from': fields.Date.to_string(result['date_from']),
            'weeks': [{'start': fields.Date.to_string(s), 'end': fields.Date.to_string(e)} for s, e in result['weeks']],
            'opening_cash': r(result['opening_cash']),
            'rows': {key: [r(result['rows'][key].get(i, 0.0)) for i in range(len(result['weeks']))] for key in FLOW_ROWS},
            'row_labels': dict(FORECAST_ROW_LABELS),
            'net': [r(v) for v in result['net']],
            'closing': [r(v) for v in result['closing']],
            'min_cash': r(result['min_cash']),
            'below_min': result['below_min'],
        }

    # ------------------------------------------------------------------
    # Fuentes
    # ------------------------------------------------------------------
    def _open_items(self, company, account_type):
        """Apuntes abiertos (no conciliados) de cuentas del tipo dado:
        ``(id, partner_id, partner_name, vencimiento, efecto en efectivo)``.
        ``amount_residual`` ya trae el signo del efecto en efectivo: positivo
        en cuentas por cobrar (entra), negativo en cuentas por pagar (sale)."""
        self.env['account.move.line'].flush_model(['amount_residual', 'reconciled', 'date_maturity', 'partner_id', 'parent_state'])
        self.env.cr.execute("""
            SELECT aml.id, aml.partner_id, COALESCE(p.name, ''), COALESCE(aml.date_maturity, aml.date), aml.amount_residual
              FROM account_move_line aml
              JOIN account_account acc ON acc.id = aml.account_id
              LEFT JOIN res_partner p ON p.id = aml.partner_id
             WHERE aml.company_id = %s AND aml.parent_state = 'posted'
               AND acc.account_type = %s AND aml.reconciled = FALSE AND aml.amount_residual != 0
        """, (company.id, account_type))
        return [(row[0], row[1], row[2], row[3], float(row[4])) for row in self.env.cr.fetchall()]

    def _partner_delays(self, company, date_from, max_delay):
        """Atraso promedio (dias, ponderado por importe) entre vencimiento y
        cobro de cada cliente en los ultimos 12 meses, acotado a [0, max]."""
        self.env['account.partial.reconcile'].flush_model()
        self.env.cr.execute("""
            SELECT d.partner_id,
                   SUM(apr.amount * (c.date - COALESCE(d.date_maturity, d.date))) / NULLIF(SUM(apr.amount), 0)
              FROM account_partial_reconcile apr
              JOIN account_move_line d ON d.id = apr.debit_move_id
              JOIN account_move_line c ON c.id = apr.credit_move_id
              JOIN account_account acc ON acc.id = d.account_id
             WHERE d.company_id = %s AND acc.account_type = 'asset_receivable'
               AND d.partner_id IS NOT NULL AND c.date >= %s AND c.date <= %s
               AND d.move_id != c.move_id
             GROUP BY d.partner_id
        """, (company.id, date_from - timedelta(days=365), date_from))
        delays = {}
        for partner_id, avg in self.env.cr.fetchall():
            if avg is None:
                continue
            delays[partner_id] = int(max(0, min(round(float(avg)), max_delay)))
        return delays

    def _open_sale_orders(self, company):
        """Pedidos de venta confirmados con importe pendiente de facturar:
        ``(id, partner_id, nombre, fecha de entrega, importe con impuestos)``."""
        if 'sale.order' not in self.env:
            return []
        Order = self.env['sale.order'].sudo()
        orders = Order.search([('company_id', '=', company.id), ('state', '=', 'sale'),
                               ('invoice_status', 'in', ('to invoice', 'no'))])
        result = []
        for order in orders:
            pending = 0.0
            for line in order.order_line:
                if line.display_type or not line.product_uom_qty:
                    continue
                remaining = line.product_uom_qty - line.qty_invoiced
                if remaining <= 0:
                    continue
                pending += line.price_total * remaining / line.product_uom_qty
            if company.currency_id.is_zero(pending):
                continue
            when = order.commitment_date or order.expected_date or order.date_order
            when = fields.Date.to_date(when) if when else fields.Date.context_today(self)
            result.append((order.id, order.partner_id.commercial_partner_id.id, order.partner_id.display_name, when, pending))
        return result

    def _open_purchase_orders(self, company):
        """Ordenes de compra confirmadas con importe pendiente de facturar."""
        if 'purchase.order' not in self.env:
            return []
        Order = self.env['purchase.order'].sudo()
        orders = Order.search([('company_id', '=', company.id), ('state', 'in', ('purchase', 'done')),
                               ('invoice_status', 'in', ('to invoice', 'no'))])
        result = []
        for order in orders:
            pending = 0.0
            when = None
            for line in order.order_line:
                if line.display_type or not line.product_qty:
                    continue
                remaining = line.product_qty - line.qty_invoiced
                if remaining <= 0:
                    continue
                pending += line.price_total * remaining / line.product_qty
                planned = fields.Date.to_date(line.date_planned) if line.date_planned else None
                if planned and (when is None or planned < when):
                    when = planned
            if company.currency_id.is_zero(pending):
                continue
            when = when or fields.Date.to_date(order.date_planned or order.date_order) or fields.Date.context_today(self)
            result.append((order.id, order.partner_id.commercial_partner_id.id, order.partner_id.display_name, when, pending))
        return result
