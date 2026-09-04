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
from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import median

from odoo import _, api, fields, models

from .cash_flow_engine import add_months, month_start

FORECAST_ROWS = [
    # (key, label, kind)  kind: 'flow' | 'balance' | 'info' (se muestra, no suma)
    ('opening', 'Saldo inicial de efectivo', 'balance'),
    ('r_overdue', 'Cobros vencidos (repartidos en las primeras semanas)', 'flow'),
    ('r_due', 'Cobros a clientes por vencimiento (con atraso histórico)', 'flow'),
    ('r_orders', 'Pedidos de venta confirmados sin facturar', 'flow'),
    ('p_overdue', 'Pagos vencidos a proveedores (repartidos en las primeras semanas)', 'flow'),
    ('p_due', 'Pagos a proveedores por vencimiento', 'flow'),
    ('p_orders', 'Órdenes de compra confirmadas sin factura', 'flow'),
    ('r_runrate', 'Cobros estimados de ventas por facturar (ritmo histórico)', 'flow'),
    ('p_runrate', 'Pagos estimados de compras por facturar (ritmo histórico)', 'flow'),
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
    ('r_stale', 'Por cobrar con antigüedad excesiva (excluido de la proyección)', 'info'),
    ('p_stale', 'Por pagar con antigüedad excesiva (excluido de la proyección)', 'info'),
]
FORECAST_ROW_LABELS = {key: label for key, label, _kind in FORECAST_ROWS}
FLOW_ROWS = [key for key, _label, kind in FORECAST_ROWS if kind == 'flow']
INFO_ROWS = [key for key, _label, kind in FORECAST_ROWS if kind == 'info']

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
    forecast_include_orders = fields.Boolean(string='Incluir pedidos confirmados sin facturar', default=False)
    forecast_stale_days = fields.Integer(
        string='Antigüedad máxima (días)', default=180,
        help='Cuentas por cobrar/pagar vencidas hace más de estos días, y pedidos más viejos, se excluyen de la '
             'proyección y se muestran en un renglón informativo.')
    forecast_min_item_amount = fields.Monetary(
        string='Importe mínimo de compromiso', currency_field='currency_id', default=5000.0,
        help='Las series del historial cuyo importe mensual queda por debajo no se siembran.')
    forecast_order_days = fields.Integer(
        string='Días de pedido a cobro/pago', default=30,
        help='Plazo que se suma a la fecha de entrega de un pedido sin facturar para estimar su cobro o pago.')
    forecast_max_delay = fields.Integer(string='Atraso máximo por cliente (días)', default=90)
    forecast_include_runrate = fields.Boolean(
        string='Complementar con ritmo histórico', default=True,
        help='A partir de la semana en que se agota el periodo real de cobro (DSO) o de pago (DPO), se estima cada '
             'semana con el promedio semanal de cobros a clientes y pagos a proveedores de los últimos meses, menos lo '
             'ya conocido por facturas abiertas.')
    forecast_history_months = fields.Integer(
        string='Meses de historial para compromisos', default=3,
        help='Meses completos que se analizan para detectar la periodicidad de nómina, impuestos, préstamos, '
             'arrendamientos e intereses.')
    currency_id = fields.Many2one(related='company_id.currency_id')

    def compute_forecast(self, date_from=None):
        """Proyeccion serializable (JSON) a partir de ``date_from`` (hoy por default).
        Usable via ``/json/2/cash.flow.config/compute_forecast``."""
        self.ensure_one()
        date_from = fields.Date.to_date(date_from) if date_from else fields.Date.context_today(self)
        return self.env['cash.flow.forecast.engine'].to_summary(
            self.env['cash.flow.forecast.engine'].compute(self, date_from))

    def action_load_forecast_items_from_history(self):
        """Siembra los compromisos detectando la periodicidad real de los pagos
        del historial (metodo directo, dia por dia): series semanales,
        quincenales y mensuales por categoria y contacto, con su importe
        mediano y su siguiente fecha. Reemplaza los sembrados antes; los
        capturados a mano se conservan."""
        self.ensure_one()
        items, window = self._learn_forecast_items()
        self.forecast_item_ids.filtered('auto').unlink()
        created = self.env['cash.flow.forecast.item'].create(items)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {'title': _('Compromisos desde el historial'),
                       'message': _('Se detectaron %(n)s compromisos analizando %(from)s – %(to)s.',
                                    n=len(created), **{'from': fields.Date.to_string(window[0]), 'to': fields.Date.to_string(window[1])}),
                       'type': 'success', 'next': {'type': 'ir.actions.act_window_close'}},
        }

    # ------------------------------------------------------------------
    # Deteccion de periodicidad
    # ------------------------------------------------------------------
    def _learn_forecast_items(self):
        """Devuelve ``(valores de cash.flow.forecast.item, (inicio, fin) de la ventana)``."""
        self.ensure_one()
        today = fields.Date.context_today(self)
        n_months = max(self.forecast_history_months or 3, 1)
        window_end = month_start(today) - timedelta(days=1)
        window_start = add_months(month_start(window_end), -(n_months - 1))
        daily = self.env['cash.flow.engine'].direct_daily(self, window_start, window_end)

        # Series por (categoria, contacto). La nomina se agrupa sin contacto:
        # sus polizas traen contactos distintos o ninguno.
        series = defaultdict(lambda: defaultdict(float))
        for day, line_key, _account_id, partner_id, amount in daily:
            category = HISTORY_TO_CATEGORY.get(line_key)
            if not category:
                continue
            partner_key = False if category == 'payroll' else (partner_id or False)
            series[(category, partner_key)][day] += amount

        values = []
        for (category, partner_id), by_day in sorted(series.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0)):
            points = sorted((day, -amount) for day, amount in by_day.items()
                            if amount < 0 and not self.currency_id.is_zero(amount))
            if not points:
                continue
            values += self._detect_patterns(category, partner_id, points, n_months, today, (window_start, window_end))
        return values, (window_start, window_end)

    def _detect_patterns(self, category, partner_id, points, n_months, today, window):
        """Descompone los pagos ``points`` = [(fecha, importe positivo)] de una
        serie en: una serie semanal (si la hay), series mensuales por ranura
        de dia del mes que se repite en la mayoria de los meses, y un resto
        irregular (promedio mensual). Devuelve valores de compromisos."""
        currency = self.currency_id
        label = dict(ITEM_CATEGORIES)[category]
        partner = self.env['res.partner'].browse(partner_id) if partner_id else self.env['res.partner']
        prefix = '%s · %s' % (label, partner.display_name) if partner else label
        values = []
        remaining = dict(points)

        # ---- serie semanal --------------------------------------------
        # Un dia de la semana con pago en la gran mayoria de las semanas de
        # la ventana es una serie semanal, aunque haya otros pagos en medio.
        n_weeks = max((window[1] - window[0]).days // 7, 1)
        weekday_counts = Counter(d.weekday() for d, _a in points)
        if weekday_counts:
            weekday, count = weekday_counts.most_common(1)[0]
            if count >= 0.75 * n_weeks and count >= 3:
                on_weekday = [a for d, a in points if d.weekday() == weekday]
                weekly_amount = median(on_weekday)
                if weekly_amount * 4 >= (self.forecast_min_item_amount or 0.0):
                    values.append(self._pattern_item_vals(
                        category, partner_id, '%s · semanal (%s)' % (prefix, self._weekday_name(weekday)),
                        -weekly_amount, self._next_weekday(today, weekday), 'weekly',
                        [(d, a) for d, a in points if d.weekday() == weekday]))
                    for d in list(remaining):
                        if d.weekday() == weekday:
                            rest = remaining[d] - weekly_amount
                            if rest > 0.25 * weekly_amount:
                                remaining[d] = rest
                            else:
                                del remaining[d]

        # ---- series mensuales por grupo de dias cercanos ---------------
        # Los pagos se agrupan por dia del mes: dias a 4 o menos de distancia
        # forman un grupo (28-31 cuentan como fin de mes). Un grupo presente
        # en la mayoria de los meses es una serie mensual con el total
        # mensual mediano del grupo (asi dos rentas fijas pagadas en dias
        # distintos del mismo mes quedan en un solo renglon con su total).
        needed_months = max(2, (2 * n_months + 2) // 3) if n_months > 1 else 1
        clusters = []                       # [[(fecha, importe), ...]]
        last_day = None
        for d, a in sorted(remaining.items(), key=lambda kv: (min(kv[0].day, 28), kv[0])):
            day = min(d.day, 28)
            if clusters and day - last_day <= 4 and day - min(clusters[-1][0][0].day, 28) <= 8:
                clusters[-1].append((d, a))
            else:
                clusters.append([(d, a)])
            last_day = day
        leftover = []
        min_amount = self.forecast_min_item_amount or 0.0
        for entries in clusters:
            per_month = defaultdict(float)
            for d, a in entries:
                per_month[(d.year, d.month)] += a
            if len(per_month) >= needed_months:
                amount = median(per_month.values())
                if amount < min_amount:
                    continue
                days = [d.day for d, _a in entries]
                typical_day = 31 if max(days) >= 28 and median(days) >= 26 else int(median(days))
                values.append(self._pattern_item_vals(
                    category, partner_id, '%s · mensual (día %s)' % (prefix, 'último' if typical_day == 31 else typical_day),
                    -amount, self._next_month_day(today, typical_day), 'monthly', entries))
            else:
                leftover += entries

        # ---- resto irregular: promedio mensual, marcado ----------------
        # Un pago aislado no se convierte en compromiso: no hay evidencia de
        # que se repita. Solo lo irregular con al menos dos ocurrencias.
        leftover_total = sum(a for _d, a in leftover)
        if len(leftover) >= 2 and leftover_total / n_months >= max(min_amount, currency.rounding):
            values.append(self._pattern_item_vals(
                category, partner_id, '%s · irregular (promedio mensual, revisar)' % prefix,
                -leftover_total / n_months, self._next_month_day(today, 31), 'monthly', leftover))
        return values

    def _pattern_item_vals(self, category, partner_id, name, amount, first_date, recurrence, observed):
        seen = '; '.join('%s %s' % (d.strftime('%d/%m'), '{:,.2f}'.format(a)) for d, a in sorted(observed)[-8:])
        return {
            'config_id': self.id, 'name': name, 'category': category, 'partner_id': partner_id or False,
            'amount': self.currency_id.round(amount), 'date_start': first_date, 'recurrence': recurrence,
            'auto': True, 'note': _('Detectado en el historial: %s', seen),
        }

    @staticmethod
    def _weekday_name(weekday):
        return ['lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo'][weekday]

    @staticmethod
    def _next_weekday(today, weekday):
        return today + timedelta(days=(weekday - today.weekday()) % 7)

    @staticmethod
    def _next_month_day(today, day):
        candidate = clamp_day(today.year, today.month, day)
        if candidate < today:
            nxt = add_months(month_start(today), 1)
            candidate = clamp_day(nxt.year, nxt.month, day)
        return candidate

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
        for key in INFO_ROWS:
            rows[key] = defaultdict(float)
            details[key] = defaultdict(list)
        stale_before = date_from - timedelta(days=max(config.forecast_stale_days or 0, 0)) if config.forecast_stale_days else None

        def spread_overdue(row, residual, partner_id, partner_name, model, ids):
            share = residual / overdue_weeks
            for idx in range(overdue_weeks):
                add(row, idx, share, partner_id, partner_name, model, ids)

        # ---- cuentas por cobrar --------------------------------------
        delays = self._partner_delays(company, date_from, config.forecast_max_delay or 90)
        overdue_weeks = max(min(config.forecast_overdue_weeks or 1, n_weeks), 1)
        for line_id, partner_id, partner_name, due, residual in self._open_items(company, 'asset_receivable'):
            if stale_before and due < stale_before:
                add('r_stale', 0, residual, partner_id, partner_name, 'account.move.line', [line_id])
                continue
            expected = due + timedelta(days=delays.get(partner_id, 0))
            if expected < date_from:
                spread_overdue('r_overdue', residual, partner_id, partner_name, 'account.move.line', [line_id])
                continue
            idx = week_of(expected)
            if idx is not None:
                add('r_due', idx, residual, partner_id, partner_name, 'account.move.line', [line_id])

        # ---- cuentas por pagar ---------------------------------------
        for line_id, partner_id, partner_name, due, residual in self._open_items(company, 'liability_payable'):
            if stale_before and due < stale_before:
                add('p_stale', 0, residual, partner_id, partner_name, 'account.move.line', [line_id])
                continue
            if due < date_from:
                spread_overdue('p_overdue', residual, partner_id, partner_name, 'account.move.line', [line_id])
                continue
            idx = week_of(due)
            if idx is not None:
                add('p_due', idx, residual, partner_id, partner_name, 'account.move.line', [line_id])

        # ---- pedidos confirmados sin facturar ------------------------
        if config.forecast_include_orders:
            term = timedelta(days=config.forecast_order_days or 0)
            for order_id, partner_id, partner_name, when, amount in self._open_sale_orders(company):
                if stale_before and when < stale_before:
                    continue
                expected = max(when, date_from) + term + timedelta(days=delays.get(partner_id, 0))
                idx = week_of(expected)
                if idx is not None:
                    add('r_orders', idx, amount, partner_id, partner_name, 'sale.order', [order_id])
            for order_id, partner_id, partner_name, when, amount in self._open_purchase_orders(company):
                if stale_before and when < stale_before:
                    continue
                expected = max(when, date_from) + term
                idx = week_of(expected)
                if idx is not None:
                    add('p_orders', idx, -amount, partner_id, partner_name, 'purchase.order', [order_id])

        # ---- ritmo historico: ventas y compras por facturar ------------
        if config.forecast_include_runrate:
            runrate = self._runrate(config, date_from, n_weeks)
            for idx in range(n_weeks):
                if idx >= runrate['dso_weeks']:
                    known = sum(rows[k].get(idx, 0.0) for k in ('r_due', 'r_overdue', 'r_orders'))
                    extra = runrate['weekly_in'] - known
                    if extra > 0 and not company.currency_id.is_zero(extra):
                        add('r_runrate', idx, extra, None, runrate['label_in'], 'cash.flow.forecast.item', [])
                if idx >= runrate['dpo_weeks']:
                    known = sum(rows[k].get(idx, 0.0) for k in ('p_due', 'p_overdue', 'p_orders'))
                    extra = runrate['weekly_out'] - known
                    if extra < 0 and not company.currency_id.is_zero(extra):
                        add('p_runrate', idx, extra, None, runrate['label_out'], 'cash.flow.forecast.item', [])

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
            'rows': {key: [r(result['rows'][key].get(i, 0.0)) for i in range(len(result['weeks']))] for key in FLOW_ROWS + INFO_ROWS},
            'row_labels': dict(FORECAST_ROW_LABELS),
            'net': [r(v) for v in result['net']],
            'closing': [r(v) for v in result['closing']],
            'min_cash': r(result['min_cash']),
            'below_min': result['below_min'],
        }

    # ------------------------------------------------------------------
    # Ritmo historico
    # ------------------------------------------------------------------
    def _runrate(self, config, date_from, n_weeks):
        """Promedio semanal de cobros a clientes y pagos a proveedores del
        metodo directo en los ultimos ``forecast_history_months`` meses
        completos, y semanas de DSO/DPO (dias entre factura y cobro/pago,
        ponderados por importe, ultimos 12 meses)."""
        company = config.company_id
        n_months = max(config.forecast_history_months or 3, 1)
        window_end = month_start(date_from) - timedelta(days=1)
        window_start = add_months(month_start(window_end), -(n_months - 1))
        weeks_in_window = max((window_end - window_start).days / 7.0, 1.0)
        total_in = total_out = 0.0
        for _day, line_key, _account_id, _partner_id, amount in self.env['cash.flow.engine'].direct_daily(config, window_start, window_end):
            if line_key == 'd_customers':
                total_in += amount
            elif line_key == 'd_suppliers':
                total_out += amount
        dso_days, dpo_days = self._dso_dpo(company, date_from)
        label = _('Promedio semanal %s – %s', fields.Date.to_string(window_start), fields.Date.to_string(window_end))
        return {
            'weekly_in': max(total_in / weeks_in_window, 0.0),
            'weekly_out': min(total_out / weeks_in_window, 0.0),
            'dso_weeks': min(max(int(round(dso_days / 7.0)), 0), n_weeks),
            'dpo_weeks': min(max(int(round(dpo_days / 7.0)), 0), n_weeks),
            'label_in': label + _(' · DSO %s días', int(dso_days)),
            'label_out': label + _(' · DPO %s días', int(dpo_days)),
        }

    def _dso_dpo(self, company, date_from):
        """Dias promedio (ponderados por importe) entre la fecha de la factura
        y la del cobro/pago conciliado, ultimos 12 meses: (DSO, DPO)."""
        self.env['account.partial.reconcile'].flush_model()
        result = {}
        for account_type, invoice_side in (('asset_receivable', 'debit'), ('liability_payable', 'credit')):
            inv, pay = ('d', 'c') if invoice_side == 'debit' else ('c', 'd')
            self.env.cr.execute("""
                SELECT SUM(apr.amount * ({pay}.date - {inv}.date)) / NULLIF(SUM(apr.amount), 0)
                  FROM account_partial_reconcile apr
                  JOIN account_move_line d ON d.id = apr.debit_move_id
                  JOIN account_move_line c ON c.id = apr.credit_move_id
                  JOIN account_account acc ON acc.id = {inv}.account_id
                  JOIN account_move im ON im.id = {inv}.move_id
                 WHERE {inv}.company_id = %s AND acc.account_type = %s
                   AND im.move_type != 'entry'
                   AND {pay}.date >= %s AND {pay}.date <= %s AND {pay}.date >= {inv}.date
            """.format(inv=inv, pay=pay), (company.id, account_type, date_from - timedelta(days=365), date_from))
            value = self.env.cr.fetchone()[0]
            result[account_type] = float(value) if value is not None else 0.0
        return result['asset_receivable'], result['liability_payable']

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
