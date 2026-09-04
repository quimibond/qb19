# -*- coding: utf-8 -*-
"""Motor de calculo del Estado de flujo de efectivo NIF B-2.

Independiente del motor de reportes Enterprise: recibe una configuracion
(``cash.flow.config``) y un rango de fechas y devuelve los importes de cada
linea por mes. El handler del reporte, el snapshot y la API JSON-2 lo usan
tal cual.

Fundamento (por que ambos metodos cuadran siempre)
--------------------------------------------------
Toda poliza registrada suma cero, asi que para cualquier periodo:

    variacion de efectivo = -(suma de saldos de las lineas que NO son efectivo)

* **Metodo indirecto**: se reparte *cada* apunte que no es efectivo (de todas
  las polizas registradas del periodo, sin las de cierre) en una linea segun
  las reglas. Las cuentas de resultados caen en "Resultado". Las partidas
  virtuales (depreciacion, resultado en venta de activo, intereses, ...) se
  presentan con signo de "se suma de vuelta" y su efecto real se manda a una
  linea espejo, de modo que la suma no cambia.
* **Metodo directo**: se reparte cada apunte que no es efectivo *de las
  polizas que tocan efectivo*. Como las polizas que no tocan efectivo no
  mueven efectivo, el total es identico al del metodo indirecto. Cuando la
  contraparte es una cuenta por cobrar/pagar conciliada con una factura, la
  parte conciliada se reclasifica por la *cuenta dominante* de esa factura
  (su linea de producto mas grande, sin impuestos): asi un pago de una
  factura de maquinaria es "activo fijo comprado" y el cobro de la venta de
  una maquina es "activo fijo vendido", sin descomponer la factura en todas
  sus lineas. El total no cambia: lo que se quita de la cuenta por pagar se
  pone en la cuenta dominante.

Ninguna de las dos particiones descarta apuntes: lo que no cae en una regla
va a "Sin clasificar" / "Otros (revisar)".

Todo se calcula con una sola consulta SQL agrupada (mes, cuenta, diario,
tipo de asiento, contacto de regla, toca-efectivo, lado) y se clasifica en
Python sobre esos grupos, nunca apunte por apunte.
"""
import calendar
from collections import defaultdict
from datetime import date, timedelta

from odoo import api, fields, models
from odoo.tools import float_round

from . import cash_flow_lines as L


def month_start(d):
    return d.replace(day=1)


def month_end(d):
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def add_months(d, n):
    y, m = divmod(d.month - 1 + n, 12)
    return date(d.year + y, m + 1, 1)


def iter_months(date_from, date_to):
    """Primer dia de cada mes entre ``date_from`` y ``date_to``."""
    cur = month_start(date_from)
    while cur <= date_to:
        yield cur
        cur = add_months(cur, 1)


class CashFlowEngine(models.AbstractModel):
    _name = 'cash.flow.engine'
    _description = 'Motor del flujo de efectivo NIF B-2'

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------
    @api.model
    def compute(self, config, date_from, date_to):
        """Calcula el flujo de efectivo de ``config.company_id`` entre
        ``date_from`` y ``date_to`` (inclusive).

        Devuelve un dict:

        * ``months``: lista de primeros de mes cubiertos, en orden;
        * ``lines``: ``{line_key: {month: importe}}`` (efecto en efectivo);
        * ``accounts``: ``{line_key: {account_id: {month: importe}}}``;
        * ``account_info``: ``{account_id: (code, type, name)}``;
        * ``cash_delta``: ``{month: variacion de efectivo sin polizas de cierre}``;
        * ``cash_delta_book``: ``{month: variacion contable (con cierre)}``;
        * ``opening_cash``: saldo contable de efectivo al inicio de ``date_from``;
        * ``cash_account_ids``: cuentas consideradas efectivo;
        * ``date_from`` / ``date_to``.
        """
        config.ensure_one()
        company = config.company_id
        accounts = config._get_accounts()
        cash_ids = set(config.get_cash_account_ids())
        rules = self._compile_rules(config, accounts)
        rule_partner_ids = sorted({r['partner_id'] for r in rules['direct'] + rules['indirect'] if r['partner_id']})

        months = list(iter_months(date_from, date_to))
        lines = defaultdict(lambda: defaultdict(float))
        acc_lines = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        cash_delta = defaultdict(float)
        for month in months:
            cash_delta[month] = 0.0

        def add(line_key, account_id, month, amount):
            lines[line_key][month] += amount
            acc_lines[line_key][account_id][month] += amount

        for row in self._query_groups(company, date_from, date_to, cash_ids, rule_partner_ids):
            month, account_id, journal_id, move_type, partner_id, cash_move, is_debit, balance, is_cash = row
            if is_cash:
                cash_delta[month] += balance
                continue
            key = (account_id, journal_id, move_type, partner_id, cash_move, is_debit)
            # ---- indirecto: todos los apuntes que no son efectivo ----
            rule = self._find_rule(rules['indirect'], key)
            if rule is None:
                add('unclassified', account_id, month, -balance)
            elif rule['mode'] == 'addback':
                add('result', account_id, month, -balance)
                add(rule['line_key'], account_id, month, balance)
                add(rule['mirror_line_key'], account_id, month, -balance)
            else:
                add(rule['line_key'], account_id, month, -balance)
            # ---- directo: solo contrapartes de polizas que tocan efectivo ----
            if cash_move:
                rule = self._find_rule(rules['direct'], key)
                add(rule['line_key'] if rule else 'd_other', account_id, month, -balance)

        # ---- directo: reclasificacion por la factura conciliada -------------
        for row in self._query_invoice_allocations(company, date_from, date_to, cash_ids, rule_partner_ids):
            (month, cp_account_id, journal_id, cp_move_type, partner_id, cp_is_debit,
             inv_account_id, inv_move_type, inv_is_debit, amount) = row
            cash_effect = -amount if cp_is_debit else amount
            cp_rule = self._find_rule(rules['direct'], (cp_account_id, journal_id, cp_move_type, partner_id, True, cp_is_debit))
            if not self._reclassifiable(cp_rule):
                continue
            # La cuenta dominante se clasifica con el tipo de la poliza del pago
            # (no el de la factura): las reglas por tipo de asiento son para
            # cobros/pagos registrados dentro de la propia factura.
            inv_rule = self._find_rule(rules['direct'], (inv_account_id, journal_id, cp_move_type, partner_id, True, inv_is_debit))
            add(cp_rule['line_key'] if cp_rule else 'd_other', cp_account_id, month, -cash_effect)
            add(inv_rule['line_key'] if inv_rule else 'd_other', inv_account_id, month, cash_effect)

        cash_delta_book = self._query_cash_book_delta(company, date_from, date_to, cash_ids)
        opening_cash = self._query_cash_balance(company, date_from - timedelta(days=1), cash_ids)

        return {
            'company_id': company.id,
            'date_from': date_from,
            'date_to': date_to,
            'months': months,
            'lines': {k: dict(v) for k, v in lines.items()},
            'accounts': {k: {a: dict(m) for a, m in v.items()} for k, v in acc_lines.items()},
            'account_info': accounts,
            'cash_delta': dict(cash_delta),
            'cash_delta_book': cash_delta_book,
            'opening_cash': opening_cash,
            'cash_account_ids': sorted(cash_ids),
            'other_threshold': config.other_threshold,
        }

    @api.model
    def direct_daily(self, config, date_from, date_to):
        """Movimientos de efectivo del metodo directo dia por dia, con el
        contacto real: ``[(fecha, line_key, account_id, partner_id, importe)]``
        (importe = efecto en efectivo). Misma clasificacion que ``compute``
        (reglas + reclasificacion por factura conciliada), a granularidad de
        dia, para aprender la periodicidad de los pagos."""
        config.ensure_one()
        company = config.company_id
        accounts = config._get_accounts()
        cash_ids = set(config.get_cash_account_ids())
        rules = self._compile_rules(config, accounts)
        rule_partners = {r['partner_id'] for r in rules['direct'] if r['partner_id']}
        out = []

        def key_for(account_id, journal_id, move_type, partner_id, is_debit):
            return (account_id, journal_id, move_type, partner_id if partner_id in rule_partners else None, True, is_debit)

        for row in self._query_groups(company, date_from, date_to, cash_ids, [],
                                      granularity='day', keep_partner=True, cash_moves_only=True):
            day, account_id, journal_id, move_type, partner_id, _cash_move, is_debit, balance, is_cash = row
            if is_cash:
                continue
            rule = self._find_rule(rules['direct'], key_for(account_id, journal_id, move_type, partner_id, is_debit))
            out.append((day, rule['line_key'] if rule else 'd_other', account_id, partner_id, -balance))
        for row in self._query_invoice_allocations(company, date_from, date_to, cash_ids, [],
                                                   granularity='day', keep_partner=True):
            (day, cp_account_id, journal_id, cp_move_type, partner_id, cp_is_debit,
             inv_account_id, _inv_move_type, inv_is_debit, amount) = row
            cash_effect = -amount if cp_is_debit else amount
            cp_rule = self._find_rule(rules['direct'], key_for(cp_account_id, journal_id, cp_move_type, partner_id, cp_is_debit))
            if not self._reclassifiable(cp_rule):
                continue
            inv_rule = self._find_rule(rules['direct'], key_for(inv_account_id, journal_id, cp_move_type, partner_id, inv_is_debit))
            out.append((day, cp_rule['line_key'] if cp_rule else 'd_other', cp_account_id, partner_id, -cash_effect))
            out.append((day, inv_rule['line_key'] if inv_rule else 'd_other', inv_account_id, partner_id, cash_effect))
        return out

    @api.model
    def slice(self, result, date_from, date_to):
        """Importes de ``result`` restringidos a los meses entre ``date_from``
        y ``date_to`` (ambos deben caer en meses cubiertos por ``result``).

        Devuelve ``{'lines': {key: importe}, 'accounts': {key: {account_id: importe}},
        'opening_cash', 'closing_cash_book', 'cash_delta', 'cash_delta_book'}``.
        """
        months = [m for m in result['months'] if date_from <= month_end(m) and m <= date_to]
        before = [m for m in result['months'] if month_end(m) < date_from]
        lines = {key: sum(vals.get(m, 0.0) for m in months) for key, vals in result['lines'].items()}
        accounts = {
            key: {aid: sum(vals.get(m, 0.0) for m in months) for aid, vals in per_acc.items()}
            for key, per_acc in result['accounts'].items()
        }
        opening = result['opening_cash'] + sum(result['cash_delta_book'].get(m, 0.0) for m in before)
        delta = sum(result['cash_delta'].get(m, 0.0) for m in months)
        delta_book = sum(result['cash_delta_book'].get(m, 0.0) for m in months)
        return {
            'lines': lines,
            'accounts': accounts,
            'opening_cash': opening,
            'cash_delta': delta,
            'cash_delta_book': delta_book,
            'closing_cash_book': opening + delta_book,
        }

    @api.model
    def totals(self, sliced):
        """Totales por seccion y las cifras de conciliacion de un ``slice``."""
        lines = sliced['lines']

        def section_total(section):
            return sum(v for k, v in lines.items() if L.LINE_SECTION.get(k) == section)

        sections = {section: section_total(section) for section, _label in L.SECTIONS}
        ind_operating = sum(sections[s] for s in L.INDIRECT_OPERATING_SECTIONS)
        ind_net = sum(sections[s] for s in L.INDIRECT_NET_SECTIONS)
        dir_net = sum(sections[s] for s in L.DIRECT_NET_SECTIONS)
        ind_fx = sections['ind_fx']
        dir_fx = sections['dir_fx']
        closing_calc = sliced['opening_cash'] + ind_net + ind_fx
        outflows = sum(v for k, v in lines.items() if k in L.DIRECT_KEYS and v < 0)
        other = lines.get('d_other', 0.0)
        return {
            'sections': sections,
            'ind_operating': ind_operating,
            'ind_investing': sections['ind_investing'],
            'ind_financing': sections['ind_financing'],
            'ind_net': ind_net,
            'ind_fx': ind_fx,
            'dir_operating': sections['dir_operating'],
            'dir_investing': sections['dir_investing'],
            'dir_financing': sections['dir_financing'],
            'dir_net': dir_net,
            'dir_fx': dir_fx,
            'opening_cash': sliced['opening_cash'],
            'closing_cash_calc': closing_calc,
            'closing_cash_book': sliced['closing_cash_book'],
            'difference': closing_calc - sliced['closing_cash_book'],
            'methods_difference': (ind_net + ind_fx) - (dir_net + dir_fx),
            'direct_outflows': outflows,
            'other_ratio': (abs(other) / abs(outflows)) if outflows else 0.0,
        }

    @api.model
    def to_summary(self, result):
        """Resumen serializable (JSON) del periodo completo de ``result``."""
        sliced = self.slice(result, result['date_from'], result['date_to'])
        totals = self.totals(sliced)
        info = result['account_info']
        currency = self.env['res.company'].browse(result['company_id']).currency_id

        def r(v):
            return float_round(v, precision_rounding=currency.rounding or 0.01)

        def accounts_of(key):
            return [
                {'account_id': aid, 'code': info.get(aid, ('', '', ''))[0], 'name': info.get(aid, ('', '', ''))[2], 'amount': r(v)}
                for aid, v in sorted(sliced['accounts'].get(key, {}).items(), key=lambda kv: info.get(kv[0], ('',))[0])
                if not currency.is_zero(v)
            ]

        return {
            'company_id': result['company_id'],
            'date_from': fields.Date.to_string(result['date_from']),
            'date_to': fields.Date.to_string(result['date_to']),
            'cash_account_ids': result['cash_account_ids'],
            'lines': {key: r(sliced['lines'].get(key, 0.0)) for key, *_ in L.LINES},
            'line_labels': dict(L.LINE_LABELS),
            'sections': {k: r(v) for k, v in totals['sections'].items()},
            'indirect': {
                'operating': r(totals['ind_operating']),
                'investing': r(totals['ind_investing']),
                'financing': r(totals['ind_financing']),
                'net_increase': r(totals['ind_net']),
                'fx_effect': r(totals['ind_fx']),
            },
            'direct': {
                'operating': r(totals['dir_operating']),
                'investing': r(totals['dir_investing']),
                'financing': r(totals['dir_financing']),
                'net_increase': r(totals['dir_net']),
                'fx_effect': r(totals['dir_fx']),
            },
            'opening_cash': r(totals['opening_cash']),
            'closing_cash_calc': r(totals['closing_cash_calc']),
            'closing_cash_book': r(totals['closing_cash_book']),
            'difference': r(totals['difference']),
            'methods_difference': r(totals['methods_difference']),
            'unclassified': accounts_of('unclassified'),
            'other': accounts_of('d_other'),
            'monthly': {
                fields.Date.to_string(m): {
                    'cash_delta': r(result['cash_delta'].get(m, 0.0)),
                    'lines': {key: r(vals.get(m, 0.0)) for key, vals in result['lines'].items() if vals.get(m)},
                }
                for m in result['months']
            },
        }

    # ------------------------------------------------------------------
    # Reglas
    # ------------------------------------------------------------------
    @api.model
    def _compile_rules(self, config, accounts):
        compiled = {'indirect': [], 'direct': []}
        for rule in config.rule_ids.filtered(lambda r: r.method in ('indirect', 'direct')).sorted(lambda r: (r.sequence, r.id)):
            compiled[rule.method].append(rule._compile(accounts))
        return compiled

    @staticmethod
    def _reclassifiable(rule):
        """Una contraparte clasificada por una regla explicita (diario o
        contacto) no se reclasifica por la factura conciliada: esas reglas
        son la decision del usuario (p. ej. todo lo del diario Nominas es
        nomina aunque venga facturado por una prestadora)."""
        return rule is None or rule.get('criterion') not in ('journal', 'partner')

    @staticmethod
    def _find_rule(rules, key):
        """Primera regla compilada que coincide con
        ``key = (account_id, journal_id, move_type, partner_id, cash_move, is_debit)``."""
        account_id, journal_id, move_type, partner_id, cash_move, is_debit = key
        for rule in rules:
            if rule['account_ids'] is not None and account_id not in rule['account_ids']:
                continue
            if rule['journal_id'] is not None and journal_id != rule['journal_id']:
                continue
            if rule['partner_id'] is not None and partner_id != rule['partner_id']:
                continue
            if rule['move_types'] is not None and move_type not in rule['move_types']:
                continue
            if rule['side'] == 'debit' and not is_debit:
                continue
            if rule['side'] == 'credit' and is_debit:
                continue
            if rule['cash_move_only'] and not cash_move:
                continue
            return rule
        return None

    # ------------------------------------------------------------------
    # SQL
    # ------------------------------------------------------------------
    def _closing_move_clause(self):
        """Condicion SQL que excluye las polizas de cierre (mes 13) si el
        campo ``l10n_mx_closing_move`` existe en la base."""
        if 'l10n_mx_closing_move' in self.env['account.move']._fields:
            return "AND COALESCE(am.l10n_mx_closing_move, FALSE) = FALSE"
        return ""

    @staticmethod
    def _period_expr(granularity):
        return "aml.date" if granularity == 'day' else "date_trunc('month', aml.date)::date"

    def _query_groups(self, company, date_from, date_to, cash_ids, rule_partner_ids,
                      granularity='month', keep_partner=False, cash_moves_only=False):
        """Apuntes registrados del periodo (sin polizas de cierre) agrupados
        por (periodo, cuenta, diario, tipo de asiento, contacto, toca-efectivo,
        lado). Los apuntes de cuentas de efectivo vienen con ``is_cash = TRUE``
        para calcular la variacion del periodo.

        ``granularity``: 'month' (reporte) o 'day' (aprendizaje de patrones).
        ``keep_partner``: conservar el contacto real en vez de solo los
        contactos con regla. ``cash_moves_only``: solo polizas que tocan
        efectivo (metodo directo)."""
        self.env['account.move.line'].flush_model(['account_id', 'journal_id', 'partner_id', 'balance', 'date', 'parent_state', 'company_id', 'move_id'])
        self.env['account.move'].flush_model(['move_type', 'state'] + (['l10n_mx_closing_move'] if 'l10n_mx_closing_move' in self.env['account.move']._fields else []))
        cash_tuple = tuple(cash_ids) or (-1,)
        partner_expr = "l.partner_id" if keep_partner else "CASE WHEN l.partner_id = ANY(%(partner_ids)s) THEN l.partner_id END"
        query = """
            WITH lines AS (
                SELECT aml.move_id, aml.account_id, aml.journal_id, aml.partner_id, aml.balance,
                       {period_expr} AS period,
                       am.move_type,
                       aml.account_id IN %(cash_ids)s AS is_cash
                  FROM account_move_line aml
                  JOIN account_move am ON am.id = aml.move_id
                 WHERE aml.company_id = %(company_id)s
                   AND aml.parent_state = 'posted'
                   AND aml.date >= %(date_from)s
                   AND aml.date <= %(date_to)s
                   AND aml.balance != 0
                   {closing_clause}
            ),
            cash_moves AS (
                SELECT DISTINCT move_id FROM lines WHERE is_cash
            )
            SELECT l.period,
                   l.account_id,
                   l.journal_id,
                   l.move_type,
                   {partner_expr} AS partner_id,
                   cm.move_id IS NOT NULL AS cash_move,
                   l.balance > 0 AS is_debit,
                   SUM(l.balance) AS balance,
                   l.is_cash
              FROM lines l
              {cash_join} cash_moves cm ON cm.move_id = l.move_id
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 9
        """.format(closing_clause=self._closing_move_clause(), period_expr=self._period_expr(granularity),
                   partner_expr=partner_expr, cash_join="JOIN" if cash_moves_only else "LEFT JOIN")
        self.env.cr.execute(query, {
            'cash_ids': cash_tuple,
            'company_id': company.id,
            'date_from': date_from,
            'date_to': date_to,
            'partner_ids': list(rule_partner_ids) or [-1],
        })
        return self.env.cr.fetchall()

    def _query_invoice_allocations(self, company, date_from, date_to, cash_ids, rule_partner_ids,
                                   granularity='month', keep_partner=False):
        """Parte de cada contraparte por cobrar/pagar (de polizas que tocan
        efectivo) que esta conciliada con una factura, agrupada por la cuenta
        dominante de esa factura. Devuelve filas
        ``(mes, cuenta_contraparte, diario, tipo_poliza, contacto_de_regla,
        contraparte_es_cargo, cuenta_dominante, tipo_factura,
        dominante_es_cargo, importe_conciliado)``."""
        if not cash_ids:
            return []
        self.env['account.partial.reconcile'].flush_model(['debit_move_id', 'credit_move_id', 'amount'])
        query = """
            WITH lines AS (
                SELECT aml.id, aml.move_id, aml.account_id, aml.journal_id, aml.partner_id, aml.balance,
                       {period_expr} AS month,
                       am.move_type,
                       aml.account_id IN %(cash_ids)s AS is_cash
                  FROM account_move_line aml
                  JOIN account_move am ON am.id = aml.move_id
                 WHERE aml.company_id = %(company_id)s
                   AND aml.parent_state = 'posted'
                   AND aml.date >= %(date_from)s
                   AND aml.date <= %(date_to)s
                   AND aml.balance != 0
                   {closing_clause}
            ),
            cash_moves AS (
                SELECT DISTINCT move_id FROM lines WHERE is_cash
            ),
            cp AS (
                SELECT l.*
                  FROM lines l
                  JOIN cash_moves cm ON cm.move_id = l.move_id
                  JOIN account_account acc ON acc.id = l.account_id
                 WHERE NOT l.is_cash
                   AND acc.account_type IN ('asset_receivable', 'liability_payable')
            ),
            parts AS (
                SELECT cp.id AS cp_id, cp.month, cp.journal_id, cp.partner_id, cp.move_id AS cp_move_id,
                       cp.account_id AS cp_account_id, cp.move_type AS cp_move_type, cp.balance > 0 AS cp_is_debit,
                       apr.amount,
                       CASE WHEN apr.debit_move_id = cp.id THEN apr.credit_move_id ELSE apr.debit_move_id END AS inv_line_id
                  FROM cp
                  JOIN account_partial_reconcile apr ON apr.debit_move_id = cp.id OR apr.credit_move_id = cp.id
            ),
            inv AS (
                SELECT p.*, il.move_id AS inv_move_id, im.move_type AS inv_move_type
                  FROM parts p
                  JOIN account_move_line il ON il.id = p.inv_line_id
                  JOIN account_move im ON im.id = il.move_id
                 WHERE im.move_type IN ('out_invoice', 'out_refund', 'in_invoice', 'in_refund', 'out_receipt', 'in_receipt')
                   AND il.move_id != p.cp_move_id
                   -- Una factura que toca efectivo (p. ej. venta de USD a casa
                   -- de cambio registrada como factura contra recibos
                   -- pendientes) ya se clasifico como poliza de efectivo: su
                   -- pago se queda en la cuenta por pagar y se neutraliza ahi.
                   AND NOT EXISTS (
                       SELECT 1 FROM account_move_line x
                        WHERE x.move_id = il.move_id AND x.account_id IN %(cash_ids)s
                   )
            ),
            dominant AS (
                SELECT DISTINCT ON (il.move_id) il.move_id, il.account_id, il.balance > 0 AS is_debit
                  FROM account_move_line il
                  JOIN account_account acc ON acc.id = il.account_id
                 WHERE il.move_id IN (SELECT inv_move_id FROM inv)
                   AND il.display_type = 'product'
                   AND il.balance != 0
                   AND acc.account_type NOT IN ('asset_receivable', 'liability_payable')
                 ORDER BY il.move_id, abs(il.balance) DESC, il.id
            )
            SELECT i.month,
                   i.cp_account_id,
                   i.journal_id,
                   i.cp_move_type,
                   {partner_expr} AS partner_id,
                   i.cp_is_debit,
                   d.account_id AS inv_account_id,
                   i.inv_move_type,
                   d.is_debit AS inv_is_debit,
                   SUM(i.amount) AS amount
              FROM inv i
              JOIN dominant d ON d.move_id = i.inv_move_id
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        """.format(closing_clause=self._closing_move_clause(), period_expr=self._period_expr(granularity),
                   partner_expr="i.partner_id" if keep_partner else "CASE WHEN i.partner_id = ANY(%(partner_ids)s) THEN i.partner_id END")
        self.env.cr.execute(query, {
            'cash_ids': tuple(cash_ids),
            'company_id': company.id,
            'date_from': date_from,
            'date_to': date_to,
            'partner_ids': list(rule_partner_ids) or [-1],
        })
        return self.env.cr.fetchall()

    def _query_cash_book_delta(self, company, date_from, date_to, cash_ids):
        """Variacion contable mensual de las cuentas de efectivo (todas las
        polizas registradas, incluidas las de cierre)."""
        if not cash_ids:
            return {}
        self.env.cr.execute("""
            SELECT date_trunc('month', date)::date AS month, SUM(balance)
              FROM account_move_line
             WHERE company_id = %s AND parent_state = 'posted'
               AND account_id IN %s AND date >= %s AND date <= %s
             GROUP BY 1
        """, (company.id, tuple(cash_ids), date_from, date_to))
        return {month: float(total or 0.0) for month, total in self.env.cr.fetchall()}

    def _query_cash_balance(self, company, date_to, cash_ids):
        """Saldo contable de las cuentas de efectivo al cierre de ``date_to``."""
        if not cash_ids:
            return 0.0
        self.env.cr.execute("""
            SELECT COALESCE(SUM(balance), 0.0)
              FROM account_move_line
             WHERE company_id = %s AND parent_state = 'posted'
               AND account_id IN %s AND date <= %s
        """, (company.id, tuple(cash_ids), date_to))
        return float(self.env.cr.fetchone()[0] or 0.0)
