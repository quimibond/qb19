# -*- coding: utf-8 -*-
"""Handler del reporte "Flujo de efectivo NIF B-2" (motor Enterprise).

Genera todas las lineas dinamicamente (``_dynamic_lines_generator``) a
partir del motor ``cash.flow.engine`` y reconstruye las columnas del
reporte (``_custom_options_initializer``) como un mes por columna mas el
acumulado, y una columna por cada periodo de comparacion nativo.

No toca ``account_reports``: solo hereda ``account.report.custom.handler``.
"""
import logging
from collections import OrderedDict

from odoo import _, fields, models
from odoo.tools.misc import format_date

from . import cash_flow_lines as L
from .cash_flow_engine import iter_months, month_end, month_start

_logger = logging.getLogger(__name__)


class CashFlowReportMixin(models.AbstractModel):
    """Utilerias compartidas por los handlers de flujo y de proyeccion:
    encabezados de columna por periodo, celdas, lineas de texto y companias."""
    _name = 'cash.flow.report.mixin'
    _description = 'Utilerías de los reportes de flujo de efectivo'

    @staticmethod
    def _make_header(options, date_from, date_to, name):
        date_opts = dict(options['date'])
        date_opts.update({
            'date_from': fields.Date.to_string(date_from),
            'date_to': fields.Date.to_string(date_to),
            'mode': 'range',
            'filter': 'custom',
            'string': name,
        })
        return {'name': name, 'forced_options': {'date': date_opts}}

    @staticmethod
    def _apply_column_headers(report, options, previous_options, headers):
        """Sustituye el primer nivel de encabezados y reconstruye columnas y
        grupos de columnas del reporte."""
        column_headers = options.get('column_headers') or [[]]
        options['column_headers'] = [headers] + list(column_headers[1:])
        init_columns = getattr(report, '_init_options_columns', None)
        if init_columns:
            try:
                init_columns(options, previous_options=previous_options)
            except TypeError:
                init_columns(options)

    def _get_company_ids(self, report, options):
        getter = getattr(report, 'get_report_company_ids', None)
        if getter:
            return list(getter(options))
        return [c['id'] for c in options.get('companies', []) if c.get('selected')] or self.env.company.ids

    def _is_zero(self, amount):
        return self.env.company.currency_id.is_zero(amount or 0.0)

    @staticmethod
    def _is_unfolded(options, line_id):
        return bool(options.get('unfold_all') or line_id in (options.get('unfolded_lines') or []))

    def _column(self, report, options, column, value):
        """Celda del reporte; ``value=None`` deja la celda vacia."""
        if value is None:
            return {'name': '', 'no_format': None, 'class': 'number', 'figure_type': column.get('figure_type', 'monetary')}
        try:
            return report._build_column_dict(value, column, options=options)
        except (TypeError, AttributeError):
            try:
                name = report.format_value(options, value, figure_type='monetary')
            except TypeError:
                name = report.format_value(value, figure_type='monetary')
            return {'name': name, 'no_format': value, 'class': 'number', 'figure_type': 'monetary'}

    def _text_line(self, report, options, markup, text):
        return {
            'id': report._get_generic_line_id(None, None, markup=markup),
            'name': text,
            'level': 3,
            'unfoldable': False,
            'unfolded': False,
            'class': 'text-warning fst-italic',
            'columns': [self._column(report, options, column, None) for column in options['columns']],
        }


class CashFlowNifReportHandler(models.AbstractModel):
    _name = 'account.cash.flow.nif.report.handler'
    _inherit = ['account.report.custom.handler', 'cash.flow.report.mixin']
    _description = 'Handler del flujo de efectivo NIF B-2'

    # ------------------------------------------------------------------
    # Opciones / columnas
    # ------------------------------------------------------------------
    def _custom_options_initializer(self, report, options, previous_options=None):
        super()._custom_options_initializer(report, options, previous_options=previous_options)
        date_from = fields.Date.to_date(options['date']['date_from'])
        date_to = fields.Date.to_date(options['date']['date_to'])

        headers = []
        spans = []
        aligned = date_from == month_start(date_from) and date_to == month_end(date_to)
        months = list(iter_months(date_from, date_to))
        if aligned and len(months) > 1:
            for month in months:
                headers.append(self._make_header(options, month, month_end(month), format_date(self.env, month, date_format='MMM yyyy')))
            headers.append(self._make_header(options, date_from, date_to, _('Acumulado')))
        else:
            headers.append(self._make_header(options, date_from, date_to, options['date'].get('string') or _('Periodo')))
        spans.append({'date_from': fields.Date.to_string(date_from), 'date_to': fields.Date.to_string(date_to)})

        for period in (options.get('comparison') or {}).get('periods', []):
            p_from, p_to = fields.Date.to_date(period['date_from']), fields.Date.to_date(period['date_to'])
            headers.append(self._make_header(options, p_from, p_to, period.get('string') or fields.Date.to_string(p_to)))
            spans.append({'date_from': period['date_from'], 'date_to': period['date_to']})

        options['cash_flow_spans'] = spans
        self._apply_column_headers(report, options, previous_options, headers)

    def _caret_options_initializer(self):
        caret = super()._caret_options_initializer()
        caret['cash_flow_account'] = [
            {'name': _('Apuntes contables'), 'action': 'cash_flow_open_journal_items'},
            {'name': _('Libro mayor'), 'action': 'caret_option_open_general_ledger'},
        ]
        return caret

    # ------------------------------------------------------------------
    # Calculo
    # ------------------------------------------------------------------
    def _compute_spans(self, report, options):
        """``{(date_from, date_to): [(company, result)]}`` por cada span
        (principal y comparaciones). Cada compania se calcula con su propia
        configuracion; nunca se mezclan definiciones entre companias."""
        Engine = self.env['cash.flow.engine']
        Config = self.env['cash.flow.config']
        companies = self.env['res.company'].browse(self._get_company_ids(report, options))
        spans = options.get('cash_flow_spans') or [{'date_from': options['date']['date_from'], 'date_to': options['date']['date_to']}]
        computed = OrderedDict()
        missing = self.env['res.company']
        for span in spans:
            date_from, date_to = fields.Date.to_date(span['date_from']), fields.Date.to_date(span['date_to'])
            results = []
            for company in companies:
                config = Config._get_for_company(company, create=False)
                if not config or not config.rule_ids:
                    missing |= company
                    continue
                results.append((company, Engine.compute(config, date_from, date_to)))
            computed[(date_from, date_to)] = results
        return computed, missing

    def _column_values(self, report, options, computed):
        """Por cada grupo de columnas: ``(slice_sumado, totales)``."""
        Engine = self.env['cash.flow.engine']
        values = {}
        for group_key, group in options['column_groups'].items():
            date_opts = (group.get('forced_options') or {}).get('date') or options['date']
            col_from, col_to = fields.Date.to_date(date_opts['date_from']), fields.Date.to_date(date_opts['date_to'])
            span = next((s for s in computed if s[0] <= col_from and col_to <= s[1]), None)
            merged = {'lines': {}, 'accounts': {}, 'opening_cash': 0.0, 'cash_delta': 0.0, 'cash_delta_book': 0.0, 'closing_cash_book': 0.0}
            account_info = {}
            other_threshold = 2.0
            for _company, result in (computed.get(span) or []):
                sliced = Engine.slice(result, col_from, col_to)
                for key, amount in sliced['lines'].items():
                    merged['lines'][key] = merged['lines'].get(key, 0.0) + amount
                for key, per_acc in sliced['accounts'].items():
                    bucket = merged['accounts'].setdefault(key, {})
                    for aid, amount in per_acc.items():
                        bucket[aid] = bucket.get(aid, 0.0) + amount
                for field in ('opening_cash', 'cash_delta', 'cash_delta_book', 'closing_cash_book'):
                    merged[field] += sliced[field]
                account_info.update(result['account_info'])
                other_threshold = result.get('other_threshold', other_threshold)
            values[group_key] = {
                'slice': merged,
                'totals': Engine.totals(merged),
                'account_info': account_info,
                'other_threshold': other_threshold,
            }
        return values

    # ------------------------------------------------------------------
    # Lineas
    # ------------------------------------------------------------------
    def _dynamic_lines_generator(self, report, options, all_column_groups_expression_totals, warnings=None):
        computed, missing = self._compute_spans(report, options)
        values = self._column_values(report, options, computed)
        lines = []

        if missing:
            lines.append(self._text_line(report, options, 'warning_config', _(
                'Sin configuración de flujo de efectivo para: %s. Contabilidad > Configuración > Flujo de efectivo NIF B-2 '
                '> "Cargar defaults Quimibond".', ', '.join(missing.mapped('name')))))

        # ---- Metodo indirecto -------------------------------------------
        lines.append(self._header_line(report, options, 'h_indirect', _('MÉTODO INDIRECTO (NIF B-2)')))
        for section in L.INDIRECT_OPERATING_SECTIONS:
            lines.extend(self._section_lines(report, options, values, section, level=2))
        lines.append(self._total_line(report, options, values, 'ind_operating', _('Flujo neto de efectivo de actividades de operación')))
        lines.extend(self._section_lines(report, options, values, 'ind_investing', level=2))
        lines.append(self._total_line(report, options, values, 'ind_investing', _('Flujo neto de efectivo de actividades de inversión')))
        lines.extend(self._section_lines(report, options, values, 'ind_financing', level=2))
        lines.append(self._total_line(report, options, values, 'ind_financing', _('Flujo neto de efectivo de actividades de financiamiento')))
        lines.append(self._total_line(report, options, values, 'ind_net', _('Incremento (disminución) neto de efectivo')))
        lines.extend(self._section_lines(report, options, values, 'ind_fx', level=2, with_header=False))
        lines.append(self._total_line(report, options, values, 'opening_cash', _('Efectivo al inicio del periodo')))
        lines.append(self._total_line(report, options, values, 'closing_cash_calc', _('Efectivo al final del periodo')))

        # ---- Metodo directo ---------------------------------------------
        lines.append(self._header_line(report, options, 'h_direct', _('MÉTODO DIRECTO (resumido)')))
        alert = self._other_alert(values)
        if alert:
            lines.append(self._text_line(report, options, 'warning_other', alert))
        lines.extend(self._section_lines(report, options, values, 'dir_operating', level=2))
        lines.append(self._total_line(report, options, values, 'dir_operating', _('Flujo neto de efectivo de actividades de operación')))
        lines.extend(self._section_lines(report, options, values, 'dir_investing', level=2))
        lines.append(self._total_line(report, options, values, 'dir_investing', _('Flujo neto de efectivo de actividades de inversión')))
        lines.extend(self._section_lines(report, options, values, 'dir_financing', level=2))
        lines.append(self._total_line(report, options, values, 'dir_financing', _('Flujo neto de efectivo de actividades de financiamiento')))
        lines.append(self._total_line(report, options, values, 'dir_net', _('Incremento (disminución) neto de efectivo')))
        lines.extend(self._section_lines(report, options, values, 'dir_fx', level=2, with_header=False))

        # ---- Conciliacion -----------------------------------------------
        lines.append(self._header_line(report, options, 'h_reconciliation', _('CONCILIACIÓN')))
        lines.append(self._total_line(report, options, values, 'opening_cash', _('Efectivo inicial (saldo contable)'), level=2))
        lines.append(self._total_line(report, options, values, 'ind_net', _('Incremento neto — método indirecto'), level=2))
        lines.append(self._total_line(report, options, values, 'dir_net', _('Incremento neto — método directo'), level=2))
        lines.append(self._total_line(report, options, values, 'methods_difference', _('Diferencia entre métodos (debe ser 0.00)'), level=2))
        lines.append(self._total_line(report, options, values, 'ind_fx', _('Efecto por cambios en el valor del efectivo'), level=2))
        lines.append(self._total_line(report, options, values, 'closing_cash_calc', _('Efectivo final calculado'), level=2))
        lines.append(self._total_line(report, options, values, 'closing_cash_book', _('Saldo contable de las cuentas de efectivo'), level=2))
        lines.append(self._total_line(report, options, values, 'difference', _('Diferencia: efectivo final calculado − saldo contable (debe ser 0.00)'), level=1))

        return [(0, line) for line in lines]

    def _section_lines(self, report, options, values, section, level=2, with_header=True):
        lines = []
        if with_header:
            lines.append(self._header_line(report, options, 'h_' + section, L.SECTION_LABELS[section], level=level))
        for key, key_section, label, _method in L.LINES:
            if key_section != section:
                continue
            amounts = {gk: v['slice']['lines'].get(key, 0.0) for gk, v in values.items()}
            hide_if_zero = key in ('nc_depreciation_ctr', 'nc_rou', 'nc_casualty', 'nc_bad_debt', 'unclassified', 'd_other')
            if hide_if_zero and all(self._is_zero(a) for a in amounts.values()):
                continue
            line_id = report._get_generic_line_id(None, None, markup=key)
            accounts = self._account_rows(values, key)
            unfoldable = bool(accounts)
            unfolded = unfoldable and self._is_unfolded(options, line_id)
            name = label
            if key in ('unclassified', 'd_other'):
                name = '⚠ ' + label
            lines.append({
                'id': line_id,
                'name': name,
                'level': level + 1,
                'unfoldable': unfoldable,
                'unfolded': unfolded,
                'columns': [self._column(report, options, column, amounts.get(column['column_group_key'], 0.0)) for column in options['columns']],
            })
            for account_id, code, acc_name, per_group in accounts:
                lines.append({
                    'id': report._get_generic_line_id('account.account', account_id, markup=key, parent_line_id=line_id),
                    'parent_id': line_id,
                    'name': ('%s %s' % (code, acc_name)).strip(),
                    'level': level + 2,
                    'caret_options': 'cash_flow_account',
                    'columns': [self._column(report, options, column, per_group.get(column['column_group_key'], 0.0)) for column in options['columns']],
                })
        return lines

    def _account_rows(self, values, key):
        """Sub-lineas por cuenta de la linea ``key``: ``[(account_id, code, name, {group_key: importe})]``."""
        per_account = {}
        info = {}
        for group_key, v in values.items():
            info.update(v['account_info'])
            for account_id, amount in v['slice']['accounts'].get(key, {}).items():
                per_account.setdefault(account_id, {})[group_key] = amount
        rows = []
        for account_id, per_group in per_account.items():
            if all(self._is_zero(a) for a in per_group.values()):
                continue
            code, _type, name = info.get(account_id, ('', '', '?'))
            rows.append((account_id, code, name, per_group))
        rows.sort(key=lambda r: (r[1], r[2]))
        return rows

    def _total_line(self, report, options, values, total_key, name, level=1):
        return {
            'id': report._get_generic_line_id(None, None, markup='total_' + total_key),
            'name': name,
            'level': level,
            'unfoldable': False,
            'unfolded': False,
            'columns': [self._column(report, options, column, values[column['column_group_key']]['totals'][total_key]) for column in options['columns']],
        }

    def _header_line(self, report, options, markup, name, level=1):
        return {
            'id': report._get_generic_line_id(None, None, markup=markup),
            'name': name,
            'level': level,
            'unfoldable': False,
            'unfolded': False,
            'columns': [self._column(report, options, column, None) for column in options['columns']],
        }

    def _other_alert(self, values):
        """Texto de alerta si "Otros (revisar)" supera el umbral en algun grupo de columnas."""
        worst = None
        for v in values.values():
            totals = v['totals']
            threshold = (v.get('other_threshold') or 0.0) / 100.0
            if totals['direct_outflows'] and totals['other_ratio'] > threshold:
                if worst is None or totals['other_ratio'] > worst:
                    worst = totals['other_ratio']
        if worst is None:
            return None
        return _('"Otros (revisar)" representa %.1f%% del total de salidas: revisa las reglas de clasificación.', worst * 100.0)

    # ------------------------------------------------------------------
    # Drill-down
    # ------------------------------------------------------------------
    def cash_flow_open_journal_items(self, options, params):
        """Apuntes que alimentan la sub-linea de cuenta seleccionada, en el
        rango de fechas del reporte. Para las lineas del metodo directo se
        restringe a las polizas que tocan efectivo."""
        report = self.env['account.report'].browse(options['report_id'])
        line_id = params.get('line_id')
        parsed = report._parse_line_id(line_id)
        account_id = None
        line_key = None
        for markup, model, value in parsed:
            if model == 'account.account':
                account_id = value
            if isinstance(markup, str) and markup in L.LINE_LABELS:
                line_key = markup
            elif isinstance(markup, dict) and markup.get('markup') in L.LINE_LABELS:
                line_key = markup['markup']
        company_ids = self._get_company_ids(report, options)
        domain = [
            ('company_id', 'in', company_ids),
            ('parent_state', '=', 'posted'),
            ('date', '>=', options['date']['date_from']),
            ('date', '<=', options['date']['date_to']),
        ]
        if account_id:
            domain.append(('account_id', '=', account_id))
        if 'l10n_mx_closing_move' in self.env['account.move']._fields:
            domain.append(('move_id.l10n_mx_closing_move', '=', False))
        if line_key in L.DIRECT_KEYS:
            cash_ids = []
            for company in self.env['res.company'].browse(company_ids):
                config = self.env['cash.flow.config']._get_for_company(company, create=False)
                if config:
                    cash_ids += config.get_cash_account_ids()
            domain.append(('move_id.line_ids.account_id', 'in', cash_ids or [-1]))
        return {
            'type': 'ir.actions.act_window',
            'name': L.LINE_LABELS.get(line_key, _('Apuntes contables')),
            'res_model': 'account.move.line',
            'view_mode': 'list,form',
            'views': [(self.env.ref('account.view_move_line_tree').id, 'list'), (False, 'form')],
            'domain': domain,
            'context': {'search_default_group_by_move': 0, 'create': False},
        }
