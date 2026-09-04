# -*- coding: utf-8 -*-
"""Handler del reporte "Proyección de flujo de efectivo" (motor Enterprise).

Una columna por semana (13 por default) mas el total, a partir de la fecha
del filtro. Las lineas se despliegan por contacto y cada sub-linea abre los
apuntes, pedidos o compromisos que la alimentan.
"""
from collections import defaultdict
from datetime import timedelta

from odoo import _, fields, models
from odoo.tools.misc import format_date

from .cash_flow_forecast import FLOW_ROWS, FORECAST_ROW_LABELS, INFO_ROWS


class CashFlowForecastReportHandler(models.AbstractModel):
    _name = 'account.cash.flow.forecast.report.handler'
    _inherit = ['account.report.custom.handler', 'cash.flow.report.mixin']
    _description = 'Handler de la proyección de flujo de efectivo'

    TOTAL_KEY = 'total'

    # ------------------------------------------------------------------
    # Opciones / columnas
    # ------------------------------------------------------------------
    def _custom_options_initializer(self, report, options, previous_options=None):
        super()._custom_options_initializer(report, options, previous_options=previous_options)
        date_from = fields.Date.to_date(options['date'].get('date_to') or options['date'].get('date_from'))
        weeks = self._forecast_weeks(report, options, date_from)
        headers = []
        for idx, (start, end) in enumerate(weeks):
            name = _('Sem. %(n)s (%(start)s)', n=idx + 1, start=format_date(self.env, start, date_format='d MMM'))
            headers.append(self._make_header(options, start, end, name))
        headers.append(self._make_header(options, weeks[0][0], weeks[-1][1], _('Total')))
        options['cash_flow_forecast'] = {'date_from': fields.Date.to_string(date_from), 'n_weeks': len(weeks)}
        self._apply_column_headers(report, options, previous_options, headers)

    def _forecast_weeks(self, report, options, date_from):
        n_weeks = 13
        for company in self.env['res.company'].browse(self._get_company_ids(report, options)):
            config = self.env['cash.flow.config']._get_for_company(company, create=False)
            if config and config.forecast_weeks:
                n_weeks = max(n_weeks, config.forecast_weeks)
        return [(date_from + timedelta(days=7 * i), date_from + timedelta(days=7 * i + 6)) for i in range(n_weeks)]

    def _caret_options_initializer(self):
        caret = super()._caret_options_initializer()
        caret['cash_flow_forecast'] = [
            {'name': _('Ver origen'), 'action': 'cash_flow_forecast_open_source'},
        ]
        return caret

    # ------------------------------------------------------------------
    # Lineas
    # ------------------------------------------------------------------
    def _forecast_results(self, report, options):
        Engine = self.env['cash.flow.forecast.engine']
        date_from = fields.Date.to_date((options.get('cash_flow_forecast') or {}).get('date_from') or options['date']['date_to'])
        results, missing = [], self.env['res.company']
        for company in self.env['res.company'].browse(self._get_company_ids(report, options)):
            config = self.env['cash.flow.config']._get_for_company(company, create=False)
            if not config or not config.rule_ids:
                missing |= company
                continue
            results.append((config, Engine.compute(config, date_from)))
        return results, missing

    def _dynamic_lines_generator(self, report, options, all_column_groups_expression_totals, warnings=None):
        results, missing = self._forecast_results(report, options)
        n_weeks = (options.get('cash_flow_forecast') or {}).get('n_weeks') or 13
        week_columns = [c for c in options['columns']]
        # Las columnas van en el mismo orden que los encabezados: semanas y total.
        group_keys = list(options['column_groups'].keys())
        total_key = group_keys[-1] if group_keys else None

        def per_group(values_by_week):
            """{group_key: importe} a partir de {idx: importe} (+ total)."""
            out = {}
            for idx, key in enumerate(group_keys[:-1]):
                out[key] = values_by_week.get(idx, 0.0)
            if total_key:
                out[total_key] = sum(values_by_week.get(i, 0.0) for i in range(n_weeks))
            return out

        shown_rows = FLOW_ROWS + INFO_ROWS
        merged_rows = {key: defaultdict(float) for key in shown_rows}
        merged_details = {key: defaultdict(lambda: defaultdict(float)) for key in shown_rows}   # row -> partner -> idx -> amt
        partner_names = {}
        opening = [0.0] * n_weeks
        net = [0.0] * n_weeks
        closing = [0.0] * n_weeks
        below = set()
        min_cash = 0.0
        for _config, result in results:
            for key in shown_rows:
                for idx, amount in result['rows'].get(key, {}).items():
                    merged_rows[key][idx] += amount
                for idx, dets in result['details'].get(key, {}).items():
                    for det in dets:
                        merged_details[key][det['partner_id'] or 0][idx] += det['amount']
                        partner_names.setdefault(det['partner_id'] or 0, det['label'] if not det['partner_id'] else det['label'])
            for idx in range(min(n_weeks, len(result['closing']))):
                opening[idx] += result['opening'][idx]
                net[idx] += result['net'][idx]
                closing[idx] += result['closing'][idx]
            below |= set(result['below_min'])
            min_cash += result['min_cash']

        lines = []
        if missing:
            lines.append(self._text_line(report, options, 'warning_config', _(
                'Sin configuración de flujo de efectivo para: %s.', ', '.join(missing.mapped('name')))))
        if below:
            lines.append(self._text_line(report, options, 'warning_min', _(
                'El saldo proyectado queda por debajo del mínimo (%s) en las semanas: %s.',
                report.format_value(options, min_cash, figure_type='monetary') if hasattr(report, 'format_value') else min_cash,
                ', '.join(str(i + 1) for i in sorted(below)))))

        lines.append(self._forecast_line(report, options, 'opening', FORECAST_ROW_LABELS['opening'],
                                         per_group(dict(enumerate(opening))), level=1, total_value=opening[0] if opening else 0.0))
        for key in FLOW_ROWS:
            amounts = per_group(merged_rows[key])
            if all(self._is_zero(v) for v in amounts.values()):
                continue
            line_id = report._get_generic_line_id(None, None, markup=key)
            unfolded = self._is_unfolded(options, line_id)
            lines.append({
                'id': line_id,
                'name': FORECAST_ROW_LABELS[key],
                'level': 2,
                'unfoldable': True,
                'unfolded': unfolded,
                'columns': [self._column(report, options, column, amounts.get(column['column_group_key'], 0.0)) for column in week_columns],
            })
            partners = sorted(merged_details[key].items(), key=lambda kv: -abs(sum(kv[1].values())))
            for partner_id, by_week in partners:
                p_amounts = per_group(by_week)
                if all(self._is_zero(v) for v in p_amounts.values()):
                    continue
                name = self.env['res.partner'].browse(partner_id).display_name if partner_id else partner_names.get(0) or _('Sin contacto')
                lines.append({
                    'id': report._get_generic_line_id('res.partner', partner_id or None, markup=key, parent_line_id=line_id),
                    'parent_id': line_id,
                    'name': name,
                    'level': 3,
                    'caret_options': 'cash_flow_forecast',
                    'columns': [self._column(report, options, column, p_amounts.get(column['column_group_key'], 0.0)) for column in week_columns],
                })
        lines.append(self._forecast_line(report, options, 'net', FORECAST_ROW_LABELS['net'], per_group(dict(enumerate(net))), level=1))
        lines.append(self._forecast_line(report, options, 'closing', FORECAST_ROW_LABELS['closing'],
                                         per_group(dict(enumerate(closing))), level=1, total_value=closing[-1] if closing else 0.0))
        # Renglones informativos (no suman): partidas excluidas por antiguedad.
        for key in INFO_ROWS:
            amounts = per_group(merged_rows[key])
            if all(self._is_zero(v) for v in amounts.values()):
                continue
            line_id = report._get_generic_line_id(None, None, markup=key)
            lines.append({
                'id': line_id,
                'name': FORECAST_ROW_LABELS[key],
                'level': 2,
                'unfoldable': True,
                'unfolded': self._is_unfolded(options, line_id),
                'class': 'text-muted fst-italic',
                'columns': [self._column(report, options, column, amounts.get(column['column_group_key'], 0.0)) for column in week_columns],
            })
            for partner_id, by_week in sorted(merged_details[key].items(), key=lambda kv: -abs(sum(kv[1].values()))):
                p_amounts = per_group(by_week)
                if all(self._is_zero(v) for v in p_amounts.values()):
                    continue
                name = self.env['res.partner'].browse(partner_id).display_name if partner_id else _('Sin contacto')
                lines.append({
                    'id': report._get_generic_line_id('res.partner', partner_id or None, markup=key, parent_line_id=line_id),
                    'parent_id': line_id,
                    'name': name,
                    'level': 3,
                    'caret_options': 'cash_flow_forecast',
                    'columns': [self._column(report, options, column, p_amounts.get(column['column_group_key'], 0.0)) for column in week_columns],
                })
        return [(0, line) for line in lines]

    def _forecast_line(self, report, options, key, name, amounts, level=1, total_value=None):
        group_keys = list(options['column_groups'].keys())
        if total_value is not None and group_keys:
            amounts = dict(amounts, **{group_keys[-1]: total_value})
        return {
            'id': report._get_generic_line_id(None, None, markup='total_' + key),
            'name': name,
            'level': level,
            'unfoldable': False,
            'unfolded': False,
            'columns': [self._column(report, options, column, amounts.get(column['column_group_key'], 0.0)) for column in options['columns']],
        }

    # ------------------------------------------------------------------
    # Drill-down
    # ------------------------------------------------------------------
    def cash_flow_forecast_open_source(self, options, params):
        report = self.env['account.report'].browse(options['report_id'])
        parsed = report._parse_line_id(params.get('line_id'))
        row_key, partner_id = None, None
        for markup, model, value in parsed:
            key = markup.get('markup') if isinstance(markup, dict) else markup
            if key in FORECAST_ROW_LABELS:
                row_key = key
            if model == 'res.partner':
                partner_id = value
        results, _missing = self._forecast_results(report, options)
        ids_by_model = defaultdict(set)
        for _config, result in results:
            for dets in result['details'].get(row_key, {}).values():
                for det in dets:
                    if (det['partner_id'] or None) == (partner_id or None):
                        ids_by_model[det['model']].update(det['ids'])
        model = max(ids_by_model, key=lambda m: len(ids_by_model[m])) if ids_by_model else 'account.move.line'
        return {
            'type': 'ir.actions.act_window',
            'name': FORECAST_ROW_LABELS.get(row_key, _('Origen')),
            'res_model': model,
            'view_mode': 'list,form',
            'domain': [('id', 'in', sorted(ids_by_model.get(model, [])))],
            'context': {'create': False},
        }
