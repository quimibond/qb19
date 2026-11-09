# -*- coding: utf-8 -*-
"""Cierre de ventas general del mes (19.0.3.13.0, diseño v3 2.8 y 2.9).

Sustituye la hoja MENSUAL del Excel: por mercado, el presupuesto aprobado, la venta real (la de
«Venta real»: cuentas del VE-02 sin saldos ni desperdicio) y el cumplimiento del mes y del
acumulado del año, con la venta del mismo periodo del año anterior.

- **PDF** «Cierre de ventas general <mes>» desde el presupuesto (botón «Cierre de ventas») o
  desde Reportes → Cierre de ventas (PDF).
- **Correo:** el mismo asistente abre el redactor del presupuesto con el PDF adjunto.
- **Cierre de mes del SGI:** al cerrar cada mes, el PDF del mes queda en el chatter del
  presupuesto aprobado del año.

Antes de calcular, la venta real del año se reconstruye hasta el fin del mes, así que el PDF no
depende del cron nocturno.
"""
import base64
from datetime import date

from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_sales_budget import MERCADOS

MONTH_NAMES = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto',
               'septiembre', 'octubre', 'noviembre', 'diciembre']
CLOSE_REPORT = 'quimibond_ventas_presupuesto.action_report_sales_close'


def last_closed_month(today):
    return (today.replace(day=1) - relativedelta(months=1))


def pct(real, budget):
    return round(real / budget * 100.0, 1) if budget else None


class SgiSalesBudgetClose(models.Model):
    _inherit = 'sgi.sales.budget'

    def _qb_close_month(self):
        """Mes del cierre: el del contexto (``qb_close_month``, primer día) o el último cerrado."""
        raw = self.env.context.get('qb_close_month')
        if raw:
            return fields.Date.to_date(raw).replace(day=1)
        return last_closed_month(fields.Date.context_today(self))

    def _qb_close_data(self):
        """Por mercado: presupuesto, venta, diferencia y % del mes y del acumulado, y la venta del
        mismo periodo del año anterior. Lee la venta real (sin saldos)."""
        self.ensure_one()
        month = self._qb_close_month()
        if month.year != self.year:
            raise UserError("El mes del cierre (%s) no es del año del presupuesto (%s)."
                            % (month.strftime('%m/%Y'), self.year))
        year_start = date(self.year, 1, 1)
        month_end = month + relativedelta(months=1, days=-1)
        prev_start, prev_month = year_start - relativedelta(years=1), month - relativedelta(years=1)
        prev_end = month_end - relativedelta(years=1)
        labels = dict(MERCADOS + [('otro', "Sin mercado")])
        keys = [k for k, _l in MERCADOS] + ['otro']
        rows = {k: {'label': labels[k], 'bud_m': 0.0, 'real_m': 0.0, 'bud_y': 0.0, 'real_y': 0.0,
                    'prev_y': 0.0} for k in keys}
        for line in self.line_ids.filtered(lambda l: l.date and l.date <= month_end):
            row = rows[line.mercado if line.mercado in rows else 'otro']
            row['bud_y'] += line.amount_budget
            if line.date >= month:
                row['bud_m'] += line.amount_budget
        Actual = self.env['qb.sales.actual'].sudo()
        base = [('company_id', '=', self.company_id.id), ('es_saldo', '=', False)]
        for mercado, start, amount in Actual._read_group(
                base + [('date', '>=', year_start), ('date', '<=', month_end)],
                ['mercado', 'month:month'], ['amount:sum']):
            row = rows[mercado if mercado in rows else 'otro']
            row['real_y'] += amount
            if start and start >= month:
                row['real_m'] += amount
        for mercado, amount in Actual._read_group(
                base + [('date', '>=', prev_start), ('date', '<=', prev_end)],
                ['mercado'], ['amount:sum']):
            rows[mercado if mercado in rows else 'otro']['prev_y'] += amount
        saldos = sum(Actual.search([('company_id', '=', self.company_id.id), ('es_saldo', '=', True),
                                    ('date', '>=', year_start), ('date', '<=', month_end)]).mapped('amount'))
        out = []
        totals = {k: 0.0 for k in ('bud_m', 'real_m', 'bud_y', 'real_y', 'prev_y')}
        for key in keys:
            row = rows[key]
            if not any(row[k] for k in totals):
                continue
            for k in totals:
                totals[k] += row[k]
            out.append(dict(row, diff_m=row['real_m'] - row['bud_m'], diff_y=row['real_y'] - row['bud_y'],
                            pct_m=pct(row['real_m'], row['bud_m']), pct_y=pct(row['real_y'], row['bud_y']),
                            growth=pct(row['real_y'] - row['prev_y'], row['prev_y'])))
        total = dict(totals, label="Total", diff_m=totals['real_m'] - totals['bud_m'],
                     diff_y=totals['real_y'] - totals['bud_y'], pct_m=pct(totals['real_m'], totals['bud_m']),
                     pct_y=pct(totals['real_y'], totals['bud_y']),
                     growth=pct(totals['real_y'] - totals['prev_y'], totals['prev_y']))
        return {
            'month': month, 'month_label': "%s %s" % (MONTH_NAMES[month.month - 1], month.year),
            'prev_label': "ene–%s %s" % (MONTH_NAMES[prev_month.month - 1][:3], prev_month.year),
            'rows': out, 'total': total, 'saldos': saldos,
            'money': lambda value: '{:,.0f}'.format(value or 0.0),
            'percent': lambda value: '' if value is None else '%.1f %%' % value,
            'objetivo': self.base_anio_anterior * (1 + self.objetivo_pct / 100.0) if self.base_anio_anterior else 0.0,
            'minimo': self.base_anio_anterior * (1 + self.minimo_pct / 100.0) if self.base_anio_anterior else 0.0,
        }

    def _qb_close_refresh(self, month):
        """Reconstruye la venta real del año hasta el fin de ``month`` (y el mismo periodo del
        año anterior, para el comparativo)."""
        self.ensure_one()
        month_end = month + relativedelta(months=1, days=-1)
        Actual = self.env['qb.sales.actual']
        Actual.action_qb_rebuild(date(month.year, 1, 1), month_end)
        Actual.action_qb_rebuild(date(month.year - 1, 1, 1), month_end - relativedelta(years=1))

    def _qb_close_pdf(self, month):
        """(nombre, contenido PDF) del cierre de ``month``."""
        self.ensure_one()
        self._qb_close_refresh(month)
        report = self.env['ir.actions.report'].with_context(qb_close_month=fields.Date.to_string(month))
        pdf, _ext = report._render_qweb_pdf(CLOSE_REPORT, self.ids)
        name = "Cierre de ventas general %s %s.pdf" % (MONTH_NAMES[month.month - 1], month.year)
        return name, pdf

    def _qb_close_attachment(self, month):
        name, pdf = self._qb_close_pdf(month)
        return self.env['ir.attachment'].create({
            'name': name, 'datas': base64.b64encode(pdf), 'res_model': self._name, 'res_id': self.id,
            'mimetype': 'application/pdf'})

    def action_qb_open_close(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "Cierre de ventas", 'res_model': 'qb.sales.close',
            'view_mode': 'form', 'target': 'new', 'context': {'default_budget_id': self.id},
        }


class QbSalesClose(models.TransientModel):
    _name = 'qb.sales.close'
    _description = "Cierre de ventas general del mes"

    @api.model
    def _default_month(self):
        return last_closed_month(fields.Date.context_today(self))

    @api.model
    def _default_budget(self):
        month = self._default_month()
        return self.env['sgi.sales.budget'].search([
            ('kind', '=', 'presupuesto'), ('year', '=', month.year), ('state', '=', 'aprobado'),
            ('company_id', '=', self.env.company.id)], limit=1)

    budget_id = fields.Many2one(
        'sgi.sales.budget', string="Presupuesto", required=True, default=_default_budget,
        domain="[('kind', '=', 'presupuesto'), ('state', '!=', 'obsoleto')]",
        help="El aprobado del año del mes; para comparar contra una revisión en captura, elíjala.")
    month = fields.Date(string="Mes", required=True, default=_default_month,
                        help="Cualquier día del mes del cierre.")

    def _qb_month(self):
        self.ensure_one()
        month = self.month.replace(day=1)
        if month.year != self.budget_id.year:
            raise UserError("El mes del cierre (%s) no es del año del presupuesto %s."
                            % (month.strftime('%m/%Y'), self.budget_id.name))
        return month

    def action_print(self):
        month = self._qb_month()
        self.budget_id._qb_close_refresh(month)
        return self.env.ref(CLOSE_REPORT).with_context(
            qb_close_month=fields.Date.to_string(month)).report_action(self.budget_id)

    def action_send(self):
        month = self._qb_month()
        budget = self.budget_id
        attachment = budget._qb_close_attachment(month)
        data = budget.with_context(qb_close_month=fields.Date.to_string(month))._qb_close_data()
        total = data['total']
        fmt = '{:,.0f}'.format
        body = Markup(
            "<p>Cierre de ventas de %s: venta %s contra presupuesto %s (%s); acumulado del año %s "
            "contra %s (%s).</p><p>Detalle por mercado en el PDF adjunto.</p>") % (
            data['month_label'], fmt(total['real_m']), fmt(total['bud_m']),
            "%s %%" % total['pct_m'] if total['pct_m'] is not None else "sin presupuesto",
            fmt(total['real_y']), fmt(total['bud_y']),
            "%s %%" % total['pct_y'] if total['pct_y'] is not None else "sin presupuesto")
        return {
            'type': 'ir.actions.act_window', 'res_model': 'mail.compose.message', 'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_model': 'sgi.sales.budget', 'default_res_ids': budget.ids,
                'default_composition_mode': 'comment',
                'default_subject': "Cierre de ventas general %s" % data['month_label'],
                'default_body': body, 'default_attachment_ids': [attachment.id],
            },
        }


class SgiCronSalesClose(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def _sgi_monthly_close_steps(self, first_prev, last_prev):
        res = super()._sgi_monthly_close_steps(first_prev, last_prev)
        self._sgi_step("PDF del cierre de ventas", lambda: self._qb_sales_close_pdf(first_prev))
        return res

    @api.model
    def _qb_sales_close_pdf(self, month):
        """El PDF del cierre del mes en el chatter del presupuesto aprobado del año."""
        for budget in self.env['sgi.sales.budget'].search([
                ('kind', '=', 'presupuesto'), ('state', '=', 'aprobado'), ('year', '=', month.year)]):
            attachment = budget._qb_close_attachment(month)
            budget.message_post(body="Cierre de ventas general de %s %s." % (
                MONTH_NAMES[month.month - 1], month.year), attachment_ids=attachment.ids)
        return True
