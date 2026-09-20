# -*- coding: utf-8 -*-
"""Snapshots del flujo de efectivo para dashboards externos.

Cada snapshot guarda el resumen JSON del motor (``cash.flow.engine``) para
un periodo. Se leen via JSON-2::

    POST /json/2/cash.flow.snapshot/search_read
    Authorization: Bearer <api_key>
    {"domain": [["company_id", "=", 1]], "fields": ["date_from", "date_to", "data"]}

El cron mensual genera el mes anterior y el acumulado del ejercicio de cada
compania con configuracion.
"""
from datetime import timedelta

from odoo import api, fields, models

from .cash_flow_engine import add_months, month_end, month_start


class CashFlowSnapshot(models.Model):
    _name = 'cash.flow.snapshot'
    _description = 'Snapshot del flujo de efectivo NIF B-2'
    _order = 'date_to desc, date_from desc, id desc'

    name = fields.Char(compute='_compute_name', store=True)
    company_id = fields.Many2one('res.company', required=True, index=True, default=lambda self: self.env.company)
    date_from = fields.Date(required=True)
    date_to = fields.Date(required=True)
    kind = fields.Selection([
        ('month', 'Mensual'),
        ('ytd', 'Acumulado del ejercicio'),
        ('custom', 'Personalizado'),
    ], default='custom', required=True)
    data = fields.Json(string='Datos', readonly=True)
    opening_cash = fields.Monetary(currency_field='currency_id', readonly=True)
    closing_cash = fields.Monetary(currency_field='currency_id', readonly=True, string='Efectivo final (calculado)')
    closing_cash_book = fields.Monetary(currency_field='currency_id', readonly=True, string='Saldo contable de efectivo')
    net_increase = fields.Monetary(currency_field='currency_id', readonly=True, string='Incremento neto')
    fx_effect = fields.Monetary(currency_field='currency_id', readonly=True, string='Efecto cambiario')
    operating = fields.Monetary(currency_field='currency_id', readonly=True, string='Operación')
    investing = fields.Monetary(currency_field='currency_id', readonly=True, string='Inversión')
    financing = fields.Monetary(currency_field='currency_id', readonly=True, string='Financiamiento')
    difference = fields.Monetary(currency_field='currency_id', readonly=True, string='Diferencia (debe ser 0)')
    unclassified = fields.Monetary(currency_field='currency_id', readonly=True, string='Sin clasificar')
    currency_id = fields.Many2one(related='company_id.currency_id')

    @api.depends('company_id', 'date_from', 'date_to', 'kind')
    def _compute_name(self):
        for snap in self:
            snap.name = '%s %s → %s' % (snap.company_id.name or '', snap.date_from or '', snap.date_to or '')

    @api.model
    def generate(self, company, date_from, date_to, kind='custom'):
        """Crea (o reemplaza) el snapshot de ``company`` para el periodo."""
        config = self.env['cash.flow.config']._get_for_company(company)
        summary = config.compute_summary(date_from, date_to)
        vals = {
            'company_id': company.id,
            'date_from': date_from,
            'date_to': date_to,
            'kind': kind,
            'data': summary,
            'opening_cash': summary['opening_cash'],
            'closing_cash': summary['closing_cash_calc'],
            'closing_cash_book': summary['closing_cash_book'],
            'net_increase': summary['indirect']['net_increase'],
            'fx_effect': summary['indirect']['fx_effect'],
            'operating': summary['indirect']['operating'],
            'investing': summary['indirect']['investing'],
            'financing': summary['indirect']['financing'],
            'difference': summary['difference'],
            'unclassified': summary['lines'].get('unclassified', 0.0),
        }
        existing = self.search([
            ('company_id', '=', company.id), ('date_from', '=', date_from),
            ('date_to', '=', date_to), ('kind', '=', kind)], limit=1)
        if existing:
            existing.write(vals)
            return existing
        return self.create(vals)

    def action_regenerate(self):
        for snap in self:
            self.generate(snap.company_id, snap.date_from, snap.date_to, snap.kind)
        return True

    @api.model
    def cron_generate_monthly_snapshots(self):
        """Cron: mes anterior y acumulado del ejercicio para cada compania
        con configuracion de flujo de efectivo."""
        today = fields.Date.context_today(self)
        prev_month_end = month_start(today) - timedelta(days=1)
        prev_month_start = month_start(prev_month_end)
        for config in self.env['cash.flow.config'].search([]):
            company = config.company_id
            self.generate(company, prev_month_start, prev_month_end, 'month')
            fy_start = company.compute_fiscalyear_dates(prev_month_end)['date_from']
            self.generate(company, fy_start, prev_month_end, 'ytd')
        return True

    @api.model
    def generate_months(self, company, date_from, date_to):
        """Genera un snapshot mensual por cada mes entre las fechas dadas."""
        snaps = self.browse()
        cur = month_start(date_from)
        while cur <= date_to:
            snaps |= self.generate(company, cur, month_end(cur), 'month')
            cur = add_months(cur, 1)
        return snaps
