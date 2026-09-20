# -*- coding: utf-8 -*-
"""Snapshot mensual: foto de capacidad, utilización y ociosidad por centro.

Las vistas SQL son siempre "en vivo" (ventana móvil); el snapshot congela
el estado al cierre de cada mes para histórico y tendencia. El costo por
producto ya queda histórico por sí mismo (qb.costo.producto guarda period).
"""
import logging
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class QbCosteoSnapshot(models.Model):
    _name = 'qb.costeo.snapshot'
    _description = 'Snapshot mensual de capacidad y ociosidad'
    _order = 'period DESC'
    _rec_name = 'period'

    period = fields.Date(required=True, index=True)
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    line_ids = fields.One2many('qb.costeo.snapshot.line', 'snapshot_id')
    notes = fields.Text()

    _period_company_uniq = models.Constraint(
        'unique(period, company_id)',
        "Ya existe un snapshot para ese período.",
    )

    @api.model
    def cron_snapshot_monthly(self):
        """Día 1: congela el mes que terminó y recalcula su costeo."""
        today = fields.Date.today()
        period = date(today.year, today.month, 1) - relativedelta(months=1)
        self.take_snapshot(period)
        self.env['qb.costo.producto'].action_recompute_period(period)

    @api.model
    def take_snapshot(self, period=None):
        if not period:
            today = fields.Date.today()
            period = date(today.year, today.month, 1) - relativedelta(months=1)
        existing = self.search([
            ('period', '=', period),
            ('company_id', '=', self.env.company.id)], limit=1)
        if existing:
            existing.line_ids.unlink()
            snapshot = existing
        else:
            snapshot = self.create({'period': period})
        Line = self.env['qb.costeo.snapshot.line']
        capacidad_by_centro = {}
        for cap in self.env['qb.capacidad'].search([]):
            if cap.centro_id:
                agg = capacidad_by_centro.setdefault(
                    cap.centro_id.id, {'hours': 0.0, 'free': 0.0})
                agg['hours'] += cap.hours_month_available
                agg['free'] += cap.free_hours_month
        for oci in self.env['qb.ociosidad'].search([]):
            cap_agg = capacidad_by_centro.get(oci.centro_id.id, {})
            Line.create({
                'snapshot_id': snapshot.id,
                'centro_id': oci.centro_id.id,
                'capacity_month_units': oci.capacity_month_units,
                'prod_month_units': oci.prod_month_units,
                'utilization_pct': oci.utilization_pct,
                'fixed_pool_month': oci.fixed_pool_month,
                'idle_cost_month': oci.idle_cost_month,
                'hours_month_available': cap_agg.get('hours', 0.0),
                'free_hours_month': cap_agg.get('free', 0.0),
            })
        _logger.info('qb.costeo.snapshot: snapshot %s con %s centros',
                     period, len(snapshot.line_ids))
        return snapshot


class QbCosteoSnapshotLine(models.Model):
    _name = 'qb.costeo.snapshot.line'
    _description = 'Snapshot por centro'
    _order = 'centro_id'

    snapshot_id = fields.Many2one(
        'qb.costeo.snapshot', required=True, ondelete='cascade')
    period = fields.Date(related='snapshot_id.period', store=True)
    centro_id = fields.Many2one('qb.costeo.centro', required=True)
    capacity_month_units = fields.Float(string='Capacidad/mes')
    prod_month_units = fields.Float(string='Producción/mes')
    utilization_pct = fields.Float(string='Utilización %')
    fixed_pool_month = fields.Float(string='Costo fijo/mes')
    idle_cost_month = fields.Float(string='Costo ocioso/mes')
    hours_month_available = fields.Float(string='Horas disponibles/mes')
    free_hours_month = fields.Float(string='Horas libres/mes')
