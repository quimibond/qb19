# -*- coding: utf-8 -*-
"""MA-03 «Calidad PQ»: % de rollos revisados sin defecto, del registro de
revisado de tela (mrp.revision.log, de mrp_revisado_telas).

Vivía en el núcleo (quimibond_sgi/models/sgi_indicator.py,
sgi_indicator_detail.py y sgi_diagnostic.py) protegido con
``if 'mrp.revision.log' in self.env``; desde quimibond_sgi 57.10.0 (A-019) el
modo se registra aquí con ``selection_add`` y el cálculo es el mismo, línea
por línea.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

from odoo.addons.quimibond_sgi.models.sgi_indicator import SgiIndicator, SgiIndicatorMeasure


class SgiIndicatorCalidadPq(models.Model):
    _inherit = 'sgi.indicator'

    # Mismo lugar en la lista que tenía en el núcleo (después de «reproceso»).
    # Si este módulo se desinstala, los indicadores en este modo pasan a
    # captura manual.
    calc_mode = fields.Selection(
        selection_add=[('reproceso',), ('calidad_pq', "Calidad PQ (rollos revisados sin defecto)")],
        ondelete={'calidad_pq': 'set default'})

    _SOURCE_INFO = dict(
        SgiIndicator._SOURCE_INFO,
        calidad_pq="Piso → revisado de telas: rollos sin defecto vs revisados.")

    def _calc_calidad_pq(self, date_from, date_to):
        """% de rollos revisados SIN defecto, según el registro de revisado de
        tela (mrp.revision.log): un defecto se marca con una causa (etiqueta
        TEJIDO-*)."""
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        logs = self.env['mrp.revision.log'].search([
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
        ])
        total = len(logs)
        if not total:
            return None
        con_defecto = len(logs.filtered(lambda l: l.causa_id))
        return round((total - con_defecto) / total * 100.0, 2)

    def _detail_calidad_pq(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        logs = self.env['mrp.revision.log'].search([
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
        ok = len(logs) - len(logs.filtered(lambda l: l.causa_id))
        return self._ratio(ok, len(logs), logs)

    def _sgi_seed_calidad_pq(self):
        """Pone «calidad_pq» a los indicadores que siguen en manual y sin
        mediciones (siembra de una base nueva). Regresa los que cambió."""
        todo = self.filtered(lambda ind: ind.calc_mode == 'manual' and not ind.measure_ids)
        todo.write({'calc_mode': 'calidad_pq'})
        return todo


class SgiIndicatorMeasureCalidadPq(models.Model):
    _inherit = 'sgi.indicator.measure'

    # Evidencia del modo: el mismo universo que _calc_calidad_pq.
    _EVIDENCE = dict(
        SgiIndicatorMeasure._EVIDENCE,
        calidad_pq=('mrp.revision.log', [], 'create_date', True))


class SgiDiagnosticRevisado(models.TransientModel):
    _inherit = 'sgi.diagnostic'

    @api.model
    def _sgi_floor_quality_lines(self, floor_alerts):
        lines = super()._sgi_floor_quality_lines(floor_alerts)
        env = self.env
        month_ago_dt = fields.Datetime.now() - relativedelta(days=30)
        revision_logs = env['mrp.revision.log'].search_count(
            [('create_date', '>=', month_ago_dt)])
        if revision_logs and not floor_alerts:
            lines.append(self._sgi_line(
                'warn', "El revisado registró %d defectos en 30 días pero hay CERO alertas de calidad de piso: el pareto de alertas está vacío (¿fuentes apagadas?)." % revision_logs))
        ma03 = env['sgi.indicator'].search(
            [('code', '=', 'MA-03'), ('calc_mode', '=', 'manual')], limit=1)
        if ma03 and revision_logs:
            lines.append(self._sgi_line(
                'warn', "MA-03 (Calidad PQ) sigue en captura manual con el revisado ya operando: puede automatizarse (calc_mode «calidad_pq») y validarse un mes contra el Excel.",
                "ficha del indicador MA-03"))
        return lines
