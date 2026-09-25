# -*- coding: utf-8 -*-
"""52.0.0 (PR 5: DIR-2, DIR-3, DIR-4, PER-3).
- DIR-2: los riesgos ya evaluados (probabilidad e impacto capturados) reciben
  `last_eval_date` = su última modificación y, si no tenían próxima revisión,
  el siguiente enero o julio; así el cron los pone en el ciclo semestral.
- DIR-3: los acuerdos de revisiones previas con tarea de proyecto se conservan
  como están (la tarea sigue ligada); los nuevos nacen como acciones.
Idempotente; grep "SGI 52" en el log."""
import logging

from odoo import SUPERUSER_ID, api, fields

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Risk = env['sgi.risk'].sudo()
    evaluated = Risk.search([('eval_probability', '!=', False), ('eval_impact', '!=', False),
                             ('last_eval_date', '=', False)])
    for risk in evaluated:
        last = fields.Datetime.to_datetime(risk.write_date or risk.create_date).date()
        vals = {'last_eval_date': last}
        if not risk.next_review_date and risk.state != 'cerrado':
            vals['next_review_date'] = Risk._sgi_next_semester(fields.Date.context_today(risk))
        risk.write(vals)
    _logger.info("SGI 52: %d riesgo(s) con fecha de evaluación y ciclo semestral.", len(evaluated))
