# -*- coding: utf-8 -*-
"""57.15.0 (entrega 8, zona horaria y días hábiles: G-007, G-008, G-009,
G-020, G-022; decisión 4 de la tanda 2).

1. ``sgi.config._sgi_load_holidays([2026, 2027, 2028])``: festivos de la LFT,
   art. 74, en el calendario de días hábiles del SGI (parámetro
   ``quimibond_sgi.business_calendar_id`` = 21 en producción). Idempotente:
   no toca un día que ya tenga ausencia global; nada se borra. Los del
   contrato colectivo no están documentados en el repositorio: RH los
   confirma y se cargan a mano.
2. ``sgi.config._sgi_move_cron_hours()``: checklists a las 05:30 y medición de
   actividades a las 03:00 hora de México; los crons de legal, contexto,
   participación, Sign/eLearning y Mi procedimiento pasan de las 20:xx-22:xx
   de México a las 06:xx del mismo día UTC. Los crons son ``noupdate``.
3. Las actividades trimestrales, semestrales o anuales sin mes ni día
   recalculan sus faltantes (nuevo ``no_timing``).

Esperado en producción (MCP, solo lectura, 2026-09-30): el calendario 21
(America/Mexico_City, compañía 1) sin ausencias globales y ninguna ausencia
global en ningún calendario desde 2025 → 21 festivos creados (7 por año;
2028 no trae el 1 de octubre). Ningún empleado (calendarios 9, 13 y 32) ni
centro de trabajo (9, 25, 31, 33) usa el 21. Crons: 215 (22:44 UTC → 11:30
UTC), 196 (22:27 → 09:00), 197 y 199 (02:39 → 12:39), 198 (2027-02-24
02:39 → 12:39), 200 (04:20 → 12:20) y 213 (04:40 → 12:40). 15 actividades
de cadencia larga sin mes ni día ganan el faltante «Sin plazo».
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Config = env['sgi.config']
    created = Config._sgi_load_holidays([2026, 2027, 2028])
    _logger.info("SGI 57.15.0: %d festivos de la LFT cargados (esperado: 21).", len(created))
    moved = Config._sgi_move_cron_hours()
    _logger.info("SGI 57.15.0: crons movidos a hora de México: %s (esperado: 7).", len(moved))
    Activity = env['sgi.process.activity'].with_context(active_test=False)
    long_cadence = Activity.search([
        ('measure_cadence', 'in', ('trimestral', 'semestral', 'anual')),
    ]).filtered(lambda a: not a.due_month and not a.due_day)
    long_cadence._sgi_refresh_spec_gaps()
    _logger.info("SGI 57.15.0: %d actividades de cadencia larga sin mes ni día revisadas "
                 "(esperado: 15): %s.", len(long_cadence),
                 ", ".join(long_cadence.mapped(lambda a: a.number or str(a.id))))
