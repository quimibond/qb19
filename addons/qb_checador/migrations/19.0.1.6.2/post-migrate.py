# -*- coding: utf-8 -*-
"""«Checadas desde» nace en 1.6.2. A los relojes que ya existían se les pone su fecha de alta: nada de lo que
mandaron después cambia, y el historial que el UA860 descargó el 20-oct-2026 (checadas de 2024 y 2025) queda
«Ignorada» en vez de convertirse en asistencias."""
from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    equipos = env['qb.checador.equipo'].with_context(active_test=False).search([('checadas_desde', '=', False)])
    for equipo in equipos:
        equipo.checadas_desde = equipo.create_date
