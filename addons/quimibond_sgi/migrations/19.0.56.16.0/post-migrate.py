# -*- coding: utf-8 -*-
"""56.16.0 (2026-09-28): líneas de negocio con lo que Odoo ya tiene.

1. «C2 Pedido a entrega» mezcla pedido industrial, de confección y
   exportación. Con los equipos de venta y las posiciones fiscales de Odoo:

   - C2.04 (pedido industrial con release) → equipo Industrial.
   - C2.08 (pedido de confección con precio de lista) → equipo Confección.
   - Etapa «E. Exportación» (C2.28 a C2.32) → equipo Industrial (Confección
     no exporta: 0 pedidos al extranjero en 2026) y mercado «Cliente
     extranjero».

2. Las áreas SGI (A, C, D…) se ligan a su departamento de Odoo por nombre.
   Ambiental (E) y SST (S) no tienen departamento y quedan sin ligar.

Solo agrega: lo que ya tenga equipo, mercado o departamento no se toca, no se
borra nada y se puede correr dos veces."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

ACTIVITY_TEAMS = {'C2.04': 'Industrial', 'C2.08': 'Confección'}
STAGE_E = ('C2', 'E', 'Industrial', 'Cliente extranjero')
AREA_DEPARTMENTS = {
    'A': 'Administracion',
    'C': 'Calidad',
    'D': 'Diseño Y Desarrollo',
    'G': 'MAST',
    'I': 'Inspeccion Y Empaque',
    'M': 'Mantenimiento',
    'P': 'Produccion',
    'V': 'Ventas',
}


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {'active_test': False})
    company = env['sgi.indicator']._sgi_kpi_company()
    in_company = ['|', ('company_id', '=', False), ('company_id', '=', company.id)]
    Team = env['crm.team']
    Activity = env['sgi.process.activity']

    def team(name):
        return Team.search([('name', '=', name)] + in_company, limit=1)

    tagged = 0
    for number, team_name in ACTIVITY_TEAMS.items():
        found = team(team_name)
        if not found:
            continue
        for activity in Activity.search([('number', '=', number), ('sale_team_ids', '=', False)]):
            activity.sale_team_ids = found
            tagged += 1

    process_code, stage_code, team_name, position_name = STAGE_E
    export = Activity.search([('process_id.code', '=', process_code), ('stage_id.code', '=', stage_code)])
    found = team(team_name)
    if found:
        for activity in export.filtered(lambda a: not a.sale_team_ids):
            activity.sale_team_ids = found
            tagged += 1
    positions = env['account.fiscal.position'].search([('name', '=', position_name)] + in_company)
    if positions:
        for activity in export.filtered(lambda a: not a.fiscal_position_ids):
            activity.fiscal_position_ids = positions

    linked = 0
    Department = env['hr.department']
    for area in env['sgi.area'].search([('department_id', '=', False)]):
        name = AREA_DEPARTMENTS.get(area.code)
        department = name and Department.search([('name', '=', name)] + in_company, limit=1)
        if department:
            area.department_id = department
            linked += 1
    _logger.info("SGI 56.16.0: %d actividades con equipo de ventas; %d áreas ligadas a su departamento.",
                 tagged, linked)
