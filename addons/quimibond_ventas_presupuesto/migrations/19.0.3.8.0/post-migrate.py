# -*- coding: utf-8 -*-
"""3.8.0: proyectos comerciales ligados a Desarrollo de producto.

1. Cada desarrollo en Cotización o más adelante, con cliente y artículo, recibe su proyecto
   comercial (los perdidos o no factibles no crean nada).
2. Cada renglón de Proyectos de un presupuesto liga el suyo (el de su desarrollo si coincide
   cliente y producto; si no, uno nuevo de origen «Presupuesto»).
No cambia ningún importe ni ningún producto de un renglón aprobado."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    devs = env['project.project'].with_context(active_test=False).search(
        [('sgi_is_ft', '=', True), ('is_template', '=', False)])
    devs._qb_sync_sales_project()
    lines = env['sgi.sales.budget.line'].search([
        ('kind', '=', 'presupuesto'),
        ('mercado', 'in', ('proyectos_industrial', 'proyectos_confeccion')),
        ('qb_project_id', '=', False)])
    lines._qb_link_projects()
    _logger.info("3.8.0: %d proyecto(s) comercial(es); %d renglón(es) de Proyectos ligados.",
                 env['qb.sales.project'].search_count([]), len(lines))
