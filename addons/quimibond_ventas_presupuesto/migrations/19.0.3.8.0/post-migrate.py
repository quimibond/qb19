# -*- coding: utf-8 -*-
"""3.8.0: proyectos comerciales ligados a Desarrollo de producto.

1. Cada desarrollo en Cotización o más adelante, con cliente y artículo, recibe su proyecto
   comercial (los perdidos o no factibles no crean nada).
2. Cada renglón de Proyectos de un presupuesto liga el suyo (el de su desarrollo si coincide
   cliente y producto; si no, uno nuevo de origen «Presupuesto»).
No cambia ningún importe ni ningún producto de un renglón aprobado.

3.14.1: cada desarrollo y cada renglón van en su propio savepoint: uno que no se pueda ligar
queda en el log y no detiene la actualización (el build de main del 9-nov se cayó aquí)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    failed = 0
    devs = env['project.project'].with_context(active_test=False).search(
        [('sgi_is_ft', '=', True), ('is_template', '=', False)])
    for dev in devs:
        try:
            with cr.savepoint():
                dev._qb_sync_sales_project()
        except Exception as exc:  # noqa: BLE001 - un desarrollo no detiene la actualización
            failed += 1
            _logger.warning("3.8.0: el desarrollo %s (%s) no se ligó: %s", dev.id, dev.display_name, exc)
    lines = env['sgi.sales.budget.line'].search([
        ('kind', '=', 'presupuesto'),
        ('mercado', 'in', ('proyectos_industrial', 'proyectos_confeccion')),
        ('qb_project_id', '=', False)])
    for line in lines:
        try:
            with cr.savepoint():
                line._qb_link_projects()
        except Exception as exc:  # noqa: BLE001
            failed += 1
            _logger.warning("3.8.0: el renglón %s no se ligó: %s", line.id, exc)
    _logger.info("3.8.0: %d proyecto(s) comercial(es); %d renglón(es) de Proyectos revisados; %d sin ligar.",
                 env['qb.sales.project'].search_count([]), len(lines), failed)
