# -*- coding: utf-8 -*-
"""3.16.3: inicio de los proyectos comerciales que nacieron de un desarrollo.

La bitácora de etapas del desarrollo empezó en octubre de 2026: los desarrollos de antes no
tenían registro de Cotización y su proyecto tomó la fecha del día en que se cargó (52 de 62 en
noviembre de 2026), así que su venta anterior contaba como línea. Ahora el inicio es la entrada a
Cotización o la fecha de alta del desarrollo, la que sea anterior. Después se reconstruye la venta
real para que el mercado (proyecto o línea) quede bien sin esperar al cron nocturno."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    moved = 0
    for project in env['qb.sales.project'].search([('origen', '=', 'desarrollo'),
                                                   ('dev_project_id', '!=', False)]):
        try:
            with cr.savepoint():
                start = project.dev_project_id._qb_dev_quote_date()
                if start and start < project.date_from:
                    project.write({'date_from': start})
                    moved += 1
        except Exception as exc:  # noqa: BLE001 - un proyecto no detiene la actualización
            _logger.warning("3.16.3: el proyecto %s no cambió de inicio: %s", project.id, exc)
    _logger.info("3.16.3: %d proyecto(s) con inicio corregido.", moved)
    try:
        with cr.savepoint():
            env['qb.sales.actual'].action_qb_rebuild()
    except Exception as exc:  # noqa: BLE001 - la reconstruye el cron nocturno
        _logger.warning("3.16.3: la venta real no se reconstruyó (la reconstruye el cron): %s", exc)
