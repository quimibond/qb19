# -*- coding: utf-8 -*-
"""57.0.0 (entrega 6, decisión de Jose): primera carga del grupo «Dueño de
proceso (SGI)» (``quimibond_sgi.group_sgi_process_owner``), que ve «Rutina por
rutina», «Procedimientos anteriores» y «Avance de la transición».

Llama ``sgi.process._sgi_sync_process_owner_group()``: entran los usuarios
activos de los dueños (``owner_id``, empleado → ``user_id``) de los procesos
activos de la empresa del SGI; salen los que ya no lo son. Solo toca ese
grupo. Idempotente: la segunda corrida no cambia nada. Nada se borra.

Esperado en producción la primera vez (MCP, solo lectura, 2026-09-29): 10
usuarios entran (14 procesos activos, 11 dueños distintos, uno sin usuario).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    result = env['sgi.process']._sgi_sync_process_owner_group()
    _logger.info("SGI 57.0.0: grupo «Dueño de proceso» cargado: %d entran %s, %d salen %s "
                 "(esperado en producción la primera vez: 10 entran).",
                 len(result['added']), result['added'], len(result['removed']), result['removed'])
