# -*- coding: utf-8 -*-
"""57.85.0: vuelve a correr la marca de equipos de reclamaciones de la 57.19.0
con la búsqueda corregida (nombre en cualquier idioma instalado, sin distinguir
mayúsculas; ``helpdesk.team._sgi_complaint_teams_by_name``).

Por qué la 57.19.0 falló: ``helpdesk.team.name`` es traducible y la migración
corre sin ``lang``; ``search([('name', 'in', …)])`` comparó solo la llave
``en_US`` del JSONB, y los equipos 2 («Reclamaciones entretelas») y 14
(«ATENCION A CLIENTES») tienen ese nombre solo en ``es_MX``. Se marcó solo el
30 (por XML ID). Jose marcó 2 y 14 a mano el 2026-10-01.

Solo AGREGA la marca donde falta; nunca la quita ni mueve tickets. En
producción no cambia nada (30, 2 y 14 ya están marcados); sirve para las bases
copiadas de producción antes del arreglo a mano (ramas de desarrollo, staging).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    marked = env['helpdesk.team']._sgi_mark_complaint_teams()
    _logger.info("SGI 57.85.0: equipos marcados como reclamación: %s "
                 "(en producción: ninguno nuevo, ya estaban 30, 2 y 14).",
                 ", ".join("%s «%s»" % (t.id, t.name) for t in marked) or "ninguno nuevo")
