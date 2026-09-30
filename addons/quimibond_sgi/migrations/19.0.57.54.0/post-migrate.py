# -*- coding: utf-8 -*-
"""57.54.0 (bloque 1 de formularios 5/5, decisión 2026-09-30 «Inventario de
formularios»): liga las actividades críticas de seguridad, salud y ambiente a
su pantalla de Odoo y su formato (``sgi.process.activity._sgi_link_activity_screens``,
tabla ``SGI_SST_ACTIVITY_LINKS`` en ``models/sgi_sst_links.py``).

Localiza las actividades por numeral en la empresa del SGI, los menús por XML
ID y los formatos por su clave vigente. Solo escribe menú, «Dónde se ejecuta
en Odoo» y formatos si están vacíos; lo que ya estaba queda en el log.
Idempotente. Nada se borra.

Esperado en producción (MCP, solo lectura, 2026-09-30), compañía 1, los tres
campos vacíos en todas salvo E2.18 (ya tiene 4 formatos, que se respetan):
E2.18 (716), E2.21 (719), E2.23 (721), E2.28 (726), E2.30 (728), E2.33 (731),
E2.34 (732), E2.35 (733), E2.37 (735), S4.34 (762), S5.14 (764), C5.23 (742),
C4.24 (769) y C2.39 (750). Formatos: F-P-S02-01 (3803), F-P-S02-02 (3804),
F-P-E03-01 (4061), F-P-A10-05 (3766), F-P-C04-06 (3919), F-IT-P-P01-08-03
(4759), F-P-A16-04 (3771). Cambiar menú o texto marca «cambió» el
procedimiento de E2, S4, S5, C2, C4 y C5 (revisión documental normal).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    written = env['sgi.process.activity']._sgi_link_activity_screens()
    _logger.info("SGI 57.54.0: actividades ligadas: %s (esperado: 14).",
                 ", ".join(sorted(written)) or "ninguna nueva")
