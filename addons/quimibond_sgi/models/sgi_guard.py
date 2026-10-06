# -*- coding: utf-8 -*-
"""F-008 (auditoría 2026-09): candado de los métodos públicos de `sgi.cron` y
`sgi.config`.

Son AbstractModel (sin tabla ni ACL), así que cualquier usuario autenticado
podía llamar por RPC los `cron_*` (correos, actividades y escalamientos a
pedido) y las siembras (`seed_parameters` escribe `ir.config_parameter` con
sudo). Solo pasan el superusuario (crons, `<function>` del XML, migraciones)
y Ajustes / Administración. Módulo sin modelos: se puede importar desde
cualquier archivo de `models/` sin mover el orden del registro.
"""
from odoo.exceptions import AccessError


def sgi_require_system(env):
    if env.su or env.user._is_superuser() or env.user.has_group('base.group_system'):
        return
    raise AccessError("Solo el sistema (acciones planificadas) o un administrador pueden "
                      "correr este proceso del SGI.")
