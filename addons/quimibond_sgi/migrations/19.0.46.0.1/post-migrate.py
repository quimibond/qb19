# -*- coding: utf-8 -*-
"""46.0.1: menús del SGI con acción borrada. Odoo no vacía `action` cuando el
menuitem deja de traerla; Inicio quedó apuntando al Panel de procesos retirado
en 45.0.0 y abría con «El registro no existe». Se vacía la acción de cualquier
menú del SGI cuya acción ya no exista (idempotente; en producción Inicio ya se
corrigió a mano el 2026-09-25)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    dangling = env['ir.ui.menu']._sgi_menu_dangling_actions()
    for menu in dangling:
        _logger.info("SGI 46.0.1: %s apuntaba a una acción borrada; se vacía.", menu.complete_name)
    if dangling:
        dangling.write({'action': False})
    else:
        _logger.info("SGI 46.0.1: ningún menú del SGI apunta a una acción borrada.")
