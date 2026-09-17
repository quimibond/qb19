# -*- coding: utf-8 -*-
"""Cuelga el menú "SAT (Syntage)" de la app Contabilidad (Enterprise,
``account_accountant.menu_accounting``) cuando existe. La dependencia no es
dura: en community (y en el CI) solo hay Facturación (``account.menu_finance``)
y ahí se queda. Idempotente; se llama al instalar y en la migración."""
import logging

_logger = logging.getLogger(__name__)


def reparent_sat_menu(env):
    root = env.ref('quimibond_sat.menu_sat_root', raise_if_not_found=False)
    accounting = env.ref('account_accountant.menu_accounting', raise_if_not_found=False)
    if not root or not accounting:
        _logger.info('quimibond_sat: sin account_accountant, el menú SAT se queda en Facturación')
        return False
    if root.parent_id != accounting:
        root.parent_id = accounting
        _logger.info('quimibond_sat: menú SAT movido a Contabilidad')
    return True


def post_init_hook(env):
    try:
        reparent_sat_menu(env)
    except Exception:  # noqa: BLE001 — nunca tumbar la instalación por un menú
        _logger.exception('quimibond_sat: no se pudo mover el menú SAT a Contabilidad')
