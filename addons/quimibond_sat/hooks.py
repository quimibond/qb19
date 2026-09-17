# -*- coding: utf-8 -*-
"""Cuelga el menú "SAT (Syntage)" de la app Contabilidad (Enterprise) cuando
existe. La dependencia no es dura: en community (y en el CI) solo hay
Facturación (``account.menu_finance``) y ahí se queda. Idempotente; se llama al
instalar y en cada migración que lo necesite. La raíz del menú es ``noupdate``
en ``views/menus.xml`` para que una actualización no la regrese a Facturación."""
import logging

_logger = logging.getLogger(__name__)

# xml id del menú raíz "Contabilidad" según la versión/edición de Odoo.
ACCOUNTING_MENU_XMLIDS = (
    'account_accountant.menu_accounting',
    'accountant.menu_accounting',
)
ACCOUNTING_MENU_ICONS = (
    'account_accountant,static/description/icon.png',
    'accountant,static/description/icon.png',
)


def accounting_root_menu(env):
    """Menú raíz de Contabilidad, o un recordset vacío si no existe."""
    for xmlid in ACCOUNTING_MENU_XMLIDS:
        menu = env.ref(xmlid, raise_if_not_found=False)
        if menu:
            return menu
    return env['ir.ui.menu'].sudo().search(
        [('parent_id', '=', False), ('web_icon', 'in', ACCOUNTING_MENU_ICONS)], limit=1)


def mark_root_noupdate(env):
    """Las bases instaladas antes de la 1.9.0 tienen el xml id de la raíz con
    noupdate=False y el ``<data noupdate="1">`` del XML no lo cambia al
    actualizar (Odoo conserva la bandera existente): hay que fijarla a mano
    una vez, si no cada actualización regresa el menú a Facturación."""
    data = env['ir.model.data'].sudo().search(
        [('module', '=', 'quimibond_sat'), ('name', '=', 'menu_sat_root'), ('noupdate', '=', False)])
    if data:
        data.write({'noupdate': True})
    return bool(data)


def reparent_sat_menu(env):
    root = env.ref('quimibond_sat.menu_sat_root', raise_if_not_found=False)
    accounting = accounting_root_menu(env)
    if not root or not accounting:
        _logger.warning('quimibond_sat: no hay menú Contabilidad (account_accountant); '
                        'el menú SAT se queda en Facturación')
        return False
    if root.parent_id != accounting:
        root.parent_id = accounting
        _logger.info('quimibond_sat: menú SAT movido a Contabilidad (menú %s)', accounting.id)
    return True


def post_init_hook(env):
    try:
        reparent_sat_menu(env)
    except Exception:  # noqa: BLE001 — nunca tumbar la instalación por un menú
        _logger.exception('quimibond_sat: no se pudo mover el menú SAT a Contabilidad')
