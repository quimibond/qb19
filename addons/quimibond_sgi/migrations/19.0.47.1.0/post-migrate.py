# -*- coding: utf-8 -*-
"""47.1.0: el Diagnóstico del SGI ya no es un wizard HTML. Se borran la acción
de ventana y el formulario viejos (Odoo no los quita solo al desaparecer del
XML) y se vacía la acción del menú si quedó apuntando a algo borrado. Todo
idempotente."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

_REMOVED = (
    'quimibond_sgi.sgi_diagnostic_action',
    'quimibond_sgi.sgi_diagnostic_view_form',
)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Data = env['ir.model.data'].sudo()
    for xmlid in _REMOVED:
        module, name = xmlid.split('.', 1)
        data = Data.search([('module', '=', module), ('name', '=', name)], limit=1)
        if not data:
            continue
        model = data.model
        record = env[model].sudo().browse(data.res_id).exists()
        if record:
            record.unlink()  # borra también su ir.model.data
        if data.exists():
            data.unlink()
        _logger.info("SGI 47.1.0: borrado %s (%s).", xmlid, model)
    dangling = env['ir.ui.menu']._sgi_menu_dangling_actions()
    if dangling:
        _logger.info("SGI 47.1.0: se vacía la acción de %s.", ", ".join(dangling.mapped('complete_name')))
        dangling.write({'action': False})
