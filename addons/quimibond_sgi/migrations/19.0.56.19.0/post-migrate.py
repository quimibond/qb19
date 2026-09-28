# -*- coding: utf-8 -*-
"""56.19.0 (2026-09-28): «Mi procedimiento» se firma en Sign antes de entrar
en vigor (decisión del CEO: el SGI usa Sign). Se enciende el parámetro una
vez; MAST lo puede apagar en Ajustes → SGI. Las revisiones ya vigentes no se
tocan."""
from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    params = env['ir.config_parameter'].sudo()
    if params.get_param('quimibond_sgi.mp_sign_required') is False:
        params.set_param('quimibond_sgi.mp_sign_required', 'True')
