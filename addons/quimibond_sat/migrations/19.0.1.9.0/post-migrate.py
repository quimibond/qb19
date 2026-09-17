# -*- coding: utf-8 -*-
"""Vuelve a colgar el menú "SAT (Syntage)" de Contabilidad: la 1.8.0 lo
intentó solo por ``account_accountant.menu_accounting`` y en producción quedó
en Facturación. Además fija noupdate en el xml id de la raíz (Odoo no cambia
esa bandera al actualizar) para que futuras actualizaciones no lo regresen."""
from odoo import SUPERUSER_ID, api

from odoo.addons.quimibond_sat.hooks import mark_root_noupdate, reparent_sat_menu


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    mark_root_noupdate(env)
    reparent_sat_menu(env)
