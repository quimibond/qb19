# -*- coding: utf-8 -*-
"""Mueve el menú "SAT (Syntage)" de Facturación a Contabilidad en las bases
que ya tenían el módulo instalado (el post_init_hook solo corre al instalar)."""
from odoo import SUPERUSER_ID, api

from odoo.addons.quimibond_sat.hooks import reparent_sat_menu


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    reparent_sat_menu(env)
