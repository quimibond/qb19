# -*- coding: utf-8 -*-
"""19.0.3.7.0 — Numeración «J»: prefijo «J» en las secuencias de OP-KG y OP-M (ver hooks.py). Idempotente. Las órdenes
que ya existen conservan su nombre y su número de baño; solo las nuevas se llaman ``…/J00001``."""
from odoo.addons.quimibond_rollos_bitacora.hooks import migrate_3_7_0


def migrate(cr, version):
    if not version:
        return
    migrate_3_7_0(cr, version)
