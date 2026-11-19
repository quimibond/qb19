# -*- coding: utf-8 -*-
"""19.0.3.19.0 — La requisición de Engomado sale del Piso a la Ubic. 2, igual que la de químicos y colorantes (ver
hooks.configurar_requisicion_surtido). Idempotente."""
from odoo.addons.quimibond_rollos_bitacora.hooks import migrate_3_19_0


def migrate(cr, version):
    if not version:
        return
    migrate_3_19_0(cr, version)
