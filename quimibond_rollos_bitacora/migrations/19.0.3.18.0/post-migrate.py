# -*- coding: utf-8 -*-
"""19.0.3.18.0 — La requisición de Tintorería (REQ-TIN) de químicos y colorantes sale del Piso a la Ubic. 2 (ver
hooks.configurar_requisicion_quimicos). Idempotente."""
from odoo.addons.quimibond_rollos_bitacora.hooks import migrate_3_18_0


def migrate(cr, version):
    if not version:
        return
    migrate_3_18_0(cr, version)
