# -*- coding: utf-8 -*-
"""19.0.3.8.0 — Ubicación «Químicos-Colorantes Piso» y rutas de surtido hacia el Piso (ver hooks.configurar_piso).
Idempotente. Las órdenes que ya existen conservan el origen de sus movimientos; solo las nuevas consumen del Piso."""
from odoo.addons.quimibond_rollos_bitacora.hooks import migrate_3_8_0


def migrate(cr, version):
    if not version:
        return
    migrate_3_8_0(cr, version)
