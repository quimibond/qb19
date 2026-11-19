# -*- coding: utf-8 -*-
"""19.0.1.8.0 — Negociaciones de precio: los modelos nuevos se exponen por MCP como los de la cotización (el
``post_init_hook`` solo corre al instalar). Idempotente: un modelo que ya está en MCP no se toca."""
from odoo import SUPERUSER_ID, api


def migrate(cr, version):
    if not version:
        return
    from odoo.addons.qb_cotizador import _habilitar_mcp
    _habilitar_mcp(api.Environment(cr, SUPERUSER_ID, {}))
