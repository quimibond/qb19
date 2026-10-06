# -*- coding: utf-8 -*-
"""1.0.1: la importación de la clasificación legada no encontraba su fuente
(leía un `_table_query`). Se vuelve a correr contra las tablas base y se
habilitan los modelos en MCP si el servidor está instalado."""
from odoo import api, SUPERUSER_ID


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    env['qb.cuenta.clase'].importar_clasificacion_legada()
    from odoo.addons.qb_costeo import _habilitar_mcp
    _habilitar_mcp(env)
