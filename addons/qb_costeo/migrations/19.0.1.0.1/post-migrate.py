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
    # El validador 1.0.0 medía colorante puro con 12/25 %; ahora mide la
    # preparación de color por kg de tela, cuyo rango normal es mayor.
    P = env['qb.parametro']
    for key, viejo, nuevo in (('colorante_max_pct', 12.0, 30.0),
                              ('colorante_total_max_pct', 25.0, 40.0)):
        for rec in P.search([('key', '=', key)]):
            if abs(rec.value_float - viejo) < 0.01:
                rec.value_float = nuevo
    if not P.search([('key', '=', 'validacion_excluir_categorias')]):
        for company in env['res.company'].search([]):
            P.create({'key': 'validacion_excluir_categorias',
                      'name': 'Categorías de producto que no se validan',
                      'tipo': 'text', 'value_text': 'Maquila',
                      'company_id': company.id,
                      'help': 'Texto que aparece en el nombre de la categoría '
                              '(separado por coma).'})
    # Se vuelven a evaluar las validaciones con las reglas nuevas.
    for company in env['res.company'].search([]):
        env['qb.producto.validacion'].with_company(company).revisar()
