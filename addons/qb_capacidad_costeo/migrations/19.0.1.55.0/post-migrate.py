# -*- coding: utf-8 -*-
"""Expone los modelos de familias por MCP, en solo lectura (1.55).

Los 25 modelos `qb.*` que ya se consultan por MCP se dieron de alta a mano
en la lista de permitidos, uno por uno. Los tres de familias nacieron con
la 1.54 y quedaron fuera, así que la verificación posterior al despliegue
—leer las familias y su carga— no se podía hacer desde el cliente MCP y
había que creerle al «no falló el update».

Se otorga SOLO lectura: sin create, sin write, sin unlink y sin llamadas a
métodos. Son modelos de configuración de capacidad del propio módulo; lo
que se gana es poder auditarlos, no tocarlos.

Degrada con gracia: si `mcp_server` no está instalado en esta base, no hay
nada que hacer y la migración no falla. Por eso va aquí y no como
dependencia del manifest — atar qb_capacidad_costeo a mcp_server obligaría
a instalar el servidor MCP para poder costear.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

MODELOS = ('qb.costeo.familia', 'qb.familia.producto', 'qb.familia.carga')


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    if 'mcp.enabled.model' not in env:
        _logger.info('qb_capacidad_costeo 1.55: mcp_server no instalado — '
                     'nada que exponer.')
        return
    Enabled = env['mcp.enabled.model']
    altas = []
    for nombre in MODELOS:
        modelo = env['ir.model'].search([('model', '=', nombre)], limit=1)
        if not modelo:
            _logger.warning('qb_capacidad_costeo 1.55: el modelo %s no está '
                            'registrado — sin exponer.', nombre)
            continue
        if Enabled.search_count([('model_id', '=', modelo.id)]):
            continue
        Enabled.create({
            'model_id': modelo.id,
            'allow_read': True,
            'allow_create': False,
            'allow_write': False,
            'allow_unlink': False,
            'allow_method_calls': False,
            'notes': 'Capacidad por familia de máquinas: se expone en solo '
                     'lectura para poder auditar la partición del centro y '
                     'la carga de cada familia después de un despliegue.',
        })
        altas.append(nombre)
    _logger.info('qb_capacidad_costeo 1.55: expuestos por MCP (solo '
                 'lectura): %s', ', '.join(altas) or 'ninguno (ya estaban)')
