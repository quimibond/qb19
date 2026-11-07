# -*- coding: utf-8 -*-
"""3.7.0 (Dirección, 2026-11-06): numeración «J».

Las órdenes «Fabricación Tela en kg» (OP-KG) y «Fabricación Tela en m» (OP-M) se llaman ``TL/OP-KG/J00001`` y
``TL/OP-M/J00001``. La «J» va en el prefijo de la secuencia de cada tipo de operación (es un dato de la base, no del
módulo), así que se pone aquí: al instalar y al actualizar a 3.7.0. Idempotente: si el prefijo ya termina en «J» no hace
nada, y solo reinicia el consecutivo (J00001) cuando pone la «J»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

TIPOS_J = ('OP-KG', 'OP-M')


def configurar_secuencias_j(env):
    tipos = env['stock.picking.type'].with_context(active_test=False).search([
        ('code', '=', 'mrp_operation'), ('sequence_code', 'in', TIPOS_J)])
    for tipo in tipos:
        secuencia = tipo.sequence_id.sudo()
        if not secuencia:
            continue
        prefijo = secuencia.prefix or ''
        if prefijo.endswith('J'):
            continue
        secuencia.write({'prefix': prefijo + 'J', 'padding': 5})
        secuencia.number_next = 1
        _logger.info("rollos 3.7.0: la secuencia de %s queda «%sJ» desde 00001.", tipo.display_name, prefijo)
    return tipos


def post_init_hook(env):
    configurar_secuencias_j(env)


def migrate_3_7_0(cr, version):
    configurar_secuencias_j(api.Environment(cr, SUPERUSER_ID, {}))
