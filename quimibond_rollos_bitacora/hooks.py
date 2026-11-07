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


NOMBRE_PISO = 'Químicos-Colorantes Piso'
PADRE_PISO = 'Toluca/Stock'
PARAM_UBICACION_PISO = 'quimibond_rollos_bitacora.ubicacion_piso'
RUTAS_PISO = ('Surtido Engomado', 'Surtido Químicos y Colorantes')


def configurar_piso(env):
    """3.8.0 (Dirección, 2026-11-06): ubicación «Químicos-Colorantes Piso».

    El engomado, los químicos y los colorantes se surten (requisición automática, como hasta ahora) al Piso de
    Tintorería, y la orden en kg los consume de ahí y no de la Ubic. 2. Aquí:
    1. crea ``Toluca/Stock/Químicos-Colorantes Piso`` si no existe (por nombre completo) y guarda su id en el parámetro
       ``quimibond_rollos_bitacora.ubicacion_piso``;
    2. en las rutas «Surtido Engomado» y «Surtido Químicos y Colorantes», la regla que surte pone el Piso de destino y
       la de consumo (tipo OP-KG) lo toma de origen; el nombre de la ruta cambia «→ Producción» por «→ Piso».
    Idempotente. Si no hay ``Toluca/Stock`` (otra base) no crea nada ni toca reglas: no inventa la ubicación."""
    Ubicacion = env['stock.location'].with_context(active_test=False)
    padre = Ubicacion.search([('complete_name', '=', PADRE_PISO)], limit=1)
    if not padre:
        _logger.warning("rollos 3.8.0: no existe «%s»; no se crea el Piso ni se tocan las rutas.", PADRE_PISO)
        return env['stock.location']
    piso = Ubicacion.search([('location_id', '=', padre.id), ('name', '=', NOMBRE_PISO)], limit=1)
    if not piso:
        piso = Ubicacion.create({
            'name': NOMBRE_PISO, 'location_id': padre.id, 'usage': 'internal', 'company_id': padre.company_id.id})
        _logger.info("rollos 3.8.0: creada la ubicación %s.", piso.complete_name)
    elif not piso.active:
        piso.active = True
    env['ir.config_parameter'].sudo().set_param(PARAM_UBICACION_PISO, str(piso.id))
    rutas = env['stock.route'].with_context(active_test=False).search(
        ['|'] + [('name', '=like', nombre + '%') for nombre in RUTAS_PISO])
    for ruta in rutas:
        for regla in ruta.rule_ids.with_context(active_test=False):
            if regla.picking_type_id.sequence_code == 'OP-KG':
                if regla.location_src_id != piso:
                    regla.location_src_id = piso
            elif regla.location_dest_id != piso:
                regla.location_dest_id = piso
        if '→ Producción)' in (ruta.name or ''):
            ruta.name = ruta.name.replace('→ Producción)', '→ Piso)')
    _logger.info("rollos 3.8.0: rutas al Piso: %s.", ', '.join(rutas.mapped('name')) or '(ninguna)')
    return piso


def post_init_hook(env):
    configurar_secuencias_j(env)
    configurar_piso(env)


def migrate_3_7_0(cr, version):
    configurar_secuencias_j(api.Environment(cr, SUPERUSER_ID, {}))


def migrate_3_8_0(cr, version):
    configurar_piso(api.Environment(cr, SUPERUSER_ID, {}))
