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
RUTA_ENGOMADO = 'Surtido Engomado'
RUTA_QUIMICOS = 'Surtido Químicos y Colorantes'
NOMBRE_RUTA_QUIMICOS = RUTA_QUIMICOS + ' (Piso → Ubic. 2)'
NOMBRE_RUTA_ENGOMADO = RUTA_ENGOMADO + ' (Piso → Ubic. 2)'


def configurar_piso(env):
    """3.8.0 (Dirección, 2026-11-06): ubicación «Químicos-Colorantes Piso».

    El engomado, los químicos y los colorantes se surten (requisición automática, como hasta ahora) al Piso de
    Tintorería, y la orden en kg los consume de ahí y no de la Ubic. 2. Aquí:
    1. crea ``Toluca/Stock/Químicos-Colorantes Piso`` si no existe (por nombre completo) y guarda su id en el parámetro
       ``quimibond_rollos_bitacora.ubicacion_piso``;
    2. 3.8.0 surtía el engomado, los químicos y los colorantes al Piso; desde 3.18.0 (químicos y colorantes) y 3.19.0
       (engomado) las tres requisiciones salen del Piso a la Ubic. 2 (ver ``configurar_requisicion_surtido``).
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
    configurar_requisicion_surtido(env, piso)
    return piso


def configurar_requisicion_surtido(env, piso):
    """3.18.0 (químicos y colorantes) y 3.19.0 (engomado; Dirección, 2026-11-11): la requisición de surtido sale del Piso a la
    Ubic. 2, no de la Ubic. 1; la orden en kg lo consume de la Ubic. 2.

    En cada ruta («Surtido Químicos y Colorantes» y «Surtido Engomado»): la regla de surtido (la que no es del tipo OP-KG)
    toma del Piso y su destino es el del tipo de operación (la Ubic. 2; con «destino de la regla» apagado Odoo ignora el
    destino de la regla y usa el del tipo), y la regla de consumo de la orden en kg toma de esa Ubic. 2. Idempotente. Si la
    ruta no existe, o el tipo no tiene un destino claro, no toca nada."""
    rutas = env['stock.route']
    for base, nombre in ((RUTA_QUIMICOS, NOMBRE_RUTA_QUIMICOS), (RUTA_ENGOMADO, NOMBRE_RUTA_ENGOMADO)):
        for ruta in env['stock.route'].with_context(active_test=False).search([('name', '=like', base + '%')]):
            reglas = ruta.rule_ids.with_context(active_test=False)
            surte = reglas.filtered(lambda r: r.picking_type_id.sequence_code != 'OP-KG')
            consumo = reglas.filtered(lambda r: r.picking_type_id.sequence_code == 'OP-KG')
            destinos = surte.picking_type_id.default_location_dest_id
            if len(destinos) != 1 or destinos == piso:
                _logger.warning("rollos 3.19.0: la ruta %s no tiene un destino claro (Ubic. 2) en su tipo de requisición; "
                                "no se toca.", ruta.name)
                continue
            ubic2 = destinos
            for regla in surte:
                valores = {}
                if regla.location_src_id != piso:
                    valores['location_src_id'] = piso.id
                if regla.location_dest_id != ubic2:
                    valores['location_dest_id'] = ubic2.id
                if regla.location_dest_from_rule:
                    valores['location_dest_from_rule'] = False
                if valores:
                    regla.write(valores)
            for regla in consumo:
                if regla.location_src_id != ubic2:
                    regla.location_src_id = ubic2
            if ruta.name != nombre:
                ruta.name = nombre
            rutas |= ruta
            _logger.info("rollos 3.19.0: %s toma del Piso y surte a %s.", ruta.name, ubic2.complete_name)
    return rutas


configurar_requisicion_quimicos = configurar_requisicion_surtido  # nombre de 3.18.0


def post_init_hook(env):
    configurar_secuencias_j(env)
    configurar_piso(env)


def migrate_3_7_0(cr, version):
    configurar_secuencias_j(api.Environment(cr, SUPERUSER_ID, {}))


def migrate_3_8_0(cr, version):
    configurar_piso(api.Environment(cr, SUPERUSER_ID, {}))


def migrate_3_18_0(cr, version):
    configurar_piso(api.Environment(cr, SUPERUSER_ID, {}))


def migrate_3_19_0(cr, version):
    configurar_piso(api.Environment(cr, SUPERUSER_ID, {}))
