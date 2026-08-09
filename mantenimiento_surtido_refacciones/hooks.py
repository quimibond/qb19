# -*- coding: utf-8 -*-
"""
Se crean por código (en vez de datos XML con xmlids fijos) porque tres
intentos distintos de referenciar xmlids "estándar" de Odoo
(`stock.stock_location_locations_virtual`, `stock.warehouse0`,
bloques internos de la vista de Ajustes) fallaron contra la instancia
real de Quimibond en v19 -- la estructura de datos base varía según
cómo se instaló/migró cada instancia. Este hook busca lo que necesita
en tiempo de instalación, con una cadena de respaldo, en vez de asumir
un xmlid concreto.
"""
from odoo import SUPERUSER_ID, api


def post_init_hook(env):
    if not isinstance(env, api.Environment):
        # Compatibilidad con la firma antigua post_init_hook(cr, registry)
        env = api.Environment(env, SUPERUSER_ID, {})

    Location = env['stock.location'].sudo()
    Sequence = env['ir.sequence'].sudo()
    PickingType = env['stock.picking.type'].sudo()
    ConfigParam = env['ir.config_parameter'].sudo()

    # 1) Ubicación "Consumo Mantenimiento" (usage='production').
    location = Location.search([
        ('name', '=', 'Consumo Mantenimiento'),
        ('usage', '=', 'production'),
    ], limit=1)

    if not location:
        parent = _find_virtual_parent(Location)
        location = Location.create({
            'name': 'Consumo Mantenimiento',
            'usage': 'production',
            'location_id': parent.id,
            'company_id': parent.company_id.id,
        })

    ConfigParam.set_param(
        'mantenimiento_surtido_refacciones.location_consumo_id', location.id)

    # 2) Tipo de operación interno dedicado, sobre el almacén principal
    #    de la primera compañía (evita depender de un xmlid de almacén).
    warehouse = env['stock.warehouse'].sudo().search([], limit=1)
    if warehouse and not PickingType.search([
        ('name', '=', 'Requerimiento de Refacciones (Mantenimiento)'),
        ('warehouse_id', '=', warehouse.id),
    ], limit=1):
        sequence = Sequence.search(
            [('code', '=', 'stock.picking.refacciones.mantto')], limit=1)
        if not sequence:
            sequence = Sequence.create({
                'name': 'Requerimiento de Refacciones - Mantenimiento',
                'code': 'stock.picking.refacciones.mantto',
                'prefix': 'REQ-MTTO/',
                'padding': 5,
                'company_id': False,
            })
        PickingType.create({
            'name': 'Requerimiento de Refacciones (Mantenimiento)',
            'code': 'internal',
            'sequence_id': sequence.id,
            'sequence_code': 'REQMTTO',
            'default_location_dest_id': location.id,
            'warehouse_id': warehouse.id,
            'show_operations': True,
        })


def _find_virtual_parent(Location):
    """Busca la mejor ubicación padre disponible, sin asumir un xmlid
    fijo. Orden de preferencia:
    1) Xmlid estándar de Odoo, si existe en esta instancia.
    2) Cualquier ubicación tipo 'view' (estructural) por nombre común.
    3) Cualquier ubicación tipo 'view' que exista, la que sea.
    4) Como último recurso, la vista raíz del primer almacén.
    """
    env = Location.env
    candidate = env.ref('stock.stock_location_locations_virtual', raise_if_not_found=False)
    if candidate:
        return candidate

    candidate = Location.search([
        ('usage', '=', 'view'),
        ('name', 'ilike', 'virtual'),
    ], limit=1)
    if candidate:
        return candidate

    candidate = Location.search([('usage', '=', 'view')], limit=1)
    if candidate:
        return candidate

    warehouse = env['stock.warehouse'].sudo().search([], limit=1)
    if warehouse and warehouse.view_location_id:
        return warehouse.view_location_id

    # Si absolutamente nada de lo anterior existe, se detiene con un
    # mensaje claro en vez de fallar con un xmlid inexistente.
    from odoo.exceptions import UserError
    raise UserError(
        'No se encontró ninguna ubicación padre válida para crear '
        '"Consumo Mantenimiento". Crea manualmente la ubicación en '
        'Inventario > Configuración > Ubicaciones (usage=Producción) y '
        'vuelve a intentar la instalación, o repórtalo para ajustar el hook.')
