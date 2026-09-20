# -*- coding: utf-8 -*-
"""Señales de operaciones (spec §4): producción, componentes, tiempos, existencias, reorden, transferencias, familias, mantenimiento."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar


@senal('op_atrasada')
def op_atrasada(env, cfg):
    n = umbral(cfg, 'dias', 7)
    mos = env['mrp.production'].sudo().search([('company_id', 'in', companias(env)), ('state', 'in', ('confirmed', 'progress')), ('date_start', '<', hace(n))], order='date_start')
    return [fila(f'op_atrasada:mrp.production:{m.id}', [doc(m, m.name, producto=m.product_id.display_name, cantidad=m.product_qty, inicio=str(m.date_start.date()))],
                 valor=dias(m.date_start), valor_texto=f'{m.product_id.display_name}: debía iniciar hace {dias(m.date_start)} días ({m.state})',
                 vence=m.date_start.date(), user=m.user_id, payload={'fecha_base': str(m.date_start.date()), 'estado': m.state, 'origen': m.origin}) for m in mos]


@senal('op_sin_componentes')
def op_sin_componentes(env, cfg):
    n = umbral(cfg, 'dias', 7)
    mos = env['mrp.production'].sudo().search([('company_id', 'in', companias(env)), ('state', '=', 'confirmed'),
                                                ('date_start', '>=', hoy()), ('date_start', '<', hace(-n)), ('reservation_state', '!=', 'assigned')])
    return [fila(f'op_sin_componentes:mrp.production:{m.id}', [doc(m, m.name, producto=m.product_id.display_name, inicio=str(m.date_start.date()))],
                 valor=1, valor_texto=f'{m.product_id.display_name}: inicia el {m.date_start.date()} y los componentes no están reservados ({m.reservation_state})',
                 vence=m.date_start.date(), user=m.user_id) for m in mos]


@senal('tiempos_excepcion')
def tiempos_excepcion(env, cfg):
    if not tiene_modelo(env, 'qb.workorder.excepcion'):
        return None
    n = umbral(cfg, 'dias', 7)
    xs = env['qb.workorder.excepcion'].sudo().search([('semana', '>=', hace(n))])
    return [fila(f'tiempos_excepcion:mrp.workorder:{x.workorder_id.id}', [doc(x.workorder_id, x.workorder_id.display_name, horas=x.horas, rendimiento=x.rendimiento)],
                 valor=abs(x.horas_desviadas or 0), valor_texto=f'{x.tipo}: {x.workcenter_id.name} · {x.product_id.display_name}, {x.horas:.1f} h registradas',
                 payload={'grupo': x.tipo, 'semana': str(x.semana)}) for x in xs]


@senal('existencia_negativa')
def existencia_negativa(env, cfg):
    quants = env['stock.quant'].sudo().search([('company_id', 'in', companias(env)), ('location_id.usage', '=', 'internal'), ('quantity', '<', 0)])
    filas = []
    for loc, qs in agrupar(quants, lambda q: q.location_id).items():
        filas.append(fila(f'existencia_negativa:stock.location:{loc.id}', [doc(q.product_id, q.product_id.display_name, cantidad=q.quantity) for q in qs[:30]],
                          valor=len(qs), valor_texto=f'{len(qs)} productos en negativo en {loc.complete_name}',
                          payload={'grupo': loc.complete_name, 'total_negativo': round(sum(q.quantity for q in qs), 2)}))
    return filas


@senal('reorden_pendiente')
def reorden_pendiente(env, cfg):
    ops = env['stock.warehouse.orderpoint'].sudo().search([('company_id', 'in', companias(env)), ('trigger', '=', 'auto')])
    ops = ops.filtered(lambda o: o.qty_to_order > 0)  # qty_to_order es computado: filtrar en Python
    return [fila(f'reorden_pendiente:stock.warehouse.orderpoint:{o.id}', [doc(o.product_id, o.product_id.display_name, a_pedir=o.qty_to_order, minimo=o.product_min_qty)],
                 valor=o.qty_to_order, valor_texto=f'{o.product_id.display_name}: pedir {o.qty_to_order:g} ({o.warehouse_id.name})') for o in ops]


@senal('transferencia_atorada')
def transferencia_atorada(env, cfg):
    n = umbral(cfg, 'dias', 7)
    picks = env['stock.picking'].sudo().search([('company_id', 'in', companias(env)), ('picking_type_code', 'in', ('internal', 'mrp_operation')),
                                                ('state', 'in', ('confirmed', 'waiting')), ('scheduled_date', '<', hace(n))], order='scheduled_date')
    return [fila(f'transferencia_atorada:stock.picking:{p.id}', [doc(p, p.name, origen=p.origin, fecha=str(p.scheduled_date.date()))],
                 valor=dias(p.scheduled_date), valor_texto=f'{p.picking_type_id.name}: {dias(p.scheduled_date)} días en {p.state}',
                 user=p.user_id, payload={'grupo': p.picking_type_id.name, 'fecha_base': str(p.scheduled_date.date())}) for p in picks]


@senal('familia_saturada')
def familia_saturada(env, cfg):
    if not tiene_modelo(env, 'qb.familia.carga'):
        return None
    pct = umbral(cfg, 'pct', 90)
    rows = env['qb.familia.carga'].sudo().search([]).filtered(lambda r: (r.utilization_pct or 0) >= pct)
    return [fila(f'familia_saturada:qb.costeo.familia:{r.familia_id.id}', [doc(r.familia_id, r.familia_id.name, utilizacion=round(r.utilization_pct, 1), capacidad=r.capacity_month_units, carga=r.load_month_units)],
                 valor=round(r.utilization_pct, 1), valor_texto=f'{r.familia_id.name}: {r.utilization_pct:.0f} % de utilización') for r in rows]


@senal('mantenimiento_abierto')
def mantenimiento_abierto(env, cfg):
    if not tiene_modelo(env, 'maintenance.request'):
        return None
    n = umbral(cfg, 'dias', 7)
    Req = env['maintenance.request'].sudo()
    reqs = Req.search([('company_id', 'in', companias(env) + [False]), ('stage_id.done', '=', False), ('archive', '=', False),
                       '|', ('request_date', '<', hace(n)), '&', ('maintenance_type', '=', 'preventive'), ('schedule_date', '<', hoy())])
    return [fila(f'mantenimiento_abierto:maintenance.request:{r.id}', [doc(r, r.name, equipo=r.equipment_id.name, etapa=r.stage_id.name)],
                 valor=dias(r.request_date), valor_texto=f'{r.equipment_id.name or "sin equipo"}: {r.maintenance_type} en {r.stage_id.name} desde hace {dias(r.request_date)} días',
                 vence=r.schedule_date.date() if r.schedule_date else None, user=r.user_id, payload={'fecha_base': str(r.request_date)}) for r in reqs]
