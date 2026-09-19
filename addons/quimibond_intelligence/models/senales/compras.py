# -*- coding: utf-8 -*-
"""Señales de compras (spec §4): recepciones, órdenes sin acuse, aprobaciones, precios, actividades, proveedores reprobados."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar
from .comercial import _pickings, _por_contraparte


@senal('recepcion_vencida')
def recepcion_vencida(env, cfg):
    return _por_contraparte(_pickings(env, 'incoming'), 'recepcion_vencida', sale=False)


@senal('oc_sin_confirmacion')
def oc_sin_confirmacion(env, cfg):
    n = umbral(cfg, 'dias', 5)
    pos = env['purchase.order'].sudo().search([('company_id', 'in', companias(env)), ('state', '=', 'purchase'),
                                               ('date_approve', '<', hace(n)), ('partner_ref', '=', False), ('effective_date', '=', False)])
    # Las recepciones de proveedor nacen en 'assigned' (sin reserva): solo una recepción hecha cuenta como acuse.
    pos = pos.filtered(lambda p: not any(pk.state == 'done' for pk in p.picking_ids))
    filas = []
    for partner, ps in agrupar(pos, lambda p: p.partner_id.commercial_partner_id).items():
        filas.append(fila(f'oc_sin_confirmacion:partner:{partner.id}', [doc(p, p.name, monto=p.amount_total, aprobada=str(p.date_approve.date())) for p in ps[:30]],
                          valor=len(ps), valor_texto=f'{len(ps)} órdenes sin acuse del proveedor, la más vieja {max(dias(p.date_approve) for p in ps)} días',
                          partner=partner, user=next((p.user_id for p in ps if p.user_id), None),
                          payload={'fecha_base': str(min(p.date_approve for p in ps).date())}))
    return filas


@senal('aprobacion_pendiente')
def aprobacion_pendiente(env, cfg):
    n = umbral(cfg, 'dias', 2)
    filas = []
    if tiene_modelo(env, 'approval.request'):
        reqs = env['approval.request'].sudo().search([('request_status', 'in', ('new', 'pending')), ('create_date', '<', hace(n))])
        reqs = reqs.filtered(lambda r: 'rh' not in (r.category_id.name or '').lower())  # las de RH van en aprobacion_rh
        for r in reqs:
            aprobador = next((a.user_id for a in r.approver_ids if a.status == 'pending'), None)
            filas.append(fila(f'aprobacion_pendiente:approval.request:{r.id}', [doc(r, r.name, categoria=r.category_id.name, solicita=r.request_owner_id.name)],
                              valor=dias(r.create_date), valor_texto=f'{r.category_id.name}: {r.name} lleva {dias(r.create_date)} días', user=aprobador or r.request_owner_id,
                              payload={'fecha_base': str(r.create_date.date())}))
    if tiene_modelo(env, 'purchase.requisition'):
        for r in env['purchase.requisition'].sudo().search([('company_id', 'in', companias(env)), ('state', 'in', ('in_progress', 'open', 'confirmed'))]):
            filas.append(fila(f'aprobacion_pendiente:purchase.requisition:{r.id}', [doc(r, r.name)], valor=dias(r.create_date),
                              valor_texto=f'acuerdo de compra {r.name} en {r.state}', user=r.user_id, payload={'fecha_base': str(r.create_date.date())}))
    if not tiene_modelo(env, 'approval.request') and not tiene_modelo(env, 'purchase.requisition'):
        return None
    return filas


@senal('precio_compra_subio')
def precio_compra_subio(env, cfg):
    pct, meses, n = umbral(cfg, 'pct', 15), umbral(cfg, 'meses', 6), umbral(cfg, 'dias', 30)
    Line = env['purchase.order.line'].sudo()
    recientes = Line.search([('company_id', 'in', companias(env)), ('state', 'in', ('purchase', 'done')), ('date_approve', '>=', hace(n)), ('price_unit', '>', 0)])
    filas = []
    for product, ls in agrupar(recientes, lambda l: l.product_id).items():
        prev = Line.read_group([('company_id', 'in', companias(env)), ('state', 'in', ('purchase', 'done')), ('product_id', '=', product.id),
                                ('date_approve', '<', hace(n)), ('date_approve', '>=', hace(n + 30 * meses)), ('price_unit', '>', 0)],
                               ['price_unit:avg'], [])
        prom = prev and prev[0].get('price_unit')
        if not prom:
            continue
        ultima = max(ls, key=lambda l: l.date_approve)
        subida = (ultima.price_unit - prom) / prom * 100
        if subida >= pct:
            filas.append(fila(f'precio_compra_subio:product:{product.id}', [doc(ultima.order_id, ultima.order_id.name, precio=ultima.price_unit, promedio=round(prom, 2))],
                              valor=round(subida, 1), valor_texto=f'{product.display_name}: {ultima.price_unit:.2f} vs promedio {prom:.2f} ({subida:+.0f} %)',
                              partner=ultima.partner_id, user=ultima.order_id.user_id))
    return filas


@senal('actividad_vencida_oc')
def actividad_vencida_oc(env, cfg):
    acts = env['mail.activity'].sudo().search([('res_model', '=', 'purchase.order'), ('date_deadline', '<', hoy())], order='date_deadline')
    filas = []
    for user, xs in agrupar(acts, lambda a: a.user_id).items():
        filas.append(fila(f'actividad_vencida_oc:user:{user.id}', [doc(a, f'{a.summary or a.activity_type_id.name} · {a.res_name}'[:120], vence=str(a.date_deadline)) for a in xs[:30]],
                          valor=len(xs), valor_texto=f'{len(xs)} actividades vencidas sobre compras, la más vieja {dias(xs[0].date_deadline)} días', user=user,
                          payload={'fecha_base': str(xs[0].date_deadline)}))
    return filas


@senal('proveedor_reprobado')
def proveedor_reprobado(env, cfg):
    if not tiene_modelo(env, 'sgi.supplier.eval'):
        return None
    minimo = umbral(cfg, 'score', 70)
    Eval = env['sgi.supplier.eval'].sudo()
    # sgi.supplier.eval: partner_id y periodo date_from/date_to (verificado); la última evaluación por proveedor es la de mayor date_to.
    vistos, filas = set(), []
    for ev in Eval.search([], order='date_to desc, id desc'):
        pid = ev.partner_id.id
        if pid in vistos:
            continue
        vistos.add(pid)
        if (ev.score or 0) < minimo:
            filas.append(fila(f'proveedor_reprobado:partner:{ev.partner_id.commercial_partner_id.id}', [doc(ev, ev.display_name, score=ev.score)],
                              valor=ev.score, valor_texto=f'{ev.partner_id.name}: calificación {ev.score:.0f} (< {minimo})', partner=ev.partner_id))
    return filas
