# -*- coding: utf-8 -*-
"""Señales comerciales (spec §4): entregas, pedidos, leads, márgenes, rentabilidad, cotizaciones."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar, texto_monto


def _pickings(env, code, extra=None):
    return env['stock.picking'].sudo().search([
        ('company_id', 'in', companias(env)), ('picking_type_code', '=', code),
        ('state', 'not in', ('done', 'cancel')), ('scheduled_date', '<', hoy()),
    ] + (extra or []), order='scheduled_date')


def _por_contraparte(picks, prefijo, sale=True):
    filas = []
    for partner, ps in agrupar(picks, lambda p: p.partner_id.commercial_partner_id if p.partner_id else None).items():
        clave = f'{prefijo}:partner:{partner.id}' if partner else f'{prefijo}:partner:0'
        mas_vieja = max(dias(p.scheduled_date) for p in ps)
        filas.append(fila(clave, [doc(p, p.name, origen=p.origin, fecha=str(p.scheduled_date.date())) for p in ps[:30]],
                          valor=len(ps), valor_texto=f'{len(ps)} entregas vencidas, la más vieja {mas_vieja} días',
                          vence=min(p.scheduled_date for p in ps).date(), partner=partner,
                          user=next((p.sale_id.user_id for p in ps if sale and p.sale_id and p.sale_id.user_id), None) or next((p.user_id for p in ps if p.user_id), None),
                          payload={'dias_max': mas_vieja, 'fecha_base': str(min(p.scheduled_date for p in ps).date())}))
    return filas


@senal('entrega_vencida')
def entrega_vencida(env, cfg):
    return _por_contraparte(_pickings(env, 'outgoing'), 'entrega_vencida')


@senal('pedido_sin_fecha')
def pedido_sin_fecha(env, cfg):
    SO = env['sale.order'].sudo()
    orders = SO.search([('company_id', 'in', companias(env)), ('state', '=', 'sale'), ('commitment_date', '=', False)])
    orders = orders.filtered(lambda o: any(l.product_uom_qty > l.qty_delivered and l.product_id.type != 'service' for l in o.order_line))
    filas = []
    for partner, ords in agrupar(orders, lambda o: o.partner_id.commercial_partner_id).items():
        filas.append(fila(f'pedido_sin_fecha:partner:{partner.id}', [doc(o, o.name, monto=o.amount_total, fecha=str(o.date_order.date())) for o in ords[:30]],
                          valor=len(ords), valor_texto=f'{len(ords)} pedidos confirmados sin fecha comprometida', partner=partner,
                          user=next((o.user_id for o in ords if o.user_id), None)))
    return filas


@senal('lead_frio')
def lead_frio(env, cfg):
    if not tiene_modelo(env, 'crm.lead'):
        return None
    n = umbral(cfg, 'dias', 14)
    leads = env['crm.lead'].sudo().search([('company_id', 'in', companias(env) + [False]), ('type', '=', 'opportunity'), ('active', '=', True),
                                           ('probability', '<', 100), ('date_last_stage_update', '<', hace(n))])
    leads = leads.filtered(lambda l: not l.activity_date_deadline or l.activity_date_deadline < hace(n))
    return [fila(f'lead_frio:crm.lead:{l.id}', [doc(l, l.name, monto=l.expected_revenue, etapa=l.stage_id.name)], valor=dias(l.date_last_stage_update),
                 valor_texto=f'{dias(l.date_last_stage_update)} días en {l.stage_id.name}', partner=l.partner_id, user=l.user_id,
                 payload={'fecha_base': str(l.date_last_stage_update.date())}) for l in leads]


@senal('venta_margen_negativo')
def venta_margen_negativo(env, cfg):
    Line = env['sale.order.line'].sudo()
    if 'margin' not in Line._fields:
        return None
    n = umbral(cfg, 'dias', 90)
    lines = Line.search([('company_id', 'in', companias(env)), ('state', '=', 'sale'), ('margin', '<', 0), ('create_date', '>=', hace(n))])
    filas = []
    for partner, ls in agrupar(lines, lambda l: l.order_id.partner_id.commercial_partner_id).items():
        malos = [l for l in ls if not l.purchase_price or not l.price_unit]
        filas.append(fila(f'venta_margen_negativo:partner:{partner.id}',
                          [doc(l, f'{l.order_id.name} · {l.product_id.display_name}'[:120], margen=round(l.margin, 2), precio=l.price_unit, costo=l.purchase_price) for l in ls[:30]],
                          valor=round(sum(l.margin for l in ls), 2), valor_texto=f'{len(ls)} líneas con margen negativo, {texto_monto(sum(l.margin for l in ls))}',
                          partner=partner, user=next((l.order_id.user_id for l in ls if l.order_id.user_id), None),
                          payload={'dato_malo': f'{len(malos)} líneas con costo 0 o precio 0' if len(malos) == len(ls) else None}))
    for f in filas:
        if not f['payload'].get('dato_malo'):
            f['payload'].pop('dato_malo', None)
    return filas


def _rentabilidad(env, modelo, prefijo, campo_id, grupo):
    if not tiene_modelo(env, modelo):
        return None
    recs = env[modelo].sudo().search([]).filtered(lambda r: r.semaforo == 'rojo')  # semaforo es computado no almacenado: filtrar en Python
    filas = []
    for r in recs:
        obj = r[campo_id]
        filas.append(fila(f'{prefijo}:{obj._name}:{obj.id}', [doc(obj, obj.display_name, ventas_12m=round(r.revenue_12m, 2), contribucion=round(r.contrib_12m, 2))],
                          valor=round(r.contrib_12m, 2), valor_texto=f'{r.veredicto or "pierde"}; ventas 12m {texto_monto(r.revenue_12m)}',
                          partner=obj if obj._name == 'res.partner' else None, payload={'grupo': grupo}))
    return filas


@senal('producto_pierde')
def producto_pierde(env, cfg):
    return _rentabilidad(env, 'qb.producto.rentabilidad', 'producto_pierde', 'product_id', 'productos')


@senal('cliente_pierde')
def cliente_pierde(env, cfg):
    return _rentabilidad(env, 'qb.cliente.rentabilidad', 'cliente_pierde', 'partner_id', 'clientes')


@senal('cotizacion_bajo_costo')
def cotizacion_bajo_costo(env, cfg):
    if not tiene_modelo(env, 'qb.cotizacion'):
        return None
    n = umbral(cfg, 'dias_vigencia', 15)
    cots = env['qb.cotizacion'].sudo().search([('state', 'in', ('draft', 'done')), ('semaforo', '=', 'rojo')])
    return [fila(f'cotizacion_bajo_costo:qb.cotizacion:{c.id}', [doc(c, c.display_name)], valor=1,
                 valor_texto=('por vencer el %s' % c.validez_hasta) if c.validez_hasta and c.validez_hasta <= hace(-n) else 'debajo del costo variable',
                 vence=c.validez_hasta, partner=c.partner_id, user=c.create_uid) for c in cots]
