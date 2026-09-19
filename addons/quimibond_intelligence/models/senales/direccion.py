# -*- coding: utf-8 -*-
"""Señales de dirección (spec §4): actividades vencidas por persona, firmas, acuses, acuerdos, obligaciones de qb_obligation (puente)."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar


@senal('carga_actividades')
def carga_actividades(env, cfg):
    zombie = int(((cfg or {}).get('reglas_calidad') or {}).get('zombie_dias') or 180)
    acts = env['mail.activity'].sudo().search([('date_deadline', '<', hoy()), ('user_id.share', '=', False)], order='date_deadline')
    filas = []
    for user, xs in agrupar(acts, lambda a: a.user_id).items():
        for sufijo, grupo, sub in (('', 'vivas', [a for a in xs if dias(a.date_deadline) <= zombie]), (':zombie', 'zombis', [a for a in xs if dias(a.date_deadline) > zombie])):
            if not sub:
                continue
            filas.append(fila(f'carga_actividades:user:{user.id}{sufijo}',
                              [doc(a, f'{a.summary or a.activity_type_id.name} · {a.res_name or a.res_model}'[:120], modelo_doc=a.res_model, vence=str(a.date_deadline)) for a in sub[:30]],
                              valor=len(sub), valor_texto=f'{user.name}: {len(sub)} actividades vencidas, la más vieja {dias(sub[0].date_deadline)} días', user=user,
                              payload={'grupo': grupo, 'fecha_base': str(sub[0].date_deadline), 'dias_max': dias(sub[0].date_deadline)}))
    return filas


@senal('firma_pendiente')
def firma_pendiente(env, cfg):
    if not tiene_modelo(env, 'sign.request'):
        return None
    n = umbral(cfg, 'dias', 7)
    reqs = env['sign.request'].sudo().search([('state', '=', 'sent'), ('create_date', '<', hace(n))], order='create_date')
    return [fila(f'firma_pendiente:sign.request:{r.id}', [doc(r, r.reference or r.display_name, firmantes=', '.join(r.request_item_ids.filtered(lambda i: i.state == 'sent').mapped('partner_id.name'))[:200])],
                 valor=dias(r.create_date), valor_texto=f'{r.reference or r.display_name}: {dias(r.create_date)} días sin firmar', user=r.create_uid,
                 payload={'fecha_base': str(r.create_date.date())}) for r in reqs]


@senal('acuse_documento')
def acuse_documento(env, cfg):
    if not tiene_modelo(env, 'sgi.document.ack'):
        return None
    acks = env['sgi.document.ack'].sudo().search([('state', '=', 'pendiente')])
    filas = []
    for user, xs in agrupar(acks.filtered(lambda a: a.user_id), lambda a: a.user_id).items():
        filas.append(fila(f'acuse_documento:user:{user.id}', [doc(a.document_id, f'{a.sgi_code or ""} {a.document_id.name}'.strip()) for a in xs[:30]],
                          valor=len(xs), valor_texto=f'{user.name}: {len(xs)} documentos sin acusar', user=user))
    return filas


@senal('acuerdo_direccion_vencido')
def acuerdo_direccion_vencido(env, cfg):
    if not tiene_modelo(env, 'sgi.management.review.agreement'):
        return None
    Ag = env['sgi.management.review.agreement'].sudo()
    dom = [('deadline', '<', hoy())]
    if 'state' in Ag._fields:
        dom.append(('state', 'not in', ('done', 'cancel', 'cerrado', 'hecho')))
    xs = Ag.search(dom)
    xs = xs.filtered(lambda x: not x.task_id or x.task_id.state not in ('1_done', '1_canceled'))
    return [fila(f'acuerdo_direccion_vencido:sgi.management.review.agreement:{x.id}', [doc(x, x.name, revision=x.review_id.display_name, limite=str(x.deadline))],
                 valor=dias(x.deadline), valor_texto=f'{x.name}: venció hace {dias(x.deadline)} días', vence=x.deadline, user=x.responsible_id,
                 payload={'fecha_base': str(x.deadline)}) for x in xs]


@senal('obligacion_legado')
def obligacion_legado(env, cfg):
    """Puente hasta retirar qb_obligation (plan B, paso 6). Antes de desinstalarlo hay que mandar un último lote vacío
    (o desactivar la señal en senales_config) para que sus situaciones se resuelvan y no queden sin_datos."""
    if not tiene_modelo(env, 'qb.obligation'):
        return None
    obs = env['qb.obligation'].sudo().search([('company_id', 'in', companias(env)), ('state', 'in', ('candidate', 'confirmed'))], order='date_deadline')
    return [fila(f'obligacion_legado:qb.obligation:{o.id}', [doc(o, o.name, descripcion=(o.description or '')[:300], documento=f'{o.res_model},{o.res_id}' if o.res_model else None)],
                 valor=dias(o.date_deadline) if o.date_deadline and o.date_deadline < hoy() else 0,
                 valor_texto=f'{o.name} ({o.state}); vence {o.date_deadline}', vence=o.date_deadline, partner=o.partner_id, user=o.user_id,
                 payload={'estado': o.state, 'tipo': o.obligation_type, 'area': o.area, 'origen': o.source, 'source_ref': o.source_ref}) for o in obs]
