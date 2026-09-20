# -*- coding: utf-8 -*-
"""Señales de RH (spec §4): aprobaciones, evaluaciones. Los pendientes de correo de RH salen de la memoria."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, doc, fila


@senal('aprobacion_rh')
def aprobacion_rh(env, cfg):
    n = umbral(cfg, 'dias', 2)
    filas, alguno = [], False
    if tiene_modelo(env, 'approval.request'):
        alguno = True
        reqs = env['approval.request'].sudo().search([('request_status', 'in', ('new', 'pending')), ('create_date', '<', hace(n))])
        for r in reqs.filtered(lambda r: 'rh' in (r.category_id.name or '').lower()):
            aprobador = next((a.user_id for a in r.approver_ids if a.status == 'pending'), None)
            filas.append(fila(f'aprobacion_rh:approval.request:{r.id}', [doc(r, r.name, categoria=r.category_id.name)], valor=dias(r.create_date),
                              valor_texto=f'{r.category_id.name}: {r.name} lleva {dias(r.create_date)} días', user=aprobador or r.request_owner_id,
                              payload={'fecha_base': str(r.create_date.date())}))
    if tiene_modelo(env, 'hr.leave'):
        alguno = True
        for l in env['hr.leave'].sudo().search([('state', 'in', ('confirm', 'validate1')), ('create_date', '<', hace(n))]):
            aprobador = l.employee_id.leave_manager_id or l.employee_id.parent_id.user_id
            filas.append(fila(f'aprobacion_rh:hr.leave:{l.id}', [doc(l, l.display_name, empleado=l.employee_id.name, desde=str(l.date_from.date()))], valor=dias(l.create_date),
                              valor_texto=f'permiso de {l.employee_id.name} ({l.holiday_status_id.name}) por aprobar desde hace {dias(l.create_date)} días', vence=l.date_from.date(),
                              user=aprobador, payload={'fecha_base': str(l.create_date.date())}))
    return filas if alguno else None


@senal('evaluacion_vencida')
def evaluacion_vencida(env, cfg):
    filas, alguno = [], False
    if tiene_modelo(env, 'hr.appraisal'):
        alguno = True
        for a in env['hr.appraisal'].sudo().search([('state', 'in', ('new', 'pending')), ('date_close', '<', hoy())]):
            filas.append(fila(f'evaluacion_vencida:hr.appraisal:{a.id}', [doc(a, a.display_name, cierre=str(a.date_close))], valor=dias(a.date_close),
                              valor_texto=f'evaluación de {a.employee_id.name} vencida hace {dias(a.date_close)} días', vence=a.date_close,
                              user=a.manager_ids[:1].user_id if a.manager_ids else None, payload={'fecha_base': str(a.date_close)}))
    if tiene_modelo(env, 'sgi.competence.gap'):
        alguno = True
        # Brecha abierta = nivel actual por debajo del requerido. Confirma si el modelo tiene `state`; si no, usa el filtro de niveles.
        for g in env['sgi.competence.gap'].sudo().search([]):
            if getattr(g, 'state', None) in (None, 'abierta', 'open') or getattr(g, 'gap', 0) > 0:
                filas.append(fila(f'evaluacion_vencida:sgi.competence.gap:{g.id}', [doc(g, f'{g.employee_id.name} · {g.skill_id.name}')], valor=1,
                                  valor_texto=f'{g.employee_id.name}: {g.skill_id.name} en {g.current_level_id.name or "sin nivel"} (requiere {g.required_level_id.name})',
                                  user=g.employee_id.parent_id.user_id))
    return filas if alguno else None
