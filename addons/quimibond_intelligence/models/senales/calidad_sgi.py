# -*- coding: utf-8 -*-
"""Señales de calidad / SGI (spec §4). Todas viven en quimibond_sgi (Enterprise): None si no está instalado."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila
from .finanzas import _medidas_rojas


@senal('indicador_rojo')
def indicador_rojo(env, cfg):
    ms = _medidas_rojas(env, cfg)
    if ms is None:
        return None
    return [fila(f'indicador_rojo:sgi.indicator:{m.indicator_id.id}', [doc(m, f'{m.indicator_id.name} {m.period_date}', valor=m.value, objetivo=m.target_objective)],
                 valor=m.value, valor_texto=f'{m.indicator_id.name}: {m.value} {m.uom or ""} (objetivo {m.target_objective})', vence=m.period_date,
                 payload={'grupo': m.indicator_id.name, 'periodo': str(m.period_date)}) for m in ms]


@senal('accion_correctiva_vencida')
def accion_correctiva_vencida(env, cfg):
    if not tiene_modelo(env, 'sgi.action.line'):
        return None
    xs = env['sgi.action.line'].sudo().search([('state', '=', 'vencida')], order='date_commit')
    return [fila(f'accion_correctiva_vencida:sgi.action.line:{x.id}', [doc(x, x.display_name, compromiso=str(x.date_commit))], valor=dias(x.date_commit),
                 valor_texto=f'{x.display_name}: vencida hace {dias(x.date_commit)} días', vence=x.date_commit, user=x.responsible_id,
                 payload={'fecha_base': str(x.date_commit)}) for x in xs]


@senal('nc_abierta')
def nc_abierta(env, cfg):
    if not tiene_modelo(env, 'quality.alert'):
        return None
    n = umbral(cfg, 'dias', 7)
    alerts = env['quality.alert'].sudo().search([('company_id', 'in', companias(env)), ('stage_id.done', '=', False), ('create_date', '<', hace(n))], order='create_date')
    return [fila(f'nc_abierta:quality.alert:{a.id}', [doc(a, a.display_name, etapa=a.stage_id.name)], valor=dias(a.create_date),
                 valor_texto=f'{a.display_name}: {dias(a.create_date)} días en {a.stage_id.name}', partner=a.partner_id, user=a.user_id,
                 payload={'fecha_base': str(a.create_date.date())}) for a in alerts]


@senal('calibracion_vencida')
def calibracion_vencida(env, cfg):
    Eq = env['maintenance.equipment'].sudo() if tiene_modelo(env, 'maintenance.equipment') else None
    if Eq is None or 'sgi_calibration_state' not in Eq._fields:
        return None
    eqs = Eq.search(['|', ('sgi_calibration_state', '=', 'vencido'), ('sgi_do_not_use', '=', True)])
    return [fila(f'calibracion_vencida:maintenance.equipment:{e.id}', [doc(e, e.name, proxima=str(e.sgi_next_calibration_date or ''))],
                 valor=dias(e.sgi_next_calibration_date) or 0, valor_texto=('NO USAR; ' if e.sgi_do_not_use else '') + f'calibración {e.sgi_calibration_state}',
                 vence=e.sgi_next_calibration_date, user=e.technician_user_id) for e in eqs]


@senal('legal_incumplido')
def legal_incumplido(env, cfg):
    if not tiene_modelo(env, 'sgi.legal.requirement'):
        return None
    xs = env['sgi.legal.requirement'].sudo().search(['|', ('compliance_state', 'in', ('no_cumple', 'parcial')), ('next_eval_date', '<=', hoy())])
    return [fila(f'legal_incumplido:sgi.legal.requirement:{x.id}', [doc(x, x.display_name, estado=x.compliance_state)], valor=1,
                 valor_texto=f'{x.display_name}: {x.compliance_state}' + (f', evaluación vencida el {x.next_eval_date}' if x.next_eval_date and x.next_eval_date <= hoy() else ''),
                 vence=x.next_eval_date, user=getattr(x, 'responsible_id', None)) for x in xs]


@senal('riesgo_sin_tratar')
def riesgo_sin_tratar(env, cfg):
    if not tiene_modelo(env, 'sgi.risk'):
        return None
    xs = env['sgi.risk'].sudo().search([('attention_level', 'in', ('inmediata', 'alto')), ('state', '=', 'identificado')])
    return [fila(f'riesgo_sin_tratar:sgi.risk:{x.id}', [doc(x, x.display_name, nivel=x.attention_level, score=x.score)], valor=x.score or 0,
                 valor_texto=f'{x.display_name}: atención {x.attention_level}, sin tratamiento', user=getattr(x, 'responsible_id', None),
                 payload={'grupo': x.process_id.name if x.process_id else 'sin proceso'}) for x in xs]


@senal('ppap_rechazado')
def ppap_rechazado(env, cfg):
    if not tiene_modelo(env, 'sgi.ppap'):
        return None
    xs = env['sgi.ppap'].sudo().search([('state', '=', 'rechazado')])
    return [fila(f'ppap_rechazado:sgi.ppap:{x.id}', [doc(x, x.display_name)], valor=1, valor_texto=f'{x.display_name}: rechazado',
                 partner=getattr(x, 'partner_id', None), user=getattr(x, 'responsible_id', None)) for x in xs]


@senal('auditoria_pendiente')
def auditoria_pendiente(env, cfg):
    if not tiene_modelo(env, 'sgi.audit.program.line'):
        return None
    xs = env['sgi.audit.program.line'].sudo().search([('state', '=', 'pendiente')])
    xs = xs.filtered(lambda x: x.planned_month and int(x.planned_month) < hoy().month and (not getattr(x.program_id, 'year', None) or int(x.program_id.year) <= hoy().year))
    return [fila(f'auditoria_pendiente:sgi.audit.program.line:{x.id}', [doc(x, x.display_name, mes=x.planned_month)], valor=hoy().month - int(x.planned_month),
                 valor_texto=f'{x.display_name}: planeada para el mes {x.planned_month}, sigue pendiente', user=x.lead_auditor_id) for x in xs]


@senal('fuente_sgi_apagada')
def fuente_sgi_apagada(env, cfg):
    if not tiene_modelo(env, 'sgi.alert.source'):
        return None
    xs = env['sgi.alert.source'].sudo().search([('enabled', '=', False), ('suppressed_count', '>', 0)])
    return [fila(f'fuente_sgi_apagada:sgi.alert.source:{x.id}', [doc(x, x.name, codigo=x.code, omitidas=x.suppressed_count)], valor=x.suppressed_count,
                 valor_texto=f'{x.name}: apagada, {x.suppressed_count} alertas omitidas') for x in xs]
