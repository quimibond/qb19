# -*- coding: utf-8 -*-
"""Señales de finanzas (spec §4): cartera, CxP, facturación, banco, CFDI/SAT, nómina, flujo, indicadores."""
from .base import senal, hoy, hace, dias, umbral, tiene_modelo, companias, doc, fila, agrupar, texto_monto


def _vencidas(env, tipo):
    Move = env['account.move'].sudo()
    return Move.search([
        ('company_id', 'in', companias(env)), ('move_type', '=', tipo), ('state', '=', 'posted'),
        ('payment_state', 'in', ('not_paid', 'partial')), ('invoice_date_due', '<', hoy()),
    ], order='invoice_date_due')


def _cartera(env, tipo, prefijo, signo):
    filas = []
    for partner, moves in agrupar(_vencidas(env, tipo), lambda m: m.partner_id.commercial_partner_id).items():
        total = sum(signo * m.amount_residual_signed for m in moves)
        mas_vieja = max(dias(m.invoice_date_due) for m in moves)
        filas.append(fila(
            f'{prefijo}:partner:{partner.id}',
            [doc(m, m.name, monto=round(signo * m.amount_residual_signed, 2), vence=str(m.invoice_date_due), moneda=m.currency_id.name) for m in moves[:30]],
            valor=round(total, 2),
            valor_texto=f'{len(moves)} facturas, la más vieja {mas_vieja} días, {texto_monto(total)}',
            vence=min(m.invoice_date_due for m in moves), partner=partner,
            user=next((m.invoice_user_id for m in moves if m.invoice_user_id), None),
            payload={'dias_max': mas_vieja, 'n': len(moves), 'fecha_base': str(min(m.invoice_date_due for m in moves))}))
    return filas


@senal('cartera_vencida')
def cartera_vencida(env, cfg):
    return _cartera(env, 'out_invoice', 'cartera_vencida', 1)


@senal('cxp_vencida')
def cxp_vencida(env, cfg):
    return _cartera(env, 'in_invoice', 'cxp_vencida', -1)


@senal('factura_proveedor_borrador')
def factura_proveedor_borrador(env, cfg):
    n = umbral(cfg, 'dias', 3)
    Move = env['account.move'].sudo()
    moves = Move.search([('company_id', 'in', companias(env)), ('move_type', '=', 'in_invoice'), ('state', '=', 'draft'),
                         ('create_date', '<', hace(n))])
    return [fila(f'factura_proveedor_borrador:account.move:{m.id}', [doc(m, m.name or m.ref or f'#{m.id}', monto=m.amount_total)],
                 valor=dias(m.create_date), valor_texto=f'{dias(m.create_date)} días en borrador', partner=m.partner_id,
                 user=m.invoice_user_id or m.create_uid, payload={'fecha_base': str(m.create_date.date())}) for m in moves]


@senal('entregado_sin_facturar')
def entregado_sin_facturar(env, cfg):
    SO = env['sale.order'].sudo()
    orders = SO.search([('company_id', 'in', companias(env)), ('state', '=', 'sale'), ('invoice_status', '=', 'to invoice')])
    filas = []
    for partner, ords in agrupar(orders, lambda o: o.partner_id.commercial_partner_id).items():
        total = sum(o.amount_total for o in ords)
        filas.append(fila(f'entregado_sin_facturar:partner:{partner.id}',
                          [doc(o, o.name, monto=o.amount_total, fecha=str(o.date_order.date())) for o in ords[:30]],
                          valor=round(total, 2), valor_texto=f'{len(ords)} pedidos por facturar, {texto_monto(total)}', partner=partner,
                          user=next((o.user_id for o in ords if o.user_id), None)))
    return filas


@senal('banco_sin_conciliar')
def banco_sin_conciliar(env, cfg):
    Line = env['account.bank.statement.line'].sudo()
    lines = Line.search([('company_id', 'in', companias(env)), ('is_reconciled', '=', False)], order='date')
    filas = []
    for journal, ls in agrupar(lines, lambda l: l.journal_id).items():
        filas.append(fila(f'banco_sin_conciliar:journal:{journal.id}',
                          [doc(l, f'{l.date} {l.payment_ref or ""}'[:80], monto=l.amount) for l in ls[:20]],
                          valor=len(ls), valor_texto=f'{len(ls)} movimientos sin conciliar, el más viejo {dias(ls[0].date)} días',
                          payload={'grupo': journal.name, 'dias_max': dias(ls[0].date), 'monto': round(sum(l.amount for l in ls), 2)}))
    return filas


@senal('cfdi_cancelacion_pendiente')
def cfdi_cancelacion_pendiente(env, cfg):
    Move = env['account.move'].sudo()
    if 'l10n_mx_edi_cfdi_state' not in Move._fields:
        return None
    moves = Move.search([('company_id', 'in', companias(env)), ('l10n_mx_edi_cfdi_state', '=', 'cancel_requested')])
    return [fila(f'cfdi_cancelacion_pendiente:account.move:{m.id}', [doc(m, m.name, monto=m.amount_total)], valor=dias(m.write_date),
                 valor_texto=f'cancelación solicitada hace {dias(m.write_date)} días', partner=m.partner_id, user=m.invoice_user_id) for m in moves]


@senal('sat_discrepancia')
def sat_discrepancia(env, cfg):
    if not tiene_modelo(env, 'sat.compare.line'):
        return None
    issues = ('monto', 'moneda', 'cancelado_odoo', 'cancelado_sat', 'solo_sat', 'solo_odoo')
    lines = env['sat.compare.line'].sudo().search([('issue', 'in', issues)])
    filas = []
    for l in lines:
        ref = l.move_id or l.cfdi_id
        filas.append(fila(f'sat_discrepancia:{l.issue}:{l.uuid or l.id}',
                          [doc(ref, l.move_name or l.uuid or '?', uuid=l.uuid)] if ref else [],
                          valor=1, valor_texto=dict(l._fields['issue'].selection).get(l.issue, l.issue),
                          partner=getattr(l, 'partner_id', None) or (l.move_id.partner_id if l.move_id else None),
                          payload={'grupo': l.issue, 'direction': l.direction}))
    return filas


@senal('sat_complemento')
def sat_complemento(env, cfg):
    if not tiene_modelo(env, 'sat.pago.compare'):
        return None
    rows = env['sat.pago.compare'].sudo().search([('issue', 'in', ('sin_complemento', 'complemento_duplicado'))])
    return [fila(f'sat_complemento:{r.issue}:{r.move_id.id if r.move_id else r.cfdi_id.id}', [doc(r.move_id or r.cfdi_id, r.move_name or '?', monto=r.total)],
                 valor=abs(r.delta_mxn or 0), valor_texto=dict(r._fields['issue'].selection).get(r.issue, r.issue), partner=r.partner_id,
                 payload={'grupo': r.issue, 'direction': r.direction, 'saldo_sat': r.saldo_sat, 'saldo_odoo': r.saldo_odoo}) for r in rows]


@senal('sat_extraccion_detenida')
def sat_extraccion_detenida(env, cfg):
    Company = env['res.company'].sudo()
    if 'sat_data_until_issued' not in Company._fields:
        return None
    n = umbral(cfg, 'dias', 3)
    filas = []
    for c in Company.browse(companias(env)):
        for campo, sentido in (('sat_data_until_issued', 'emitidos'), ('sat_data_until_received', 'recibidos')):
            hasta = c[campo]
            if hasta and dias(hasta) > n:
                filas.append(fila(f'sat_extraccion_detenida:{c.id}:{sentido}', [doc(c, c.name)], valor=dias(hasta),
                                  valor_texto=f'{sentido}: datos del SAT hasta {hasta} ({dias(hasta)} días)', payload={'sentido': sentido}))
    return filas


@senal('nomina_borrador')
def nomina_borrador(env, cfg):
    if not tiene_modelo(env, 'hr.payslip'):
        return None
    n = umbral(cfg, 'dias', 3)
    slips = env['hr.payslip'].sudo().search([('company_id', 'in', companias(env)), ('state', '=', 'draft'),
                                             ('date_to', '<', hace(n))])
    return [fila(f'nomina_borrador:hr.payslip:{s.id}', [doc(s, s.name or s.number or f'#{s.id}', periodo=f'{s.date_from}..{s.date_to}')],
                 valor=dias(s.date_to), valor_texto=f'{s.employee_id.name}: periodo al {s.date_to} sin confirmar',
                 payload={'grupo': (s.payslip_run_id.name if s.payslip_run_id else str(s.date_to)[:7]), 'fecha_base': str(s.date_to)}) for s in slips]


@senal('cash_bajo_piso')
def cash_bajo_piso(env, cfg):
    if not tiene_modelo(env, 'cash.flow.forecast.engine') or not tiene_modelo(env, 'cash.flow.config'):
        return None
    filas = []
    for config in env['cash.flow.config'].sudo().search([('company_id', 'in', companias(env))]):
        if not config.forecast_min_cash:
            continue
        res = env['cash.flow.forecast.engine'].compute(config, hoy())
        below = res.get('below_min') or []
        if not below:
            continue
        weeks = res['weeks']
        closing = res['closing']
        runway = next((i for i, v in enumerate(closing) if v <= 0), None)
        primera = weeks[below[0]][0]
        filas.append(fila(f'cash_bajo_piso:company:{config.company_id.id}', [doc(config, config.display_name)],
                          valor=round(min(closing), 2),
                          valor_texto=(f'{len(below)} semanas bajo el piso desde {primera}; mínimo {texto_monto(min(closing))}'
                                       + (f'; efectivo ≤ 0 en la semana del {weeks[runway][0]}' if runway is not None else '')),
                          vence=primera, payload={'semanas_bajo_piso': [str(weeks[i][0]) for i in below], 'piso': config.forecast_min_cash,
                                                  'runway': str(weeks[runway][0]) if runway is not None else None}))
    return filas


def _medidas_rojas(env, cfg, filtro_nombres=None):
    if not tiene_modelo(env, 'sgi.indicator.measure'):
        return None
    n = umbral(cfg, 'dias', 60)
    Measure = env['sgi.indicator.measure'].sudo()
    ms = Measure.search([('semaphore', '=', 'rojo'), ('state', '=', 'validado'), ('period_date', '>=', hace(n))], order='period_date desc')
    if filtro_nombres:
        pats = [p.lower() for p in filtro_nombres]
        ms = ms.filtered(lambda m: any(p in (m.indicator_id.name or '').lower() for p in pats))
    vistos = set()
    out = []
    for m in ms:  # solo la medida más reciente por indicador
        if m.indicator_id.id in vistos:
            continue
        vistos.add(m.indicator_id.id)
        out.append(m)
    return out


@senal('indicador_financiero_rojo')
def indicador_financiero_rojo(env, cfg):
    ms = _medidas_rojas(env, cfg, umbral(cfg, 'nombres', ['DSO', 'cartera', 'DPO']))
    if ms is None:
        return None
    return [fila(f'indicador_financiero_rojo:sgi.indicator:{m.indicator_id.id}', [doc(m, f'{m.indicator_id.name} {m.period_date}', valor=m.value, objetivo=m.target_objective)],
                 valor=m.value, valor_texto=f'{m.indicator_id.name}: {m.value} {m.uom or ""} (objetivo {m.target_objective})', vence=m.period_date,
                 user=getattr(m.indicator_id, 'responsible_id', None) or getattr(m.indicator_id, 'user_id', None),
                 payload={'grupo': m.indicator_id.name, 'periodo': str(m.period_date)}) for m in ms]
