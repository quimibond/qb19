# -*- coding: utf-8 -*-
"""Cálculo puro de los reportes de nómina con el formato de NOI (sin Odoo).

Los reportes de NOI agrupan las líneas del recibo en **conceptos** con clave
propia (P001 SUELDO, D001 ISR…) y clave del SAT. En Odoo cada concepto puede
salir de varias reglas: SUELDO es ``BASIC`` + ``DESC_FALTAS``, HORAS EXTRAS es
``HE_TAX`` (gravado) + ``HE_EXEMPT`` (exento), el ISR que NOI imprime es el ISR
menos el subsidio aplicado. El catálogo (``qb.nomina.concepto.noi``) dice qué
reglas forman cada concepto; aquí sólo se suma.

Convención de signos (la de las líneas del recibo de Odoo):

* percepción: importe = Σ total de sus reglas (las percepciones son positivas);
* deducción e informativo: importe = −Σ total (las deducciones son negativas);
* un código con ``-`` al frente entra con el signo contrario. ``IMSS`` usa
  ``-IMSS_EMPLOYEE_TOTAL`` porque esa línea de Odoo es positiva.

Lo que tiene concepto de CFDI (``l10n_mx_concept``), sale en el recibo, no
está en el catálogo y no es cero se reporta solo, con el nombre de la regla y
sin clave NOI: así nada del recibo se pierde aunque falte darlo de alta.

Este archivo no importa Odoo para poder probarlo con pytest puro
(``tests_puros/nomina``)."""

TIPOS = ('percepcion', 'deduccion', 'informativo')
_ORDEN_TIPO = {t: i for i, t in enumerate(TIPOS)}
CFDI_A_TIPO = {'perception': 'percepcion', 'deduction': 'deduccion', 'other': 'informativo'}


def parsear_codigos(texto):
    """``'ISR, SUBSIDY,-X'`` → ``[('ISR', 1), ('SUBSIDY', 1), ('X', -1)]``."""
    res = []
    for parte in (texto or '').replace(';', ',').replace('\n', ',').split(','):
        parte = parte.strip()
        if not parte:
            continue
        signo = 1
        if parte.startswith('-'):
            signo, parte = -1, parte[1:].strip()
        elif parte.startswith('+'):
            parte = parte[1:].strip()
        if parte:
            res.append((parte, signo))
    return res


def r2(valor):
    return round(valor + 0.0, 2) + 0.0


def clave_orden(clave, nombre=''):
    """Orden de NOI: por clave (P001 < P003 < P010); sin clave, al final por nombre."""
    return (0 if clave else 1, clave or '', nombre or '')


def clave_empleado_orden(referencia):
    """``S-2`` antes que ``S-10``; luego Q-, C-… por prefijo. Sin referencia al final."""
    ref = (referencia or '').strip()
    if not ref:
        return ('~', 0, '')
    prefijo, _, numero = ref.rpartition('-')
    try:
        return (prefijo, int(numero), ref)
    except ValueError:
        return (prefijo or ref, 0, ref)


def conceptos_de_recibo(lineas, conceptos):
    """Agrupa las líneas de UN recibo en conceptos de NOI.

    :param lineas: ``{codigo: {'total': float, 'nombre': str, 'en_recibo': bool,
        'cfdi_tipo': 'perception'|'deduction'|'other'|None, 'sat': str,
        'gravable': bool}}``
    :param conceptos: lista de ``{'id', 'clave', 'nombre', 'tipo', 'sat',
        'codigos', 'codigos_exento', 'secuencia'}`` (``codigos`` y
        ``codigos_exento`` en texto, como en el catálogo).
    :return: lista de renglones ``{'llave', 'clave', 'nombre', 'tipo', 'sat',
        'importe', 'gravado', 'exento'}`` con importe distinto de cero, en el
        orden de NOI.
    """
    usados = set()
    renglones = []
    for c in conceptos:
        codigos = parsear_codigos(c.get('codigos'))
        exentos = {cod for cod, _s in parsear_codigos(c.get('codigos_exento'))}
        if not codigos:
            continue
        signo_tipo = 1 if c['tipo'] == 'percepcion' else -1
        importe = exento = 0.0
        for cod, signo in codigos:
            usados.add(cod)
            linea = lineas.get(cod)
            if not linea:
                continue
            valor = signo_tipo * signo * (linea.get('total') or 0.0)
            importe += valor
            if cod in exentos:
                exento += valor
        importe = r2(importe)
        if abs(importe) < 0.005:
            continue
        exento = r2(exento) if c['tipo'] == 'percepcion' else 0.0
        gravado = r2(importe - exento) if c['tipo'] == 'percepcion' else 0.0
        renglones.append({
            'llave': c.get('clave') or c.get('nombre'),
            'clave': c.get('clave') or '',
            'nombre': c.get('nombre') or '',
            'tipo': c['tipo'],
            'sat': c.get('sat') or '',
            'importe': importe,
            'gravado': gravado,
            'exento': exento,
            'secuencia': c.get('secuencia', 0),
        })
    # Lo que el catálogo no cubre y sí va en el CFDI: se reporta con el nombre de la regla.
    for cod, linea in lineas.items():
        if cod in usados or not linea.get('en_recibo'):
            continue
        tipo = CFDI_A_TIPO.get(linea.get('cfdi_tipo') or '')
        if not tipo:
            continue
        signo_tipo = 1 if tipo == 'percepcion' else -1
        importe = r2(signo_tipo * (linea.get('total') or 0.0))
        if abs(importe) < 0.005:
            continue
        gravable = linea.get('gravable')
        renglones.append({
            'llave': 'regla:%s' % cod,
            'clave': '',
            'nombre': linea.get('nombre') or cod,
            'tipo': tipo,
            'sat': linea.get('sat') or '',
            'importe': importe,
            'gravado': importe if tipo == 'percepcion' and gravable else 0.0,
            'exento': importe if tipo == 'percepcion' and not gravable else 0.0,
            'secuencia': 9999,
        })
    renglones.sort(key=lambda r: (_ORDEN_TIPO[r['tipo']],) + clave_orden(r['clave'], r['nombre']))
    return renglones


def totales_de_recibo(renglones, neto=None):
    """Totales del bloque de un trabajador, como el pie de cada recibo de NOI."""
    perc = r2(sum(r['importe'] for r in renglones if r['tipo'] == 'percepcion'))
    ded = r2(sum(r['importe'] for r in renglones if r['tipo'] == 'deduccion'))
    calculado = r2(perc - ded)
    res = {
        'percepciones': perc,
        'deducciones': ded,
        'gravado': r2(sum(r['gravado'] for r in renglones if r['tipo'] == 'percepcion')),
        'exento': r2(sum(r['exento'] for r in renglones if r['tipo'] == 'percepcion')),
        'informativo': r2(sum(r['importe'] for r in renglones if r['tipo'] == 'informativo')),
        'neto': calculado if neto is None else r2(neto),
    }
    # Si el neto del recibo no es percepciones − deducciones, falta (o sobra) un concepto en el catálogo.
    res['diferencia'] = r2(res['neto'] - calculado)
    return res


def resumen(recibos):
    """Hoja final de NOI: importe, gravado y exento por concepto y los totales.

    :param recibos: lista de ``{'renglones': [...], 'totales': {...}}``.
    """
    por_llave = {}
    for rec in recibos:
        for r in rec['renglones']:
            acc = por_llave.setdefault(r['llave'], dict(r, importe=0.0, gravado=0.0, exento=0.0, trabajadores=0))
            acc['importe'] += r['importe']
            acc['gravado'] += r['gravado']
            acc['exento'] += r['exento']
            acc['trabajadores'] += 1
    filas = []
    for acc in por_llave.values():
        acc['importe'], acc['gravado'], acc['exento'] = r2(acc['importe']), r2(acc['gravado']), r2(acc['exento'])
        filas.append(acc)
    filas.sort(key=lambda r: (_ORDEN_TIPO[r['tipo']],) + clave_orden(r['clave'], r['nombre']))
    tot = [rec['totales'] for rec in recibos]
    return {
        'percepciones': [f for f in filas if f['tipo'] == 'percepcion'],
        'deducciones': [f for f in filas if f['tipo'] == 'deduccion'],
        'informativos': [f for f in filas if f['tipo'] == 'informativo'],
        'total_percepciones': r2(sum(t['percepciones'] for t in tot)),
        'total_gravado': r2(sum(t['gravado'] for t in tot)),
        'total_exento': r2(sum(t['exento'] for t in tot)),
        'total_deducciones': r2(sum(t['deducciones'] for t in tot)),
        'total_informativo': r2(sum(t['informativo'] for t in tot)),
        'neto': r2(sum(t['neto'] for t in tot)),
        'diferencia': r2(sum(t['diferencia'] for t in tot)),
        'recibos': len(recibos),
    }


def por_concepto(recibos, llaves=None):
    """Reporte «Percepción/deducción» de NOI: por concepto, los trabajadores con
    su importe y el total. Un trabajador con varios recibos (varias semanas) se
    suma en un renglón.

    :param recibos: lista de ``{'empleado': {'id', 'clave', 'nombre'}, 'renglones': [...]}``.
    :param llaves: conjunto de llaves de concepto a incluir; ``None`` = todos.
    """
    conceptos = {}
    for rec in recibos:
        emp = rec['empleado']
        for r in rec['renglones']:
            if llaves is not None and r['llave'] not in llaves:
                continue
            c = conceptos.setdefault(r['llave'], {
                'llave': r['llave'], 'clave': r['clave'], 'nombre': r['nombre'], 'tipo': r['tipo'],
                'sat': r['sat'], 'empleados': {}, 'total': 0.0})
            e = c['empleados'].setdefault(emp['id'], {'clave': emp['clave'], 'nombre': emp['nombre'], 'importe': 0.0})
            e['importe'] += r['importe']
            c['total'] += r['importe']
    res = []
    for c in conceptos.values():
        empleados = [dict(e, importe=r2(e['importe'])) for e in c['empleados'].values()]
        empleados = [e for e in empleados if abs(e['importe']) >= 0.005]
        if not empleados:
            continue
        empleados.sort(key=lambda e: clave_empleado_orden(e['clave']))
        res.append(dict(c, empleados=empleados, total=r2(c['total'])))
    res.sort(key=lambda c: (_ORDEN_TIPO[c['tipo']],) + clave_orden(c['clave'], c['nombre']))
    return res
