# -*- coding: utf-8 -*-
"""Cálculo puro del nodo ``nomina12:HorasExtra`` (sin Odoo, para poder probarlo
en cualquier lado).

El módulo ``l10n_mx_hr_payroll_account_edi`` de Odoo 19 nunca emite el nodo:
escribe cada ``nomina12:Percepcion`` como elemento vacío, incluidas las de
``TipoPercepcion="019"`` (horas extra). El Anexo 20 del SAT obliga a que toda
percepción 019 lleve al menos un ``HorasExtra``; sin él el PAC rechaza el
CFDI. En una semana normal, 39 de 86 recibos traen horas extra.

Cuatro atributos, los cuatro obligatorios:

* ``TipoHoras`` — catálogo ``c_TipoHoras``: ``01`` dobles, ``02`` triples,
  ``03`` simples. Las horas sencillas de Quimibond NO llevan nodo: van al
  concepto 038, no al 019.
* ``HorasExtra`` — entero, número de horas.
* ``ImportePagado`` — lo pagado por esas horas (gravado + exento).
* ``Dias`` — ver abajo.

``Dias`` es un dato capturado, no una fórmula
----------------------------------------------

``Dias`` es **el número real de días en que se generó el tiempo extra**, y en
NOI se captura: tres horas en un solo día son 1 día; una hora en tres días
son 3 días. Las mismas tres horas dan ``Dias=1`` en un recibo (Ricardo
Salgado, quincena 18 de CDMX: ``Dias="1" HorasExtra="3"``) y ``Dias=3`` en
otro (semanal de Toluca). No se deduce de las horas.

La primera versión de este módulo traía una regla sacada de medir 142 nodos
de NOI (3 por semana del periodo, 1 si fueron una o dos horas). Describía bien
el caso más común (9 horas en 3 días de 3, el 55 % de los casos) y lo
generalizó de más: la quincena de CDMX la desmintió.

Hoy:

1. Si el recibo trae la entrada ``HE_DIAS`` ("Días con tiempo extra") con
   cantidad > 0, **ese valor es ``Dias``**, sin tocarlo. Es el dato que RH
   captura junto con las horas. Aplica a **todos** los nodos del recibo
   (dobles y triples): son los días en que hubo tiempo extra, no los días de
   cada tipo.
2. Si no la trae, se **estima** con la heurística acotada por las horas:
   ``max(1, min(3 × semanas, ceil(horas)))``. 3 horas dan 3, 9 dan 3, 18 en
   quincena dan 6, 1 da 1. Sigue equivocándose en casos como el de Ricardo
   (no hay manera de acertar sin el dato) pero nunca emite un valor imposible
   (más días que horas, cero, o más de 3 por semana). ``Dias`` es informativo
   y el SAT no lo valida contra nada más.
"""
import math

# Entrada (hr.payslip.input.type) con los días en que hubo tiempo extra.
INPUT_HE_DIAS = 'HE_DIAS'

# Código del tipo de entrada (hr.payslip.input.type) → c_TipoHoras del SAT.
TIPO_HORAS_POR_INPUT = {
    'HE_DOBLE': '01',
    'HE_TRIPLE': '02',
}

# Peso relativo de cada tipo al repartir ImportePagado cuando un recibo trae
# dobles y triples: una hora doble se paga al 200 %, una triple al 300 %.
FACTOR_POR_TIPO = {
    '01': 2.0,
    '02': 3.0,
    '03': 1.0,
}


def horas_extra_dias(horas, dias_periodo, dias_capturados=None):
    """``Dias`` del nodo. Con ``dias_capturados`` (entrada ``HE_DIAS`` del
    recibo) devuelve ese valor; sin él, la ESTIMACIÓN acotada por las horas.

    >>> horas_extra_dias(3, 7), horas_extra_dias(9, 7), horas_extra_dias(18, 15), horas_extra_dias(1, 7)
    (3, 3, 6, 1)
    >>> horas_extra_dias(3, 7, dias_capturados=1)
    1
    """
    if dias_capturados:
        return max(1, int(round(dias_capturados)))
    semanas = max(int(dias_periodo / 7), 1)      # 1 en semanal, 2 en quincenal
    # Tope legal (LFT art. 66: 3 días por semana), nunca más días que horas
    # (1 hora → 1 día) y nunca 0.
    return max(1, min(3 * semanas, int(math.ceil(horas))))


def horas_extra_nodos(horas_por_tipo, importe_total, dias_periodo, dias_capturados=None):
    """Lista de nodos ``HorasExtra`` (dicts con las llaves en minúsculas) para
    un recibo.

    ``horas_por_tipo``: ``{'01': 9.0, '02': 3.0}`` (sólo tipos con horas > 0
    producen nodo). ``importe_total``: lo pagado por TODAS las horas extra del
    recibo (gravado + exento). Con un solo tipo, el nodo lleva el total; con
    varios se reparte en proporción a horas × factor y el último absorbe el
    redondeo, de modo que la suma de ``ImportePagado`` es exactamente el total.
    ``dias_capturados``: la entrada ``HE_DIAS`` del recibo; vale para todos los
    nodos. Sin ella, ``Dias`` se estima (ver arriba).
    """
    tipos = [(t, float(h)) for t, h in sorted(horas_por_tipo.items()) if h and h > 0]
    if not tipos:
        return []
    peso_total = sum(h * FACTOR_POR_TIPO.get(t, 1.0) for t, h in tipos)
    nodos, acumulado = [], 0.0
    for i, (tipo, horas) in enumerate(tipos):
        if i == len(tipos) - 1:
            importe = round(importe_total - acumulado, 2)
        else:
            importe = round(importe_total * horas * FACTOR_POR_TIPO.get(tipo, 1.0) / peso_total, 2)
            acumulado += importe
        nodos.append({
            'tipo_horas': tipo,
            'horas_extra': int(round(horas)),
            'dias': horas_extra_dias(horas, dias_periodo, dias_capturados),
            'importe_pagado': '%.2f' % importe,
        })
    return nodos


def es_percepcion_019(item):
    """¿Este elemento de ``percepcion_list`` es la percepción de horas extra?

    No se conoce la forma exacta del dict (las llaves las pone el módulo de
    Odoo), así que se busca ``'019'`` primero en las llaves que hablan de tipo
    y, si no hay, en cualquier valor."""
    if isinstance(item, dict):
        pares = list(item.items())
    else:
        pares = [(k, getattr(item, k)) for k in dir(item) if not k.startswith('_')]
    con_tipo = [v for k, v in pares if 'tipo' in str(k).lower()]
    candidatos = con_tipo or [v for _, v in pares]
    return any(isinstance(v, (str, int)) and str(v).strip() == '019' for v in candidatos)


def importe_de_percepcion(item):
    """Gravado + exento de un elemento de ``percepcion_list``, o None si no se
    reconocen las llaves."""
    if not isinstance(item, dict):
        return None
    gravado = exento = None
    for k, v in item.items():
        lk = str(k).lower()
        if 'gravado' in lk:
            gravado = v
        elif 'exento' in lk:
            exento = v
    if gravado is None and exento is None:
        return None
    try:
        return float(gravado or 0.0) + float(exento or 0.0)
    except (TypeError, ValueError):
        return None


NS_NOMINA12 = 'http://www.sat.gob.mx/nomina12'


def loop_var_de_percepcion(arch_root):
    """Nombre de la variable del ``t-foreach`` que genera ``nomina12:Percepcion``
    en la plantilla del CFDI (un ``lxml`` ya parseado), o None si no se
    reconoce la estructura.

    Se exige que el ``t-foreach`` itere exactamente ``percepcion_list``: es la
    lista que ``_qb_add_horas_extra`` anota por índice. Si Odoo cambia la
    plantilla, esto devuelve None y la herencia no se activa (más vale un CFDI
    sin nodo que una instalación rota o un nodo en el lugar equivocado)."""
    percepciones = [el for el in arch_root.iter()
                    if isinstance(el.tag, str) and el.tag == '{%s}Percepcion' % NS_NOMINA12]
    if len(percepciones) != 1:
        return None
    el = percepciones[0]
    while el is not None:
        expr = el.get('t-foreach')
        if expr is not None:
            if expr.strip() != 'percepcion_list' or not el.get('t-as'):
                return None
            return el.get('t-as').strip()
        el = el.getparent()
    return None


def arch_herencia_horas_extra(loop_var):
    """Arch de la vista que hereda la plantilla del CFDI y mete los nodos
    ``HorasExtra`` dentro de cada ``nomina12:Percepcion``.

    Los nodos se leen de ``qb_horas_extra_por_indice`` (dict índice → lista),
    usando el índice del ``t-foreach`` (``<var>_index``) y no el elemento
    mismo: así no importa cómo esté hecho cada elemento de ``percepcion_list``.
    El xpath localiza el nodo por nombre local, sin depender del prefijo."""
    return (
        '<data>\n'
        '  <xpath expr="//*[local-name()=\'Percepciones\']//*[local-name()=\'Percepcion\']" position="inside">\n'
        '    <t t-foreach="(qb_horas_extra_por_indice or {}).get(%(var)s_index) or []" t-as="qb_he">\n'
        '      <nomina12:HorasExtra xmlns:nomina12="%(ns)s"\n'
        '          t-att-Dias="qb_he[\'dias\']"\n'
        '          t-att-TipoHoras="qb_he[\'tipo_horas\']"\n'
        '          t-att-HorasExtra="qb_he[\'horas_extra\']"\n'
        '          t-att-ImportePagado="qb_he[\'importe_pagado\']"/>\n'
        '    </t>\n'
        '  </xpath>\n'
        '</data>'
    ) % {'var': loop_var, 'ns': NS_NOMINA12}
