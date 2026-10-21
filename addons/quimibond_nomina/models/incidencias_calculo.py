# -*- coding: utf-8 -*-
"""Cálculo de la lista de incidencias semanal desde las checadas del reloj.

Funciones puras (sin Odoo) para poder probarlas solas; ``incidencias.py`` las
alimenta con las checadas, el horario del contrato y las ausencias.

Las reglas son las de la lista F-P-A01-49 de RH (semanas 1 a 43 de 2026) y las
respuestas de RH del 20 y 21 de octubre de 2026:

* **La base es el horario**, no las horas checadas: lunes a jueves 10 y viernes
  8 (48 a la semana), también para quien trabaja turnos de 12 horas. Lo que se
  trabaja de más es tiempo extra.
* **Turno**: las checadas de una persona separadas por menos de 8 horas son un
  mismo turno (entrada, salida a comer, regreso, salida). Cuenta la primera y la
  última; la comida va dentro de la jornada. El turno es del día de su **hora de
  entrada de referencia**, la más cercana entre las entradas de la planta
  (07:00 y 19:00): el turno de 19:00 a 07:00 es del día en que entra.
* **Falta**: un día con horario, dentro del contrato, sin turno y sin ausencia
  registrada. Quita el día y 2 horas más de lunes a jueves (12 horas; el viernes
  solo las 8), como la lista de RH; NOI lo descuenta como horas / 8.
* **Incapacidad** (ausencia del IMSS): «I», sin castigo; cuenta días naturales.
  **Vacaciones y permisos con goce**: el día se paga. **Permiso sin goce**: se
  quitan sus horas, sin las 2 de castigo.
* **Retardo**: desde el minuto 1 después de la entrada de referencia (los 10
  minutos de tolerancia son para entrar a la planta, no para el premio). Las
  **salidas antes** no se descuentan (se reponen); solo se anotan.
* **Tiempo extra según checador**: lo que el turno pasa de las horas del día
  (todo el turno en sábado y domingo), contado desde la entrada de referencia.
  Se proponen las horas completas; el supervisor autoriza (nadie trabaja tiempo
  extra sin solicitud). Las primeras 9 de la semana son dobles y el resto
  sencillas, como hoy.
* **Prima dominical**: las horas del turno que toca el domingo, para el que
  entra el sábado en la noche y para el que entra el domingo.
* Sin ninguna checada en la semana (la persona no ha registrado su huella), los
  días no se marcan como falta sino como «?».
"""
from datetime import datetime, time, timedelta
from math import floor

SEPARACION_TURNOS = timedelta(hours=8)
ENTRADAS_DEFAULT = (time(7, 0), time(19, 0))
HORAS_CASTIGO_FALTA = 2          # lunes a jueves, como la lista de RH
DIA_SIN_CASTIGO = 4              # viernes: la falta quita solo las horas del día
HE_DOBLES_TOPE = 9               # por semana (LFT art. 66-68)
UN_MINUTO = timedelta(minutes=1)
CODIGOS_INCAPACIDAD = ('I',)
CODIGOS_PAGADOS = ('V', 'P')
CODIGO_SIN_GOCE = 'SG'
NOMBRES_DIA = ('Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa', 'Do')


def leer_entradas(texto):
    """``"07:00,19:00"`` → tupla de ``time``. Vacío o inválido → las de la planta."""
    entradas = []
    for parte in (texto or '').split(','):
        parte = parte.strip()
        if not parte:
            continue
        try:
            h, m = parte.split(':')
            entradas.append(time(int(h), int(m)))
        except (ValueError, TypeError):
            return ENTRADAS_DEFAULT
    return tuple(entradas) or ENTRADAS_DEFAULT


def agrupar_turnos(marcas, separacion=SEPARACION_TURNOS):
    """Checadas (datetimes locales) → ``[(primera, última, cuántas)]``."""
    turnos = []
    for marca in sorted(marcas):
        if turnos and marca - turnos[-1][1] < separacion:
            turnos[-1][1] = marca
            turnos[-1][2] += 1
        else:
            turnos.append([marca, marca, 1])
    return [tuple(t) for t in turnos]


def entrada_referencia(inicio, entradas=ENTRADAS_DEFAULT):
    """La entrada de la planta más cercana a la primera checada del turno."""
    candidatos = [datetime.combine(inicio.date() + timedelta(days=d), e)
                  for d in (-1, 0, 1) for e in entradas]
    return min(candidatos, key=lambda c: abs((inicio - c).total_seconds()))


def _fmt(horas):
    return ('%.1f' % horas).rstrip('0').rstrip('.')


def calcular_semana(lunes, programado, marcas, ausencias=None, contrato_desde=None,
                    contrato_hasta=None, hoy=None, entradas=ENTRADAS_DEFAULT):
    """Una persona, una semana de nómina (lunes a domingo).

    ``programado``: ``{0..6: horas}`` del horario. ``marcas``: checadas en hora
    local. ``ausencias``: ``{date: (código, horas_sin_goce)}`` con código ``I``,
    ``V``, ``P`` o ``SG``. ``hoy``: los días posteriores se proyectan con el
    horario (como hace RH al reportar el martes)."""
    ausencias = ausencias or {}
    domingo = lunes + timedelta(days=6)
    r = {
        'dias': [''] * 7, 'horas_base': 0.0, 'faltas': 0, 'falta_horas': 0.0, 'incap_dias': 0,
        'horas_sin_goce': 0.0, 'retardos': 0, 'retardo_min': 0, 'salidas_antes': 0,
        'salida_min': 0, 'extra_horas': 0.0, 'he_propuestas': 0, 'dias_con_extra': 0,
        'dom_horas': 0.0, 'sin_checadas': False, 'proyectado': False, 'notas': [],
    }

    def en_contrato(dia):
        return (not contrato_desde or dia >= contrato_desde) and (not contrato_hasta or dia <= contrato_hasta)

    turnos_dia = {}
    for inicio, fin, n in agrupar_turnos(marcas):
        ref = entrada_referencia(inicio, entradas)
        if lunes <= ref.date() <= domingo:
            turnos_dia.setdefault(ref.date(), []).append((ref, inicio, fin, n))
    hay_checadas = bool(turnos_dia)

    # Días: lo que va en las columnas de la lista.
    for i in range(7):
        dia = lunes + timedelta(days=i)
        prog = programado.get(i, 0.0)
        if not en_contrato(dia):
            r['dias'][i] = '—'
            continue
        codigo, horas_sg = ausencias.get(dia, (None, 0.0))
        if codigo in CODIGOS_INCAPACIDAD:
            r['dias'][i] = 'I'
            r['incap_dias'] += 1
            continue
        if not prog:
            continue
        if codigo in CODIGOS_PAGADOS:
            r['dias'][i] = codigo
            r['horas_base'] += prog
        elif codigo == CODIGO_SIN_GOCE:
            quita = min(horas_sg or prog, prog)
            r['dias'][i] = _fmt(prog - quita)
            r['horas_base'] += prog - quita
            r['horas_sin_goce'] += quita
            r['notas'].append('%s permiso sin goce %s h' % (NOMBRES_DIA[i], _fmt(quita)))
        elif turnos_dia.get(dia):
            r['dias'][i] = _fmt(prog)
            r['horas_base'] += prog
        elif hoy and dia > hoy:
            r['dias'][i] = _fmt(prog)
            r['horas_base'] += prog
            r['proyectado'] = True
        elif not hay_checadas:
            r['dias'][i] = '?'
            r['horas_base'] += prog
            r['sin_checadas'] = True
        else:
            r['dias'][i] = '0'
            r['faltas'] += 1
            r['falta_horas'] += prog + (HORAS_CASTIGO_FALTA if i < DIA_SIN_CASTIGO else 0)
            r['notas'].append('%s falta' % NOMBRES_DIA[i])

    # Turnos: retardos, salidas antes, tiempo extra y prima dominical.
    for dia in sorted(turnos_dia):
        i = dia.weekday()
        incapacitado = ausencias.get(dia, (None,))[0] in CODIGOS_INCAPACIDAD
        prog = programado.get(i, 0.0) if en_contrato(dia) and not incapacitado else 0.0
        for ref, inicio, fin, n in turnos_dia[dia]:
            tarde = inicio - ref
            if prog and tarde >= UN_MINUTO:
                minutos = int(tarde.total_seconds() // 60)
                r['retardos'] += 1
                r['retardo_min'] += minutos
                r['notas'].append('%s retardo %d min (%s)' % (NOMBRES_DIA[i], minutos, inicio.strftime('%H:%M')))
            if n < 2:
                r['notas'].append('%s una sola checada (%s)' % (NOMBRES_DIA[i], inicio.strftime('%H:%M')))
                continue
            horas = (fin - max(inicio, ref)).total_seconds() / 3600.0
            extra = max(0.0, horas - prog)
            r['extra_horas'] += extra
            if floor(extra) >= 1:
                r['he_propuestas'] += floor(extra)
                r['dias_con_extra'] += 1
            antes = ref + timedelta(hours=prog) - fin
            if prog and antes >= UN_MINUTO:
                minutos = int(antes.total_seconds() // 60)
                r['salidas_antes'] += 1
                r['salida_min'] += minutos
                r['notas'].append('%s salida antes %d min (%s)' % (NOMBRES_DIA[i], minutos, fin.strftime('%H:%M')))
            if inicio.weekday() == 6 or fin.weekday() == 6 or (fin - inicio).days >= 1:
                r['dom_horas'] += round(horas)
            if i >= 5:
                r['dias'][i] = _fmt(round(horas))

    if r['sin_checadas']:
        r['notas'].insert(0, 'Sin checadas en la semana: no se marcan faltas')
    return r


def repartir_horas_extra(autorizadas, sencillas_extra=0.0):
    """``(dobles, sencillas)``: las primeras 9 de la semana dobles y el resto
    sencillas (más las sencillas que se capturan a mano: bono rama, pendientes)."""
    autorizadas = max(autorizadas or 0, 0)
    dobles = min(autorizadas, HE_DOBLES_TOPE)
    return dobles, autorizadas - dobles + (sencillas_extra or 0.0)
