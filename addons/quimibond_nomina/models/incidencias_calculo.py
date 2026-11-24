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
  última; la comida va dentro de la jornada. Una checada sola acepta su salida
  hasta 16 horas después (el turno de 12 de noche sin checar a la comida), la
  misma regla que ``qb_checador`` 1.7.0 usa para las asistencias. El turno es del día de su **hora de
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

**Turnos de la planta** (archivo «TURNOS NOVIEMBRE 2026» de RH, 11-nov-2026;
rotan cada 15 días salvo los de turno fijo). Un mismo horario en el contrato no
sirve para todos: quien trabaja de noche de lunes a jueves o el fin de semana
salía con faltas los días que no le tocan. Por eso la lista **reconoce el turno
de la semana desde las checadas** (``inferir_turno``) y usa el de RH como
confirmación; con el turno, ``calcular_semana`` mide contra él:

* **Faltas**: solo en los días del turno. Los días del horario que no son del
  turno se pagan (la base sigue siendo lunes a jueves 10 y viernes 8). Una falta
  en un día del turno sin horas en la base (sábado o domingo) quita las horas
  del turno.
* **Retardos y salidas antes**: contra la entrada y la duración del turno de ese
  día (turno 2: entra a las 08:00, no es retardo llegar 07:45).
* **Tiempo extra**: lo que el turno trabajado pasa de las horas del turno ese
  día, y todo lo trabajado en un día que no es del turno. Las 12 horas del turno
  de noche ya no salen como 2 de extra diarias.
* **Prima dominical**: igual que sin turno.

Ajustes de 1.21.0, al comparar la semana 47 con lo que capturó RH:

* **Turno 3**: cuatro días de 12 horas entre lunes y viernes; un día que no vino
  se repone con otro (casi siempre el viernes) y no es falta. Solo falta si
  trabajó menos de cuatro (``TURNOS_DIAS``).
* **Tiempo extra de los turnos 3 y 4**: se mide contra las horas del horario
  ese día (10 de lunes a jueves, 8 el viernes), como lo paga RH (9 dobles y el
  resto sencillas). Los turnos 5 y 6 no llevan extra dentro de su turno
  (``EXTRA_SOBRE_HORARIO``). Pendiente de confirmar con RH la regla exacta.
* **Prima dominical**: a lo más 12 horas por semana (``DOM_TOPE``); el turno 6
  que entra sábado y domingo en la noche cobra 12, no 24.
* **El turno de RH pesa más**: el del checador solo gana si explica la semana
  con más de un turno completo de diferencia (``MARGEN_RH`` = 12 horas).

**Comida** (1.22.0): las checadas de en medio del turno (salida y regreso de
comer) no cambian las horas; la comida va dentro de la jornada. La lista anota
cuánto duró (el hueco más largo entre checadas intermedias, con cuatro o más
checadas en el turno) y, si pasa de ``comida_max`` minutos (parámetro
``quimibond_nomina.incidencias_comida_max``, 60 si no está), lo deja en
Observaciones. **Solo se anota, no se descuenta** (Dirección, 19-nov-2026).

**Reglas de RH del 23-nov-2026** (correo de Miguel Medina, 1.24.0; aplican desde
la nómina 49):

* **Turno 3** (cuatro días de 12 horas): no genera tiempo extra. Se mide contra
  las 12 horas del turno; solo un quinto día trabajado sale como extra.
* **Turno 4** (noche de lunes a jueves): la jornada nocturna legal es de 42
  horas y cubren 48, así que lleva **6 horas extra fijas** si trabaja completo
  el turno del jueves (``HE_FIJAS``): checó entrada y salida y no salió antes de
  la hora. Dentro del turno no hay más extra.
* **Turnos 5 y 6** (Tejido): sin tiempo extra; **prima dominical de 12 horas**
  si trabaja completos los turnos que tocan el domingo (``DOM_COMPLETO``; turno
  5: el domingo de día; turno 6: las noches del sábado y del domingo). Si no
  los completa no lleva prima y queda la nota.
* **Comida** permitida (solo se anota): empleados 60 minutos; sindicalizados
  30 en jornadas de 10 horas o menos y 45 en las de 12 (``comida_permitida``).
* **Turno 7** (Ciruelos, CDMX): 09:00 a 16:00 de lunes a jueves y 09:00 a 14:00
  el viernes. El checador no lo propone; solo cuenta si RH lo tiene en la ficha.
* **Festivos** (calendario de RH, ``qb.nomina.festivo``): un festivo sin
  trabajar se paga («F», no es falta); trabajado se anota en «Festivos
  trabajados» para pagarlo como el art. 75 de la LFT (salario doble además del
  día). La prima dominical de un festivo en domingo sale como siempre.

**Respuestas de RH del 24-nov-2026** (correo de Miguel Medina, 1.26.0):

* Las **6 horas extra del turno 4** solo se pagan en Inspección, Acabado,
  Tintorería y Mantenimiento: la casilla «Paga las 6 HE del turno 4» de la
  cuadrilla (``he_fijas``). Fuera de esas cuadrillas el supervisor las autoriza
  a mano cuando tocan.
* **Turno completo**: 10 minutos de tolerancia a la entrada
  (``TOLERANCIA_COMPLETO``); salir antes, aunque sea un minuto, ya no es turno
  completo. El retardo se sigue contando desde el minuto 1.
* **Prima dominical de los turnos 5 y 6**: proporcional a las horas trabajadas
  en domingo (de 00:00 a 24:00) dentro de los turnos que tocan el domingo, con
  tope de 12. Con el turno completo da 12: turno 5, domingo de 07:00 a 19:00;
  turno 6, de 00:00 a 07:00 del sábado en la noche y de 19:00 a 24:00 del
  domingo.
"""
from datetime import datetime, time, timedelta
from math import floor

SEPARACION_TURNOS = timedelta(hours=8)
TURNO_MAX = timedelta(hours=16)
DUPLICADA = timedelta(minutes=2)    # checó dos veces seguidas: es la misma checada
ENTRADAS_DEFAULT = (time(7, 0), time(19, 0))
HORAS_CASTIGO_FALTA = 2          # lunes a jueves, como la lista de RH
DIA_SIN_CASTIGO = 4              # viernes: la falta quita solo las horas del día
COMIDA_MAX_DEFAULT = 60          # minutos de comida antes de anotar «comida larga»
HE_DOBLES_TOPE = 9               # por semana (LFT art. 66-68)
DOM_TOPE = 12.0                  # horas de prima dominical por semana, como las paga RH
UN_MINUTO = timedelta(minutes=1)
CODIGOS_INCAPACIDAD = ('I',)
CODIGOS_PAGADOS = ('V', 'P')
CODIGO_SIN_GOCE = 'SG'
NOMBRES_DIA = ('Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa', 'Do')

# Turnos de RH: {día de la semana: (entrada, horas)}. El viernes de los turnos 1 y 2 es de 8 horas,
# como el horario de la planta (07:00 a 15:00 y 08:00 a 16:00).
TURNOS = {
    '1': {0: (time(7, 0), 10.0), 1: (time(7, 0), 10.0), 2: (time(7, 0), 10.0), 3: (time(7, 0), 10.0),
          4: (time(7, 0), 8.0)},
    '2': {0: (time(8, 0), 10.0), 1: (time(8, 0), 10.0), 2: (time(8, 0), 10.0), 3: (time(8, 0), 10.0),
          4: (time(8, 0), 8.0)},
    '3': {d: (time(7, 0), 12.0) for d in range(5)},
    '4': {d: (time(19, 0), 12.0) for d in range(4)},
    '5': {1: (time(19, 0), 12.0), 2: (time(19, 0), 12.0), 3: (time(19, 0), 12.0), 6: (time(7, 0), 12.0)},
    '6': {5: (time(19, 0), 12.0), 6: (time(19, 0), 12.0), 0: (time(19, 0), 12.0)},
    '7': {0: (time(9, 0), 7.0), 1: (time(9, 0), 7.0), 2: (time(9, 0), 7.0), 3: (time(9, 0), 7.0),
          4: (time(9, 0), 5.0)},
}
# Turnos que el checador no propone: solo cuentan si RH los tiene en la ficha (el 7 es de Ciruelos).
SOLO_RH = ('7',)
# Turnos que no trabajan todos sus días: cuántos de 12 horas les tocan a la semana.
TURNOS_DIAS = {'3': 4}
# Turnos cuyo tiempo extra se mide contra el horario del día y no contra las horas del turno. Desde el
# 23-nov-2026 ninguno: los turnos 3 a 6 no generan extra dentro de su turno (RH, correo de Miguel).
EXTRA_SOBRE_HORARIO = ()
# Extra fijo por turno: {turno: (día de la semana, horas)} si trabaja completo ese día. Turno 4: 48 horas
# de noche contra las 42 de la jornada nocturna legal; se pagan 6 si completa el jueves.
HE_FIJAS = {'4': (3, 6)}
# Turnos cuya prima dominical (12 horas) pide completar los turnos que tocan el domingo.
DOM_COMPLETO = ('5', '6')
# Retardo con el que el turno todavía cuenta como completo (RH, 24-nov-2026). El retardo se anota desde el minuto 1.
TOLERANCIA_COMPLETO = timedelta(minutes=10)
# Comida permitida (minutos; solo se anota). Sindicalizados: 30 en jornadas de 10 horas o menos, 45 en las
# de 12; empleados: el parámetro (60).
COMIDA_SINDICALIZADO_CORTA = 30
COMIDA_SINDICALIZADO_LARGA = 45
JORNADA_LARGA = 11.0                    # horas: a partir de aquí la jornada es «de 12»
CODIGO_FESTIVO = 'F'
TURNOS_NOMBRE = [
    ('1', "1 · 07:00 a 17:00, lunes a viernes"),
    ('2', "2 · 08:00 a 18:00, lunes a viernes"),
    ('3', "3 · 07:00 a 19:00, cuatro días de lunes a viernes"),
    ('4', "4 · 19:00 a 07:00, lunes a jueves"),
    ('5', "5 · 19:00 a 07:00, martes a jueves, y domingo 07:00 a 19:00"),
    ('6', "6 · 19:00 a 07:00, sábado a lunes"),
    ('7', "7 · 09:00 a 16:00, lunes a viernes (Ciruelos)"),
]
ENTRADAS_TURNOS = (time(7, 0), time(8, 0), time(19, 0))
VENTANA_TURNO = timedelta(hours=4)      # la checada de entrada más lejos que esto no es de ese turno
MEDIA_CHECADA = timedelta(hours=4)      # una checada sola cuenta como 8 h alrededor (para reconocer el turno)
MARGEN_RH = 12.0                        # horas de puntaje con las que el turno de RH gana al del checador


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


def agrupar_turnos(marcas, separacion=SEPARACION_TURNOS, turno_max=TURNO_MAX):
    """Checadas (datetimes locales) → ``[(primera, última, cuántas)]``."""
    turnos = []
    for marca in sorted(marcas):
        if turnos:
            inicio, fin, n = turnos[-1]
            if marca - fin <= DUPLICADA:
                turnos[-1][1] = marca
                continue
            if marca - inicio <= turno_max and (n == 1 or marca - fin < separacion):
                turnos[-1][1] = marca
                turnos[-1][2] += 1
                continue
        turnos.append([marca, marca, 1])
    return [tuple(t) for t in turnos]


def comida(marcas, inicio, fin):
    """``(salida, regreso)`` de la comida de un turno, o ``None``: el hueco más largo entre las checadas
    de en medio (sin la primera ni la última). Hacen falta cuatro checadas o más; las repetidas en menos
    de dos minutos cuentan una vez."""
    dentro = []
    for marca in sorted(m for m in marcas if inicio <= m <= fin):
        if dentro and marca - dentro[-1] <= DUPLICADA:
            dentro[-1] = marca
        else:
            dentro.append(marca)
    if len(dentro) < 4:
        return None
    pares = [(dentro[i], dentro[i + 1]) for i in range(1, len(dentro) - 2)]
    return max(pares, key=lambda p: p[1] - p[0])


def entrada_referencia(inicio, entradas=ENTRADAS_DEFAULT):
    """La entrada de la planta más cercana a la primera checada del turno."""
    candidatos = [datetime.combine(inicio.date() + timedelta(days=d), e)
                  for d in (-1, 0, 1) for e in entradas]
    return min(candidatos, key=lambda c: abs((inicio - c).total_seconds()))


def _jornadas_turno(lunes, turno, semanas=(-1, 0, 1)):
    """Las jornadas del turno alrededor de la semana: ``[(día, inicio, fin, horas)]``."""
    jornadas = []
    for semana in semanas:
        for dow, (entrada, horas) in sorted(turno.items()):
            dia = lunes + timedelta(days=7 * semana + dow)
            inicio = datetime.combine(dia, entrada)
            jornadas.append((dia, inicio, inicio + timedelta(hours=horas), horas))
    return jornadas


def _traslape(a0, a1, b0, b1):
    return max(0.0, (min(a1, b1) - max(a0, b0)).total_seconds() / 3600.0)


def inferir_turno(lunes, marcas, ausencias=None, contrato_desde=None, contrato_hasta=None, hoy=None,
                  preferido=None, margen=MARGEN_RH):
    """El turno de RH que mejor explica las checadas de la semana: ``(código, {código: puntaje})``.

    Puntaje de cada turno, en horas: lo trabajado dentro de sus jornadas, menos lo
    que faltó de cubrir de ellas (sin contar días fuera del contrato, con ausencia
    o posteriores a ``hoy``), menos la mitad de lo trabajado fuera de ellas. Una
    checada sola cuenta como 8 horas a su alrededor. ``preferido`` (el turno de
    RH) gana si queda a menos de ``margen`` horas del mejor: así se distinguen el
    1 y el 2, que solo cambian por una hora. Sin checadas en la semana: ``None``."""
    ausencias = ausencias or {}
    domingo = lunes + timedelta(days=6)
    trabajado = []
    for inicio, fin, n in agrupar_turnos(marcas):
        if not lunes <= entrada_referencia(inicio, ENTRADAS_TURNOS).date() <= domingo:
            continue
        if n < 2:
            inicio, fin = inicio - MEDIA_CHECADA, inicio + MEDIA_CHECADA
        trabajado.append((inicio, fin))
    if not trabajado:
        return None, {}
    total = sum((f - i).total_seconds() / 3600.0 for i, f in trabajado)

    def cuenta(dia):
        return ((not contrato_desde or dia >= contrato_desde) and (not contrato_hasta or dia <= contrato_hasta)
                and dia not in ausencias and not (hoy and dia > hoy))

    puntajes = {}
    for codigo, turno in TURNOS.items():
        if codigo in SOLO_RH and codigo != preferido:
            continue
        dentro = 0.0
        faltantes = []
        for dia, j0, j1, horas in _jornadas_turno(lunes, turno, semanas=(0,)):
            cubierto = sum(_traslape(i, f, j0, j1) for i, f in trabajado)
            dentro += cubierto
            if cuenta(dia):
                faltantes.append(max(0.0, horas - cubierto))
        # El turno 3 no trabaja todos sus días: los que le sobran no cuentan como faltantes.
        libres = len(turno) - TURNOS_DIAS.get(codigo, len(turno))
        faltante = sum(sorted(faltantes)[:len(faltantes) - libres] if libres else faltantes)
        puntajes[codigo] = round(dentro - faltante - 0.5 * max(0.0, total - dentro), 2)
    mejor = max(sorted(puntajes), key=lambda c: puntajes[c])
    if preferido in puntajes and puntajes[preferido] >= puntajes[mejor] - margen:
        mejor = preferido
    return mejor, puntajes


def comida_permitida(horas_jornada, sindicalizado, comida_empleados=COMIDA_MAX_DEFAULT):
    """Minutos de comida permitidos en una jornada (RH, 23-nov-2026): empleados ``comida_empleados``
    (60); sindicalizados 30 en jornadas de 10 horas o menos y 45 en las de 12."""
    if not sindicalizado:
        return comida_empleados
    return COMIDA_SINDICALIZADO_LARGA if horas_jornada >= JORNADA_LARGA else COMIDA_SINDICALIZADO_CORTA


def _toca_domingo(dow, entrada, horas):
    """La jornada que entra ese día de la semana a esa hora pisa el domingo."""
    inicio = datetime.combine(datetime(2026, 11, 23).date() + timedelta(days=dow), entrada)  # un lunes
    fin = inicio + timedelta(hours=horas)
    return inicio.weekday() == 6 or (fin - timedelta(minutes=1)).weekday() == 6


def _fmt(horas):
    return ('%.1f' % horas).rstrip('0').rstrip('.')


def calcular_semana(lunes, programado, marcas, ausencias=None, contrato_desde=None,
                    contrato_hasta=None, hoy=None, entradas=ENTRADAS_DEFAULT, turno=None,
                    comida_max=COMIDA_MAX_DEFAULT, sindicalizado=False, festivos=None, he_fijas=True):
    """Una persona, una semana de nómina (lunes a domingo).

    ``programado``: ``{0..6: horas}`` del horario (la base de pago). ``marcas``:
    checadas en hora local. ``ausencias``: ``{date: (código, horas_sin_goce)}``
    con código ``I``, ``V``, ``P`` o ``SG``. ``hoy``: los días posteriores se
    proyectan con el horario (como hace RH al reportar el martes). ``turno``: el
    código de la semana (``'1'`` a ``'6'``) o sus jornadas (``TURNOS[código]``);
    sin él, se mide contra el horario y las entradas de la planta, como antes.
    ``comida_max``: minutos de comida a partir de los cuales se anota (empleados; a un ``sindicalizado``
    le tocan 30 o 45 según la jornada, ``comida_permitida``). ``festivos``: fechas de descanso obligatorio
    de la semana. ``he_fijas``: si al turno 4 le tocan sus 6 horas extra fijas (solo en las áreas que las
    pagan, casilla de la cuadrilla)."""
    ausencias = ausencias or {}
    festivos = set(festivos or ())
    codigo_turno = turno if isinstance(turno, str) else None
    if codigo_turno:
        turno = TURNOS.get(codigo_turno)
    extra_sobre_horario = codigo_turno in EXTRA_SOBRE_HORARIO
    domingo = lunes + timedelta(days=6)
    r = {
        'dias': [''] * 7, 'horas_base': 0.0, 'faltas': 0, 'falta_horas': 0.0, 'incap_dias': 0,
        'horas_sin_goce': 0.0, 'retardos': 0, 'retardo_min': 0, 'salidas_antes': 0,
        'salida_min': 0, 'extra_horas': 0.0, 'he_propuestas': 0, 'dias_con_extra': 0,
        'dom_horas': 0.0, 'comida_min': 0, 'comidas_largas': 0, 'sin_checadas': False, 'proyectado': False,
        'festivos_trabajados': 0, 'notas': [],
    }

    def en_contrato(dia):
        return (not contrato_desde or dia >= contrato_desde) and (not contrato_hasta or dia <= contrato_hasta)

    # Cada turno trabajado → (día, entrada de referencia, horas que le tocan ese día).
    turnos_dia = {}
    jornadas = _jornadas_turno(lunes, turno) if turno else []
    usadas = set()
    for inicio, fin, n in agrupar_turnos(marcas):
        jornada = None
        if turno:
            cerca = [j for j in jornadas if j[1] not in usadas and abs(inicio - j[1]) <= VENTANA_TURNO]
            if cerca:
                jornada = min(cerca, key=lambda j: abs((inicio - j[1]).total_seconds()))
                usadas.add(jornada[1])
        if jornada:
            dia, ref, horas_turno = jornada[0], jornada[1], jornada[3]
        else:
            ref = entrada_referencia(inicio, entradas)
            dia = ref.date()
            # Sin turno, las horas del día son las del horario; con turno, fuera de su jornada todo es extra.
            horas_turno = None if not turno else 0.0
        if lunes <= dia <= domingo:
            turnos_dia.setdefault(dia, []).append((ref, inicio, fin, n, horas_turno))
    hay_checadas = bool(turnos_dia)

    # Turno 3: de sus cinco días le tocan cuatro; el que no vino (empezando por el viernes) no es falta.
    libres = set()
    sobran = len(turno) - TURNOS_DIAS.get(codigo_turno, len(turno)) if turno else 0
    if sobran > 0:
        # Si trabajó más días que los suyos, los últimos son extra: sin retardo y todas sus horas extra.
        trabajados = [i for i in sorted(turno) if turnos_dia.get(lunes + timedelta(days=i))]
        for i in trabajados[len(turno) - sobran:]:
            libres.add(i)
            dia = lunes + timedelta(days=i)
            turnos_dia[dia] = [(t[0], t[1], t[2], t[3], 0.0) for t in turnos_dia[dia]]
        for i in sorted(turno, reverse=True):
            dia = lunes + timedelta(days=i)
            if len(libres) >= sobran:
                break
            if not turnos_dia.get(dia) and en_contrato(dia) and dia not in ausencias and not (hoy and dia > hoy):
                libres.add(i)

    # Días: lo que va en las columnas de la lista.
    for i in range(7):
        dia = lunes + timedelta(days=i)
        prog = programado.get(i, 0.0)
        del_turno = turno.get(i) if turno and i not in libres else None
        if not en_contrato(dia):
            r['dias'][i] = '—'
            continue
        codigo, horas_sg = ausencias.get(dia, (None, 0.0))
        if codigo in CODIGOS_INCAPACIDAD:
            r['dias'][i] = 'I'
            r['incap_dias'] += 1
            continue
        if dia in festivos:
            if turnos_dia.get(dia):
                # Trabajó el festivo: el día se paga como siempre y además el doble (art. 75 LFT).
                r['festivos_trabajados'] += 1
                r['notas'].append('%s festivo trabajado' % NOMBRES_DIA[i])
            elif prog or del_turno:
                # Descanso obligatorio: se paga, no es falta.
                r['dias'][i] = CODIGO_FESTIVO
                r['horas_base'] += prog
                continue
        if turno and del_turno and not prog:
            # Sábado o domingo del turno: no está en la base, pero faltar sí cuenta.
            if (turnos_dia.get(dia) or codigo in CODIGOS_PAGADOS or codigo == CODIGO_SIN_GOCE
                    or (hoy and dia > hoy) or not hay_checadas):
                if codigo in CODIGOS_PAGADOS:
                    r['dias'][i] = codigo
                continue
            r['dias'][i] = '0'
            r['faltas'] += 1
            r['falta_horas'] += del_turno[1]
            r['horas_base'] -= del_turno[1]
            r['notas'].append('%s falta' % NOMBRES_DIA[i])
            continue
        if not prog:
            continue
        if turno and not del_turno and codigo not in CODIGOS_PAGADOS and codigo != CODIGO_SIN_GOCE:
            # Día del horario que no es del turno: se paga, haya o no checadas.
            r['dias'][i] = _fmt(prog)
            r['horas_base'] += prog
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
    completos = set()           # días del turno trabajados completos (entrada, salida, sin salir antes)
    con_extra = set()
    dom_turno = 0.0
    dom_calendario = 0.0        # horas trabajadas dentro del domingo (00:00 a 24:00) en jornadas del turno
    for dia in sorted(turnos_dia):
        i = dia.weekday()
        incapacitado = ausencias.get(dia, (None,))[0] in CODIGOS_INCAPACIDAD
        prog_dia = programado.get(i, 0.0) if en_contrato(dia) and not incapacitado else 0.0
        for ref, inicio, fin, n, horas_turno in turnos_dia[dia]:
            prog = prog_dia if horas_turno is None else horas_turno
            if incapacitado or not en_contrato(dia):
                prog = 0.0
            # EXTRA_SOBRE_HORARIO: el extra es lo que pasa del horario del día (hoy ningún turno).
            prog_extra = prog_dia if horas_turno and extra_sobre_horario else prog
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
            hueco = comida(marcas, inicio, fin)
            if hueco:
                minutos = int((hueco[1] - hueco[0]).total_seconds() // 60)
                r['comida_min'] += minutos
                permitida = comida_permitida(prog or horas, sindicalizado, comida_max) if comida_max else 0
                if permitida and minutos > permitida:
                    r['comidas_largas'] += 1
                    r['notas'].append('%s comida %d min (%s a %s)' % (
                        NOMBRES_DIA[i], minutos, hueco[0].strftime('%H:%M'), hueco[1].strftime('%H:%M')))
            extra = max(0.0, horas - prog_extra)
            r['extra_horas'] += extra
            if floor(extra) >= 1:
                r['he_propuestas'] += floor(extra)
                r['dias_con_extra'] += 1
                con_extra.add(dia)
            antes = ref + timedelta(hours=prog) - fin
            if prog and antes >= UN_MINUTO:
                minutos = int(antes.total_seconds() // 60)
                r['salidas_antes'] += 1
                r['salida_min'] += minutos
                r['notas'].append('%s salida antes %d min (%s)' % (NOMBRES_DIA[i], minutos, fin.strftime('%H:%M')))
            elif horas_turno and tarde <= TOLERANCIA_COMPLETO:
                completos.add(dia)
            if inicio.weekday() == 6 or fin.weekday() == 6 or (fin - inicio).days >= 1:
                r['dom_horas'] += round(horas)
                if horas_turno:
                    dom_turno += round(horas)
                    d0 = datetime.combine(domingo, time(0, 0))
                    dom_calendario += _traslape(max(inicio, ref), fin, d0, d0 + timedelta(days=1))
            if i >= 5:
                r['dias'][i] = _fmt(round(horas))

    # Turno 4: 6 horas extra fijas si completa el turno del jueves (RH, 23-nov-2026).
    fija = HE_FIJAS.get(codigo_turno) if he_fijas else None
    if fija:
        dia_fijo = lunes + timedelta(days=fija[0])
        if dia_fijo in completos:
            r['extra_horas'] += fija[1]
            r['he_propuestas'] += fija[1]
            if dia_fijo not in con_extra:
                r['dias_con_extra'] += 1
            r['notas'].append('%s turno completo: %d HE (jornada nocturna de 42 h)' % (NOMBRES_DIA[fija[0]], fija[1]))
        elif en_contrato(dia_fijo) and not (hoy and dia_fijo > hoy) and hay_checadas:
            r['notas'].append('%s sin turno completo: sin las %d HE del turno' % (NOMBRES_DIA[fija[0]], fija[1]))

    # Turnos 5 y 6: prima dominical de 12 horas con los turnos que tocan el domingo completos; si no, las horas
    # trabajadas dentro del domingo (RH, 24-nov-2026: «de acuerdo con las horas trabajadas»).
    if codigo_turno in DOM_COMPLETO and turno:
        requeridos = [lunes + timedelta(days=d) for d, (entrada, horas_j) in turno.items()
                      if _toca_domingo(d, entrada, horas_j)]
        futuros = [d for d in requeridos if hoy and d > hoy]
        if requeridos and not futuros and all(d in completos for d in requeridos):
            r['dom_horas'] = max(0.0, r['dom_horas'] - dom_turno) + DOM_TOPE
        elif dom_turno or any(turnos_dia.get(d) for d in requeridos):
            proporcional = min(float(round(dom_calendario)), DOM_TOPE)
            r['dom_horas'] = max(0.0, r['dom_horas'] - dom_turno) + proporcional
            if not futuros:
                r['notas'].append('Do sin turno completo: prima dominical de %s h trabajadas en domingo'
                                  % _fmt(proporcional))
    r['horas_base'] = max(r['horas_base'], 0.0)
    r['dom_horas'] = min(r['dom_horas'], DOM_TOPE)

    if r['sin_checadas']:
        r['notas'].insert(0, 'Sin checadas en la semana: no se marcan faltas')
    return r


def premio_asistencia(faltas, retardos, pct_un_retardo=4.5):
    """Porcentaje del premio de asistencia del mes (Contrato Colectivo, respuesta de RH del 21-oct-2026):
    5.5 % con 0 faltas y 0 retardos, 4.5 % con 2 retardos o 1 falta, 0 % con más de 2 retardos o más de 1
    falta. Con **un** retardo RH no lo dijo: ``pct_un_retardo`` (parámetro, 4.5 mientras no se confirme)."""
    if faltas > 1 or retardos > 2:
        return 0.0
    if faltas == 1 or retardos == 2:
        return 4.5
    if retardos == 1:
        return pct_un_retardo
    return 5.5


def repartir_horas_extra(autorizadas, sencillas_extra=0.0):
    """``(dobles, sencillas)``: las primeras 9 de la semana dobles y el resto
    sencillas (más las sencillas que se capturan a mano: bono rama, pendientes)."""
    autorizadas = max(autorizadas or 0, 0)
    dobles = min(autorizadas, HE_DOBLES_TOPE)
    return dobles, autorizadas - dobles + (sencillas_extra or 0.0)
