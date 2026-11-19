# -*- coding: utf-8 -*-
"""Reglas que Ventas aplicaba a mano después de subir un release (Jose, 19-nov-2026).

Python puro, sin Odoo: se prueba en ``tests_puros/ventas_presupuesto/test_reglas.py``. Ninguna regla
conoce a un cliente: todo sale de la configuración del perfil del release y de la parte del cliente.

Por producto, con cantidades en una sola unidad (la de venta del producto) y semanas como el lunes:

1. **Semanas congeladas** (``congeladas`` = N): las N semanas desde la semana en curso se quedan con lo
   que ya estaba programado. Lo que el cliente sube en ellas entra en la semana N+1; lo que baja se
   resta de la semana N+1 y, si no alcanza, de las siguientes, sin dejar ninguna bajo cero.
2. **Embarques cada N semanas** (``agrupar`` = N, desde ``inicio_bloques``): después de las
   congeladas, las semanas de cada bloque de N se suman en la primera semana del bloque. Los bloques
   se cuentan desde una fecha fija para que no se recorran cada semana.
3. **Redondeo a rollos** (``redondear``): cada semana después de las congeladas se sube al múltiplo
   del rollo (505 yd con rollos de 500 → 1000); las congeladas se quedan como estaban. La función la da quien llama porque el rollo puede estar en otra unidad.
"""
from datetime import timedelta

EPS = 1e-6


def _week(monday, n):
    return monday + timedelta(weeks=n)


def aplicar_reglas(pide, programado, semana_actual, congeladas=0, agrupar=0, inicio_bloques=None,
                   redondear=None):
    """Devuelve ``(semanas, notas)``.

    ``pide``: {lunes: cantidad} del release. ``programado``: {lunes: cantidad} que ya tenía el
    pronóstico (para las congeladas). ``redondear``: función cantidad → cantidad redondeada (o None).
    ``semanas``: {lunes: cantidad} a escribir; ``notas``: {lunes: texto} de lo que hizo cada regla.
    Las semanas anteriores a la semana en curso quedan como las manda el cliente.
    """
    result = {w: q for w, q in pide.items()}
    notes = {}

    def note(week, text):
        notes[week] = "%s; %s" % (notes[week], text) if week in notes else text

    # 1. Semanas congeladas.
    if congeladas and congeladas > 0:
        frozen = [_week(semana_actual, i) for i in range(congeladas)]
        target = _week(semana_actual, congeladas)
        delta = 0.0
        for week in frozen:
            asked = pide.get(week, 0.0)
            kept = programado.get(week, 0.0)
            delta += asked - kept
            if week in pide or week in programado:
                result[week] = kept
                if abs(asked - kept) > EPS:
                    note(week, "Congelada: el cliente pide %s, se queda %s" % (_fmt(asked), _fmt(kept)))
        if delta > EPS:
            result[target] = result.get(target, 0.0) + delta
            note(target, "Recibe +%s de las semanas congeladas" % _fmt(delta))
        elif delta < -EPS:
            missing = -delta
            for week in sorted(w for w in result if w >= target):
                if missing <= EPS:
                    break
                take = min(result[week], missing)
                if take > EPS:
                    result[week] -= take
                    missing -= take
                    note(week, "Descuenta %s que bajó en las semanas congeladas" % _fmt(take))
            if missing > EPS:
                note(target, "Quedan %s de baja sin semana de donde descontar" % _fmt(missing))
    else:
        target = semana_actual

    # 2. Embarques cada N semanas.
    if agrupar and agrupar > 1:
        anchor = inicio_bloques or target
        grouped = {}
        for week in sorted(result):
            if week < target:
                grouped[week] = grouped.get(week, 0.0) + result[week]
                continue
            offset = ((week - anchor).days // 7) % agrupar
            ship = week - timedelta(weeks=offset)
            if ship < target:
                ship = target
            grouped[ship] = grouped.get(ship, 0.0) + result[week]
            if ship != week and abs(result[week]) > EPS:
                note(ship, "Junta la semana del %s" % week.strftime('%d-%m'))
        for week in result:
            if week not in grouped:
                grouped[week] = 0.0
        result = grouped

    # 3. Redondeo a rollos.
    if redondear:
        for week in sorted(result):
            if week < target or result[week] <= EPS:
                continue
            rounded = redondear(result[week])
            if rounded is not None and abs(rounded - result[week]) > EPS:
                note(week, "Redondeado de %s a %s" % (_fmt(result[week]), _fmt(rounded)))
                result[week] = rounded
    return result, notes


def restar_dias_habiles(fecha, dias):
    """``fecha`` menos ``dias`` hábiles, sin contar sábados ni domingos (Jose, 19-nov-2026: «5 días y
    descartar los fines de semana»). El resultado nunca cae en fin de semana; con 0 días, una fecha en
    sábado o domingo pasa al viernes anterior."""
    if not fecha:
        return fecha
    day = fecha
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    left = max(int(dias or 0), 0)
    while left:
        day -= timedelta(days=1)
        if day.weekday() < 5:
            left -= 1
    return day


def redondear_arriba(qty, rollo):
    """``qty`` al múltiplo de ``rollo`` hacia arriba (con tolerancia para no subir 1000.0000001)."""
    if not rollo or rollo <= 0 or qty <= EPS:
        return qty
    rolls = int(qty / rollo)
    if qty - rolls * rollo > EPS * max(1.0, rollo):
        rolls += 1
    return rolls * rollo


def _fmt(qty):
    return ("%.2f" % (qty or 0.0)).rstrip('0').rstrip('.')
