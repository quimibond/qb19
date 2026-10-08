# -*- coding: utf-8 -*-
"""Motor de ritmos de tejido: funciones puras sobre listas de rollos.

Sin Odoo: recibe los rollos pesados (máquina, artículo, hora local, kilos),
los descansos del calendario y los parámetros, y devuelve los intervalos
clasificados, los paros generales y los ritmos por máquina y artículo. Así
se prueba con datos sintéticos y la lógica queda en un solo lugar.

Reglas: especificación «Tiempos y ritmos de tejido desde el pesaje»
(2026-10-08). Los umbrales llegan en `params` (ver PARAMS_DEFAULT).
"""
import random
import statistics
from collections import defaultdict
from datetime import timedelta

PARAMS_DEFAULT = {
    'kg_max_rollo': 150.0,          # rollos de más → carga en bloque
    'min_minutos': 10.0,            # menos de esto = pesado junto / captura atrasada
    'rollos_atrasados': 3,          # grupo de N o más rollos juntos = captura atrasada
    'factor_paro': 3.0,             # netas > factor × referencia = paro
    'paro_general_h': 5.0,          # hueco sin pesajes en toda la planta
    'min_intervalos_ref': 8,        # intervalos limpios para referencia por máquina
    'min_rollos_ritmo': 5,          # rollos en corrida para publicar un ritmo
    'ventana_rollos': 6,            # rollos por ventana de kg/h técnica
    'bootstrap_reps': 600,
    'bootstrap_min_dias': 5,
    'referencia_cuenta_corrida': True,  # la referencia del rollo cuenta como corrida
    'turno_dia_desde': 7.0,
    'turno_dia_hasta': 19.0,
}

CLASES = ('primer_rollo', 'captura_atrasada', 'cambio', 'pesado_junto',
          'sin_referencia', 'paro', 'corrida')
CLASES_CORRIDA = ('corrida', 'pesado_junto')


def _h(td):
    return td.total_seconds() / 3600.0


# ----------------------------------------------------------------------
# Descansos: ventanas semanales con vigencia + festivos
# ----------------------------------------------------------------------
class Descansos:
    """`reglas`: [(vigente_desde: date|None, vigente_hasta: date|None,
    dow_from, hour_from, dow_to, hour_to)] en hora local, dow 0 = lunes.
    `festivos`: [(inicio, fin)] datetimes locales. `horas(a, b)` es el
    tiempo de [a, b] que cae en descanso (unión, sin doble conteo)."""

    def __init__(self, reglas=(), festivos=()):
        self.reglas = list(reglas)
        self.festivos = [(a, b) for a, b in festivos if b > a]

    def _regla_en(self, fecha):
        for regla in self.reglas:
            desde, hasta = regla[0], regla[1]
            if (desde is None or fecha >= desde) and (hasta is None or fecha <= hasta):
                return regla
        return None

    def _ventanas_semanales(self, a, b):
        """Ventanas de descanso semanal que tocan [a, b], según la regla
        vigente en la semana de cada ventana."""
        out = []
        # Recorrer semana por semana desde el lunes anterior a `a`.
        lunes = (a - timedelta(days=a.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        semana = lunes - timedelta(days=7)
        while semana <= b:
            regla = self._regla_en(semana.date() + timedelta(days=3))
            if regla:
                _d, _h_, dow_from, hour_from, dow_to, hour_to = regla
                ini = semana + timedelta(days=dow_from, hours=hour_from)
                fin = semana + timedelta(days=dow_to, hours=hour_to)
                if fin <= ini:
                    fin += timedelta(days=7)
                out.append((ini, fin))
            semana += timedelta(days=7)
        return out

    def intervalos(self, a, b):
        """Intervalos de descanso dentro de [a, b], fusionados."""
        if b <= a:
            return []
        trozos = []
        for ini, fin in self._ventanas_semanales(a, b) + self.festivos:
            lo, hi = max(a, ini), min(b, fin)
            if hi > lo:
                trozos.append((lo, hi))
        trozos.sort()
        fusion = []
        for lo, hi in trozos:
            if fusion and lo <= fusion[-1][1]:
                fusion[-1] = (fusion[-1][0], max(fusion[-1][1], hi))
            else:
                fusion.append((lo, hi))
        return fusion

    def horas(self, a, b):
        return sum(_h(hi - lo) for lo, hi in self.intervalos(a, b))

    def horas_programadas(self, a, b):
        """Horas del calendario en [a, b]: todo menos descansos y festivos."""
        if b <= a:
            return 0.0
        return _h(b - a) - self.horas(a, b)


def _horas_netas(descansos, a, b, paros_generales):
    """(brutas, descanso, paro_general, netas) del intervalo [a, b]."""
    brutas = _h(b - a)
    desc = descansos.horas(a, b)
    pg = 0.0
    for ini, fin, _hrs in paros_generales:
        lo, hi = max(a, ini), min(b, fin)
        if hi > lo:
            pg += _h(hi - lo) - descansos.horas(lo, hi)
    netas = max(0.0, brutas - desc - pg)
    return brutas, desc, pg, netas


# ----------------------------------------------------------------------
# Limpieza y clasificación
# ----------------------------------------------------------------------
def limpiar(rollos, params, articulos_excluidos=(), prefijos_excluidos=(),
            centros_excluidos=(), usuarios_atrasados=()):
    """Rollos que sí miden: fuera muestras, maquila, cargas en bloque y
    capturas atrasadas por usuario."""
    exc = {a.strip().upper() for a in articulos_excluidos if a.strip()}
    pref = tuple(p.strip().upper() for p in prefijos_excluidos if p.strip())
    cen = {c.strip().upper() for c in centros_excluidos if c.strip()}
    usr = {str(u).strip() for u in usuarios_atrasados if str(u).strip()}
    out = []
    for r in rollos:
        code = (r.get('product_code') or '').strip().upper()
        if code in exc or (pref and code.startswith(pref)):
            continue
        if (r.get('wc_name') or '').strip().upper() in cen:
            continue
        if (r.get('kg') or 0) > params['kg_max_rollo'] or (r.get('kg') or 0) <= 0:
            continue
        if str(r.get('user') or '') in usr:
            continue
        if not r.get('wc') or not r.get('t'):
            continue
        out.append(r)
    return out


def paros_generales(rollos, descansos, params):
    """[(inicio, fin, horas_netas)]: huecos sin pesajes en toda la planta
    de más de `paro_general_h` dentro del calendario."""
    ts = sorted({r['t'] for r in rollos})
    out = []
    for a, b in zip(ts, ts[1:]):
        netas = _h(b - a) - descansos.horas(a, b)
        if netas > params['paro_general_h']:
            out.append((a, b, netas))
    return out


def _mediana(xs):
    return statistics.median(xs) if xs else None


def _percentil(xs, p):
    if not xs:
        return None
    ys = sorted(xs)
    k = (len(ys) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(ys) - 1)
    return ys[lo] + (ys[hi] - ys[lo]) * (k - lo)


def clasificar(rollos, descansos, params, paros=None):
    """Lista de intervalos (dict por rollo, en orden por máquina y hora).

    Claves: id, wc, wc_name, product, product_code, t, semana (lunes),
    mes (día 1), turno, kg, brutas, descanso, paro_general, netas,
    referencia, clase, h_corrida, h_paro, h_cambio, h_sin_medir,
    h_ref_paro (solo cuando la referencia no cuenta como corrida),
    kg_corrida, kg_paro, prev_product, limpio.
    """
    rollos = sorted(rollos, key=lambda r: (r['wc'], r['t'], r.get('id') or 0))
    if paros is None:
        paros = paros_generales(rollos, descansos, params)
    min_h = params['min_minutos'] / 60.0
    cuenta = bool(params.get('referencia_cuenta_corrida', True))

    # Intervalos brutos por máquina.
    por_wc = defaultdict(list)
    for r in rollos:
        por_wc[r['wc']].append(r)
    filas = []
    for wc, lista in por_wc.items():
        prev = None
        for r in lista:
            fila = dict(r)
            fila.update(prev_product=prev['product'] if prev else None,
                        brutas=0.0, descanso=0.0, paro_general=0.0, netas=0.0,
                        referencia=None, clase='primer_rollo',
                        h_corrida=0.0, h_paro=0.0, h_cambio=0.0, h_sin_medir=0.0,
                        h_ref_paro=0.0, kg_corrida=0.0, kg_paro=0.0, limpio=False,
                        atrasada=False)
            if prev is not None:
                br, de, pg, ne = _horas_netas(descansos, prev['t'], r['t'], paros)
                fila.update(brutas=br, descanso=de, paro_general=pg, netas=ne)
            filas.append(fila)
            prev = r
        # Captura atrasada: grupos de N o más rollos con menos de min_h entre sí.
        grupo = []
        idx = [i for i in range(len(filas) - len(lista), len(filas))]
        for k, i in enumerate(idx):
            f = filas[i]
            if k > 0 and f['brutas'] < min_h and f['brutas'] >= 0:
                grupo.append(i)
            else:
                if len(grupo) >= params['rollos_atrasados']:
                    for j in grupo:
                        filas[j]['atrasada'] = True
                grupo = [i]
        if len(grupo) >= params['rollos_atrasados']:
            for j in grupo:
                filas[j]['atrasada'] = True

    # Intervalos limpios para la referencia.
    for f in filas:
        f['limpio'] = (f['clase'] != 'primer_rollo' or f['prev_product'] is not None) and \
            f['prev_product'] == f['product'] and not f['atrasada'] and \
            f['netas'] > min_h and f['prev_product'] is not None
    ref_wc = defaultdict(list)
    ref_prod = defaultdict(list)
    for f in filas:
        if f['limpio']:
            ref_wc[(f['wc'], f['product'])].append(f['netas'])
            ref_prod[f['product']].append(f['netas'])

    def referencia(wc, product):
        xs = ref_wc.get((wc, product), [])
        if len(xs) >= params['min_intervalos_ref']:
            return _mediana(xs)
        return _mediana(ref_prod.get(product, []))

    # Clasificación.
    for f in filas:
        f['semana'] = (f['t'] - timedelta(days=f['t'].weekday())).date()
        f['mes'] = f['t'].date().replace(day=1)
        hora = f['t'].hour + f['t'].minute / 60.0
        f['turno'] = 'dia' if params['turno_dia_desde'] <= hora < params['turno_dia_hasta'] else 'noche'
        if f['prev_product'] is None:
            f['clase'] = 'primer_rollo'
            continue
        ref = referencia(f['wc'], f['product'])
        f['referencia'] = ref
        ne = f['netas']
        if f['atrasada']:
            f['clase'] = 'captura_atrasada'
            f['h_sin_medir'] = ne
        elif ne <= 0:
            # Todo el intervalo cayó en descanso o paro general: el rollo se
            # tejió en tiempo que no se puede medir. No cuenta kilos ni horas.
            f['clase'] = 'sin_referencia'
        elif f['prev_product'] != f['product']:
            f['clase'] = 'cambio'
            base = min(ref or 0.0, ne)
            f['h_cambio'] = ne - base
            if cuenta:
                f['h_corrida'], f['kg_corrida'] = base, f['kg']
        elif f['brutas'] < min_h:
            f['clase'] = 'pesado_junto'
            f['h_corrida'], f['kg_corrida'] = ne, f['kg']
        elif ref is None:
            f['clase'] = 'sin_referencia'
            f['h_sin_medir'] = ne
        elif ne > params['factor_paro'] * ref:
            f['clase'] = 'paro'
            f['h_paro'] = ne - ref
            if cuenta:
                f['h_corrida'], f['kg_corrida'] = ref, f['kg']
            else:
                f['h_ref_paro'], f['kg_paro'] = ref, f['kg']
        else:
            f['clase'] = 'corrida'
            f['h_corrida'], f['kg_corrida'] = ne, f['kg']
    return filas, paros


# ----------------------------------------------------------------------
# Ritmos por máquina, artículo y periodo
# ----------------------------------------------------------------------
def _ventanas(filas_wc_prod, n):
    """kg/h de ventanas de `n` rollos seguidos en la misma máquina, todos
    en corrida (consecutivos en la secuencia de la máquina)."""
    out = []
    racha = []
    for f in filas_wc_prod:
        if f['clase'] in CLASES_CORRIDA and f['h_corrida'] > 0:
            racha.append(f)
        else:
            racha = []
            continue
        if len(racha) >= n:
            v = racha[-n:]
            h = sum(x['h_corrida'] for x in v)
            if h > 0:
                out.append(sum(x['kg_corrida'] for x in v) / h)
    return out


def _bootstrap(filas, params, seed=7):
    por_dia = defaultdict(lambda: [0.0, 0.0])
    for f in filas:
        if f['clase'] in CLASES_CORRIDA and f['h_corrida'] > 0:
            d = por_dia[f['t'].date()]
            d[0] += f['kg_corrida']
            d[1] += f['h_corrida']
    dias = list(por_dia.values())
    if len(dias) < params['bootstrap_min_dias']:
        return None, None
    rnd = random.Random(seed)
    vals = []
    for _ in range(int(params['bootstrap_reps'])):
        kg = h = 0.0
        for _k in range(len(dias)):
            d = rnd.choice(dias)
            kg += d[0]
            h += d[1]
        if h > 0:
            vals.append(kg / h)
    return _percentil(vals, 5), _percentil(vals, 95)


def ritmos(filas, params, horas_programadas=None, estandar=None):
    """Indicadores por (wc, product, periodo) y totales por (wc, periodo).

    `horas_programadas(wc, clave_periodo, tipo) -> horas` y
    `estandar(wc, product) -> kg/h` son opcionales. `tipo` es 'semana' o
    'mes'; `clave_periodo` la fecha de inicio. Las filas con product=None
    son el total de la máquina en el periodo (horas por clase, programadas
    y sin explicar)."""
    out = []
    for tipo in ('semana', 'mes'):
        grupos = defaultdict(list)
        totales = defaultdict(list)
        for f in filas:
            if f['clase'] == 'primer_rollo':
                continue
            grupos[(f['wc'], f['product'], f[tipo])].append(f)
            totales[(f['wc'], f[tipo])].append(f)
        for (wc, product, per), fs in grupos.items():
            fs.sort(key=lambda f: f['t'])
            corr = [f for f in fs if f['clase'] in CLASES_CORRIDA]
            h_corr = sum(f['h_corrida'] for f in fs)
            kg_corr = sum(f['kg_corrida'] for f in fs)
            h_paro = sum(f['h_paro'] for f in fs)
            h_ref_paro = sum(f['h_ref_paro'] for f in fs)
            kg_paro = sum(f['kg_paro'] for f in fs)
            row = {
                'tipo': tipo, 'periodo': per, 'wc': wc, 'wc_name': fs[0].get('wc_name'),
                'product': product, 'product_code': fs[0].get('product_code'),
                'rollos': len(fs), 'rollos_corrida': len(corr),
                'kg': sum(f['kg'] for f in fs), 'kg_corrida': kg_corr,
                'h_corrida': h_corr, 'h_paro': h_paro,
                'h_cambio': sum(f['h_cambio'] for f in fs),
                'h_sin_medir': sum(f['h_sin_medir'] for f in fs),
                'n_paros': sum(1 for f in fs if f['clase'] == 'paro'),
                'kg_h_corrida': None, 'kg_h_tecnica': None, 'kg_h_tipica': None,
                'kg_h_lenta': None, 'kg_h_efectiva': None, 'kg_h_p5': None,
                'kg_h_p95': None, 'kg_h_estandar': None,
                'ciclo_min': None, 'peso_tipico': _mediana([f['kg'] for f in fs]),
                'suficiente': len(corr) >= params['min_rollos_ritmo'],
            }
            if row['suficiente'] and h_corr > 0:
                row['kg_h_corrida'] = kg_corr / h_corr
                vent = _ventanas(fs, int(params['ventana_rollos']))
                row['kg_h_tecnica'] = _percentil(vent, 90)
                row['kg_h_tipica'] = _mediana(vent)
                row['kg_h_lenta'] = _percentil(vent, 10)
                den = h_corr + h_paro + h_ref_paro
                row['kg_h_efectiva'] = (kg_corr + kg_paro) / den if den > 0 else None
                row['kg_h_p5'], row['kg_h_p95'] = _bootstrap(fs, params)
                row['ciclo_min'] = _mediana([f['netas'] * 60 for f in corr if f['netas'] > 0])
            if estandar:
                row['kg_h_estandar'] = estandar(wc, product)
            out.append(row)
        for (wc, per), fs in totales.items():
            h_corr = sum(f['h_corrida'] for f in fs)
            h_paro = sum(f['h_paro'] for f in fs)
            h_cambio = sum(f['h_cambio'] for f in fs)
            h_sm = sum(f['h_sin_medir'] for f in fs)
            prog = horas_programadas(wc, per, tipo) if horas_programadas else None
            kg_corr = sum(f['kg_corrida'] for f in fs)
            row = {
                'tipo': tipo, 'periodo': per, 'wc': wc, 'wc_name': fs[0].get('wc_name'),
                'product': None, 'product_code': None,
                'rollos': len(fs), 'rollos_corrida': sum(1 for f in fs if f['clase'] in CLASES_CORRIDA),
                'kg': sum(f['kg'] for f in fs), 'kg_corrida': kg_corr,
                'h_corrida': h_corr, 'h_paro': h_paro, 'h_cambio': h_cambio,
                'h_sin_medir': h_sm, 'n_paros': sum(1 for f in fs if f['clase'] == 'paro'),
                'h_programadas': prog,
                'h_sin_explicar': (prog - (h_corr + h_paro + h_cambio + h_sm)) if prog is not None else None,
                'pct_corrida': (h_corr / prog) if prog else None,
                'kg_h_corrida': (kg_corr / h_corr) if h_corr > 0 else None,
                'kg_h_efectiva': ((kg_corr + sum(f['kg_paro'] for f in fs))
                                  / (h_corr + h_paro + sum(f['h_ref_paro'] for f in fs)))
                if (h_corr + h_paro) > 0 else None,
                'kg_perdidos': ((h_paro + h_cambio) * kg_corr / h_corr) if h_corr > 0 else 0.0,
                'suficiente': True,
            }
            out.append(row)
    return out
