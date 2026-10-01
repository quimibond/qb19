#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Valida el análisis «Del Dropbox a Odoo — rutina por rutina» antes de importarlo.

Uso:
    python3 validar_rutinas.py RUTA.xlsx            # libro completo (openpyxl)
    python3 validar_rutinas.py RUTA.csv             # solo la hoja «Rutina por rutina» en CSV
    python3 validar_rutinas.py RUTA.xlsx --errores errores.csv

Opciones:
    --procedimientos CSV   foto de producción de los procedimientos
                           (default: procedimientos_2026-09-29.csv junto a este script)
    --actividades CSV      foto de las actividades activas (default: actividades_activas_2026-09-29.csv)
    --documentos CSV       foto de los documentos del Dropbox (default: documentos_dropbox_2026-09-29.csv)
    --esperado N,C,R,P     filas, cubiertas, reemplazadas, pendientes (default 815,709,68,38)
    --procs N              procedimientos distintos esperados (default 49)
    --errores CSV          escribe cada error/advertencia en un CSV

Python puro. openpyxl solo si la entrada es .xlsx. No escribe en Odoo ni en el libro.
Nunca imprime el texto de una fila de un procedimiento excluido (P-I01): solo su posición.
Sale con 0 si no hay errores y 1 si hay.
"""
import argparse
import collections
import csv
import os
import re
import sys
import unicodedata

HERE = os.path.dirname(os.path.abspath(__file__))

COLUMNAS = ['clave', 'procedimiento', 'n', 'rutina', 'frecuencia',
            'responsable_anterior', 'estado', 'actividades_odoo', 'motivo']
COLUMNAS_OPCIONALES = ['revision', 'comentario']
ESTADOS = {'cubierta', 'reemplazada', 'eliminada', 'pendiente'}
ALIAS_ESTADO = {'odoo': 'reemplazada', 'la hace odoo': 'reemplazada',
                'omitida': 'eliminada', 'cubierto': 'cubierta'}
EXCLUIDAS = {'P-I01'}          # va aparte: contiene credenciales (decisiones.md)
RE_CLAVE = re.compile(r'^P-[A-Z]\d{2}$')
RE_NUMERAL = re.compile(r'^[CSE]\d\.\d{2}$')
RE_CREDENCIAL = re.compile(
    r'contrase[nñ]a|password|passwd|\bpwd\b|usuario\s*[:=]|user\s*[:=]|'
    r'\btoken\b|api[_ -]?key|eyJhbGci|sb_secret_', re.I)
CONTROL_OPERACIONAL = {'P-A17', 'P-A18', 'P-A19', 'P-A20', 'P-S03'}


def norm(texto):
    texto = unicodedata.normalize('NFKD', str(texto or '')).encode('ascii', 'ignore').decode()
    return re.sub(r'\s+', '_', texto.strip().lower())


def celda(valor):
    if valor is None:
        return ''
    if isinstance(valor, float) and valor.is_integer():
        return str(int(valor))
    return str(valor).strip()


def leer_libro(ruta):
    """Devuelve {nombre_hoja_normalizado: [filas como listas]}."""
    if ruta.lower().endswith(('.xlsx', '.xlsm')):
        try:
            import openpyxl
        except ImportError:
            sys.exit("Falta openpyxl: instálalo o exporta la hoja «Rutina por rutina» a CSV.")
        libro = openpyxl.load_workbook(ruta, read_only=True, data_only=True)
        return {norm(h.title): [list(f) for f in h.iter_rows(values_only=True)]
                for h in libro.worksheets}
    with open(ruta, newline='', encoding='utf-8-sig') as fh:
        return {'rutina_por_rutina': [fila for fila in csv.reader(fh)]}


def val(fila, i):
    """Celda i de una fila que puede venir corta (openpyxl en modo lectura)."""
    return celda(fila[i]) if fila is not None and i is not None and i < len(fila) else ''


def leer_csv(ruta):
    if not os.path.exists(ruta):
        return None
    with open(ruta, newline='', encoding='utf-8') as fh:
        return list(csv.DictReader(fh))


class Reporte:
    def __init__(self):
        self.items = []

    def add(self, nivel, regla, detalle, fila='', clave='', n=''):
        self.items.append({'nivel': nivel, 'regla': regla, 'fila': fila,
                           'clave': clave, 'n': n, 'detalle': detalle})

    def error(self, *a, **k):
        self.add('error', *a, **k)

    def aviso(self, *a, **k):
        self.add('aviso', *a, **k)

    @property
    def errores(self):
        return [i for i in self.items if i['nivel'] == 'error']


def validar(args):
    rep = Reporte()
    hojas = leer_libro(args.archivo)
    hoja = hojas.get('rutina_por_rutina')
    if hoja is None:
        rep.error('hoja', "No existe la hoja «Rutina por rutina» (hojas: %s)" % ', '.join(hojas))
        return rep, {}
    encabezado = [norm(c) for c in hoja[0]]
    faltan = [c for c in COLUMNAS if c not in encabezado]
    if faltan:
        rep.error('columnas', "Faltan columnas: %s" % ', '.join(faltan))
        return rep, {}
    extra = [c for c in encabezado if c and c not in COLUMNAS + COLUMNAS_OPCIONALES]
    if extra:
        rep.aviso('columnas', "Columnas que el importador ignora: %s" % ', '.join(extra))
    idx = {c: encabezado.index(c) for c in COLUMNAS + COLUMNAS_OPCIONALES if c in encabezado}

    procs = leer_csv(args.procedimientos) or []
    vigentes = collections.defaultdict(list)
    for p in procs:
        if p['estado_sgi'] in ('vigente', 'piloto'):
            vigentes[p['clave']].append(p)
    actividades = {a['numeral'] for a in (leer_csv(args.actividades) or [])}
    if not procs:
        rep.aviso('foto', "Sin foto de procedimientos: no se revisa contra producción")
    if not actividades:
        rep.aviso('foto', "Sin foto de actividades: no se revisan los numerales")

    filas, vistos = [], {}
    conteo_estado = collections.Counter()
    por_clave = collections.defaultdict(collections.Counter)
    for num, crudo in enumerate(hoja[1:], start=2):
        if not any(v not in (None, '') for v in crudo):
            continue
        f = {c: celda(crudo[i]) if i < len(crudo) else '' for c, i in idx.items()}
        clave, n_txt = f['clave'].upper(), f['n']
        if clave in EXCLUIDAS:
            # Nunca se repite el contenido de estas filas.
            rep.error('excluida', "Fila de un procedimiento que va aparte; quítala del archivo",
                      fila=num, clave=clave)
            continue
        texto = ' '.join(f.get(c, '') for c in ('rutina', 'motivo', 'responsable_anterior'))
        if RE_CREDENCIAL.search(texto):
            rep.error('credencial', "Texto con forma de credencial; revisa la fila a mano (no se imprime)",
                      fila=num, clave=clave, n=n_txt)
        if not RE_CLAVE.match(clave):
            rep.error('clave', "Clave con formato inválido %r (se espera P-Xnn)" % clave, num, clave, n_txt)
        try:
            n = int(float(n_txt))
            if n <= 0 or float(n_txt) != n:
                raise ValueError
        except ValueError:
            rep.error('n', "n debe ser entero positivo (llegó %r)" % n_txt, num, clave, n_txt)
            n = None
        if (clave, n) in vistos:
            rep.error('duplicado', "(clave, n) repetido; primera vez en la fila %d" % vistos[(clave, n)],
                      num, clave, n)
        else:
            vistos[(clave, n)] = num
        if not f['rutina']:
            rep.error('rutina', "Rutina vacía", num, clave, n)
        estado = norm(f['estado']).replace('_', ' ')
        estado = ALIAS_ESTADO.get(estado, estado)
        if estado not in ESTADOS:
            rep.error('estado', "Estado %r no válido (cubierta, reemplazada, eliminada, pendiente)"
                      % f['estado'], num, clave, n)
        conteo_estado[estado] += 1
        por_clave[clave][estado] += 1
        tokens = [t.strip() for t in re.split(r'[;,]', f['actividades_odoo']) if t.strip()]
        for t in tokens:
            if not RE_NUMERAL.match(t):
                rep.error('numeral', "Numeral %r con formato inválido (se espera C2.06)" % t, num, clave, n)
            elif actividades and t not in actividades:
                rep.error('numeral', "La actividad %s no está entre las 310 activas" % t, num, clave, n)
        if len(set(tokens)) != len(tokens):
            rep.aviso('numeral', "Actividad repetida en la misma fila", num, clave, n)
        if estado == 'cubierta' and not tokens:
            rep.error('cubierta', "Rutina cubierta sin actividad de Odoo", num, clave, n)
        if estado in ('reemplazada', 'eliminada', 'pendiente') and not f['motivo']:
            rep.error('motivo', "Rutina %s sin motivo" % estado, num, clave, n)
        if estado == 'pendiente' and tokens:
            rep.error('pendiente', "Rutina pendiente con actividades: o está cubierta o no las lleva",
                      num, clave, n)
        if procs and RE_CLAVE.match(clave):
            docs = vigentes.get(clave, [])
            if not docs:
                rep.error('procedimiento', "%s no existe en producción como procedimiento vigente" % clave,
                          num, clave, n)
            elif len(docs) > 1:
                rep.error('procedimiento', "%s tiene %d procedimientos vigentes (ids %s): la llave es ambigua"
                          % (clave, len(docs), ', '.join(d['id'] for d in docs)), num, clave, n)
        rev = norm(f.get('revision', ''))
        if rev and rev not in ('ok', 'corregir'):
            rep.aviso('revision', "Revisión %r: se espera OK o Corregir" % f.get('revision'), num, clave, n)
        if rev == 'corregir':
            rep.aviso('revision', "Marcada «Corregir» por el revisor: se importa como está", num, clave, n)
        filas.append((clave, n, estado))

    # Totales
    esperado = [int(x) for x in args.esperado.split(',')]
    total = len(filas)
    if total != esperado[0]:
        rep.error('total', "Filas: %d, se esperaban %d" % (total, esperado[0]))
    for estado, meta in zip(('cubierta', 'reemplazada', 'pendiente'), esperado[1:]):
        if conteo_estado[estado] != meta:
            rep.error('total', "%s: %d, se esperaban %d" % (estado, conteo_estado[estado], meta))
    if len(por_clave) != args.procs:
        rep.error('total', "Procedimientos distintos: %d, se esperaban %d" % (len(por_clave), args.procs))
    if procs:
        esperadas = {c for c in vigentes if c not in EXCLUIDAS}
        sin_rutinas = sorted(esperadas - set(por_clave))
        if sin_rutinas:
            rep.aviso('cobertura', "Procedimientos vigentes sin ninguna rutina: %s" % ', '.join(sin_rutinas))
        for clave, c in sorted(por_clave.items()):
            docs = vigentes.get(clave) or [{}]
            doc = docs[0]
            if doc.get('sustituido_por') and c['pendiente']:
                rep.error('sustitucion', "%s ya tiene proceso que lo sustituye (%s) y %d rutina(s) pendiente(s)"
                          % (clave, doc['sustituido_por'], c['pendiente']), clave=clave)
            if clave in CONTROL_OPERACIONAL and doc.get('sustituido_por'):
                rep.error('sustitucion', "%s es control operacional y no se sustituye" % clave, clave=clave)

    # Hoja «Resumen por procedimiento»
    resumen = hojas.get('resumen_por_procedimiento')
    if resumen:
        enc = [norm(c) for c in resumen[0]]
        pos = {c: enc.index(c) for c in ('clave', 'proceso_nuevo', 'rutinas', 'cubiertas',
                                           'reemplazadas', 'pendientes') if c in enc}
        for num, crudo in enumerate(resumen[1:], start=2):
            clave = val(crudo, pos['clave']).upper()
            if not clave or clave == 'TOTAL':
                continue
            c = por_clave.get(clave, collections.Counter())
            pares = (('rutinas', sum(c.values())), ('cubiertas', c['cubierta']),
                     ('reemplazadas', c['reemplazada']), ('pendientes', c['pendiente']))
            for col, real in pares:
                if col in pos:
                    try:
                        dicho = int(float(val(crudo, pos[col]) or 0))
                    except ValueError:
                        dicho = None
                    if dicho != real:
                        rep.error('resumen', "Resumen dice %s=%s y la hoja de rutinas da %d"
                                  % (col, dicho, real), fila=num, clave=clave)
            if procs and 'proceso_nuevo' in pos and clave in vigentes:
                doc = vigentes[clave][0]
                esperado_proc = doc.get('sustituido_por') or doc.get('proceso')
                dicho_proc = val(crudo, pos['proceso_nuevo']).split(' ')[0]
                if dicho_proc != esperado_proc:
                    rep.error('resumen', "Proceso nuevo %s en el libro; en Odoo el documento dice %s"
                              % (dicho_proc, esperado_proc), fila=num, clave=clave)

    # Hoja «Pendientes»
    pend = hojas.get('pendientes')
    if pend:
        enc = [norm(c) for c in pend[0]]
        n_pend = sum(1 for r in pend[1:] if r and any(v not in (None, '') for v in r))
        if n_pend != conteo_estado['pendiente']:
            rep.error('pendientes', "Hoja Pendientes: %d filas; rutinas pendientes: %d"
                      % (n_pend, conteo_estado['pendiente']))
        col_dec = next((i for i, c in enumerate(enc) if c.startswith('decision')), None)
        if col_dec is not None:
            sin = sum(1 for r in pend[1:] if r and any(r) and not val(r, col_dec))
            if sin:
                rep.aviso('pendientes', "%d pendientes sin decisión (crear actividad / regla / eliminar)" % sin)

    # Hoja «Documentos por migrar»: solo comparación, Odoo manda
    docs_hoja = hojas.get('documentos_por_migrar')
    docs_prod = leer_csv(args.documentos)
    if docs_hoja and docs_prod:
        por_codigo = collections.defaultdict(list)
        for d in docs_prod:
            por_codigo[d['clave']].append(d)
        enc = [norm(c) for c in docs_hoja[0]]
        i_c, i_e = enc.index('clave'), enc.index('estado')
        for num, crudo in enumerate(docs_hoja[1:], start=2):
            clave = val(crudo, i_c)
            if not clave:
                continue
            vivos = [d for d in por_codigo.get(clave, []) if d['estado_sgi'] != 'obsoleto']
            if not vivos:
                rep.aviso('documentos', "Documento %s no está en la foto de producción" % clave, fila=num)
            elif vivos[0]['estado_migracion'] != val(crudo, i_e):
                rep.aviso('documentos', "%s: libro dice %s, Odoo dice %s (Odoo manda)"
                          % (clave, val(crudo, i_e), vivos[0]['estado_migracion']), fila=num)

    resumen_datos = {
        'filas': total,
        'estados': dict(conteo_estado),
        'procedimientos': len(por_clave),
    }
    return rep, resumen_datos


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('archivo')
    ap.add_argument('--procedimientos', default=os.path.join(HERE, 'procedimientos_2026-09-29.csv'))
    ap.add_argument('--actividades', default=os.path.join(HERE, 'actividades_activas_2026-09-29.csv'))
    ap.add_argument('--documentos', default=os.path.join(HERE, 'documentos_dropbox_2026-09-29.csv'))
    ap.add_argument('--esperado', default='815,709,68,38')
    ap.add_argument('--procs', type=int, default=49)
    ap.add_argument('--errores')
    args = ap.parse_args()
    rep, datos = validar(args)
    print("Archivo: %s" % os.path.basename(args.archivo))
    print("Filas: %s · estados: %s · procedimientos: %s"
          % (datos.get('filas'), datos.get('estados'), datos.get('procedimientos')))
    por_regla = collections.Counter((i['nivel'], i['regla']) for i in rep.items)
    for (nivel, regla), n in sorted(por_regla.items()):
        print("  %-6s %-14s %d" % (nivel, regla, n))
    for i in rep.items[:60]:
        print("  [%s] %s fila %s %s %s: %s" % (i['nivel'], i['regla'], i['fila'], i['clave'], i['n'], i['detalle']))
    if len(rep.items) > 60:
        print("  … %d más (usa --errores)" % (len(rep.items) - 60))
    if args.errores:
        with open(args.errores, 'w', newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=['nivel', 'regla', 'fila', 'clave', 'n', 'detalle'])
            w.writeheader()
            w.writerows(rep.items)
    print("RESULTADO: %s (%d errores, %d avisos)" % (
        'OK' if not rep.errores else 'CON ERRORES', len(rep.errores), len(rep.items) - len(rep.errores)))
    return 1 if rep.errores else 0


if __name__ == '__main__':
    sys.exit(main())
