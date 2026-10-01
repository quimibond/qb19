# -*- coding: utf-8 -*-
"""Clasifica los 5,113 elementos de inventario/universo.csv (fase 2).

Uso: python3 docs/audit/scripts/clasificar_universo.py
Entrada: inventario/universo.csv, 99-consolidado/hallazgos_consolidados.csv,
         99-consolidado/hallazgos.csv y los CSV por elemento de los agentes.
Salida:  99-consolidado/universo_clasificado.csv (mismas columnas + `fuente` y `nota`).

Orden de las reglas (la primera que aplica fija la clasificación; los IDs se juntan):
  (0) reglas por modelo/archivo que salen de decisiones.md (módulos que salen, procesos viejos,
      migraciones, demo): tienen prioridad porque ya están decididas;
  (a) CSV por elemento de los agentes (vistas, métodos, menús, acciones, crons, campos sin help,
      pruebas, reglas);
  (b) nombre del elemento citado en `elemento`/`propuesta` de un hallazgo consolidado;
  (c) por defecto «Se queda».
Los IDs se escriben como `id_final` del consolidado (un absorbido se traduce a su principal).
"""
import collections
import csv
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
AUDIT = os.path.dirname(HERE)
OUT = os.path.join(AUDIT, '99-consolidado')

Q, C, E, S = 'Se queda', 'Se corrige', 'Se elimina', 'Sale a otro módulo'
RANK = {E: 4, S: 3, C: 2, Q: 1}


def rd(rel):
    with open(os.path.join(AUDIT, rel), encoding='utf-8') as f:
        return list(csv.DictReader(f))


cons = rd('99-consolidado/hallazgos_consolidados.csv')
to_final = {}
for r in cons:
    to_final[r['id_final']] = r['id_final']
    for a in r['ids_absorbidos'].split():
        to_final[a] = r['id_final']
by_final = {r['id_final']: r for r in cons}
raw = {r['id']: r for r in rd('99-consolidado/hallazgos.csv')}

IDRE = re.compile(r'\b[A-L]-\d{3}\b')


def ids_in(text):
    return [to_final.get(i, i) for i in IDRE.findall(text or '')]


# ---------------------------------------------------------------- (0) decisiones
EXIT_MODELS = {  # modelo -> (clasificación, hallazgo)
    'sgi.sales.budget': (S, 'A-016'), 'sgi.sales.budget.line': (S, 'A-016'),
    'sgi.sales.budget.import': (S, 'A-016'), 'budget.analytic': (S, 'A-016'),
    'sgi.lock.date.log': (S, 'A-017'), 'sgi.inventory.value': (S, 'A-017'),
    'sgi.ppap': (S, 'A-018'), 'sgi.ppap.element': (S, 'A-018'), 'sgi.ppap.element.template': (S, 'A-018'),
    'sgi.fmea': (S, 'A-018'), 'sgi.fmea.line': (S, 'A-018'), 'sgi.msa.study': (S, 'A-018'),
    'sgi.process.responsibility': (E, 'B-002'),
}
EXIT_FILES = [  # regex sobre ubicacion -> (clasificación, hallazgo)
    (r'/migrations/', E, 'A-026'),
    (r'/tools/(carga_documental|post_carga_documental|reporte_telas_rollout)\.py', E, 'A-003'),
    (r'data/sgi_process_data\.xml|data/sgi_process_flows_extra\.xml', E, 'A-002'),
    (r'data/sgi_indicator_formula_data\.xml', S, 'A-004'),
    (r'demo/', E, 'A-028'),
    (r'sgi_sales_budget|sgi_budget_analytic|report_sales_budget|test_sales_budget', S, 'A-016'),
    (r'sgi_ppap|sgi_fmea|sgi_msa|report_fmea|report_ppap|test_ppap|test_fmea|test_msa', S, 'A-018'),
    (r'models/sgi_groups_cleanup\.py|tests/test_perm_groups\.py', E, 'B-005'),
    (r'addons/quimibond_sgi/README\.md$', C, 'K-003'),
]
DATA_MODELS = {
    'sgi.process': (E, 'A-002'), 'sgi.process.flow': (E, 'A-002'),
    'sgi.indicator.term': (S, 'A-004'), 'sgi.ppap.element.template': (S, 'A-018'),
    'sgi.indicator': (Q, 'J-019'), 'sgi.objective': (Q, 'J-019'), 'sgi.control.plan': (Q, 'J-019'),
}
DATA_XMLIDS = {
    'sgi_approval_category_purchase': (E, 'B-022'), 'sgi_approval_category_moc': (E, 'B-022'),
    'sgi_spreadsheet_dashboard_health': (E, 'E-017'), 'sgi_spreadsheet_dashboard_group': (E, 'E-017'),
    'format_map_sales_budget': (S, 'A-016'), 'sgi_project_dyd': (C, 'A-012'),
    'sgi_helpdesk_team_complaints': (C, 'D-006'),
}


def model_of(tipo, elemento, detalle):
    if tipo in ('modelo propio', 'modelo heredado'):
        return elemento
    if tipo in ('campo', 'método'):
        return elemento.rsplit('.', 1)[0]
    if tipo == 'vista':
        return (detalle or '').split(' ')[0]
    if tipo == 'acceso':
        m = re.match(r'model_(\S+)', detalle or '')
        return m.group(1).replace('_', '.') if m else ''
    if tipo == 'regla':
        m = re.search(r'model_(\S+)', detalle or '')
        return m.group(1).replace('_', '.') if m else ''
    return ''


def norm_model(m):
    """model_sgi_sales_budget_line -> sgi.sales.budget.line (probando los modelos conocidos)."""
    return m


# ---------------------------------------------------------------- (a) CSV por elemento
def v_vista(v):
    if v.startswith('Sale'):
        return S
    return {'Se queda': Q, 'Se corrige': C, 'Se elimina': E}[v]


vistas = {r['xml_id']: r for r in rd('04-vistas/revision_por_vista.csv') if r['addon'] == 'quimibond_sgi'}
metodos = {(r['modelo'] + '.' + r['metodo']): r for r in rd('02-obsoleto/metodos_103.csv')}
sin_help = {(r['modelo'] + '.' + r['campo']): r for r in rd('03-modelo/campos_sin_help.csv')}
menus = {r['xml_id']: r for r in rd('05-menus/menus_final.csv') if r['addon'] == 'quimibond_sgi'}
acciones = {r['xml_id']: r for r in rd('05-menus/acciones_revision.csv') if r['addon'] == 'quimibond_sgi'}
crons = {r['xml_id']: r for r in rd('07-logica/crons.csv')}
pruebas = {r['archivo']: r for r in rd('10-calidad/pruebas.csv')}
reglas = {r['xml_id']: r for r in rd('06-seguridad/reglas_evaluacion.csv')}
sudo = collections.defaultdict(list)
for r in rd('06-seguridad/sudo_clasificacion.csv'):
    if r['riesgo'] not in ('justificado', 'lectura/acotado', ''):
        sudo[r['modelo'] + '.' + r['metodo']].append(r['riesgo'])

MENU_V = {
    'Se queda': Q, 'Se queda (fuera)': Q, 'Grupos': C, 'Secuencia': C, 'Se renombra': C,
    'Renombra + grupos': C, 'Se renombra (fuera)': C, 'Secuencia (fuera)': C, 'Parent real': C,
    'Se mueve y renombra': C, 'Se mueve (módulo de ventas)': S, 'Se mueve (satélite)': S,
    'Se mueve o se archiva (contabilidad)': S, 'Se archiva (sin alimentación)': E,
}
# grupos (06-seguridad; decisiones tanda 1 #3)
GRUPOS = {'group_sgi_director': (C, 'F-013'), 'group_sgi_auditor': (C, 'F-012'),
          'group_sgi_efficiency_capture': (Q, 'F-020')}
# accesos con hallazgo propio
ACC_SPECIFIC = {'access_sgi_sign_request_wizard_user': (C, 'F-003'),
                'access_sgi_health_record_hr': (C, 'F-004'),
                'access_sgi_incident_user': (C, 'F-009')}
F010_MODELS = set(re.findall(r'`(sgi\.[a-z_.]+)`', raw['F-010']['elemento'] + raw['F-010']['hallazgo']))


def per_element(row, model):
    """(a): devuelve (clasificación, [ids], nota) o None."""
    t, el = row['tipo'], row['elemento']
    if t in ('vista', 'qweb'):
        v = vistas.get(el)
        if v:
            return v_vista(v['veredicto']), ids_in(v['hallazgos'] + ' ' + v['notas']), '04-vistas'
    if t == 'método':
        v = metodos.get(el)
        if v:
            cl = {'Se queda': Q, 'Se elimina': E, 'Se elimina con su migracion': E}[v['veredicto']]
            if v['metodo'] == 'sgi_next_code':  # contradicción B-012 vs C-004: gana C-004
                return Q, ['C-004'], '02-obsoleto (corregido: lo usa C-004)'
            return cl, ids_in(v['hallazgo']), '02-obsoleto'
        if el in sudo:
            pass
    if t == 'campo':
        v = sin_help.get(el)
        if v:
            return C, ['C-018'], '03-modelo/campos_sin_help (' + v['prioridad'] + ')'
    if t == 'menú':
        v = menus.get(el)
        if v:
            idz = ids_in(v['nota']) or {'Se mueve (módulo de ventas)': ['A-016'],
                                        'Se mueve (satélite)': ['A-018']}.get(v['veredicto'], [])
            if v['veredicto'] in ('Se renombra', 'Renombra + grupos', 'Se renombra (fuera)', 'Se mueve y renombra'):
                idz = idz or ['E-007']
            if v['veredicto'] in ('Grupos', 'Renombra + grupos'):
                idz = idz + ['E-009']
            if v['veredicto'].startswith('Secuencia'):
                idz = idz or ['E-013']
            return MENU_V.get(v['veredicto'], C), idz, '05-menus/menus_final'
    if t == 'acción':
        v = acciones.get(el)
        if v:
            idz = ids_in(v['nota'])
            if 'B-014' in idz:
                return E, idz, '05-menus/acciones_revision'
            if v['redefinida_en']:
                idz.append('B-015')
            if v['nombre_propuesto'] or v['clave_dropbox_en_nombre'] or idz:
                if v['clave_dropbox_en_nombre']:
                    idz.append('D-004')
                if v['nombre_propuesto'] and not idz:
                    idz.append('E-007')  # título = nombre del menú (E-007 absorbe D-005)
                return C, idz, '05-menus/acciones_revision'
            return Q, [], '05-menus/acciones_revision'
    if t == 'cron':
        v = crons.get(el)
        if v:
            idz = ids_in(v['hallazgos'])
            if el == 'sgi_cron_weekly_digest':
                return E, ['B-017'], '07-logica/crons (B-017 pendiente de decisión)'
            return (C if idz else Q), idz, '07-logica/crons'
    if t == 'regla':
        v = reglas.get(el)
        if v:
            idz = ids_in(v['evaluacion'])
            if el.startswith('rule_sgi_staff_eff_') and el.endswith('_hr_all'):
                idz.append('F-004')
            return (C if idz else Q), idz, '06-seguridad/reglas_evaluacion'
    if t == 'grupo' and el in GRUPOS:
        cl, i = GRUPOS[el]
        return cl, [i], '06-seguridad'
    if t == 'grupo':
        return Q, [], '06-seguridad'
    if t == 'acceso':
        if el in ACC_SPECIFIC:
            cl, i = ACC_SPECIFIC[el]
            return cl, [i], '06-seguridad'
        if model in F010_MODELS and el.endswith('_user'):
            return C, ['F-010'], '06-seguridad (F-010)'
        return None
    if t == 'archivo':
        base = os.path.basename(row['ubicacion'])
        v = pruebas.get(base)
        if v:
            idz = ids_in(v['toca_obsoleto'])
            return (C if idz else Q), idz, '10-calidad/pruebas'
    return None


# ---------------------------------------------------------------- (b) coincidencia de nombres
def acc_to_class(fid):
    r = by_final.get(fid)
    if not r:
        return None
    a = r['accion']
    if r['pr_propuesto'].startswith('e2-sale') or fid in ('A-004', 'A-019', 'A-020'):
        return S
    return {'Eliminar': E, 'Corregir': C, 'Agregar': C, 'Mover': C}.get(a, Q)


# Un nombre citado en la columna `elemento` del hallazgo toma la acción del hallazgo; uno citado
# solo en `propuesta` (p. ej. «usar _sgi_pending_records») aporta el ID pero a lo más «Se corrige».
tokens = collections.defaultdict(set)  # (texto citado, en_elemento) -> ids finales
for rid, r in raw.items():
    fid = to_final.get(rid, rid)
    for col, is_el in (('elemento', True), ('propuesta', False)):
        for tok in re.findall(r'`([^`]+)`', r[col]):
            tok = tok.strip()
            if len(tok) >= 6:
                tokens[(tok, is_el)].add(fid)
tok_list = list(tokens.items())
CONTAINERS = ('modelo propio', 'modelo heredado', 'archivo', 'qweb', 'vista')
KEEP = {'sgi.config.seed_parameters': 'B-001'}  # B-001: «dejar solo seed_parameters»
# B-001/A-006 (propuesta): estas siembras se borran; las dos últimas pasan a cron o a botón.
EXPLICIT = {
    'sgi.config.activate_auto_indicators': (E, ['B-001', 'A-006']),
    'sgi.config.fix_kpi_seeds': (E, ['B-001', 'A-006']),
    'sgi.config.harden_noupdate': (E, ['B-001', 'A-006']),
    'sgi.config.seed_process_purposes': (E, ['B-001', 'A-003']),
    'sgi.config.recompute_pending_measures': (C, ['A-006']),
    'sgi.config.migrate_document_families': (C, ['A-006', 'E-001']),
}

universe = rd('inventario/universo.csv')
name_count = collections.Counter(u['elemento'].rsplit('.', 1)[-1] for u in universe if u['tipo'] in ('campo', 'método'))


def by_name(row):
    t, el = row['tipo'], row['elemento']
    keys = {el}
    if t in ('campo', 'método'):
        short = el.rsplit('.', 1)[-1]
        if len(short) >= 12 and name_count[short] == 1:
            keys.add(short)
    if t == 'archivo':
        loc = row['ubicacion'].replace('addons/quimibond_sgi/', '')
        keys = {loc}
    found = {}
    for (tok, is_el), fids in tok_list:
        for k in keys:
            if k == tok or (len(k) >= 10 and re.search(r'(?<![\w.])' + re.escape(k) + r'(?![\w])', tok)):
                for f in fids:
                    found[f] = found.get(f, False) or is_el
    return found


# ---------------------------------------------------------------- clasificación
out = []
for u in universe:
    t, el, loc = u['tipo'], u['elemento'], u['ubicacion']
    model = model_of(t, el, u['detalle'])
    cls, ids, fuente, nota = None, [], '', ''
    # (0)
    if model in EXIT_MODELS and t in ('modelo propio', 'modelo heredado', 'campo', 'método', 'vista', 'acceso', 'regla'):
        if not (t == 'modelo heredado' and model != 'budget.analytic' and model not in EXIT_MODELS):
            cls, i = EXIT_MODELS[model]
            ids, fuente = [i], 'decisión (módulo que sale / modelo que se retira)'
    if cls is None:
        for rx, c, i in EXIT_FILES:
            if re.search(rx, loc):
                cls, ids, fuente = c, [i], 'decisión (archivo)'
                break
    if cls is None and t.startswith('dato '):
        dm = t[5:]
        if el in DATA_XMLIDS:
            cls, i = DATA_XMLIDS[el]
            ids, fuente = [i], 'hallazgo del dato'
        elif dm in DATA_MODELS:
            cls, i = DATA_MODELS[dm]
            ids, fuente = [i], 'hallazgo por modelo del dato'
            if i == 'J-019':
                nota = 'se queda mientras se decide D-01 (catálogo base o quimibond_sgi_mapa)'
        elif loc.endswith('sgi_audit_data.xml'):
            cls, ids, fuente = E, ['B-009'], 'hallazgo del dato (encuesta legado)'
    # (a)
    if cls is None:
        pe = per_element(u, model)
        if pe:
            cls, ids, fuente = pe
    # (b)
    extra = by_name(u)
    if cls is None and el in EXPLICIT:
        cls, ids, fuente = EXPLICIT[el][0], list(EXPLICIT[el][1]), 'hallazgo (propuesta de B-001/A-006)'
    if cls is None and el in KEEP:
        cls, ids, fuente = Q, [KEEP[el]], 'hallazgo (se conserva)'
    if cls is None and extra:
        cands = []
        for f, is_el in extra.items():
            c = acc_to_class(f)
            if not c:
                continue
            if not is_el or t in CONTAINERS:
                c = min(c, C, key=lambda x: RANK[x]) if c != Q else Q
            cands.append(c)
        if cands:
            cls = max(cands, key=lambda c: RANK[c])
            fuente = 'coincidencia de nombre en hallazgo'
    ids = sorted(set(ids) | set(extra))
    # (c)
    if cls is None:
        cls, fuente = Q, 'defecto'
    out.append(dict(u, clasificacion=cls, hallazgo=' '.join(ids), fuente=fuente, nota=nota))

path = os.path.join(OUT, 'universo_clasificado.csv')
with open(path, 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=list(out[0]))
    w.writeheader()
    w.writerows(out)

# ---------------------------------------------------------------- resumen
TIPO = lambda t: 'dato' if t.startswith('dato ') else t  # noqa: E731
tab = collections.defaultdict(collections.Counter)
for r in out:
    tab[TIPO(r['tipo'])][r['clasificacion']] += 1
print('tipo|Se queda|Se corrige|Se elimina|Sale a otro módulo|total')
for t in sorted(tab, key=lambda t: -sum(tab[t].values())):
    c = tab[t]
    print(f"{t}|{c[Q]}|{c[C]}|{c[E]}|{c[S]}|{sum(c.values())}")
tot = collections.Counter(r['clasificacion'] for r in out)
print(f"TOTAL|{tot[Q]}|{tot[C]}|{tot[E]}|{tot[S]}|{len(out)}")
print('vacíos', sum(1 for r in out if not r['clasificacion']))
print('fuente', collections.Counter(r['fuente'].split(' (')[0] for r in out))
sens = [r for r in out if r['fuente'] == 'defecto' and r['tipo'] in ('modelo propio', 'modelo heredado', 'grupo', 'regla', 'cron')]
print('sensibles solo por defecto', collections.Counter(r['tipo'] for r in sens))
with open(os.path.join(OUT, 'universo_solo_por_defecto_sensibles.csv'), 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=['n', 'tipo', 'elemento', 'ubicacion', 'detalle'])
    w.writeheader()
    for r in sens:
        w.writerow({k: r[k] for k in w.fieldnames})
