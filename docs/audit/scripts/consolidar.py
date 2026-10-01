# -*- coding: utf-8 -*-
"""Junta las tablas de hallazgos de docs/audit/NN-*.md en un CSV (fase 2)."""
import collections
import csv
import glob
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
AUDIT = os.path.dirname(HERE)
COLS = ['id', 'elemento', 'hallazgo', 'evidencia', 'severidad', 'accion', 'propuesta', 'esfuerzo', 'depende']


def split_row(line):
    cells, cur, tick, i = [], '', False, 0
    s = line.strip()
    if s.startswith('|'):
        s = s[1:]
    if s.endswith('|'):
        s = s[:-1]
    while i < len(s):
        ch = s[i]
        if ch == '\\' and i + 1 < len(s) and s[i + 1] == '|':
            cur += '|'
            i += 2
            continue
        if ch == '`':
            tick = not tick
        if ch == '|' and not tick:
            cells.append(cur.strip())
            cur = ''
        else:
            cur += ch
        i += 1
    cells.append(cur.strip())
    return cells


def clean(value):
    return re.sub(r'[*_]', '', value or '').strip()


def load():
    rows = []
    for path in sorted(glob.glob(os.path.join(AUDIT, '[01][0-9]-*.md'))):
        for line in open(path, encoding='utf-8'):
            if not re.match(r'^\|\s*[A-L]-\d{3}\s*\|', line):
                continue
            cells = split_row(line)
            if len(cells) > 9:  # celdas con | sin backticks: se pegan en «hallazgo»
                extra = len(cells) - 9
                cells = cells[:2] + [' | '.join(cells[2:3 + extra])] + cells[3 + extra:]
            cells += [''] * (9 - len(cells))
            row = dict(zip(COLS, cells))
            row['archivo'] = os.path.basename(path)
            sev = clean(row['severidad']).split()
            row['sev'] = sev[0] if sev else ''
            acc = clean(row['accion']).split()
            row['acc'] = acc[0] if acc else ''
            nums = re.findall(r'\d+(?:\.\d+)?', row['esfuerzo'])
            row['horas'] = float(nums[0]) if nums else 0.0
            rows.append(row)
    return rows


if __name__ == '__main__':
    rows = load()
    print(len(rows))
    print(collections.Counter(r['sev'] for r in rows))
    print(collections.Counter(r['acc'] for r in rows))
    bad = [r['id'] for r in rows if r['sev'] not in ('Crítica', 'Alta', 'Media', 'Baja')]
    print('sev raras', bad)
    bad = [r['id'] for r in rows if r['acc'] not in ('Eliminar', 'Corregir', 'Agregar', 'Mover', 'Documentar', 'Decidir')]
    print('acc raras', bad)
    print('horas', sum(r['horas'] for r in rows))
    with open(os.path.join(AUDIT, '99-consolidado', 'hallazgos.csv'), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['id', 'archivo', 'sev', 'acc', 'horas'] + COLS[1:4] + COLS[6:])
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, '') for k in w.fieldnames})


# ---------------------------------------------------------------------------
# Fase 2 — consolidación (un renglón por hallazgo único)
# Uso: python3 docs/audit/scripts/consolidar.py consolidar
# Todo lo que decide la consolidación está en las tablas de abajo, para que se
# pueda revisar y volver a correr. `decisiones.md` manda sobre los informes.
# ---------------------------------------------------------------------------

SEV_ORDER = {'Crítica': 4, 'Alta': 3, 'Media': 2, 'Baja': 1}

# PR -> (entrega, rama sugerida, tema)
PRS = {
    'PR452': ('1', 'claude/sgi-entrega-1 (PR #452)', 'Seguridad crítica e instalación limpia (hecho en código)'),
    'e1-candados-rpc': ('1', 'sgi/e1-candados-rpc', 'Seguridad: puertas por RPC y botones de firma'),
    'e1-fichas-estandar': ('1', 'sgi/e1-fichas-estandar', 'Seguridad: fichas estándar y reportes sin grupo'),
    'e1-reglas-captura': ('1', 'sgi/e1-reglas-captura', 'Seguridad: quién edita incidentes, planes, AMEF, PPAP'),
    'e1b-mis-pendientes': ('1b', 'sgi/e1b-mis-pendientes', 'Mis pendientes: archivado, empresa y avisos de reglas archivadas'),
    'e1c-docs-sin-propietario': ('1', 'sgi/e1c-documentos-sin-propietario', 'Seguridad: documentos controlados sin propietario'),
    'qb_mcp_politica': ('1', 'sgi/qb-mcp-politica', 'MCP: política en código'),
    'e2-changelog': ('2', 'sgi/e2-changelog', 'Limpieza: CHANGELOG antes de retirar migraciones'),
    'e2-sin-migraciones': ('2', 'sgi/e2-sin-migraciones', 'Limpieza: retirar migraciones ya aplicadas'),
    'e2-procesos-viejos': ('2', 'sgi/e2-procesos-viejos', 'Limpieza: procesos MP-*/P-* fuera del manifest'),
    'e2-siembras': ('2', 'sgi/e2-siembras-y-funciones', 'Limpieza: funciones que corren en cada update'),
    'e2-codigo-muerto': ('2', 'sgi/e2-codigo-muerto', 'Limpieza: métodos, campos, acciones y archivos sin uso'),
    'e2-legado': ('2', 'sgi/e2-legado', 'Limpieza: campos y modos de cálculo legado'),
    'e2-dependencias': ('2', 'sgi/e2-dependencias', 'Limpieza: dependencias del manifest'),
    'e2-decisiones-limpieza': ('2', '(según decisión)', 'Limpieza pendiente de decisión'),
    'e2-reglas-salida': ('2', '(en cada PR de salida)', 'Reglas comunes para sacar módulos'),
    'e2-sale-presupuesto': ('2', 'sgi/e2-sale-presupuesto-ventas', 'Sale del SGI: presupuesto de ventas'),
    'e2-sale-contabilidad': ('2', 'sgi/e2-sale-contabilidad', 'Sale del SGI: bitácora contable y valor de inventario'),
    'e2-sale-automotriz': ('2', 'sgi/e2-sale-automotriz', 'Sale del SGI: PPAP, AMEF y MSA'),
    'e2-satelites': ('2', 'sgi/e2-ganchos-satelites', 'Frontera con satélites (revisado, pesaje)'),
    'e3-format-map': ('3', 'sgi/e3-format-map-documento', 'Modelo: pie de formato ligado al documento'),
    'e3-procedimiento-proceso': ('3', 'sgi/e3-procedimiento-proceso', 'Modelo: el documento es la fuente de la sustitución'),
    'e3-clave-anterior': ('3', 'sgi/e3-clave-anterior', 'Modelo: clave anterior y clave nueva PR-{proceso}'),
    'e3-integridad': ('3', 'sgi/e3-integridad', 'Modelo: ondelete, requeridos, tipos y archivos'),
    'e3-multiempresa': ('3', 'sgi/e3-multiempresa', 'Modelo: una sola empresa'),
    'e3-mapa': ('3', 'sgi/e3-quimibond-sgi-mapa', 'Modelo: exportador y módulo quimibond_sgi_mapa'),
    'e4-arbol': ('4', 'sgi/e4-arbol-de-menus', 'Menús y acciones: árbol decidido'),
    'e4-grupos': ('4', 'sgi/e4-grupos-direccion-auditor', 'Menús y grupos: Dirección, Auditor, Usuario'),
    'e5-nomenclatura': ('5', 'sgi/e5-nomenclatura-en-pantallas', 'Vistas: sin claves del Dropbox'),
    'e5-herencias': ('5', 'sgi/e5-herencias-propias', 'Vistas: herencias propias y de fichas estándar'),
    'e5-reclamaciones': ('5', 'sgi/e5-reclamaciones', 'Vistas: reclamaciones y Generar NC'),
    'e5-fichas': ('5', 'sgi/e5-fichas-y-busquedas', 'Vistas: confirmaciones, búsquedas y ayudas'),
    'e6-seccion': ('6', 'sgi/e6-seccion-del-dropbox', 'Del Dropbox a Odoo: sección y permisos'),
    'e6-rutinas': ('6', 'sgi/e6-rutinas', 'Del Dropbox a Odoo: rutinas, buscador, importador y avance'),
    'e8-mediciones': ('8', 'sgi/e8-mediciones-y-bandeja', 'Lógica: mediciones, Mis pendientes y escalamientos'),
    'e8-avisos-cron': ('8', 'sgi/e8-avisos-de-crons', 'Lógica: actividades que agendan los crons'),
    'e8-calendario': ('8', 'sgi/e8-zona-horaria-y-dias-habiles', 'Lógica: zona horaria y días hábiles'),
    'e8-medicion': ('8', 'sgi/e8-medicion-robusta', 'Lógica: medición automática sin errores silenciosos'),
    'e8-calibracion': ('8', 'sgi/e8-calibracion', 'Lógica: calibración avisa y no bloquea'),
    'e8-rendimiento': ('8', 'sgi/e8-rendimiento', 'Rendimiento'),
    'e8-checklist-pin': ('8', 'sgi/e8-checklist-pin', 'Lógica: firma de checklist'),
    'e9-ci': ('9', 'sgi/e9-ci-y-checkers', 'Pruebas: CI, checkers y linters'),
    'e9-pruebas': ('9', 'sgi/e9-pruebas', 'Pruebas: huecos de cobertura'),
    'e10-historico': ('10', 'sgi/e10-docs-historico', 'Documentación: archivar lo viejo'),
    'e10-readme': ('10', 'sgi/e10-readme-y-reglas', 'Documentación: README, CLAUDE.md, manifest'),
    'e10-tecnica': ('10', 'sgi/e10-docs-generadas', 'Documentación técnica generada'),
    'e10-help': ('10', 'sgi/e10-help', 'Ayuda en campos y pantallas'),
    'e10-manuales': ('10', 'sgi/e10-manuales', 'Manuales por rol y guía de transición'),
    'prod': ('Prod', '(sin rama: producción)', 'Acción en producción'),
}

# principal -> (PR, [absorbidos])
GRUPOS = {
    # Entrega 1
    'A-001': ('PR452', []), 'B-004': ('PR452', []), 'B-001': ('PR452', []),
    'F-003': ('PR452', []), 'F-004': ('PR452', ['D-022']), 'I-013': ('PR452', []),
    'J-002': ('PR452', []),
    'F-005': ('e1-candados-rpc', []), 'F-006': ('e1-candados-rpc', []), 'F-017': ('e1-candados-rpc', []),
    'F-007': ('e1-candados-rpc', []), 'F-008': ('e1-candados-rpc', []), 'F-015': ('e1-candados-rpc', []),
    'I-014': ('e1-candados-rpc', []),
    'D-001': ('e1-fichas-estandar', []), 'E-011': ('e1-fichas-estandar', ['F-016']),
    'F-009': ('e1-reglas-captura', []), 'F-010': ('e1-reglas-captura', []),
    'I-004': ('e1b-mis-pendientes', ['E-003', 'G-014']),
    'N-001': ('e1c-docs-sin-propietario', []), 'N-002': ('e1b-mis-pendientes', []),
    'N-003': ('e1b-mis-pendientes', []),
    'F-001': ('prod', ['F-002']), 'L-001': ('prod', []),
    # Entrega 2
    'K-018': ('e2-changelog', []),
    'A-026': ('e2-sin-migraciones', ['B-003', 'A-027']), 'B-005': ('e2-sin-migraciones', []),
    'A-002': ('e2-procesos-viejos', ['B-021']), 'A-003': ('e2-procesos-viejos', ['B-018']),
    'B-002': ('e2-procesos-viejos', []),
    'A-006': ('e2-siembras', []), 'E-008': ('e2-siembras', []), 'A-007': ('e2-siembras', []),
    'A-005': ('e2-siembras', []),
    'B-011': ('e2-codigo-muerto', []), 'I-022': ('e2-codigo-muerto', ['G-013']),
    'B-012': ('e2-codigo-muerto', []), 'B-013': ('e2-codigo-muerto', []), 'B-014': ('e2-codigo-muerto', []),
    'B-016': ('e2-codigo-muerto', []), 'D-012': ('e2-codigo-muerto', []), 'B-006': ('e2-codigo-muerto', []),
    'A-023': ('e2-codigo-muerto', ['B-019']),
    'B-009': ('e2-legado', ['D-011', 'C-017']), 'B-010': ('e2-legado', []),
    'A-011': ('e2-dependencias', []), 'A-012': ('e2-dependencias', []), 'A-015': ('e2-dependencias', []),
    'A-014': ('e2-dependencias', []), 'A-010': ('e2-dependencias', []),
    'B-007': ('e2-decisiones-limpieza', []), 'B-017': ('e2-decisiones-limpieza', []),
    'B-022': ('e2-decisiones-limpieza', ['H-020']),
    'E-002': ('e2-reglas-salida', []), 'J-018': ('e2-reglas-salida', []),
    'A-016': ('e2-sale-presupuesto', ['A-013', 'E-014']),
    'A-017': ('e2-sale-contabilidad', ['G-018']),
    'A-018': ('e2-sale-automotriz', []),
    'A-019': ('e2-satelites', []), 'A-020': ('e2-satelites', []),
    # Entrega 3
    'C-006': ('e3-format-map', []),
    'C-001': ('e3-procedimiento-proceso', ['C-014', 'D-017', 'L-003']),
    'C-002': ('e3-procedimiento-proceso', []), 'C-015': ('e3-procedimiento-proceso', []),
    'L-005': ('e3-procedimiento-proceso', ['C-003']),
    'C-004': ('e3-clave-anterior', ['L-004']), 'C-005': ('e3-clave-anterior', []),
    'C-007': ('e3-clave-anterior', []), 'C-009': ('e3-clave-anterior', []),
    'C-013': ('e3-integridad', ['C-022']), 'C-010': ('e3-integridad', []), 'C-019': ('e3-integridad', []),
    'C-021': ('e3-integridad', []), 'B-008': ('e3-integridad', []), 'A-022': ('e3-integridad', []),
    'C-012': ('e3-multiempresa', ['H-021']), 'F-014': ('e3-multiempresa', []),
    'H-022': ('e3-mapa', []), 'H-007': ('e3-mapa', []), 'H-014': ('e3-mapa', []), 'H-010': ('e3-mapa', []),
    'A-004': ('e3-mapa', []), 'J-019': ('e3-mapa', []),
    # Entrega 4
    'E-007': ('e4-arbol', ['D-005']), 'E-013': ('e4-arbol', []), 'A-025': ('e4-arbol', []),
    'E-012': ('e4-arbol', []), 'E-010': ('e4-arbol', []), 'I-009': ('e4-arbol', []),
    'B-015': ('e4-arbol', ['A-009']), 'E-017': ('e4-arbol', []),
    'E-009': ('e4-grupos', ['F-011']), 'F-013': ('e4-grupos', []), 'F-012': ('e4-grupos', []),
    'D-009': ('e4-grupos', []), 'I-015': ('e4-grupos', []),
    # Entrega 5
    'D-002': ('e5-nomenclatura', []), 'D-003': ('e5-nomenclatura', ['D-020']),
    'D-004': ('e5-nomenclatura', ['E-006']),
    'A-008': ('e5-herencias', []), 'D-019': ('e5-herencias', []), 'D-018': ('e5-herencias', []),
    'D-006': ('e5-reclamaciones', ['D-010']),
    'D-007': ('e5-fichas', []), 'D-008': ('e5-fichas', []), 'D-013': ('e5-fichas', ['E-016']),
    'D-014': ('e5-fichas', []), 'D-015': ('e5-fichas', ['E-015']), 'D-016': ('e5-fichas', []),
    'D-021': ('e5-fichas', []),
    # Entrega 6
    'E-001': ('e6-seccion', ['L-007', 'I-016', 'B-020']), 'E-004': ('e6-seccion', []),
    'L-010': ('e6-seccion', ['E-005']), 'C-011': ('e6-seccion', []), 'L-011': ('e6-seccion', []),
    'L-012': ('e6-seccion', []), 'L-013': ('e6-seccion', []), 'L-014': ('e6-seccion', []),
    'L-002': ('e6-rutinas', ['L-015', 'L-021']), 'L-017': ('e6-rutinas', []),
    'L-018': ('e6-rutinas', ['C-016']), 'L-006': ('e6-rutinas', []),
    'L-009': ('e6-rutinas', ['L-019']), 'L-008': ('e6-rutinas', ['L-016', 'L-020']),
    # Entrega 8
    'G-001': ('e8-mediciones', ['I-006']), 'I-001': ('e8-mediciones', []), 'I-007': ('e8-mediciones', []),
    'I-012': ('e8-mediciones', []), 'G-017': ('e8-mediciones', []), 'I-021': ('e8-mediciones', []),
    'G-004': ('e8-avisos-cron', ['I-008', 'G-021']), 'G-005': ('e8-avisos-cron', ['G-025']),
    'G-011': ('e8-avisos-cron', []), 'G-010': ('e8-avisos-cron', []), 'G-012': ('e8-avisos-cron', []),
    'G-024': ('e8-avisos-cron', []), 'I-003': ('e8-avisos-cron', []), 'I-002': ('e8-avisos-cron', []),
    'G-007': ('e8-calendario', []), 'G-008': ('e8-calendario', []), 'G-009': ('e8-calendario', []),
    'G-022': ('e8-calendario', []), 'G-020': ('e8-calendario', []),
    'G-002': ('e8-medicion', ['H-005']), 'G-003': ('e8-medicion', []), 'H-006': ('e8-medicion', []),
    'G-019': ('e8-medicion', []), 'H-016': ('e8-medicion', []), 'G-026': ('e8-medicion', []),
    'H-018': ('e8-medicion', []),
    'G-006': ('e8-calibracion', []),
    'G-015': ('e8-rendimiento', []), 'G-016': ('e8-rendimiento', []), 'G-023': ('e8-rendimiento', []),
    'I-005': ('e8-checklist-pin', []),
    # Entrega 9
    'J-003': ('e9-ci', ['J-001']), 'J-023': ('e9-ci', []), 'J-022': ('e9-ci', []), 'J-021': ('e9-ci', []),
    'J-024': ('e9-ci', []),
    'J-004': ('e9-pruebas', []), 'J-005': ('e9-pruebas', []), 'J-006': ('e9-pruebas', []),
    'J-007': ('e9-pruebas', []), 'J-008': ('e9-pruebas', []), 'J-009': ('e9-pruebas', []),
    'J-010': ('e9-pruebas', []), 'J-011': ('e9-pruebas', []), 'J-012': ('e9-pruebas', []),
    'J-013': ('e9-pruebas', []), 'J-014': ('e9-pruebas', []), 'J-015': ('e9-pruebas', []),
    'J-016': ('e9-pruebas', []), 'J-017': ('e9-pruebas', []), 'J-020': ('e9-pruebas', []),
    'J-025': ('e9-pruebas', []), 'J-026': ('e9-pruebas', []),
    # Entrega 10
    'K-001': ('e10-historico', []), 'K-002': ('e10-historico', []), 'K-005': ('e10-historico', []),
    'K-006': ('e10-historico', []), 'K-007': ('e10-historico', []), 'K-008': ('e10-historico', []),
    'K-009': ('e10-historico', []), 'K-010': ('e10-historico', []), 'K-011': ('e10-historico', []),
    'K-012': ('e10-historico', []),
    'K-003': ('e10-readme', ['B-023']), 'K-004': ('e10-readme', []), 'K-016': ('e10-readme', []),
    'K-013': ('e10-readme', []), 'A-028': ('e10-readme', ['K-014']), 'K-015': ('e10-readme', []),
    'A-021': ('e10-readme', []), 'A-024': ('e10-readme', []), 'A-029': ('e10-readme', []),
    'A-030': ('e10-readme', []), 'C-020': ('e10-readme', []), 'C-023': ('e10-readme', []),
    'H-017': ('e10-readme', []), 'F-018': ('e10-readme', []), 'F-019': ('e10-readme', []),
    'F-021': ('e10-readme', []),
    'K-021': ('e10-tecnica', []), 'K-020': ('e10-tecnica', []), 'K-017': ('e10-tecnica', []),
    'C-018': ('e10-help', ['K-019']),
    'K-022': ('e10-manuales', []), 'K-023': ('e10-manuales', []), 'K-024': ('e10-manuales', []),
    # Producción (sin código)
    'H-003': ('prod', []), 'C-008': ('prod', []), 'H-001': ('prod', ['I-010']), 'H-002': ('prod', []),
    'I-017': ('prod', []), 'I-018': ('prod', []), 'I-019': ('prod', []), 'I-020': ('prod', []),
    'F-020': ('prod', []), 'H-004': ('prod', []), 'H-008': ('prod', []), 'H-009': ('prod', []),
    'H-011': ('prod', []), 'H-012': ('prod', []), 'H-013': ('prod', []), 'H-015': ('prod', []),
    'H-019': ('prod', []), 'I-011': ('prod', []),
}

# Pedidos nuevos de Jose que no traen ID de agente (decisiones.md).
NUEVOS = {
    'N-001': dict(archivo='decisiones.md', sev='Alta', acc='Agregar', horas=3.0,
                  elemento='documents.document (controlados) · access_internal / owner_id',
                  hallazgo='Pedido de Jose tras L-001: un documento controlado sin propietario no puede quedar abierto a todos los internos sin revisión.',
                  propuesta='Regla en create/write de documents.document: si sgi_is_controlled y no hay propietario (sgi_owner_id), access_internal no puede ser «view»/«edit» salvo que lo marque Jefe MAST; aviso en Diagnóstico con la lista. Prueba with_user.',
                  depende='H-015 (llenar propietarios antes de encender la regla)'),
    'N-002': dict(archivo='decisiones.md', sev='Media', acc='Corregir', horas=1.5,
                  elemento='models/sgi_my_pending.py (sin filtro de company_id)',
                  hallazgo='Entrega 1b: Mis pendientes no filtra por empresa; las aprobaciones de PV15254/PV15323 son de la empresa 4 (BDC BOSQUES).',
                  propuesta='Filtrar cada tipo por la empresa del SGI (o env.companies del usuario). Prueba con una segunda empresa.',
                  depende='C-012 (una sola empresa)'),
    'N-003': dict(archivo='decisiones.md', sev='Media', acc='Agregar', horas=2.0,
                  elemento='studio.approval.rule (archivar) → mail.activity «Conceder aprobación»',
                  hallazgo='Entrega 1b: al archivar una regla de aprobación sus avisos abiertos siguen vivos en la campana; hay que proponer qué pasa con ellos.',
                  propuesta='Al archivar la regla (write active=False): listar sus solicitudes y actividades abiertas y ofrecer cerrarlas con nota «regla archivada» (no se borran). Mis pendientes ya las oculta (sgi_archived_filters.py:98); falta prueba.',
                  depende='—'),
}

ESTADO = {i: 'En PR 452' for i in ('A-001', 'B-004', 'B-001', 'F-003', 'F-004', 'I-013', 'J-002')}
ESTADO.update({
    'L-001': 'Resuelto en producción',
    'F-001': 'Acción de Jose', 'H-003': 'Acción de Jose', 'C-008': 'Acción de Jose',
    'H-001': 'Acción de Jose', 'H-002': 'Acción de Jose', 'I-018': 'Acción de Jose',
    'I-019': 'Acción de Jose', 'I-020': 'Acción de Jose', 'H-004': 'Acción de Jose',
    'H-013': 'Acción de Jose', 'H-015': 'Acción de Jose',
})
for i in ('A-010', 'B-007', 'B-017', 'B-022', 'C-012', 'E-017', 'F-009', 'F-010', 'I-002', 'I-011',
          'I-015', 'I-005', 'J-019', 'K-024', 'J-003', 'F-020', 'I-017', 'B-010'):
    ESTADO[i] = 'Decisión pendiente'

# Severidad del grupo = la más alta, salvo cuando el absorbido fue corregido
# por el principal (su severidad salía de una premisa falsa).
SEV_OVERRIDE = {'I-022': 'Baja'}  # G-013 (Alta) resultó código muerto (I-022)

RESUMEN = {
    'A-001': 'Instalación limpia revienta por XML IDs usados antes de definirse. Corregido en PR 452 (orden del manifest + regla en check_odoo_views).',
    'B-001': 'Siembras que pisaban a MAST en cada update. PR 452 quita 4 de 6; recompute_pending_measures y migrate_document_families siguen (A-006).',
    'F-004': 'Exámenes médicos abiertos a 20 «Encargados de empleados». PR 452 crea «Salud ocupacional (SGI)»; salarios de eficiencias: depurar los 20 en producción (decisión 2).',
    'I-013': 'Cabos de salud: (a) menú con -hr.group_hr_user en PR 452; (b) avisos a 88 se resuelven metiendo a 88 al grupo (decisión «Salud ocupacional»); (c) = F-004.',
    'L-001': 'P-I01 abierto a todos: acceso cerrado por Jose (3560 access_internal=none, verificado 2026-09-29). Falta rotar credenciales (Sistemas).',
    'F-001': 'MCP con CRUD en modelos técnicos. Jose lo aplica en Ajustes (30 solo lectura + 9 desactivados, mcp_propuesta.csv); después qb_mcp_politica.',
    'H-003': '15 actividades periódicas sin mes/día: Jose cargó 13 (verificado por MCP); faltan E2.21 (MAST) y S4.30 (RH).',
    'C-001': 'Dos fuentes de «procedimiento sustituido». Documento manda (M2O restrict); proceso pasa a One2many inversa; la migración solo respalda (L-003).',
    'C-004': 'Clave del Dropbox vive en sgi_code. Migración sgi_code→sgi_previous_code sobre 490 documentos (L-004), después de C-006.',
    'L-005': 'Estado de migración de procedimientos: 44 a «En curso» y 5 a «No aplica»; P-I01 aparte (decisión C-003).',
    'I-004': 'Corrige E-003: 56.24.0 ya oculta la regla archivada 36; queda filtrar procesos archivados en acción/NC/documento/legal (G-014) y la prueba.',
    'I-022': 'Las 5 listas de pendientes de Mi procedimiento no se muestran (código muerto): se quitan. Corrige a G-013.',
    'G-001': 'Mediciones automáticas salen como «Medir…» y nacen atrasadas. «Validar» es del dueño del indicador en 3 días hábiles (I-006, decisión #3).',
    'A-017': 'Bitácora de bloqueo y valor de inventario salen a contabilidad (decisión 5). sgi_snapshot sí se llama (G-018) pero hay 0 fotos: quitar si nada lo alimenta.',
    'B-015': '17 act_window definidas dos veces (13 en sgi_diagram_views.xml, 2 hierarchy, 2 ventas). Corrige el conteo de A-009 (15).',
    'B-012': '8 métodos sin llamador se borran, salvo sgi_next_code (lo usa C-004 para la clave nueva).',
    'I-014': 'Botón «Firmar» visible para usuarios sin permiso de Firma tras F-003. Sigue abierto: la decisión «Salud ocupacional» cita I-014 pero resuelve I-013(b).',
}


def _first_sentence(text, n=170):
    t = re.sub(r'[*`]', '', text or '').strip()
    m = re.split(r'(?<=[.;])\s', t, maxsplit=1)
    s = m[0] if m else t
    return s if len(s) <= n else s[:n - 1].rstrip() + '…'


def consolidar():
    raw = {r['id']: r for r in load()}
    for k, v in NUEVOS.items():
        v = dict(v, id=k)
        raw[k] = v
    seen = collections.Counter()
    for p, (_, absb) in GRUPOS.items():
        seen[p] += 1
        for a in absb:
            seen[a] += 1
    missing = sorted(set(raw) - set(seen))
    dup = sorted(k for k, v in seen.items() if v > 1)
    extra = sorted(set(seen) - set(raw))
    assert not missing and not dup and not extra, (missing, dup, extra)
    out = []
    for p, (pr, absb) in GRUPOS.items():
        grp = [raw[p]] + [raw[a] for a in absb]
        sev = SEV_OVERRIDE.get(p) or max((g['sev'] for g in grp), key=lambda s: SEV_ORDER.get(s, 0))
        entrega, rama, tema = PRS[pr]
        estado = ESTADO.get(p, 'Abierto')
        deps = raw[p].get('depende', '') or '—'
        out.append({
            'id_final': p, 'ids_absorbidos': ' '.join(absb), 'tema': tema, 'severidad': sev,
            'accion': raw[p]['acc'], 'estado': estado, 'entrega': entrega,
            'pr_propuesto': pr if pr != 'prod' else 'producción',
            'esfuerzo_h': max(float(g['horas']) for g in grp),
            'depende_de': re.sub(r'\s+', ' ', re.sub(r'[*`]', '', deps)).strip(),
            'resumen': RESUMEN.get(p) or _first_sentence(raw[p]['hallazgo']),
        })
    order = {k: i for i, k in enumerate(['1', '1b', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'Prod'])}
    out.sort(key=lambda r: (order[r['entrega']], r['pr_propuesto'], r['id_final']))
    path = os.path.join(AUDIT, '99-consolidado', 'hallazgos_consolidados.csv')
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(out[0]))
        w.writeheader()
        w.writerows(out)
    total_ids = sum(1 + len(r['ids_absorbidos'].split()) for r in out)
    print('renglones', len(out), 'ids cubiertos', total_ids, 'de', len(raw),
          '(agentes', len([k for k in raw if not k.startswith('N-')]), '+ nuevos', len(NUEVOS), ')')
    print('estado', collections.Counter(r['estado'] for r in out))
    print('severidad', collections.Counter(r['severidad'] for r in out))
    return out


if __name__ == '__main__' and len(__import__('sys').argv) > 1 and __import__('sys').argv[1] == 'consolidar':
    consolidar()
