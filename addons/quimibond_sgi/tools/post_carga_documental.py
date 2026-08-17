# -*- coding: utf-8 -*-
"""Post-carga documental SGI: liga claves, entregables y procesos (quimibond_sgi).

Se ejecuta DESPUÉS de tools/carga_documental.py y hace, en la misma base:

  1. FLUJOS ↔ FORMATOS: asigna `document_id` en los flujos del mapa de procesos
     buscando el documento vigente por su clave (F-P-A12-01, F-P-A28-13, ...).
  2. DOCUMENTOS ↔ PROCESOS: llena `sgi_process_id` de todo documento controlado
     que no lo tenga, deduciéndolo de la familia de su clave (A28→Ventas,
     A12→Planeación, C/I→Calidad/Inspección, ...).
  3. RESCATE DE CLAVES FUERA DE NORMA: revisa la carpeta POR CLASIFICAR y
     re-detecta claves con reglas relajadas (prefijos numéricos "01. ",
     guiones extra tipo F-P-A-16-01 → F-P-A16-01); si rescata una clave y no
     existe otro vigente igual, promueve el documento a controlado.
  4. LISTAS DE TRABAJO (CSV en /tmp): lo que requiere decisión humana:
     - /tmp/worklist_worksheets.csv  → formatos clase "B" a configurar como
       hojas de trabajo de Calidad (con el id del documento cargado).
     - /tmp/worklist_puestos.csv     → plantilla clave,puesto para asignar
       puestos aplicables en masa (ver carga con PUESTOS_CSV abajo).
  5. PUESTOS APLICABLES (opcional): si existe /tmp/puestos_documentos.csv con
     columnas clave,puesto (una fila por pareja), asigna sgi_job_ids en masa.

MODO SEGURO: DRY_RUN=True por defecto (solo imprime el plan y escribe CSVs).
Idempotente: re-ejecutable sin duplicar nada.

Uso (shell de Odoo.sh):
    odoo-bin shell --no-http < addons/quimibond_sgi/tools/post_carga_documental.py
"""
import csv
import re

DRY_RUN = True                          # ← poner False para aplicar
PUESTOS_CSV = '/tmp/puestos_documentos.csv'
WORKLIST_WS = '/tmp/worklist_worksheets.csv'
WORKLIST_PUESTOS = '/tmp/worklist_puestos.csv'
COMMIT_EVERY = 100

# --- 1) Flujo (xmlid) -> clave del formato con que se entrega ---------------
FLOW_DOCUMENT = {
    'flow_ventas_plan_pronostico': 'F-P-A28-13',
    'flow_ventas_plan_presupuesto': 'F-P-A28-14',
    'flow_plan_prod': 'F-P-A12-01',
    'flow_plan_almacen': 'F-P-A12-02',
    'flow_plan_ventas': 'F-P-A12-03',
    'flow_rh_direccion': 'F-P-A01-01',
    # Flujos de la mini-fase 4.8 (sgi_process_flows_extra.xml)
    'flow_prodtac_mto': 'F-P-M01-01',
    'flow_prodent_mto': 'F-P-M01-01',
    'flow_inspeccion_sgi_nc': 'F-P-G05-01',
    'flow_ventas_diseno': 'F-P-A28-03',
}

# --- 2) Familia de clave -> xmlid del proceso -------------------------------
#     (más específico primero; la letra sale de P-Xnn dentro de la clave)
FAMILY_PROCESS = [
    ('A28', 'proc_ventas'), ('V01', 'proc_ventas'),
    ('A12', 'proc_planeacion'), ('A03', 'proc_planeacion'),
    ('A02', 'proc_compras'),
    ('A07', 'proc_almacen_mp'),
    ('A16', 'proc_logistica'),
    ('A13', 'proc_facturacion'), ('A25', 'proc_facturacion'), ('A26', 'proc_facturacion'),
    ('A22', 'proc_cxc'),
    ('A01', 'proc_rh'),
    ('A10', 'proc_direccion'),
    ('A14', 'proc_sgi'), ('A24', 'proc_sgi'), ('A04', 'proc_sgi'),
    ('A17', 'proc_sgi'), ('A18', 'proc_sgi'), ('A19', 'proc_sgi'),
    ('A20', 'proc_sgi'), ('A33', 'proc_sgi'), ('A30', 'proc_sgi'),
    ('I01', 'proc_inspeccion'),
    ('C05', 'proc_laboratorio'), ('C06', 'proc_laboratorio'),
    ('P01', 'proc_prod_tac'), ('P02', 'proc_prod_ent'), ('P03', 'proc_almacen_mp'),
    ('M01', 'proc_mto'),
    ('D01', 'proc_dis'), ('D02', 'proc_dis'),
]
LETTER_PROCESS = {
    'C': 'proc_cal', 'I': 'proc_inspeccion', 'G': 'proc_sgi', 'E': 'proc_sgi',
    'S': 'proc_sgi', 'M': 'proc_mto', 'D': 'proc_dis', 'P': 'proc_mfg',
    'A': 'proc_adm', 'V': 'proc_ventas',
}

# --- 2.5) Clasificación de migración por familia (prefijo -> clase, destino) ---
MIGRATION_RULES = [
    ('F-P-G01-03', 'a', 'Vista de Documentos (lista maestra)'),
    ('F-P-G01-09', 'a', 'Vista de Documentos (lista maestra externa)'),
    ('F-P-G01-06', 'a', 'Aprobaciones > Modificación de documento SGI'),
    ('F-P-G01-16', 'c', 'Reporte NEWS mensual'),
    ('F-P-G01', 'd', 'Plantilla de elaboración de documentos'),
    ('F-P-G03-01', 'a', 'SGI > Auditorías > Programa anual'),
    ('F-P-G03-03', 'c', 'Reporte: plan de auditoría'),
    ('F-P-G03', 'a', 'SGI > Auditorías'),
    ('F-P-G04', 'a', 'Alertas de calidad + cuarentena'),
    ('F-P-G05-01', 'a', 'SGI > No Conformidades'),
    ('F-P-G05-02', 'a', 'SGI > NC > Concentrado (vista)'),
    ('F-P-A01-01', 'a', 'Empleados > Puestos (descripción + skills)'),
    ('F-P-A01-17', 'b', 'Encuesta DNC'),
    ('F-P-A01', 'a', 'Empleados (RH nativo)'),
    ('F-P-A02', 'a', 'Compras (requisición/OC nativas)'),
    ('F-P-A04', 'd', 'Conocimiento (matriz de comunicación)'),
    ('F-P-A07', 'a', 'Inventario (almacén nativo)'),
    ('F-P-A10-01', 'a', 'SGI > Revisión por la Dirección'),
    ('F-P-A10-02', 'a', 'Proyecto > Mejora Continua SGI'),
    ('F-P-A10-03', 'a', 'SGI > Medición > Indicadores'),
    ('F-P-A10-04', 'a', 'Helpdesk > Quejas y Sugerencias'),
    ('F-P-A10-05', 'b', 'Encuesta de consulta y participación'),
    ('F-P-A12', 'a', 'Fabricación > Plan Maestro (MPS)'),
    ('F-P-A13', 'a', 'Contabilidad nativa'),
    ('F-P-A14-02', 'a', 'SGI > Riesgos (patrimonial)'),
    ('F-P-A14', 'd', 'Documento MAST/SST'),
    ('F-P-A16', 'a', 'Inventario > Entregas (logística nativa)'),
    ('F-P-A22', 'a', 'Contabilidad > Seguimientos de cobranza'),
    ('F-P-A25', 'a', 'Contabilidad'),
    ('F-P-A26', 'a', 'Nómina (localización MX)'),
    ('F-P-A28-13', 'a', 'CRM > Pronóstico'),
    ('F-P-A28-14', 'a', 'Contabilidad > Presupuestos'),
    ('F-P-A28', 'a', 'Ventas nativas'),
    ('F-P-C01', 'a', 'Helpdesk > Reclamaciones'),
    ('F-P-C03', 'a', 'SGI > Metrología > Calibraciones'),
    ('F-P-C07', 'c', 'Certificado de Calidad del lote (CoA)'),
    ('F-P-C09', 'a', 'SGI > Riesgos'),
    ('F-P-C16', 'b', 'Punto de control con hoja de trabajo'),
    ('F-P-C', 'b', 'Punto de control de Calidad'),
    ('F-P-P03-01', 'b', 'Punto de control MP (ya configurado)'),
    ('F-IT-P-P01-08', 'a', 'Módulos de pesaje/revisado (piso)'),
    ('F-IT-P-P01', 'b', 'Worksheet/bitácora por máquina'),
    ('F-IT-P-C05', 'b', 'Laboratorio: worksheet'),
    ('F-IT-P-G03-01-01', 'b', 'Encuesta: evaluación de auditores'),
    ('F-IT-P', 'b', 'Worksheet según proceso'),
    ('F-P-P', 'b', 'Registro de producción (worksheet/wizard)'),
    ('F-P-I01', 'b', 'Inspección/empaque: quality points'),
    ('F-P-M01', 'a', 'Mantenimiento > Solicitudes (OT)'),
    ('F-P-S01', 'a', 'SGI > Riesgos (IPER)'),
    ('F-P-S02', 'a', 'SGI > SST > Incidentes'),
    ('F-P-S03', 'a', 'Mantenimiento > Equipos (EPP)'),
    ('F-P-S', 'd', 'Documento SST'),
    ('F-P-E01', 'a', 'SGI > Riesgos (ambiental)'),
    ('F-P-E02', 'd', 'Documento externo con revisión (legal)'),
    ('F-P-E', 'd', 'Documento ambiental'),
    ('F-P-D', 'a', 'Diseño (proyecto/PLM)'),
    ('F-P-V01', 'a', 'Ventas nativas'),
]


def migration_of(code):
    for prefix, cls, target in MIGRATION_RULES:
        if code.startswith(prefix):
            return cls, target
    return 'x', ''


# --- 3) Familias clase "B" (candidatas a hoja de trabajo de Calidad) --------
B_FAMILIES = ('F-P-C16', 'F-P-C04', 'F-P-C02', 'F-P-I01', 'F-IT-P-P01',
              'F-IT-P-C05', 'F-P-P03')

CODE_IN_NAME = re.compile(
    r'(MIID'
    r'|F-IT-P-[AGCDEIMPSV]-?\d{2}-\d{2}-\d{2}'
    r'|IT-P-[AGCDEIMPSV]-?\d{2}-\d{2}'
    r'|F-P-[AGCDEIMPSV]-?\d{2}-\d{2}'
    r'|P-[AGCDEIMPSV]-?\d{2}'
    r'|DAT[ \-]?P-[AGCDEIMPSV]-?\d{2}-\d{2}(?:-\d{2})?'
    r'|PROT-\d{2}'
    r'|ANEXO \d{1,2})', re.IGNORECASE)
FAMILY_IN_CODE = re.compile(r'P-([AGCDEIMPSV])(\d{2})')


def _norm_code(raw):
    """Normaliza: mayúsculas y quita el guion extra tipo A-16 -> A16."""
    code = raw.upper().strip()
    return re.sub(r'P-([AGCDEIMPSV])-(\d{2})', r'P-\1\2', code)


def _doc_type_of(code):
    for prefix, dtype in [('F-IT-P-', 'formato_it'), ('F-P-', 'formato'),
                          ('IT-P-', 'instructivo'), ('P-', 'procedimiento'),
                          ('MIID', 'miid'), ('DAT', 'dat'), ('PROT-', 'protocolo'),
                          ('ANEXO', 'anexo')]:
        if code.startswith(prefix):
            return dtype
    return None


def run(env):
    Doc = env['documents.document']
    ref = lambda xmlid: env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
    done = {'flujos': 0, 'procesos': 0, 'rescatados': 0, 'puestos': 0, 'clasificados': 0}
    pending_commit = [0]

    def tick():
        pending_commit[0] += 1
        if not DRY_RUN and pending_commit[0] % COMMIT_EVERY == 0:
            env.cr.commit()

    def vigente(code):
        return Doc.search([('sgi_code', '=', code), ('sgi_state', '=', 'vigente')], limit=1)

    # ---- 1) Flujos <-> formatos -------------------------------------------
    print("\n1) FLUJOS <-> FORMATOS")
    for flow_xmlid, code in FLOW_DOCUMENT.items():
        flow = ref(flow_xmlid)
        doc = vigente(code)
        if not flow or flow.document_id:
            continue
        if not doc:
            print("   - %-32s %s: documento no encontrado" % (flow_xmlid, code))
            continue
        print("   ✓ %-32s -> %s" % (flow_xmlid, code))
        if not DRY_RUN:
            flow.document_id = doc.id
        done['flujos'] += 1

    # ---- 2) Documentos <-> procesos ---------------------------------------
    print("\n2) DOCUMENTOS <-> PROCESOS (sgi_process_id por familia de clave)")
    docs = Doc.search([('sgi_is_controlled', '=', True), ('sgi_process_id', '=', False),
                       ('sgi_code', '!=', False)])
    for doc in docs:
        proc_xmlid = None
        m = FAMILY_IN_CODE.search(doc.sgi_code)
        if m:
            fam = m.group(1) + m.group(2)
            proc_xmlid = dict(FAMILY_PROCESS).get(fam) or LETTER_PROCESS.get(m.group(1))
        elif doc.sgi_code.startswith(('MIID', 'ANEXO', 'PROT')):
            proc_xmlid = 'proc_sgi'
        proc = ref(proc_xmlid) if proc_xmlid else None
        if proc:
            if not DRY_RUN:
                doc.sgi_process_id = proc.id
            done['procesos'] += 1
            tick()
    print("   %d documento(s) ligados a su proceso" % done['procesos'])

    # ---- 2.5) Clasificación de migración de formatos ----------------------
    print("\n2.5) CLASIFICACIÓN DE MIGRACIÓN (formatos sin clase)")
    formatos = Doc.search([('sgi_is_controlled', '=', True),
                           ('sgi_doc_type', 'in', ('formato', 'formato_it')),
                           ('sgi_migration_class', '=', False),
                           ('sgi_code', '!=', False)])
    classified = 0
    for doc in formatos:
        cls, target = migration_of(doc.sgi_code)
        vals = {'sgi_migration_class': cls}
        if target:
            vals['sgi_migration_target'] = target
        if not DRY_RUN:
            doc.write(vals)
        classified += 1
        tick()
    done['clasificados'] = classified
    print("   %d formato(s) clasificados por familia" % classified)

    # ---- 3) Rescate de claves fuera de norma en POR CLASIFICAR ------------
    print("\n3) RESCATE EN 'POR CLASIFICAR'")
    unclassified = Doc.search([('sgi_is_controlled', '=', False), ('type', '=', 'binary'),
                               ('sgi_code', '=', False)])
    for doc in unclassified:
        stem = (doc.name or '').rsplit('.', 1)[0]
        stem = re.sub(r'^\d{1,2}[.\-]\s*', '', stem)      # quita "01. " inicial
        m = CODE_IN_NAME.match(stem.strip())
        if not m:
            continue
        code = _norm_code(m.group(1))
        dtype = _doc_type_of(code)
        if not dtype or vigente(code):
            continue
        print("   ✓ %-45s -> %s" % ((doc.name or '')[:45], code))
        if not DRY_RUN:
            doc.write({'sgi_is_controlled': True, 'sgi_code': code,
                       'sgi_doc_type': dtype, 'sgi_state': 'vigente',
                       'sgi_revision': doc.sgi_revision or '00'})
        done['rescatados'] += 1
        tick()

    # ---- 4) Listas de trabajo ---------------------------------------------
    b_docs = Doc.search([('sgi_is_controlled', '=', True), ('sgi_state', '=', 'vigente')])
    b_docs = b_docs.filtered(lambda d: (d.sgi_code or '').startswith(B_FAMILIES))
    with open(WORKLIST_WS, 'w', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['clave', 'nombre', 'id_documento', 'estatus_worksheet'])
        for doc in b_docs.sorted(lambda d: d.sgi_code):
            w.writerow([doc.sgi_code, doc.name, doc.id, 'PENDIENTE'])
    print("\n4) %d formato(s) clase B en %s" % (len(b_docs), WORKLIST_WS))

    with open(WORKLIST_PUESTOS, 'w', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['clave', 'puesto'])
        w.writerow(['# Llenar una fila por pareja clave-puesto (nombre exacto del', ''])
        w.writerow(['# puesto en Empleados > Puestos) y guardar como %s' % PUESTOS_CSV, ''])
    print("   Plantilla de puestos en %s" % WORKLIST_PUESTOS)

    # ---- 5) Puestos aplicables desde CSV (opcional) -----------------------
    import os
    if os.path.exists(PUESTOS_CSV):
        print("\n5) PUESTOS APLICABLES desde %s" % PUESTOS_CSV)
        Job = env['hr.job']
        with open(PUESTOS_CSV) as fh:
            for line in csv.DictReader(fh):
                code = (line.get('clave') or '').strip()
                job_name = (line.get('puesto') or '').strip()
                if not code or not job_name or code.startswith('#'):
                    continue
                doc = vigente(code)
                job = Job.search([('name', '=ilike', job_name)], limit=1)
                if not doc or not job:
                    print("   - %s / %s: no encontrado" % (code, job_name))
                    continue
                if job in doc.sgi_job_ids:
                    continue
                if not DRY_RUN:
                    doc.sgi_job_ids = [(4, job.id)]
                done['puestos'] += 1
                tick()
        print("   %d asignación(es) de puesto" % done['puestos'])
    else:
        print("\n5) (sin %s: se omite la asignación de puestos)" % PUESTOS_CSV)

    # ---- resumen -----------------------------------------------------------
    print("\n" + "=" * 60)
    mode = "DRY-RUN (no se escribió nada)" if DRY_RUN else "APLICADO"
    print("Post-carga SGI — %s" % mode)
    print("Flujos ligados: %(flujos)d | Docs->proceso: %(procesos)d | "
          "Rescatados: %(rescatados)d | Clasificados: %(clasificados)d | "
          "Puestos: %(puestos)d" % done)
    if not DRY_RUN:
        env.cr.commit()
        print(">>> Cambios CONFIRMADOS en la base de datos (commit).")
    else:
        print(">>> Revisa el plan. Para aplicar: DRY_RUN=False y re-ejecuta.")


if 'env' in dir():
    run(env)  # noqa: F821
