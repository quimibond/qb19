# -*- coding: utf-8 -*-
"""Carga documental masiva Dropbox/Drive → Odoo Documents (quimibond_sgi).

Sube el SGI documental (estructura de carpetas numeradas 00-23 dentro de un ZIP)
a la app Documentos, creando el árbol espejo de carpetas y, cuando el nombre del
archivo hace match con la nomenclatura de PNTQ, el documento controlado del SGI
(clave, tipo, área, revisión, vigencia).

MODO SEGURO: DRY_RUN=True por defecto. La primera corrida SOLO imprime el plan y
escribe el CSV; para cargar de verdad hay que poner DRY_RUN=False.

Uso (shell de Odoo.sh):
    # 1) Dry-run (no escribe nada en la BD), revisa /tmp/carga_documental_reporte.csv
    odoo-bin shell --no-http < addons/quimibond_sgi/tools/carga_documental.py
    # 2) Carga real: editar DRY_RUN=False abajo y volver a correr (idempotente).

Procedimiento: correr PRIMERO en staging, validar el CSV, y solo con visto bueno
ejecutar en producción. NUNCA DRY_RUN=False en producción sin el CSV validado.
"""
import base64
import csv
import re
import zipfile
from datetime import date, datetime

from dateutil.relativedelta import relativedelta

from odoo.addons.quimibond_sgi.models.sgi_document import SGI_CODE_REGEX

# --- Parámetros -------------------------------------------------------------
ZIP_PATH = '/tmp/SGI.zip'
DRY_RUN = True                          # ← poner False para cargar de verdad
ROOT_FOLDER_NAME = 'SGI'                # carpeta raíz en Documentos
UNCLASSIFIED_NAME = 'POR CLASIFICAR'
CSV_PATH = '/tmp/carga_documental_reporte.csv'
ALLOWED_EXT = {'pdf', 'xlsx', 'xls', 'docx', 'doc', 'pptx'}
IGNORE_EXT = {'lnk', 'tmp', 'log'}
SKIP_DIR_TOKENS = ('obsolet', 'baja', 'anterior')
# ----------------------------------------------------------------------------

# Prefijo de clave al inicio del nombre (más específico primero). Reusa la misma
# nomenclatura que el módulo; la validación final es contra SGI_CODE_REGEX.
# Nota DAT: las claves reales son «DAT P-G01-02», «DAT-P-A14-02», etc. — DAT +
# espacio/guion opcional + la estructura P-Xnn-nn(-nn). Un «DAT» suelto sin esa
# estructura NO es clave válida → va a POR CLASIFICAR.
CODE_PREFIX = re.compile(
    r'^(MIID'
    r'|F-IT-P-[AGCDEIMPSV]\d{2}-\d{2}-\d{2}'
    r'|IT-P-[AGCDEIMPSV]\d{2}-\d{2}'
    r'|F-P-[AGCDEIMPSV]\d{2}-\d{2}'
    r'|P-[AGCDEIMPSV]\d{2}'
    r'|DAT[ \-]?P-[AGCDEIMPSV]\d{2}-\d{2}(?:-\d{2})?'
    r'|PROT-\d{2}'
    r'|DF-[\w.\-]*'
    r'|R-[\w.\-]*'
    r'|ANEXO \d{1,2})',
    re.IGNORECASE)

REV_RE = re.compile(r'\bREV\.?\s*(\d{1,2})', re.IGNORECASE)
AREA_RE = re.compile(r'P-([AGCDEIMPSV])\d{2}')


def _doc_type_of(code):
    checks = [
        ('F-IT-P-', 'formato_it'), ('F-P-', 'formato'), ('IT-P-', 'instructivo'),
        ('P-', 'procedimiento'), ('MIID', 'miid'), ('DAT', 'dat'),
        ('PROT-', 'protocolo'), ('DF-', 'diagrama'), ('R-', 'reglamento'),
        ('ANEXO', 'anexo'),
    ]
    for prefix, dtype in checks:
        if code.startswith(prefix):
            return dtype
    return None


def _detect_code(filename):
    """Devuelve (code, doc_type, area_letter) o (None, None, None)."""
    stem = filename.rsplit('.', 1)[0]
    match = CODE_PREFIX.match(stem.strip())
    if not match:
        return None, None, None
    code = match.group(1).upper().strip()
    if not SGI_CODE_REGEX.match(code):
        return None, None, None
    area_match = AREA_RE.search(code)
    return code, _doc_type_of(code), (area_match.group(1) if area_match else None)


def _revision_of(filename):
    match = REV_RE.search(filename)
    return match.group(1).zfill(2) if match else '00'


def _zip_date(info):
    try:
        return datetime(*info.date_time).date()
    except Exception:
        return date.today()


def _fix_name(info):
    """Corrige mojibake: zipfile decodifica en cp437 cuando el ZIP no marca UTF-8
    (0x800). Si es el caso, re-decodifica cp437→utf-8 (así «COMUNICACIÓN» no llega
    roto de un ZIP hecho en Windows/Dropbox)."""
    name = info.filename
    if not (info.flag_bits & 0x800):
        try:
            name = name.encode('cp437').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
    return name


class Loader:
    def __init__(self, env):
        self.env = env
        self.Doc = env['documents.document']
        self.Area = env['sgi.area']
        self._folder_cache = {}
        self._area_cache = {}
        self._seen_codes = set()
        self.rows = []
        self.counters = {}

    # --- carpetas -----------------------------------------------------------
    def _get_area(self, letter):
        if not letter:
            return self.env['sgi.area']
        if letter not in self._area_cache:
            self._area_cache[letter] = self.Area.search([('code', '=', letter)], limit=1)
        return self._area_cache[letter]

    def _get_folder(self, path_parts):
        """Devuelve (o crea, si no es dry-run) la carpeta espejo por su ruta."""
        key = tuple(path_parts)
        if key in self._folder_cache:
            return self._folder_cache[key]
        parent = self.env['documents.document']
        accumulated = []
        for part in path_parts:
            accumulated.append(part)
            sub_key = tuple(accumulated)
            folder = self._folder_cache.get(sub_key)
            if not folder:
                domain = [('type', '=', 'folder'), ('name', '=', part)]
                domain.append(('folder_id', '=', parent.id if parent else False))
                folder = self.Doc.search(domain, limit=1)
                if not folder and not DRY_RUN:
                    folder = self.Doc.create({
                        'name': part, 'type': 'folder',
                        'folder_id': parent.id if parent else False,
                    })
                self._folder_cache[sub_key] = folder
            parent = folder
        return parent

    # --- carga de un archivo ------------------------------------------------
    def _count(self, section, key):
        self.counters.setdefault(section, {})
        self.counters[section][key] = self.counters[section].get(key, 0) + 1

    def _report(self, section, folder, filename, code, dtype, area, revision,
                status, reason=''):
        self.rows.append({
            'seccion': section, 'carpeta': folder, 'archivo': filename,
            'clave': code or '', 'tipo': dtype or '', 'area': area or '',
            'revision': revision or '', 'estado_carga': status, 'motivo': reason,
        })
        self._count(section, status)

    def process(self, zf, info, root_strip):
        raw_path = _fix_name(info)
        parts = [p for p in raw_path.split('/') if p]
        if root_strip and parts and parts[0] == root_strip:
            parts = parts[1:]
        if not parts:
            return
        filename = parts[-1]
        dir_parts = parts[:-1]
        section = dir_parts[0] if dir_parts else '(raíz)'
        ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''

        if ext in IGNORE_EXT or ext not in ALLOWED_EXT:
            self._report(section, '/'.join(dir_parts), filename, None, None, None,
                         None, 'IGNORADO', 'extensión .%s no soportada' % ext)
            return
        if any(tok in p.lower() for p in dir_parts for tok in SKIP_DIR_TOKENS):
            self._report(section, '/'.join(dir_parts), filename, None, None, None,
                         None, 'SALTADO', 'carpeta obsoleta/baja/anterior')
            return

        code, dtype, area = _detect_code(filename)
        revision = _revision_of(filename)
        folder_path = [ROOT_FOLDER_NAME] + dir_parts

        # --- documento controlado ---
        if code and dtype:
            if code in self._seen_codes:
                self._report(section, '/'.join(dir_parts), filename, code, dtype,
                             area, revision, 'DUPLICADO', 'clave repetida en el ZIP')
                return
            existing = self.Doc.search([
                ('sgi_code', '=', code), ('sgi_state', '=', 'vigente')], limit=1)
            if existing:
                self._seen_codes.add(code)
                self._report(section, '/'.join(dir_parts), filename, code, dtype,
                             area, revision, 'SALTADO', 'ya existe vigente con esa clave')
                return
            self._seen_codes.add(code)
            if not DRY_RUN:
                folder = self._get_folder(folder_path)
                issue = _zip_date(info)
                self.Doc.create({
                    'name': filename,
                    'type': 'binary',
                    'folder_id': folder.id,
                    'datas': base64.b64encode(zf.read(info)),
                    'sgi_is_controlled': True,
                    'sgi_code': code,
                    'sgi_doc_type': dtype,
                    'sgi_area_id': self._get_area(area).id,
                    'sgi_revision': revision,
                    'sgi_state': 'vigente',
                    'sgi_issue_date': issue,
                    'sgi_next_review_date': issue + relativedelta(years=2),
                })
            self._report(section, '/'.join(dir_parts), filename, code, dtype, area,
                         revision, 'CREADO')
            return

        # --- sin clave: POR CLASIFICAR (espejando la carpeta de origen para no
        #     colisionar homónimos de distintas carpetas) ---
        unclassified_path = [ROOT_FOLDER_NAME, UNCLASSIFIED_NAME] + dir_parts
        carpeta_label = '/'.join([UNCLASSIFIED_NAME] + dir_parts)
        folder = self._get_folder(unclassified_path)  # vacío en dry-run
        existing = folder and self.Doc.search([
            ('name', '=', filename), ('sgi_is_controlled', '=', False),
            ('folder_id', '=', folder.id)], limit=1)
        if existing:
            self._report(section, carpeta_label, filename, None, None, None,
                         None, 'SALTADO', 'ya existe sin clasificar')
            return
        if not DRY_RUN:
            self.Doc.create({
                'name': filename, 'type': 'binary', 'folder_id': folder.id,
                'datas': base64.b64encode(zf.read(info)),
                'sgi_is_controlled': False,
            })
        self._report(section, carpeta_label, filename, None, None, None, None,
                     'POR_CLASIFICAR', 'el nombre no coincide con la nomenclatura')

    def run(self):
        with zipfile.ZipFile(ZIP_PATH) as zf:
            members = [i for i in zf.infolist() if not i.is_dir()]
            # Detecta y quita una posible carpeta raíz común (envoltorio del ZIP).
            tops = {_fix_name(i).split('/')[0] for i in members if '/' in _fix_name(i)}
            root_strip = tops.pop() if len(tops) == 1 else None
            for info in sorted(members, key=lambda i: _fix_name(i)):
                self.process(zf, info, root_strip)

        with open(CSV_PATH, 'w', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=[
                'seccion', 'carpeta', 'archivo', 'clave', 'tipo', 'area',
                'revision', 'estado_carga', 'motivo'])
            writer.writeheader()
            writer.writerows(self.rows)

        self._print_summary()

    def _print_summary(self):
        mode = "DRY-RUN (no se escribió nada)" if DRY_RUN else "CARGA REAL"
        print("=" * 60)
        print("Carga documental SGI — %s" % mode)
        print("ZIP: %s | CSV: %s" % (ZIP_PATH, CSV_PATH))
        print("=" * 60)
        totals = {}
        for section in sorted(self.counters):
            print("• %s" % section)
            for status, n in sorted(self.counters[section].items()):
                print("    %-14s %d" % (status, n))
                totals[status] = totals.get(status, 0) + n
        print("-" * 60)
        print("TOTALES: " + " | ".join("%s=%d" % (k, v) for k, v in sorted(totals.items())))
        print("Filas en CSV: %d" % len(self.rows))
        if DRY_RUN:
            print("\n>>> Revisa el CSV. Para cargar de verdad: DRY_RUN=False y re-ejecuta.")


def cargar(env):
    import os
    if not os.path.exists(ZIP_PATH):
        print("ERROR: no existe %s. Copia el ZIP del SGI a esa ruta." % ZIP_PATH)
        return
    Loader(env).run()
    if not DRY_RUN:
        # `odoo-bin shell` revierte la transacción al salir; en carga real hay
        # que confirmar explícitamente los documentos creados.
        env.cr.commit()
        print(">>> Cambios CONFIRMADOS en la base de datos (commit).")


# Auto-ejecución en `odoo-bin shell` (la variable `env` está disponible).
if 'env' in dir():
    cargar(env)  # noqa: F821
