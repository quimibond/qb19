# -*- coding: utf-8 -*-
"""Asistente «Importar rutinas» (19.0.57.0.0; L-008, L-016, L-020; mismo
patrón que el asistente de quimibond_sgi_mapa).

1. **Probar** lee el libro (XLSX de Jose o su hoja «Rutina por rutina» en CSV
   UTF-8, con el encabezado de docs/audit/12-transicion/
   plantilla_rutina_por_rutina.csv) y corre ``sgi.legacy.routine.
   load_routines`` en modo de prueba: nada se escribe. Mismas reglas que
   docs/audit/12-transicion/validar_rutinas.py.
2. **Cargar** solo funciona después de probar ESE mismo archivo (se compara
   el SHA-256) y de marcar «Entiendo que se escribe en la base».

Solo el Jefe MAST (o superior). El archivo no se guarda: al terminar se
vacía del asistente.
"""
import base64
import csv
import hashlib
import io

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError

from .sgi_legacy_routine import sgi_cell, sgi_norm

_ACTIONS = [
    ('created', "Se crea"),
    ('updated', "Se actualiza"),
    ('archived', "Se archiva"),
    ('error', "Error"),
    ('warning', "Advertencia"),
]
ROUTINE_COLUMNS = ('clave', 'procedimiento', 'n', 'rutina', 'frecuencia',
                   'responsable_anterior', 'estado', 'actividades_odoo', 'motivo')
OPTIONAL_COLUMNS = ('revision', 'comentario')


def _check_manager(env):
    if not (env.su or env.user.has_group('quimibond_sgi.group_sgi_manager')):
        raise AccessError("Solo el Jefe MAST importa las rutinas del Dropbox.")


def _key(text):
    return sgi_norm(text).replace(' ', '_')


def read_book(raw, filename):
    """{hoja normalizada: [filas]} de un XLSX o de un CSV (una sola hoja)."""
    name = (filename or '').lower()
    if name.endswith(('.xlsx', '.xlsm')):
        try:
            import openpyxl
        except ImportError:
            raise UserError("openpyxl no está disponible en este servidor: sube la hoja en CSV.")
        try:
            book = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        except Exception as exc:
            raise UserError("No se pudo abrir el Excel: %s" % exc)
        return {_key(sheet.title): [list(row) for row in sheet.iter_rows(values_only=True)]
                for sheet in book.worksheets}
    try:
        text = raw.decode('utf-8-sig')
    except UnicodeDecodeError:
        raise UserError("El CSV debe estar en UTF-8.")
    return {'rutina_por_rutina': [row for row in csv.reader(io.StringIO(text))]}


def _rows(sheet, wanted):
    """Filas como dict por encabezado normalizado (solo las columnas pedidas)."""
    if not sheet:
        return [], []
    header = [_key(c) for c in sheet[0]]
    index = {c: header.index(c) for c in wanted if c in header}
    rows = []
    for number, raw in enumerate(sheet[1:], start=2):
        if not raw or not any(v not in (None, '') for v in raw):
            continue
        row = {c: (raw[i] if i < len(raw) else None) for c, i in index.items()}
        row['fila'] = number
        rows.append(row)
    return header, rows


def book_to_payload(book):
    """El libro → payload de load_routines, más los errores de estructura y
    las advertencias de la hoja «Documentos por migrar» (solo comparación)."""
    errors, extra = [], {}
    sheet = book.get('rutina_por_rutina')
    if sheet is None:
        errors.append("No existe la hoja «Rutina por rutina» (hojas: %s)." % ", ".join(book))
        return None, errors
    header, rows = _rows(sheet, ROUTINE_COLUMNS + OPTIONAL_COLUMNS)
    missing = [c for c in ROUTINE_COLUMNS if c not in header]
    if missing:
        errors.append("Faltan columnas: %s." % ", ".join(missing))
        return None, errors
    ignored = [c for c in header if c and c not in ROUTINE_COLUMNS + OPTIONAL_COLUMNS]
    if ignored:
        extra['ignored'] = ignored
    routines = [{
        'fila': r['fila'], 'clave': sgi_cell(r.get('clave')), 'procedimiento': sgi_cell(r.get('procedimiento')),
        'n': sgi_cell(r.get('n')), 'rutina': sgi_cell(r.get('rutina')),
        'frecuencia': sgi_cell(r.get('frecuencia')),
        'responsable_anterior': sgi_cell(r.get('responsable_anterior')),
        'estado': sgi_cell(r.get('estado')), 'actividades': sgi_cell(r.get('actividades_odoo')),
        'motivo': sgi_cell(r.get('motivo')), 'revision': sgi_cell(r.get('revision')),
        'comentario': sgi_cell(r.get('comentario')),
    } for r in rows]
    payload = {'routines': routines, 'pending': [], 'summary': []}
    pending = book.get('pendientes')
    if pending:
        header = [_key(c) for c in pending[0]]
        pick = {}
        for name, prefix in (('clave', 'clave'), ('n', 'n'), ('rutina', 'rutina'),
                             ('decision', 'decision'), ('responsable', 'responsable'),
                             ('fecha', 'fecha')):
            col = next((i for i, c in enumerate(header) if 'anterior' not in c and (
                c == prefix or (prefix != 'n' and c.startswith(prefix)))), None)
            if col is not None:
                pick[name] = col
        for number, raw in enumerate(pending[1:], start=2):
            if not raw or not any(v not in (None, '') for v in raw):
                continue
            item = {name: (raw[i] if i < len(raw) else None) for name, i in pick.items()}
            item = {k: (v if k == 'fecha' else sgi_cell(v)) for k, v in item.items()}
            item['fila'] = number
            payload['pending'].append(item)
    summary = book.get('resumen_por_procedimiento')
    if summary:
        _header, rows = _rows(summary, ('clave', 'proceso_nuevo', 'rutinas', 'cubiertas',
                                        'reemplazadas', 'pendientes'))
        payload['summary'] = [{k: sgi_cell(v) for k, v in r.items()} for r in rows]
    docs = book.get('documentos_por_migrar')
    if docs:
        _header, rows = _rows(docs, ('clave', 'estado'))
        extra['documents'] = [{k: sgi_cell(v) for k, v in r.items()} for r in rows]
    payload['_extra'] = extra
    return payload, errors


class SgiLegacyRoutineImport(models.TransientModel):
    _name = 'sgi.legacy.routine.import'
    _description = "Importar rutinas del Dropbox"

    file = fields.Binary(string="Libro (XLSX o CSV)", attachment=False)
    filename = fields.Char(string="Nombre del archivo")
    archive_missing = fields.Boolean(
        string="Archivar las rutinas que ya no vienen",
        help="Del mismo procedimiento. Se archivan, nunca se borran.")
    state = fields.Selection([
        ('draft', "Sin probar"),
        ('tested', "Probado"),
        ('loaded', "Cargado"),
    ], default='draft', readonly=True)
    tested_hash = fields.Char(readonly=True)
    confirm = fields.Boolean(string="Entiendo que se escribe en la base")
    dry_run_ok = fields.Boolean(readonly=True)
    summary = fields.Text(string="Resumen", readonly=True)
    line_ids = fields.One2many('sgi.legacy.routine.import.line', 'wizard_id', string="Resultado",
                               readonly=True)
    error_count = fields.Integer(compute='_compute_counts')
    warning_count = fields.Integer(compute='_compute_counts')

    @api.depends('line_ids.action')
    def _compute_counts(self):
        for wizard in self:
            actions = wizard.line_ids.mapped('action')
            wizard.error_count = actions.count('error')
            wizard.warning_count = actions.count('warning')

    @api.model_create_multi
    def create(self, vals_list):
        _check_manager(self.env)
        return super().create(vals_list)

    def _reopen(self):
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new', 'name': "Importar rutinas"}

    def _compare_documents(self, rows):
        """Hoja «Documentos por migrar»: solo compara; Odoo manda."""
        warnings = []
        Doc = self.env['documents.document']
        for row in rows or []:
            code = row.get('clave')
            if not code:
                continue
            docs = Doc.search([('sgi_is_controlled', '=', True), ('sgi_state', '!=', 'obsoleto'),
                               '|', ('sgi_previous_code', '=', code), ('sgi_code', '=', code)], limit=1)
            if not docs:
                warnings.append((row.get('fila'), code, "Documento %s no está en Odoo." % code))
            elif row.get('estado') and docs.sgi_migration_state != row['estado']:
                warnings.append((row.get('fila'), code, "%s: libro dice %s, Odoo dice %s (Odoo manda)."
                                 % (code, row['estado'], docs.sgi_migration_state)))
        return warnings

    def _run(self, dry_run):
        self.ensure_one()
        _check_manager(self.env)
        if not self.file:
            raise UserError("Sube el libro de rutinas (XLSX o la hoja «Rutina por rutina» en CSV).")
        raw = base64.b64decode(self.file)
        digest = hashlib.sha256(raw).hexdigest()
        if not dry_run:
            if self.state != 'tested' or self.tested_hash != digest:
                raise UserError("Primero corre «Probar» con este mismo archivo: la carga real solo va "
                                "después del modo de prueba.")
            if not self.dry_run_ok:
                raise UserError("La prueba tuvo errores: corrígelos en el libro y vuelve a probar.")
            if not self.confirm:
                raise UserError("Marca «Entiendo que se escribe en la base» para cargar.")
        payload, structure_errors = book_to_payload(read_book(raw, self.filename))
        lines = [(5, 0, 0)]
        if structure_errors:
            for message in structure_errors:
                lines.append((0, 0, {'action': 'error', 'message': message}))
            self.write({'line_ids': lines, 'state': 'draft', 'dry_run_ok': False,
                        'summary': "El archivo no tiene el formato de la plantilla."})
            return self._reopen()
        extra = payload.pop('_extra', {})
        payload['archive_missing'] = self.archive_missing
        result = self.env['sgi.legacy.routine'].load_routines(payload, dry_run=dry_run)
        for change in result['changes']:
            lines.append((0, 0, {'action': change['action'], 'clave': change['clave'],
                                 'n': str(change['n']), 'message': ", ".join(change['fields'])}))
        for level, entries in (('error', result['errors']), ('warning', result['warnings'])):
            for entry in entries:
                lines.append((0, 0, {'action': level, 'fila': str(entry.get('fila') or ''),
                                     'clave': entry.get('clave') or '', 'n': str(entry.get('n') or ''),
                                     'message': entry['message']}))
        if extra.get('ignored'):
            lines.append((0, 0, {'action': 'warning', 'message': "Columnas que se ignoran: %s."
                                 % ", ".join(extra['ignored'])}))
        for fila, code, message in self._compare_documents(extra.get('documents')):
            lines.append((0, 0, {'action': 'warning', 'fila': str(fila or ''), 'clave': code,
                                 'message': message}))
        counts = result['summary']
        states = {}
        for row in payload['routines']:
            state = sgi_norm(row.get('estado'))
            states[state] = states.get(state, 0) + 1
        summary = "%s%d creadas, %d actualizadas, %d archivadas, %d sin cambio. El libro trae %d " \
                  "filas (%s)." % (
                      "Modo de prueba (no se escribió nada). " if dry_run else "Cargado. ",
                      counts['created'], counts['updated'], counts['archived'], counts['unchanged'],
                      len(payload['routines']),
                      ", ".join("%s %d" % (k or 'sin estado', v) for k, v in sorted(states.items())))
        vals = {
            'line_ids': lines, 'summary': summary, 'confirm': False,
            'dry_run_ok': bool(dry_run and result['ok']),
            'state': 'tested' if dry_run else 'loaded',
            'tested_hash': digest if dry_run else self.tested_hash,
        }
        if not dry_run:
            vals.update(file=False, filename=False)  # el archivo no se queda
        self.write(vals)
        return self._reopen()

    def action_test(self):
        return self._run(dry_run=True)

    def action_load(self):
        return self._run(dry_run=False)


class SgiLegacyRoutineImportLine(models.TransientModel):
    _name = 'sgi.legacy.routine.import.line'
    _description = "Resultado de la importación de rutinas"
    _order = 'id'

    wizard_id = fields.Many2one('sgi.legacy.routine.import', required=True, ondelete='cascade')
    action = fields.Selection(_ACTIONS, string="Resultado")
    fila = fields.Char(string="Fila")
    clave = fields.Char(string="Clave")
    n = fields.Char(string="N.º")
    message = fields.Char(string="Detalle")
