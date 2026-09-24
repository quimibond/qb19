# -*- coding: utf-8 -*-
"""Carga idempotente del catálogo por API (JSON-RPC / MCP).

``sgi.process.load_payload(payload)`` recibe entregables, procesos,
actividades con sus roles, lo que reciben y entregan, evidencia y
automatización, e indicadores, y da de alta o actualiza por llave natural:

- entregable: ``code`` (por empresa);
- proceso: ``code`` (por empresa);
- actividad: clave del proceso + paso (``number`` = «C2.03» o 3);
- rol: actividad + rol + puesto;
- recibe: actividad + entregable (con su plazo en días hábiles);
- indicador: ``code``.

Las ligas entre actividades y los flujos entre procesos NO se cargan: salen
solos de lo que entrega y recibe cada actividad. Las llaves de la versión
anterior (``links_to``, ``links``, ``flows`` e ``inputs``/``outputs`` como
texto del proceso) son error, para que ningún JSON viejo cargue a medias.

Correr dos veces el mismo JSON no duplica nada y la segunda corrida reporta
cero cambios. ``dry_run`` hace todo dentro de un savepoint que se deshace al
final: reporta exactamente lo que se crearía, actualizaría o archivaría, y
los errores que la base daría, sin escribir.

Los puestos se resuelven por id o por nombre normalizado y NUNCA se crean:
si no existen, la actividad no se carga y el error viene en la respuesta.
Un rol puede ir a un puesto (``job``), a una familia de puestos (``family``,
por su código; las familias se dan de alta en el bloque ``families``, que se
procesa primero) o a un rol relativo (``relative``: solicitante, jefe del
solicitante, quien detecta, área responsable, dueño del proceso). Un puesto
sin empleados activos es error, salvo que tenga vacante aprobada y vigente
(entonces, advertencia); una familia, solo si todos sus puestos están vacíos.
Cada proceso es una transacción: si una de sus actividades falla, el proceso
completo se deshace y se reporta.
"""
import json
import logging

from odoo import models, api, fields, Command
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tools.safe_eval import safe_eval

from .sgi_catalog import SGI_RELATIVE_ROLES

_logger = logging.getLogger(__name__)

_PROCESS_TYPES = {
    'estrategico': 'estrategico', 'estratégico': 'estrategico',
    'cadena de valor': 'cop', 'cadena_de_valor': 'cop', 'cadena_valor': 'cop',
    'cop': 'cop', 'soporte': 'soporte',
}
_PROCESS_TEXT_FIELDS = (
    'name', 'purpose', 'scope', 'env_aspects')
_LEGACY_KEYS = {
    'links_to': "«links_to» ya no existe: declara «outputs» en quien entrega e "
                "«inputs» en quien recibe; la liga sale sola.",
    'links': "«links» ya no existe: las ligas salen de «inputs»/«outputs».",
    'flows': "«flows» ya no existe: los flujos entre procesos salen de «inputs»/«outputs».",
}
_ACTIVITY_TEXT_FIELDS = (
    'name', 'description', 'odoo_ref', 'note', 'responsible_role',
    'check_against', 'how_steps', 'done_criteria', 'on_fail')
_INDICATOR_FIELDS = (
    'name', 'uom', 'direction', 'target_objective', 'target_acceptable',
    'frequency', 'calc_mode', 'monthly_budget', 'nc_on_red', 'formula', 'source',
    'baseline_value', 'target_date')
_DIRECTIONS = {'up': 'higher_better', 'down': 'lower_better',
               'higher_better': 'higher_better', 'lower_better': 'lower_better'}

# Llaves válidas de cada nivel del JSON. Una llave que no está aquí es error:
# una llave que se ignora en silencio es como se perdieron los indicadores de
# C2 (venían dentro del proceso). (llaves, {llave: (tipo, sub-esquema)}).
_KEYS_ROLE = ({'role', 'job', 'job_id', 'family', 'relative', 'condition', 'after_days'}, {})
_KEYS_INPUT = ({'code', 'days', 'applies_domain', 'applies_note', 'match'}, {})
_KEYS_WHERE = ({'channel', 'menu', 'external_system', 'location', 'workcenter', 'place'}, {})
_KEYS_DUE = ({'weekday', 'business_day'}, {})
_KEYS_MEASURE = ({'method', 'proxy', 'deliverable', 'justification', 'sample_cadence',
                  'cadence'}, {})
_KEYS_AUTOMATION = ({'current', 'target', 'method'}, {})
_KEYS_EVIDENCE = ({'source_type', 'model', 'domain', 'date_field', 'user_field'}, {})
_KEYS_ACTIVITY = ({
    'process', 'number', 'step', 'sequence', 'name', 'description', 'odoo_ref',
    'note', 'responsible_role', 'stage', 'section', 'block', 'value_class',
    'cadence', 'roles', 'inputs', 'outputs', 'instruction', 'related_procedure',
    'formats', 'evidence', 'measure', 'automation',
    'check_against', 'where', 'how_steps', 'done_criteria', 'on_fail', 'due',
    'links_to',             # anterior: error propio
}, {'roles': (list, _KEYS_ROLE), 'inputs': (list, _KEYS_INPUT),
    'where': (dict, _KEYS_WHERE), 'due': (dict, _KEYS_DUE),
    'measure': (dict, _KEYS_MEASURE), 'automation': (dict, _KEYS_AUTOMATION),
    'evidence': (list, _KEYS_EVIDENCE)})
_KEYS_PROCESS = ({
    'code', 'name', 'purpose', 'scope', 'env_aspects', 'process_type', 'type',
    'owner', 'owner_job', 'owner_employee_id', 'parent', 'replaced_documents',
    'replaces', 'activities', 'state', 'publish',
    'inputs', 'outputs', 'start_trigger', 'end_trigger',   # anteriores
}, {'activities': (list, _KEYS_ACTIVITY)})
_KEYS_FAMILY = ({'code', 'name', 'jobs'}, {})
_KEYS_DELIVERABLE = ({'code', 'name', 'document', 'model', 'domain', 'date_field',
                      'user_field', 'acceptance_criteria', 'complete_domain',
                      'complete_criteria'}, {})
_KEYS_INDICATOR = ({'code', 'process', 'responsible', 'responsible_employee_id',
                    'target', 'unit', 'activity', 'deliverable', *_INDICATOR_FIELDS}, {})
_KEYS_PAYLOAD = ({
    'dry_run', 'company_id', 'archive_missing', 'families', 'deliverables',
    'processes', 'activities', 'indicators',
    'links', 'flows',       # anteriores: error propio
}, {'families': (list, _KEYS_FAMILY), 'deliverables': (list, _KEYS_DELIVERABLE),
    'processes': (list, _KEYS_PROCESS), 'activities': (list, _KEYS_ACTIVITY),
    'indicators': (list, _KEYS_INDICATOR)})


def _unknown_keys(node, schema, path=''):
    """[(ruta, mensaje)] de cada llave desconocida o de tipo equivocado."""
    keys, children = schema
    if not isinstance(node, dict):
        return [(path or '(raíz)', "debe ser un objeto JSON")]
    problems = []
    for key, value in node.items():
        where = "%s.%s" % (path, key) if path else key
        if key not in keys:
            problems.append((where, "llave desconocida. Válidas aquí: %s." % (
                ', '.join(sorted(keys)))))
            continue
        if key not in children or value is None:
            continue
        kind, sub = children[key]
        if kind is dict:
            problems += _unknown_keys(value, sub, where)
        elif not isinstance(value, list):
            problems.append((where, "debe ser una lista"))
        else:
            for index, item in enumerate(value):
                # Un «recibe» puede ser solo el código del entregable.
                if sub is _KEYS_INPUT and isinstance(item, str):
                    continue
                problems += _unknown_keys(item, sub, "%s[%d]" % (where, index))
    return problems


class _Rollback(Exception):
    """Deshace el savepoint que la envuelve (dry-run o proceso con errores)."""


class SgiLoadReport:
    def __init__(self, dry_run):
        self.dry_run = dry_run
        self.changes = []
        self.errors = []
        self.warnings = []

    def change(self, kind, key, action, fields=None):
        entry = {'kind': kind, 'key': key, 'action': action}
        if fields:
            entry['fields'] = sorted(fields)
        self.changes.append(entry)

    def error(self, kind, key, message):
        self.errors.append({'kind': kind, 'key': key, 'message': str(message)})

    def warn(self, kind, key, message):
        self.warnings.append({'kind': kind, 'key': key, 'message': str(message)})

    def summary(self):
        out = {}
        for entry in self.changes:
            out.setdefault(entry['action'], {}).setdefault(entry['kind'], 0)
            out[entry['action']][entry['kind']] += 1
        return out

    def as_dict(self, company):
        return {
            'dry_run': self.dry_run,
            'company': company.name,
            'ok': not self.errors,
            'summary': self.summary(),
            'changes': self.changes,
            'errors': self.errors,
            'warnings': self.warnings,
        }


def _diff(record, vals):
    """Solo lo que cambia: así la segunda corrida no escribe nada."""
    changed = {}
    for name, value in vals.items():
        field = record._fields[name]
        current = record[name]
        if field.type == 'many2one':
            if current.id != (value or False):
                changed[name] = value or False
        elif field.type == 'many2many':
            if set(current.ids) != set(value or []):
                changed[name] = [Command.set(value or [])]
        elif field.type in ('integer', 'float'):
            if (current or 0) != (value or 0):
                changed[name] = value or 0
        elif field.type == 'boolean':
            if bool(current) != bool(value):
                changed[name] = bool(value)
        else:
            if (current or False) != (value or False):
                changed[name] = value or False
    return changed


class SgiProcessLoad(models.Model):
    _inherit = 'sgi.process'

    @api.model
    def load_payload(self, payload, dry_run=None):
        """Carga el catálogo del SGI. Ver el docstring del módulo para el
        formato; responde {ok, summary, changes, errors, warnings}.

        Solo Administrador SGI."""
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_admin')):
            raise AccessError("Solo un Administrador SGI puede cargar el catálogo.")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError as exc:
                raise UserError("El payload no es JSON válido: %s" % exc)
        if not isinstance(payload, dict):
            raise UserError("El payload debe ser un objeto JSON.")
        if dry_run is None:
            dry_run = bool(payload.get('dry_run'))
        company = self.env.company
        if payload.get('company_id'):
            company = self.env['res.company'].browse(int(payload['company_id'])).exists()
            if not company or company not in self.env.user.company_ids:
                raise UserError("Empresa %s no válida para este usuario."
                                % payload['company_id'])
        ctx = {}
        if dry_run:
            # Sin seguimiento ni mensajes: el savepoint se deshace y el
            # precommit del chatter apuntaría a registros que ya no existen.
            ctx.update(tracking_disable=True, mail_notrack=True,
                       mail_create_nolog=True, sgi_bypass_dirty=True)
        loader = _SgiLoader(self.with_context(**ctx).with_company(company),
                            company, SgiLoadReport(dry_run), payload)
        cr = self.env.cr
        try:
            with cr.savepoint():
                loader.run()
                if dry_run:
                    raise _Rollback()
        except _Rollback:
            pass
        self.env.invalidate_all()
        result = loader.report.as_dict(company)
        _logger.info("SGI load_payload%s: %s, %d error(es).",
                     " (dry-run)" if dry_run else "", result['summary'],
                     len(result['errors']))
        return result


class _SgiLoader:
    def __init__(self, env_model, company, report, payload):
        self.env = env_model.env
        self.company = company
        self.report = report
        self.payload = payload
        self.Process = self.env['sgi.process'].with_context(active_test=False)
        # La revisión «sin medir / método incompleto» de los procedimientos en
        # piloto o vigente se hace al final, cuando ya se resolvieron las
        # actividades que prueban a otras (proxy).
        self.Activity = self.env['sgi.process.activity'].with_context(
            active_test=False, sgi_defer_measure_check=True)
        self.proxies = []       # (actividad, referencia del proxy, clave de proceso)
        self.Job = self.env['hr.job']
        self.Doc = self.env['documents.document']
        self.processes = {}     # code -> sgi.process
        self.activities = {}    # (process code, number) -> sgi.process.activity
        self.job_cache = {}
        self.replaces = []      # (código del proceso nuevo, [códigos que sustituye])
        self.publish = []       # códigos de proceso a publicar al final
        self.family_cache = {}

    # ------------------------------------------------------------------
    def run(self):
        payload = self.payload
        problems = _unknown_keys(payload, _KEYS_PAYLOAD)
        if problems:
            # Nada se carga: un JSON con llaves que no se entienden no carga a medias.
            for where, message in problems:
                self.report.error('payload', where, "%s: %s" % (where, message))
            return
        for key in ('links', 'flows'):
            if payload.get(key):
                self.report.error(key, None, _LEGACY_KEYS[key])
        self._load_families(payload.get('families') or [])
        self._load_deliverables(payload.get('deliverables') or [])
        activities_by_process = {}
        for item in payload.get('activities') or []:
            activities_by_process.setdefault(item.get('process'), []).append(item)
        processes = list(payload.get('processes') or [])
        for proc in processes:
            nested = proc.get('activities')
            if nested is not None:
                activities_by_process.setdefault(proc.get('code'), []).extend(nested)
        known = {p.get('code') for p in processes}
        # Actividades de procesos que ya existen y no vienen en «processes».
        for code in activities_by_process:
            if code not in known:
                processes.append({'code': code, '_only_activities': True})
        archive_missing = payload.get('archive_missing', True)
        for proc in processes:
            code = proc.get('code')
            if not code:
                self.report.error('process', None, "Proceso sin «code».")
                continue
            self._load_process_tx(
                proc, activities_by_process.get(code), archive_missing)
        self._load_proxies()
        self._load_replaces()
        self._load_indicators(payload.get('indicators') or [])
        self._report_spec_gaps()
        self._load_publish()

    def _savepoint(self, fn, kind, key):
        """Corre fn en su savepoint; un error de la base o de validación se
        reporta y deshace solo ese pedazo. Devuelve True si pasó."""
        try:
            with self.env.cr.savepoint():
                fn()
            return True
        except (ValidationError, UserError, AccessError, ValueError) as exc:
            self.report.error(kind, key, exc.args[0] if exc.args else exc)
        except Exception as exc:  # noqa: BLE001 - errores de la BD (unicidad…)
            self.report.error(kind, key, exc)
        return False

    # ------------------------------------------------------------------
    # Procesos y sus actividades (una transacción por proceso)
    # ------------------------------------------------------------------
    def _load_process_tx(self, proc, activities, archive_missing):
        code = proc['code']
        errors_before = len(self.report.errors)
        changes_before = len(self.report.changes)
        try:
            with self.env.cr.savepoint():
                process = self._upsert_process(proc)
                if process and activities is not None:
                    self._load_activities(process, activities, archive_missing)
                if len(self.report.errors) > errors_before:
                    raise _Rollback()
        except _Rollback:
            # El proceso completo se deshace: sus cambios no se reportan como
            # hechos, pero sí sus errores.
            del self.report.changes[changes_before:]
            self.processes.pop(code, None)
            for key in [k for k in self.activities if k[0] == code]:
                del self.activities[key]
            self.report.error(
                'process', code,
                "El proceso %s no se cargó: %d error(es) en su contenido." % (
                    code, len(self.report.errors) - errors_before))
        except Exception as exc:  # noqa: BLE001 - el savepoint ya deshizo el proceso
            del self.report.changes[changes_before:]
            self.processes.pop(code, None)
            for key in [k for k in self.activities if k[0] == code]:
                del self.activities[key]
            self.report.error('process', code, exc.args[0] if exc.args else exc)

    def _resolve_job(self, ref):
        key = ref if isinstance(ref, int) else ' '.join(str(ref or '').split()).casefold()
        if key not in self.job_cache:
            self.job_cache[key] = self.Job._sgi_find_job(ref, self.company)
        return self.job_cache[key]

    def _resolve_owner(self, proc):
        """El dueño es un empleado: por id, o el único empleado activo con
        usuario que ocupa el puesto dado."""
        Employee = self.env['hr.employee']
        if proc.get('owner_employee_id'):
            emp = Employee.browse(int(proc['owner_employee_id'])).exists()
            if not emp:
                raise ValidationError("El empleado %s no existe." % proc['owner_employee_id'])
            return emp
        job_ref = proc.get('owner_job') or proc.get('owner')
        if not job_ref:
            return None
        job, error = self._resolve_job(job_ref)
        if error:
            raise ValidationError("Dueño: %s" % error)
        emps = Employee.search([('job_id', '=', job.id), ('user_id', '!=', False),
                                ('company_id', '=', self.company.id)])
        if len(emps) != 1:
            self.report.warn(
                'process', proc['code'],
                "Dueño no asignado: el puesto «%s» tiene %d empleado(s) activo(s) "
                "con usuario; se necesita exactamente uno (o manda "
                "owner_employee_id)." % (' '.join(job.name.split()), len(emps)))
            return None
        return emps

    def _upsert_process(self, proc):
        code = proc['code']
        process = self.Process.search([('code', '=', code),
                                       ('company_id', '=', self.company.id)], limit=1)
        if proc.get('_only_activities'):
            if not process:
                raise ValidationError(
                    "Hay actividades del proceso %s pero el proceso no existe "
                    "ni viene en «processes»." % code)
            self.processes[code] = process
            return process
        for legacy in ('inputs', 'outputs'):
            if proc.get(legacy):
                raise ValidationError(
                    "«%s» del proceso ya no es texto: sale de lo que reciben y "
                    "entregan sus actividades." % legacy)
        for legacy in ('start_trigger', 'end_trigger'):
            if proc.get(legacy):
                self.report.warn('process', code, (
                    "«%s» se ignora: el inicio y el fin del proceso salen de los "
                    "entregables que recibe de fuera y entrega hacia fuera." % legacy))
        vals = {name: proc[name] for name in _PROCESS_TEXT_FIELDS if name in proc}
        if 'process_type' in proc or 'type' in proc:
            raw = str(proc.get('process_type') or proc.get('type') or '').strip().lower()
            if raw not in _PROCESS_TYPES:
                raise ValidationError("Tipo de proceso «%s» desconocido (estratégico, "
                                      "cadena de valor, soporte)." % raw)
            vals['process_type'] = _PROCESS_TYPES[raw]
        owner = self._resolve_owner(proc)
        if owner:
            vals['owner_id'] = owner.id
        if 'parent' in proc:
            parent = self.Process.search([('code', '=', proc['parent']),
                                          ('company_id', '=', self.company.id)], limit=1) \
                if proc['parent'] else self.Process
            if proc['parent'] and not parent:
                raise ValidationError("Macroproceso %s no existe." % proc['parent'])
            vals['parent_id'] = parent.id
        if proc.get('replaces'):
            self.replaces.append((code, proc['replaces']))
        if 'state' in proc:
            if proc['state'] not in ('borrador', 'piloto'):
                raise ValidationError("«state» va en borrador o piloto; para vigente "
                                      "usa «publish»: true.")
            vals['state'] = proc['state']
        if proc.get('publish'):
            self.publish.append(code)
        if 'replaced_documents' in proc:
            docs = []
            for doc_code in proc['replaced_documents'] or []:
                doc = self.Doc._sgi_find_by_code(doc_code, states=None)
                if doc:
                    docs.append(doc.id)
                else:
                    self.report.warn('process', code,
                                     "Documento sustituido %s no encontrado." % doc_code)
            vals['replaced_document_ids'] = docs
        if process:
            if not process.active:
                vals['active'] = True
            changed = _diff(process, vals)
            if changed:
                process.write(changed)
                self.report.change('process', code, 'updated', changed)
        else:
            if not vals.get('name'):
                raise ValidationError("El proceso nuevo %s necesita «name»." % code)
            vals['replaced_document_ids'] = [Command.set(vals.get('replaced_document_ids') or [])]
            process = self.Process.create(dict(vals, code=code, company_id=self.company.id))
            self.report.change('process', code, 'created')
        self.processes[code] = process
        return process

    def _load_activities(self, process, items, archive_missing):
        seen = set()
        for index, item in enumerate(items):
            number = item.get('number') or item.get('step')
            key = "%s/%s" % (process.code, number)
            step = self.Activity._sgi_parse_number(process, number) if number else None
            if not step:
                self.report.error('activity', key, (
                    "El numeral debe ser «%s.nn» (clave del proceso + paso) o el paso "
                    "como número." % process.code))
                continue
            if step in seen:
                self.report.error('activity', key, "Paso repetido en el payload.")
                continue
            seen.add(step)
            self._savepoint(lambda: self._upsert_activity(process, item, index, step),
                            'activity', key)
        if archive_missing:
            stale = self.Activity.search([('process_id', '=', process.id),
                                          ('active', '=', True),
                                          ('step', 'not in', list(seen) or [0])])
            for activity in stale:
                self.report.change('activity', "%s/%s" % (process.code, activity.number),
                                   'archived')
            if stale:
                stale.write({'active': False})

    def _resolve_deliverable(self, code, what):
        deliverable = self.env['sgi.deliverable'].with_context(active_test=False).search(
            [('code', '=', code), ('company_id', '=', self.company.id)], limit=1)
        if not code or not deliverable:
            raise ValidationError("%s: el entregable «%s» no existe (decláralo en "
                                  "«deliverables»)." % (what, code))
        return deliverable

    def _resolve_deliverables(self, codes, key, what):
        return [self._resolve_deliverable(code, what).id for code in codes or []]

    def _resolve_inputs(self, items):
        """«inputs»: ["C2-PEDIDO"] o [{"code": "C2-PEDIDO", "days": 2,
        "applies_domain": "[...]", "applies_note": "...", "match": "sale_id"}].
        Devuelve [(entregable id, {max_days, applies_domain, applies_note,
        match_path})] en orden."""
        out, seen = [], set()
        for item in items or []:
            code, days = (item.get('code'), item.get('days') or 0) if isinstance(item, dict) \
                else (item, 0)
            extra = item if isinstance(item, dict) else {}
            if isinstance(days, bool) or not isinstance(days, int) or days < 0:
                raise ValidationError("Recibe %s: «days» debe ser un entero de días "
                                      "hábiles (0 = sin plazo)." % code)
            deliverable = self._resolve_deliverable(code, "Recibe")
            if deliverable.id in seen:
                raise ValidationError("Recibe %s dos veces." % code)
            seen.add(deliverable.id)
            out.append((deliverable.id, {
                'max_days': days,
                'applies_domain': extra.get('applies_domain') or False,
                'applies_note': extra.get('applies_note') or False,
                'match_path': extra.get('match') or False,
            }))
        return out

    def _activity_vals(self, process, item, index):
        key = "%s/%s" % (process.code, item.get('number'))
        vals = {name: item[name] for name in _ACTIVITY_TEXT_FIELDS if name in item}
        vals['sequence'] = item.get('sequence') or (index + 1) * 10
        stage_text = item.get('stage') or item.get('section')
        if 'stage' in item or 'section' in item:
            vals['stage_id'] = self.env['sgi.process.stage']._sgi_get_or_create(
                process, stage_text).id if stage_text else False
        if 'links_to' in item:
            raise ValidationError(_LEGACY_KEYS['links_to'])
        if 'where' in item:
            vals.update(self._where_vals(item['where'] or {}))
        if 'due' in item:
            vals.update(self._due_vals(item['due'] or {}))
        if 'outputs' in item:
            vals['output_deliverable_ids'] = self._resolve_deliverables(item['outputs'], key, "Entrega")
        Activity = self.Activity
        for name in ('block', 'value_class'):
            if name in item:
                valid = dict(Activity._fields[name].selection)
                if item[name] and item[name] not in valid:
                    raise ValidationError("«%s» inválido: %s (válidos: %s)." % (
                        name, item[name], ', '.join(valid)))
                vals[name] = item[name] or False
        if 'cadence' in item:
            valid = dict(Activity._fields['measure_cadence'].selection)
            if item['cadence'] not in valid:
                raise ValidationError("Cadencia «%s» inválida (válidas: %s)." % (
                    item['cadence'], ', '.join(valid)))
            vals['measure_cadence'] = item['cadence']
        automation = item.get('automation') or {}
        for src, dst in (('current', 'automation_level_current'),
                         ('target', 'automation_level_target'),
                         ('method', 'automation_method')):
            if src in automation:
                valid = dict(Activity._fields[dst].selection)
                if automation[src] and automation[src] not in valid:
                    raise ValidationError("Automatización «%s» inválida: %s (válidos: %s)." % (
                        src, automation[src], ', '.join(valid)))
                vals[dst] = automation[src] or (dst == 'automation_level_current' and 'manual') or False
        for src, dst in (('instruction', 'instruction_id'),
                         ('related_procedure', 'related_procedure_id')):
            if src in item:
                doc = self.Doc._sgi_find_by_code(item[src], states=None) if item[src] else self.Doc
                if item[src] and not doc:
                    self.report.warn('activity', key, "Documento %s no encontrado; "
                                     "«%s» queda como estaba." % (item[src], src))
                else:
                    vals[dst] = doc.id
        if 'formats' in item:
            docs = []
            for code in item['formats'] or []:
                doc = self.Doc._sgi_find_by_code(code, states=None)
                if doc:
                    docs.append(doc.id)
                else:
                    self.report.warn('activity', key, "Formato %s no encontrado." % code)
            vals['format_document_ids'] = docs
        if 'evidence' in item:
            vals.update(self._evidence_vals(key, item['evidence'] or []))
        measure = item.get('measure') or {}
        method = measure.get('method') or ('odoo' if item.get('evidence') else None)
        if method:
            valid = dict(Activity._fields['measure_method'].selection)
            if method not in valid:
                raise ValidationError("Método de medición «%s» inválido (%s)." % (
                    method, ', '.join(valid)))
            vals['measure_method'] = method
            if method == 'muestreo':
                cadence = measure.get('sample_cadence') or measure.get('cadence')
                if cadence not in dict(Activity._fields['sample_cadence'].selection):
                    raise ValidationError("Muestreo sin «sample_cadence» (semanal o mensual).")
                vals['sample_cadence'] = cadence
            if method == 'no_aplica':
                vals['measure_justification'] = measure.get('justification') or False
            if method != 'consecuencia':
                vals['measure_proxy_activity_id'] = False
            if method == 'entregable':
                if measure.get('deliverable'):
                    deliverable = self._resolve_deliverable(measure['deliverable'], "Medición")
                    vals['measure_deliverable_id'] = deliverable.id
                if item.get('evidence'):
                    raise ValidationError("«Por su entregable» no lleva «evidence»: el "
                                          "modelo, el filtro y la fecha son los del entregable.")
            else:
                vals['measure_deliverable_id'] = False
        return vals

    def _evidence_vals(self, key, evidence):
        """Fase 1: la primera evidencia `odoo_model` va a los campos de medición
        actuales de la actividad. Las demás fuentes (correo, manual, externa,
        user_field, ciclo) se guardan en la fase 2 (sgi.activity.evidence);
        hoy se avisan para que la carga se repita entonces."""
        models_ev = [e for e in evidence if e.get('source_type', 'odoo_model') == 'odoo_model']
        if len(evidence) > len(models_ev[:1]):
            # TODO(fase 2): sgi.activity.evidence guarda todas las fuentes.
            self.report.warn('activity', key,
                             "Solo se guarda la primera evidencia de modelo de Odoo "
                             "hasta la fase 2; el resto se cargará entonces.")
        if not models_ev:
            return {}
        ev = models_ev[0]
        model_name = ev.get('model')
        if not model_name or model_name not in self.env:
            raise ValidationError("Evidencia: el modelo «%s» no existe." % model_name)
        Model = self.env[model_name]
        domain_txt = ev.get('domain') or '[]'
        try:
            domain = safe_eval(domain_txt)
            if not isinstance(domain, (list, tuple)):
                raise ValueError("no es una lista")
            Model.sudo().search_count(list(domain), limit=1)
        except Exception as exc:  # noqa: BLE001 - el mensaje va al reporte
            raise ValidationError("Evidencia: dominio inválido para %s: %s" % (model_name, exc))
        date_field = ev.get('date_field') or 'create_date'
        user_field = ev.get('user_field') or False
        for fname in (date_field, user_field):
            if fname and fname not in Model._fields:
                raise ValidationError("Evidencia: %s no tiene el campo «%s»." % (model_name, fname))
        if user_field:
            field = Model._fields[user_field]
            if field.type != 'many2one' or field.comodel_name != 'res.users' or not field.store:
                raise ValidationError(
                    "Evidencia: «%s» de %s no es un campo de usuario (res.users) "
                    "almacenado." % (user_field, model_name))
        return {
            'measure_model_name': model_name,
            'measure_domain': domain_txt,
            'measure_date_field': date_field,
            'measure_user_field': user_field,
        }

    # ------------------------------------------------------------------
    # Entregables (lo que pasa de una actividad a otra)
    # ------------------------------------------------------------------
    def _load_deliverables(self, deliverables):
        Deliverable = self.env['sgi.deliverable'].with_context(active_test=False)
        for item in deliverables:
            key = item.get('code')

            def run(item=item, key=key):
                if not key:
                    raise ValidationError("Entregable sin «code».")
                vals = {}
                if 'name' in item:
                    vals['name'] = item['name']
                if 'acceptance_criteria' in item:
                    vals['acceptance_criteria'] = item['acceptance_criteria'] or False
                if 'document' in item:
                    doc = self.Doc._sgi_find_by_code(item['document'], states=None) \
                        if item['document'] else self.Doc
                    if item['document'] and not doc:
                        self.report.warn('deliverable', key,
                                         "Formato %s no encontrado." % item['document'])
                    else:
                        vals['document_id'] = doc.id
                if 'model' in item:
                    model = self.env['ir.model']._get(item['model']) if item['model'] else False
                    if item['model'] and not model:
                        raise ValidationError("Modelo %s no existe." % item['model'])
                    vals['odoo_model_id'] = model.id if model else False
                for src, dst, default in (('domain', 'measure_domain', '[]'),
                                          ('date_field', 'measure_date_field', 'create_date'),
                                          ('user_field', 'measure_user_field', False),
                                          ('complete_domain', 'complete_domain', False),
                                          ('complete_criteria', 'complete_criteria', False)):
                    if src in item:
                        vals[dst] = item[src] or default
                deliverable = Deliverable.search([('code', '=', key),
                                                  ('company_id', '=', self.company.id)], limit=1)
                if deliverable:
                    if not deliverable.active:
                        vals['active'] = True
                    changed = _diff(deliverable, vals)
                    if changed:
                        deliverable.write(changed)
                        self.report.change('deliverable', key, 'updated', changed)
                else:
                    if not vals.get('name'):
                        raise ValidationError("El entregable nuevo %s necesita «name»." % key)
                    Deliverable.create(dict(vals, code=key, company_id=self.company.id))
                    self.report.change('deliverable', key, 'created')
            self._savepoint(run, 'deliverable', key)

    # ------------------------------------------------------------------
    # Familias de puestos
    # ------------------------------------------------------------------
    def _load_families(self, families):
        Family = self.env['sgi.job.family'].with_context(active_test=False)
        Job = self.env['hr.job'].with_context(active_test=False)
        for item in families:
            key = item.get('code')

            def run(item=item, key=key):
                if not key:
                    raise ValidationError("Familia sin «code».")
                vals = {}
                if 'name' in item:
                    vals['name'] = item['name']
                if 'jobs' in item:
                    ids = [int(j) for j in item['jobs'] or []]
                    jobs = Job.browse(ids).exists()
                    missing = sorted(set(ids) - set(jobs.ids))
                    if missing:
                        raise ValidationError("Puestos inexistentes: %s." % missing)
                    other = jobs.filtered(lambda j: j.company_id and j.company_id != self.company)
                    if other:
                        raise ValidationError("Puestos de otra empresa: %s." % other.ids)
                    vals['job_ids'] = ids
                family = Family.search([('code', '=', key),
                                        ('company_id', '=', self.company.id)], limit=1)
                if family:
                    if not family.active:
                        vals['active'] = True
                    changed = _diff(family, vals)
                    if changed:
                        family.write(changed)
                        self.report.change('family', key, 'updated', changed)
                else:
                    if not vals.get('name'):
                        raise ValidationError("La familia nueva %s necesita «name»." % key)
                    vals['job_ids'] = [Command.set(vals.get('job_ids') or [])]
                    family = Family.create(dict(vals, code=key, company_id=self.company.id))
                    self.report.change('family', key, 'created')
                family.flush_recordset()
            self._savepoint(run, 'family', key)

    def _resolve_family(self, code):
        if code not in self.family_cache:
            family = self.env['sgi.job.family'].search(
                [('code', '=', code), ('company_id', '=', self.company.id)], limit=1)
            self.family_cache[code] = family
        family = self.family_cache[code]
        if not family:
            raise ValidationError("No existe la familia de puestos «%s» en %s." % (
                code, self.company.name))
        return family

    def _check_staffing(self, jobs, label, key):
        """Puesto (o familia) sin empleados activos: error, salvo vacante
        aprobada y vigente (advertencia)."""
        state = jobs._sgi_staffing_state()
        if state == 'vacante':
            self.report.warn('activity', key, "%s no tiene personas; se acepta por "
                                              "vacante aprobada y vigente." % label)
        elif state == 'sin_persona':
            raise ValidationError(
                "%s no tiene empleados activos ni vacante aprobada vigente." % label)

    def _role_target(self, role, key):
        """(vals del destino, llave natural) de un rol del payload."""
        given = [k for k in ('job', 'job_id', 'family', 'relative') if role.get(k) not in (None, '', False)]
        if len(given) != 1:
            raise ValidationError(
                "Rol %s: indica exactamente uno de «job», «family» o «relative»."
                % role.get('role'))
        kind = given[0]
        if kind in ('job', 'job_id'):
            job, error = self._resolve_job(role[kind])
            if error:
                raise ValidationError("Rol %s: %s" % (role.get('role'), error))
            self._check_staffing(job, "El puesto «%s» (id %d)" % (
                ' '.join(job.name.split()), job.id), key)
            return {'target_type': 'job', 'job_id': job.id}, ('job', job.id)
        if kind == 'family':
            family = self._resolve_family(role['family'])
            self._check_staffing(family.job_ids, "La familia %s" % family.code, key)
            return {'target_type': 'family', 'family_id': family.id}, ('family', family.id)
        relative = role['relative']
        if relative not in dict(SGI_RELATIVE_ROLES):
            raise ValidationError("Rol relativo «%s» inválido (%s)." % (
                relative, ', '.join(dict(SGI_RELATIVE_ROLES))))
        return {'target_type': 'relative', 'relative_role': relative}, ('relative', relative)

    def _roles_commands(self, activity, item, key):
        """Comandos para dejar los roles exactamente como el payload."""
        wanted = []
        for seq, role in enumerate(item.get('roles') or [], start=1):
            if role.get('role') not in ('ejecuta', 'aprueba', 'participa', 'informa', 'escala'):
                raise ValidationError("Rol «%s» inválido (ejecuta, aprueba, participa, "
                                      "informa, escala)." % role.get('role'))
            target, target_key = self._role_target(role, key)
            wanted.append((target_key, dict(
                target, role=role['role'], condition=role.get('condition') or False,
                after_days=role.get('after_days') or 0, sequence=seq * 10)))
        current = {(r.role,) + r._sgi_target_key(): r for r in activity.role_ids} \
            if activity else {}
        commands, touched = [], False
        for target_key, vals in wanted:
            existing = current.pop((vals['role'],) + target_key, None)
            if existing:
                changed = _diff(existing, {'condition': vals['condition'],
                                           'after_days': vals['after_days'],
                                           'sequence': vals['sequence']})
                if changed:
                    commands.append(Command.update(existing.id, changed))
                    touched = True
            else:
                commands.append(Command.create(vals))
                touched = True
        for leftover in current.values():
            commands.append(Command.delete(leftover.id))
            touched = True
        return commands, touched

    def _upsert_activity(self, process, item, index, step):
        number = item.get('number') or item.get('step')
        key = "%s/%s" % (process.code, number)
        activity = self.Activity.search([('process_id', '=', process.id),
                                         ('step', '=', step)], limit=1)
        vals = self._activity_vals(process, item, index)
        role_cmds, roles_touched = self._roles_commands(activity, item, key) \
            if 'roles' in item else ([], False)
        input_cmds = self._inputs_commands(activity, item) if 'inputs' in item else []
        if activity:
            if not activity.active:
                vals['active'] = True
            changed = _diff(activity, vals)
            if roles_touched:
                changed['role_ids'] = role_cmds
            if input_cmds:
                changed['input_ids'] = input_cmds
            if changed:
                activity.write(changed)
                self.report.change('activity', key, 'updated', changed)
        else:
            vals.update(process_id=process.id, step=step, role_ids=role_cmds,
                        input_ids=input_cmds)
            for m2m in ('format_document_ids', 'output_deliverable_ids'):
                if m2m in vals:
                    vals[m2m] = [Command.set(vals[m2m])]
            activity = self.Activity.create(vals)
            self.report.change('activity', key, 'created')
        # Se encuentra por el numeral como vino en el JSON y por el calculado.
        self.activities[(process.code, number)] = activity
        self.activities[(process.code, activity.number)] = activity
        measure = item.get('measure') or {}
        if measure.get('method') == 'consecuencia':
            if not measure.get('proxy'):
                raise ValidationError("«Por consecuencia» necesita «proxy» (la actividad "
                                      "que la prueba).")
            self.proxies.append((activity, measure['proxy'], process.code))
        # Los roles, el instructivo y la evidencia se validan en la base; la
        # restricción de «exactamente un ejecutor» corre aquí dentro.
        activity.flush_recordset()

    def _where_vals(self, where):
        """«where»: dónde se hace. Lo que no viene queda vacío (declarativo)."""
        from .sgi_activity_spec import SGI_EXEC_CHANNELS
        channel = where.get('channel')
        if channel not in dict(SGI_EXEC_CHANNELS):
            raise ValidationError("«where.channel» es obligatorio y va entre: %s." % (
                ', '.join(dict(SGI_EXEC_CHANNELS))))
        vals = {'exec_channel': channel, 'odoo_menu_id': False, 'external_system': False,
                'location_id': False, 'workcenter_id': False, 'place_note': False}
        if where.get('menu'):
            menu = self.env.ref(where['menu'], raise_if_not_found=False)
            if not menu or menu._name != 'ir.ui.menu':
                raise ValidationError("«where.menu»: el menú %s no existe." % where['menu'])
            vals['odoo_menu_id'] = menu.id
        if where.get('external_system'):
            vals['external_system'] = where['external_system']
        if where.get('location'):
            location = self.env['stock.location'].search([
                ('complete_name', '=', where['location']),
                ('company_id', 'in', [self.company.id, False])], limit=1)
            if not location:
                raise ValidationError("«where.location»: la ubicación %s no existe."
                                      % where['location'])
            vals['location_id'] = location.id
        if where.get('workcenter'):
            workcenter = self.env['mrp.workcenter'].search([
                ('code', '=', where['workcenter']),
                ('company_id', 'in', [self.company.id, False])], limit=1)
            if not workcenter:
                raise ValidationError("«where.workcenter»: el centro de trabajo %s no "
                                      "existe." % where['workcenter'])
            vals['workcenter_id'] = workcenter.id
        if where.get('place'):
            vals['place_note'] = where['place']
        return vals

    def _due_vals(self, due):
        """«due»: {"weekday": 0-6} o {"business_day": 1-23}."""
        vals = {'due_weekday': False, 'due_business_day': 0}
        if 'weekday' in due and due['weekday'] is not None:
            day = due['weekday']
            if isinstance(day, bool) or not isinstance(day, int) or not 0 <= day <= 6:
                raise ValidationError("«due.weekday» va de 0 (lunes) a 6 (domingo).")
            vals['due_weekday'] = str(day)
        if 'business_day' in due and due['business_day'] is not None:
            day = due['business_day']
            if isinstance(day, bool) or not isinstance(day, int) or not 1 <= day <= 23:
                raise ValidationError("«due.business_day» va de 1 a 23.")
            vals['due_business_day'] = day
        return vals

    def _inputs_commands(self, activity, item):
        """Comandos para dejar los «recibe» exactamente como vienen (con su
        plazo); vacío si ya están así."""
        wanted = self._resolve_inputs(item['inputs'])
        current = activity.input_ids if activity else self.env['sgi.activity.input']
        keys = ('max_days', 'applies_domain', 'applies_note', 'match_path')
        if [(line.deliverable_id.id, {k: line[k] for k in keys}) for line in current] == wanted:
            return []
        by_deliverable = {line.deliverable_id.id: line for line in current}
        cmds = []
        for seq, (deliverable_id, extra) in enumerate(wanted, start=1):
            line = by_deliverable.pop(deliverable_id, None)
            if line:
                cmds.append(Command.update(line.id, dict(extra, sequence=seq * 10)))
            else:
                cmds.append(Command.create(dict(extra, deliverable_id=deliverable_id,
                                                sequence=seq * 10)))
        cmds.extend(Command.delete(line.id) for line in by_deliverable.values())
        return cmds

    # ------------------------------------------------------------------
    # Actividades por referencia («se prueba con»)
    # ------------------------------------------------------------------
    def _find_activity(self, ref, default_process_code):
        """«C6.23» → actividad. Acepta «PROC:NUM». Busca primero en el mismo
        proceso, luego un numeral único en la empresa."""
        if isinstance(ref, dict):
            proc_code, number = ref.get('process') or default_process_code, ref.get('number')
        elif ':' in str(ref):
            proc_code, number = str(ref).split(':', 1)
        else:
            proc_code, number = default_process_code, str(ref)
        activity = self.activities.get((proc_code, number))
        if activity:
            return activity
        Activity = self.env['sgi.process.activity']
        process = self.processes.get(proc_code) or self.Process.search(
            [('code', '=', proc_code), ('company_id', '=', self.company.id)], limit=1)
        if process:
            activity = Activity.search([('process_id', '=', process.id),
                                        ('number', '=', number)], limit=1)
            if activity:
                return activity
        matches = Activity.search([('number', '=', number),
                                   ('company_id', '=', self.company.id)])
        if len(matches) == 1:
            return matches
        raise ValidationError(
            "Actividad «%s» %s." % (
                ref, "ambigua (%d procesos)" % len(matches) if matches else "no existe"))

    def _load_proxies(self):
        """Resuelve «se prueba con» cuando ya están todas las actividades y
        luego aplica la revisión de medición de los procedimientos en piloto
        o vigente, por proceso."""
        by_process = {}
        for activity, ref, proc_code in self.proxies:
            by_process.setdefault(proc_code, []).append((activity, ref))
        for proc_code in {code for (code, _n) in self.activities}:
            entries = by_process.get(proc_code, [])

            def run(entries=entries, proc_code=proc_code):
                for activity, ref in entries:
                    proxy = self._find_activity(ref, proc_code)
                    if activity.measure_proxy_activity_id != proxy:
                        activity.write({'measure_proxy_activity_id': proxy.id})
                        self.report.change('activity', "%s/%s" % (proc_code, activity.number),
                                           'updated', ['measure_proxy_activity_id'])
                activities = self.env['sgi.process.activity'].browse(
                    [a.id for (code, _n), a in self.activities.items() if code == proc_code])
                activities.with_context(sgi_defer_measure_check=False)._sgi_check_measure_strict()
            self._savepoint(run, 'measure', proc_code)

    # ------------------------------------------------------------------
    # Indicadores
    # ------------------------------------------------------------------
    def _find_process(self, code):
        process = self.processes.get(code) or self.Process.search(
            [('code', '=', code), ('company_id', '=', self.company.id)], limit=1)
        if not process:
            raise ValidationError("Proceso %s no existe." % code)
        return process

    def _load_replaces(self):
        """«replaces»: el proceso nuevo archiva a los que sustituye junto con
        sus actividades, sus ligas y sus flujos, y adopta sus indicadores y
        riesgos activos (una sola vez; con dry_run solo se reporta). Todo
        queda en el reporte y en el chatter del proceso archivado. Nada se
        borra: lo archivado conserva su texto. Los documentos del proceso
        viejo NO se vuelven obsoletos aquí: eso pasa al publicar el nuevo."""
        for code, olds in self.replaces:
            new = self.processes.get(code)
            if not new:
                continue    # su proceso no se cargó: el error ya está reportado

            def run(new=new, olds=olds, code=code):
                for old_code in olds:
                    if old_code == code:
                        raise ValidationError("%s no puede sustituirse a sí mismo." % code)
                    old = self.Process.search([('code', '=', old_code),
                                               ('company_id', '=', self.company.id)], limit=1)
                    if not old:
                        raise ValidationError("«replaces»: el proceso %s no existe." % old_code)
                    if not old.active:
                        continue
                    self.report.change('process', old_code, 'archived')
                    acts = self.env['sgi.process.activity'].search(
                        [('process_id', '=', old.id), ('active', '=', True)])
                    for act in acts:
                        self.report.change('activity', "%s/%s" % (
                            old_code, act.legacy_number or act.number), 'archived')
                    if acts:
                        acts.write({'active': False})
                    reason = "Proceso %s sustituido por %s." % (old_code, code)
                    ends = ['|', ('from_process_id', '=', old.id),
                            ('to_process_id', '=', old.id)]
                    # Ligas y flujos que tocan al proceso viejo: se archivan con
                    # su motivo (las calculadas lo exigen).
                    for model, kind in (('sgi.activity.link', 'link'),
                                        ('sgi.process.flow', 'flow')):
                        conns = self.env[model].with_context(sgi_connection_sync=True).search(
                            [('active', '=', True)] + ends)
                        for conn in conns:
                            self.report.change(kind, "%s/%s" % (old_code, conn.name), 'archived')
                        if conns:
                            conns.write({'active': False, 'inactive_reason': reason})
                    # Indicadores y riesgos activos: pasan al proceso nuevo
                    # (un indicador que el payload asigna a otro proceso se
                    # reubica después, en «indicators»).
                    for model, kind, label in (('sgi.indicator', 'indicator', 'code'),
                                               ('sgi.risk', 'risk', 'folio')):
                        recs = self.env[model].search([('process_id', '=', old.id)])
                        for rec in recs:
                            self.report.change(kind, "%s → %s: %s" % (
                                old_code, code, rec[label] or rec.name), 'moved')
                        if recs:
                            recs.write({'process_id': new.id})
                    old.write({'active': False})
                    if not self.report.dry_run:
                        old.message_post(body="Sustituido por %s — %s." % (
                            new.code, new.name))
            self._savepoint(run, 'process', code)

    def _report_spec_gaps(self):
        """Lo que le falta a cada proceso cargado, agrupado por faltante."""
        for code, process in self.processes.items():
            acts = process.procedure_activity_ids.filtered('active')
            by_code = {}
            for gap in acts.spec_gap_ids:
                by_code.setdefault((gap.severity, gap.code), []).append(
                    gap.activity_id.number or gap.activity_id.name)
            for (severity, gap_code), numbers in sorted(by_code.items()):
                self.report.warn('spec', code, "%s %s (%d): %s" % (
                    "✖" if severity == 'error' else "⚠", gap_code, len(numbers),
                    ", ".join(sorted(numbers))))
            for indicator in process.indicator_ids:
                problems = indicator._sgi_spec_problems()
                if problems:
                    self.report.warn('spec', code, "✖ indicador %s: %s" % (
                        indicator.code, ", ".join(problems)))

    def _load_publish(self):
        for code in self.publish:
            process = self.processes.get(code)
            if not process:
                continue

            def run(process=process, code=code):
                if process.state != 'vigente':
                    process.action_sgi_publish()
                    self.report.change('process', code, 'updated', ['state'])
            self._savepoint(run, 'publish', code)

    def _load_indicators(self, indicators):
        Indicator = self.env['sgi.indicator'].with_context(active_test=False)
        Users = self.env['res.users']
        for item in indicators:
            key = item.get('code')

            def run(item=item, key=key):
                if not key:
                    raise ValidationError("Indicador sin «code».")
                vals = {name: (item[name] if item[name] is not None else False)
                        for name in _INDICATOR_FIELDS if name in item}
                if vals.get('target_date'):
                    vals['target_date'] = fields.Date.to_date(vals['target_date'])
                if 'target' in item:
                    vals['target_objective'] = item['target'] or 0.0
                if 'unit' in item:
                    vals['uom'] = item['unit'] or False
                if 'direction' in vals:
                    if vals['direction'] not in _DIRECTIONS:
                        raise ValidationError("«direction» va en up o down.")
                    vals['direction'] = _DIRECTIONS[vals['direction']]
                if 'process' in item:
                    vals['process_id'] = self._find_process(item['process']).id \
                        if item['process'] else False
                # Modos genéricos: la actividad («C2.17» o «PROC:NUM») y el
                # entregable (por código) que mide el indicador.
                if 'activity' in item:
                    vals['activity_id'] = self._find_activity(
                        item['activity'], item.get('process')).id if item['activity'] else False
                if 'deliverable' in item:
                    deliverable = self.env['sgi.deliverable'].search([
                        ('code', '=', item['deliverable']),
                        ('company_id', '=', self.company.id)], limit=1) \
                        if item['deliverable'] else self.env['sgi.deliverable']
                    if item['deliverable'] and not deliverable:
                        raise ValidationError("Entregable «%s» no existe." % item['deliverable'])
                    vals['deliverable_id'] = deliverable.id
                if item.get('responsible_employee_id') and item.get('responsible'):
                    raise ValidationError("Indica «responsible» o «responsible_employee_id», "
                                          "no los dos.")
                if 'responsible_employee_id' in item:
                    emp_id = item['responsible_employee_id']
                    emp = self.env['hr.employee'].browse(int(emp_id)).exists() \
                        if emp_id else self.env['hr.employee']
                    if emp_id and not emp:
                        raise ValidationError("El empleado %s no existe." % emp_id)
                    if emp and not emp.user_id:
                        raise ValidationError(
                            "El empleado %s (%s) no tiene usuario: no puede ser "
                            "responsable del indicador." % (emp.id, emp.name))
                    vals['responsible_id'] = emp.user_id.id
                if 'responsible' in item:
                    ref = item['responsible']
                    user = Users.browse(ref).exists() if isinstance(ref, int) else \
                        Users.search([('login', '=', ref)], limit=1) if ref else Users
                    if ref and not user:
                        raise ValidationError("Responsable «%s» no existe (id o login)." % ref)
                    vals['responsible_id'] = user.id
                indicator = Indicator.search([('code', '=', key)], limit=1)
                if indicator:
                    if not indicator.active:
                        vals['active'] = True
                    changed = _diff(indicator, vals)
                    if changed:
                        indicator.write(changed)
                        self.report.change('indicator', key, 'updated', changed)
                else:
                    if not vals.get('name'):
                        raise ValidationError("El indicador nuevo %s necesita «name»." % key)
                    # Con responsable, una medición roja validada abre NC.
                    vals.setdefault('nc_on_red', bool(vals.get('responsible_id')))
                    Indicator.create(dict(vals, code=key))
                    self.report.change('indicator', key, 'created')
                if not vals.get('responsible_id') and not (indicator and indicator.responsible_id):
                    self.report.warn('indicator', key, "Indicador sin responsable.")
            self._savepoint(run, 'indicator', key)
