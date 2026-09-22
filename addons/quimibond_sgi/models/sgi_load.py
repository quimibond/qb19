# -*- coding: utf-8 -*-
"""Carga idempotente del catálogo por API (JSON-RPC / MCP).

``sgi.process.load_payload(payload)`` recibe procesos, actividades con sus
roles, evidencia y automatización, ligas, flujos e indicadores, y da de alta
o actualiza por llave natural:

- proceso: ``code`` (por empresa);
- actividad: clave del proceso + ``number``;
- rol: actividad + rol + puesto;
- liga: actividad origen + actividad destino;
- flujo: proceso origen + proceso destino + entregable;
- indicador: ``code``.

Correr dos veces el mismo JSON no duplica nada y la segunda corrida reporta
cero cambios. ``dry_run`` hace todo dentro de un savepoint que se deshace al
final: reporta exactamente lo que se crearía, actualizaría o archivaría, y
los errores que la base daría, sin escribir.

Los puestos se resuelven por id o por nombre normalizado y NUNCA se crean:
si no existen, la actividad no se carga y el error viene en la respuesta.
Cada proceso es una transacción: si una de sus actividades falla, el proceso
completo se deshace y se reporta.
"""
import json
import logging

from odoo import models, api, Command
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tools.safe_eval import safe_eval

_logger = logging.getLogger(__name__)

_PROCESS_TYPES = {
    'estrategico': 'estrategico', 'estratégico': 'estrategico',
    'cadena de valor': 'cop', 'cadena_de_valor': 'cop', 'cadena_valor': 'cop',
    'cop': 'cop', 'soporte': 'soporte',
}
_PROCESS_TEXT_FIELDS = (
    'name', 'purpose', 'scope', 'start_trigger', 'end_trigger', 'inputs',
    'outputs', 'env_aspects')
_ACTIVITY_TEXT_FIELDS = (
    'section', 'name', 'description', 'odoo_ref', 'note', 'responsible_role')
_INDICATOR_FIELDS = (
    'name', 'uom', 'direction', 'target_objective', 'target_acceptable',
    'frequency', 'calc_mode', 'monthly_budget', 'nc_on_red')


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
        self.Activity = self.env['sgi.process.activity'].with_context(active_test=False)
        self.Job = self.env['hr.job']
        self.Doc = self.env['documents.document']
        self.processes = {}     # code -> sgi.process
        self.activities = {}    # (process code, number) -> sgi.process.activity
        self.job_cache = {}

    # ------------------------------------------------------------------
    def run(self):
        payload = self.payload
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
        self._load_links()
        self._load_flows(payload.get('flows') or [])
        self._load_indicators(payload.get('indicators') or [])

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
            number = item.get('number')
            key = "%s/%s" % (process.code, number)
            if not number:
                self.report.error('activity', key, "Actividad sin «number».")
                continue
            if number in seen:
                self.report.error('activity', key, "Numeral repetido en el payload.")
                continue
            seen.add(number)
            self._savepoint(lambda: self._upsert_activity(process, item, index),
                            'activity', key)
        if archive_missing:
            stale = self.Activity.search([('process_id', '=', process.id),
                                          ('active', '=', True),
                                          ('number', 'not in', list(seen) or [''])])
            for activity in stale:
                self.report.change('activity', "%s/%s" % (process.code, activity.number),
                                   'archived')
            if stale:
                stale.write({'active': False})

    def _activity_vals(self, process, item, index):
        key = "%s/%s" % (process.code, item['number'])
        vals = {name: item[name] for name in _ACTIVITY_TEXT_FIELDS if name in item}
        vals['sequence'] = item.get('sequence') or (index + 1) * 10
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
        return vals

    def _evidence_vals(self, key, evidence):
        """Fase 1: la primera evidencia `odoo_model` va a los campos de medición
        actuales de la actividad. Las demás fuentes (correo, manual, externa,
        user_field, ciclo) se guardan en la fase 2 (sgi.activity.evidence);
        hoy se avisan para que la carga se repita entonces."""
        models_ev = [e for e in evidence if e.get('source_type', 'odoo_model') == 'odoo_model']
        if len(evidence) > len(models_ev[:1]):
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
        for fname in (date_field, ev.get('user_field')):
            if fname and fname not in Model._fields:
                raise ValidationError("Evidencia: %s no tiene el campo «%s»." % (model_name, fname))
        return {
            'measure_model_name': model_name,
            'measure_domain': domain_txt,
            'measure_date_field': date_field,
        }

    def _roles_commands(self, activity, item, key):
        """Comandos para dejar los roles exactamente como el payload."""
        wanted = []
        for seq, role in enumerate(item.get('roles') or [], start=1):
            job, error = self._resolve_job(role.get('job_id') or role.get('job'))
            if error:
                raise ValidationError("Rol %s: %s" % (role.get('role'), error))
            if role.get('role') not in ('ejecuta', 'aprueba', 'participa', 'informa'):
                raise ValidationError("Rol «%s» inválido (ejecuta, aprueba, participa, "
                                      "informa)." % role.get('role'))
            wanted.append({'role': role['role'], 'job_id': job.id,
                           'condition': role.get('condition') or False,
                           'sequence': seq * 10})
        current = {(r.role, r.job_id.id): r for r in activity.role_ids} if activity else {}
        commands, touched = [], False
        for vals in wanted:
            existing = current.pop((vals['role'], vals['job_id']), None)
            if existing:
                changed = _diff(existing, {'condition': vals['condition'],
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

    def _upsert_activity(self, process, item, index):
        number = item['number']
        key = "%s/%s" % (process.code, number)
        activity = self.Activity.search([('process_id', '=', process.id),
                                         ('number', '=', number)], limit=1)
        vals = self._activity_vals(process, item, index)
        role_cmds, roles_touched = self._roles_commands(activity, item, key) \
            if 'roles' in item else ([], False)
        if activity:
            if not activity.active:
                vals['active'] = True
            changed = _diff(activity, vals)
            if roles_touched:
                changed['role_ids'] = role_cmds
            if changed:
                activity.write(changed)
                self.report.change('activity', key, 'updated', changed)
        else:
            vals.update(process_id=process.id, number=number, role_ids=role_cmds)
            if 'format_document_ids' in vals:
                vals['format_document_ids'] = [Command.set(vals['format_document_ids'])]
            activity = self.Activity.create(vals)
            self.report.change('activity', key, 'created')
        self.activities[(process.code, number)] = activity
        # Los roles, el instructivo y la evidencia se validan en la base; la
        # restricción de «exactamente un ejecutor» corre aquí dentro.
        activity.flush_recordset()

    # ------------------------------------------------------------------
    # Ligas entre actividades
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
            "Actividad destino «%s» %s." % (
                ref, "ambigua (%d procesos)" % len(matches) if matches else "no existe"))

    def _load_links(self):
        wanted = {}   # from activity id -> {to activity id: name} (solo las declaradas)
        declared = set()
        by_process = {}
        items = []
        for item in self.payload.get('activities') or []:
            items.append((item.get('process'), item))
        for proc in self.payload.get('processes') or []:
            for item in proc.get('activities') or []:
                items.append((proc.get('code'), item))
        for proc_code, item in items:
            if 'links_to' not in item:
                continue
            source = self.activities.get((proc_code, item.get('number')))
            if not source:
                continue  # su proceso no se cargó: el error ya está reportado
            declared.add(source.id)
            by_process.setdefault(proc_code, []).append((source, item['links_to']))
        for link in self.payload.get('links') or []:
            proc_code = link.get('process') or str(link.get('from', '')).split(':')[0]
            by_process.setdefault(proc_code, []).append((link, None))

        for proc_code, entries in by_process.items():
            def run(entries=entries, proc_code=proc_code):
                for source, targets in entries:
                    if targets is None:   # liga suelta en «links»
                        link = source
                        frm = self._find_activity(link.get('from'), proc_code)
                        to = self._find_activity(link.get('to'), frm.process_id.code)
                        wanted.setdefault(frm.id, {})[to.id] = link.get('name') or to.name or to.number
                        continue
                    wanted.setdefault(source.id, {})
                    for target in targets or []:
                        ref = target.get('to') if isinstance(target, dict) else target
                        to = self._find_activity(ref, proc_code)
                        name = (target.get('name') if isinstance(target, dict) else None) \
                            or to.name or to.number
                        wanted[source.id][to.id] = name
                self._sync_links(wanted, declared, [s for s, t in entries if t is not None])
            self._savepoint(run, 'link', proc_code)

    def _sync_links(self, wanted, declared, sources):
        Link = self.env['sgi.activity.link']
        Activity = self.env['sgi.process.activity']
        froms = Activity.browse(list(wanted))
        for frm in froms:
            targets = wanted[frm.id]
            existing = {link.to_activity_id.id: link for link in frm.out_link_ids}
            for to_id, name in targets.items():
                key = "%s → %s" % (frm.display_name, Activity.browse(to_id).display_name)
                link = existing.pop(to_id, None)
                if link:
                    if link.name != name:
                        link.write({'name': name})
                        self.report.change('link', key, 'updated', ['name'])
                else:
                    Link.create({'from_activity_id': frm.id, 'to_activity_id': to_id,
                                 'name': name})
                    self.report.change('link', key, 'created')
            # Solo se quitan las ligas de actividades que declararon links_to.
            if frm.id in declared and frm in sources:
                for link in existing.values():
                    self.report.change('link', "%s → %s" % (
                        frm.display_name, link.to_activity_id.display_name), 'deleted')
                    link.unlink()
        for frm in froms:
            wanted.pop(frm.id, None)

    # ------------------------------------------------------------------
    # Flujos entre procesos e indicadores
    # ------------------------------------------------------------------
    def _find_process(self, code):
        process = self.processes.get(code) or self.Process.search(
            [('code', '=', code), ('company_id', '=', self.company.id)], limit=1)
        if not process:
            raise ValidationError("Proceso %s no existe." % code)
        return process

    def _load_flows(self, flows):
        Flow = self.env['sgi.process.flow']
        for item in flows:
            key = "%s → %s: %s" % (item.get('from'), item.get('to'), item.get('name'))

            def run(item=item, key=key):
                if not item.get('name'):
                    raise ValidationError("Flujo sin «name» (entregable).")
                frm, to = self._find_process(item.get('from')), self._find_process(item.get('to'))
                vals = {}
                if 'acceptance_criteria' in item:
                    vals['acceptance_criteria'] = item['acceptance_criteria']
                if 'model' in item:
                    model = self.env['ir.model']._get(item['model']) if item['model'] else False
                    if item['model'] and not model:
                        raise ValidationError("Modelo %s no existe." % item['model'])
                    vals['odoo_model_id'] = model.id if model else False
                if 'document' in item:
                    doc = self.Doc._sgi_find_by_code(item['document'], states=None) \
                        if item['document'] else self.Doc
                    vals['document_id'] = doc.id
                flow = Flow.search([('from_process_id', '=', frm.id),
                                    ('to_process_id', '=', to.id),
                                    ('name', '=', item['name'])], limit=1)
                if flow:
                    changed = _diff(flow, vals)
                    if changed:
                        flow.write(changed)
                        self.report.change('flow', key, 'updated', changed)
                else:
                    Flow.create(dict(vals, from_process_id=frm.id, to_process_id=to.id,
                                     name=item['name']))
                    self.report.change('flow', key, 'created')
            self._savepoint(run, 'flow', key)

    def _load_indicators(self, indicators):
        Indicator = self.env['sgi.indicator'].with_context(active_test=False)
        Users = self.env['res.users']
        for item in indicators:
            key = item.get('code')

            def run(item=item, key=key):
                if not key:
                    raise ValidationError("Indicador sin «code».")
                vals = {name: item[name] for name in _INDICATOR_FIELDS if name in item}
                if 'process' in item:
                    vals['process_id'] = self._find_process(item['process']).id \
                        if item['process'] else False
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
