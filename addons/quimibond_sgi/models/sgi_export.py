# -*- coding: utf-8 -*-
"""Exportar el mapa (H-022, entrega 3): ``sgi.process.export_payload()``.

Es el inverso exacto de ``load_payload`` (sgi_load.py): devuelve el JSON que
``load_payload`` vuelve a cargar sin duplicar y que, cargado con ``dry_run``
sobre la misma base, reporta cero cambios. Lo usa el módulo de datos
``quimibond_sgi_mapa`` (decisión 6: el SGI se instala vacío; el mapa de
producción vive en un módulo aparte para staging y recuperación).

Todo va por llave natural, nunca por id de la base:

- procesos por clave; actividades por numeral congelado (decisión 8, con sus
  huecos); entregables y familias por código;
- puestos por nombre (el cargador nunca los crea); el dueño del proceso por
  su puesto (``owner_job``), no por empleado; nada de usuarios;
- documentos por clave (``sgi_code``); puntos de la norma por etiqueta
  («9001 8.5.1»); menús por XML ID; ubicaciones por nombre completo;
- los filtros (dominios) con IDs fijos pasan a marcas ``%(r1)s`` con su
  referencia portable (XML ID, llave natural o, como último recurso, id +
  nombre, que solo se resuelve en una copia de producción). Ver
  sgi_domain_refs.py.

No se exporta lo operativo ni lo calculado: semáforos, mediciones, faltantes,
ligas y flujos (salen solos de entradas y salidas), numeral anterior
(``legacy_number``), procedimientos sustituidos (viajan con el documento,
decisión 3), responsables de indicadores (usuarios) ni los puntos de control
de los planes (quality.point, por producto).

Lo que no se puede exportar de forma portable queda en ``meta.review``.
"""
import datetime
import json

from odoo import api, fields, models
from odoo.exceptions import AccessError

from . import sgi_domain_refs
from .sgi_load import _INDICATOR_FIELDS, _TERM_FIELDS, sgi_ref_search

EXPORT_FORMAT = 'quimibond_sgi.payload'
EXPORT_VERSION = 1

# Llave natural por modelo para las referencias de los filtros (cuando el
# registro no tiene XML ID de un módulo). Sin entrada: «name».
_REF_KEYS = {
    'stock.picking.type': ('name', 'warehouse_id.name'),
    'stock.location': ('complete_name',),
    'product.category': ('complete_name',),
    'account.journal': ('code',),
    'res.users': ('login',),
    'quality.point': ('title',),
    'survey.survey': ('title',),
}
# XML IDs que no sirven fuera de la base de origen.
_LOCAL_MODULES = ('__export__', '__import__')
_ACTIVITY_TEXT = ('name', 'description', 'odoo_ref', 'note', 'responsible_role',
                  'check_against', 'how_steps', 'done_criteria', 'on_fail')


def _clean(name):
    return ' '.join((name or '').split())


class _Refs:
    """Marcas de un objeto (entregable, entrada, término): la misma
    referencia se reutiliza con el mismo nombre."""

    def __init__(self, exporter):
        self.exporter = exporter
        self.by_record = {}
        self.refs = {}

    def name_for(self, record):
        key = (record._name, record.id)
        if key not in self.by_record:
            name = 'r%d' % (len(self.by_record) + 1)
            self.by_record[key] = name
            self.refs[name] = self.exporter.ref(record)
        return self.by_record[key]


class _SgiExporter:
    def __init__(self, env, company, process_codes=None):
        self.env = env
        self.company = company
        self.process_codes = set(process_codes) if process_codes else None
        self.review = []
        self.xmlid_cache = {}

    # ------------------------------------------------------------------
    def note(self, where, message):
        entry = "%s: %s" % (where, message)
        if entry not in self.review:
            self.review.append(entry)

    def xmlid(self, record):
        key = (record._name, record.id)
        if key not in self.xmlid_cache:
            data = self.env['ir.model.data'].sudo().search(
                [('model', '=', record._name), ('res_id', '=', record.id),
                 ('module', 'not in', _LOCAL_MODULES)], order='id', limit=1)
            self.xmlid_cache[key] = "%s.%s" % (data.module, data.name) if data else None
        return self.xmlid_cache[key]

    def ref(self, record):
        """Referencia portable de un registro (ver sgi_domain_refs)."""
        record = record.sudo().with_context(active_test=False)
        model = record._name
        if model == 'res.company' and record == self.company:
            return {'model': model, 'company': True}
        xmlid = self.xmlid(record)
        if xmlid:
            return {'model': model, 'xmlid': xmlid}
        keys = _REF_KEYS.get(model) or (('name',) if 'name' in record._fields else ())
        if keys:
            values = {}
            for path in keys:
                value = record.mapped(path)
                value = value[0] if isinstance(value, list) and value else value
                if isinstance(value, models.BaseModel) or not value:
                    values = None
                    break
                values[path] = value
            if values and sgi_ref_search(record.browse(), self.company, values) == record:
                return {'model': model, 'key': values}
        self.note(model, "%s (id %d) sin llave única: se exporta por id + nombre y solo "
                         "se resuelve en una copia de producción" % (record.display_name, record.id))
        return {'model': model, 'id': record.id, 'name': record.display_name}

    def comodel(self, model_name, path):
        """Modelo al que apunta la ruta de campos, o None si no es relación."""
        if model_name not in self.env:
            return None
        Model = self.env[model_name]
        parts = path.split('.')
        for index, part in enumerate(parts):
            if part == 'id' and index == len(parts) - 1:
                return Model._name
            field = Model._fields.get(part)
            if field is None or field.type not in ('many2one', 'many2many', 'one2many'):
                return None
            Model = self.env[field.comodel_name]
        return Model._name

    def domain(self, text, model_name, marks, where):
        """Filtro con sus IDs fijos convertidos en marcas (en ``marks``)."""
        if not text or not model_name:
            return text or False

        def decide(path, value):
            comodel = self.comodel(model_name, path)
            if not comodel:
                return None
            record = self.env[comodel].sudo().with_context(active_test=False).browse(value).exists()
            if not record:
                self.note(where, "%s = %d no existe; se deja el número" % (path, value))
                return None
            return marks.name_for(record)
        template, _used = sgi_domain_refs.templatize(text, decide)
        return template

    # ------------------------------------------------------------------
    def run(self):
        Process = self.env['sgi.process'].with_context(active_test=False)
        domain = [('company_id', '=', self.company.id), ('active', '=', True)]
        if self.process_codes:
            domain.append(('code', 'in', sorted(self.process_codes)))
        processes = Process.search(domain, order='code')
        activities = processes.procedure_activity_ids.filtered('active') \
            if 'procedure_activity_ids' in Process._fields else \
            self.env['sgi.process.activity'].search([('process_id', 'in', processes.ids)])
        deliverables = self._deliverables(activities)
        families = self._families(activities)
        indicators = self._indicators(processes)
        payload = {
            'meta': {},
            'tolerant': True,
            'archive_missing': True,
            'families': [self._family(f) for f in families],
            'deliverables': [self._deliverable(d) for d in deliverables],
            'objectives': [self._objective(o) for o in self._objectives(indicators)],
            'processes': [self._process(p) for p in processes],
            'control_plans': [self._control_plan(c) for c in self._control_plans()],
            'indicators': [self._indicator(i) for i in indicators],
        }
        stages = activities.stage_id
        payload['meta'] = {
            'format': EXPORT_FORMAT,
            'version': EXPORT_VERSION,
            'generated': fields.Datetime.to_string(fields.Datetime.now()),
            'generator': 'sgi.process.export_payload',
            'company': self.company.name,
            'counts': {
                'processes': len(processes), 'stages': len(stages),
                'activities': len(activities), 'roles': len(activities.role_ids),
                'inputs': len(activities.input_ids), 'deliverables': len(deliverables),
                'families': len(families), 'indicators': len(indicators),
                'terms': sum(len(i['terms']) for i in payload['indicators']),
                'objectives': len(payload['objectives']),
                'control_plans': len(payload['control_plans']),
            },
            'review': self.review,
        }
        return payload

    # ------------------------------------------------------------------
    def _deliverables(self, activities):
        Deliverable = self.env['sgi.deliverable']
        if self.process_codes:
            used = activities.output_deliverable_ids | activities.input_ids.deliverable_id \
                | activities.measure_deliverable_id
            return used.filtered('active').sorted('code')
        return Deliverable.search([('company_id', '=', self.company.id), ('active', '=', True)],
                                  order='code')

    def _families(self, activities):
        Family = self.env['sgi.job.family']
        if self.process_codes:
            return activities.role_ids.family_id.filtered('active').sorted('code')
        return Family.search([('company_id', '=', self.company.id), ('active', '=', True)],
                             order='code')

    def _indicators(self, processes):
        Indicator = self.env['sgi.indicator']
        if self.process_codes:
            return Indicator.search([('process_id', 'in', processes.ids)], order='code')
        return Indicator.search([], order='code')

    def _objectives(self, indicators):
        if self.process_codes:
            return indicators.objective_id.sorted('id')
        return self.env['sgi.objective'].search([], order='id')

    def _control_plans(self):
        if self.process_codes:
            return self.env['sgi.control.plan']
        Plan = self.env['sgi.control.plan']
        domain = [('company_id', '=', self.company.id)] if 'company_id' in Plan._fields else []
        return Plan.search(domain, order='id')

    # ------------------------------------------------------------------
    def _doc_code(self, doc, where):
        if not doc:
            return None
        if not doc.sgi_code:
            self.note(where, "documento %s sin clave: no se exporta" % doc.display_name)
            return None
        return doc.sgi_code

    def _family(self, family):
        return {'code': family.code, 'name': family.name,
                'jobs': sorted(_clean(job.name) for job in family.job_ids)}

    def _deliverable(self, deliverable):
        where = "entregable %s" % deliverable.code
        model_name = deliverable.odoo_model_id.model or None
        marks = _Refs(self)
        out = {'code': deliverable.code, 'name': deliverable.name}
        out['document'] = self._doc_code(deliverable.document_id, where)
        out['model'] = model_name
        out['domain'] = self.domain(deliverable.measure_domain, model_name, marks, where) or '[]'
        out['date_field'] = deliverable.measure_date_field or None
        out['user_field'] = deliverable.measure_user_field or None
        out['complete_domain'] = self.domain(deliverable.complete_domain, model_name, marks, where) \
            or None
        out['complete_criteria'] = deliverable.complete_criteria or None
        out['acceptance_criteria'] = deliverable.acceptance_criteria or None
        if 'require_signed' in deliverable._fields:
            out['require_signed'] = bool(deliverable.require_signed)
        if 'survey_id' in deliverable._fields:
            out['survey'] = self.ref(deliverable.survey_id) if deliverable.survey_id else None
        out['boundary'] = deliverable.boundary or None
        if marks.refs:
            out['refs'] = marks.refs
        return out

    def _process(self, process):
        out = {'code': process.code, 'name': process.name}
        for name in ('purpose', 'scope', 'env_aspects'):
            if process[name]:
                out[name] = process[name]
        out['type'] = process.process_type
        job = process.owner_id.job_id
        if job:
            out['owner_job'] = _clean(job.name)
        elif process.owner_id:
            self.note("proceso %s" % process.code, "el dueño no tiene puesto: no se exporta")
        out['parent'] = process.parent_id.code or None
        if process.state == 'vigente':
            out['publish'] = True
        elif process.state in ('borrador', 'piloto'):
            out['state'] = process.state
        acts = process.procedure_activity_ids.filtered('active') \
            if 'procedure_activity_ids' in process._fields else \
            self.env['sgi.process.activity'].search([('process_id', '=', process.id)])
        acts = acts.sorted(lambda a: (a.stage_id.sequence, a.stage_id.id, a.sequence, a.step, a.id))
        out['activities'] = [self._activity(a) for a in acts]
        return out

    def _activity(self, act):
        where = "actividad %s" % act.number
        out = {'number': act.number, 'sequence': act.sequence}
        if act.stage_id:
            out['stage'] = act.stage_id.display_name
        for name in _ACTIVITY_TEXT:
            if act[name]:
                out[name] = act[name]
        if act.block:
            out['block'] = act.block
        if act.value_class:
            out['value_class'] = act.value_class
        if act.measure_cadence:
            out['cadence'] = act.measure_cadence
        out['roles'] = [self._role(r) for r in act.role_ids.sorted(lambda r: (r.sequence, r.id))]
        inputs = []
        for line in act.input_ids:
            marks = _Refs(self)
            entry = {'code': line.deliverable_id.code, 'days': line.max_days}
            if line.applies_domain:
                entry['applies_domain'] = self.domain(
                    line.applies_domain, line.deliverable_id.odoo_model_id.model or None, marks,
                    "%s recibe %s" % (where, line.deliverable_id.code))
            for src, dst in (('applies_note', 'applies_note'), ('match_path', 'match'),
                             ('due_field', 'due_field'), ('offset_days', 'offset_days')):
                if line[src]:
                    entry[dst] = line[src]
            if marks.refs:
                entry['refs'] = marks.refs
            inputs.append(entry)
        out['inputs'] = inputs
        out['outputs'] = sorted(act.output_deliverable_ids.mapped('code'))
        if act.exec_channel:
            out['where'] = self._where(act, where)
        due = {}
        if act.due_weekday:
            due['weekday'] = int(act.due_weekday)
        if act.due_business_day:
            due['business_day'] = act.due_business_day
        if act.due_month and act.due_day:
            due.update(month=int(act.due_month), day=act.due_day)
        if due:
            out['due'] = due
        out['measure'] = self._measure(act)
        if act.measure_method == 'odoo' and act.measure_model_name:
            marks = _Refs(self)
            evidence = {'source_type': 'odoo_model', 'model': act.measure_model_name,
                        'domain': self.domain(act.measure_domain, act.measure_model_name, marks, where),
                        'date_field': act.measure_date_field or 'create_date',
                        'user_field': act.measure_user_field or None}
            if marks.refs:
                evidence['refs'] = marks.refs
            out['evidence'] = [evidence]
        out['automation'] = {'current': act.automation_level_current or None,
                             'target': act.automation_level_target or None,
                             'method': act.automation_method or None}
        instruction = self._doc_code(act.instruction_id, where)
        if instruction:
            out['instruction'] = instruction
        procedure = self._doc_code(act.related_procedure_id, where)
        if procedure:
            out['related_procedure'] = procedure
        formats = [self._doc_code(d, where) for d in act.format_document_ids]
        if formats:
            out['formats'] = sorted(code for code in formats if code)
        if 'norm_clause_ids' in act._fields and act.norm_clause_ids:
            out['complies_with'] = sorted(act.norm_clause_ids.mapped('short_label'))
        if 'fiscal_position_ids' in act._fields and act.fiscal_position_ids:
            out['markets'] = [self.ref(r) for r in act.fiscal_position_ids.sorted('id')]
        if 'sale_team_ids' in act._fields and act.sale_team_ids:
            out['teams'] = [self.ref(r) for r in act.sale_team_ids.sorted('id')]
        return out

    def _role(self, role):
        out = {'role': role.role}
        if role.target_type == 'family':
            out['family'] = role.family_id.code
        elif role.target_type == 'relative':
            out['relative'] = role.relative_role
        else:
            out['job'] = _clean(role.job_id.name)
        if role.condition:
            out['condition'] = role.condition
        if role.after_days:
            out['after_days'] = role.after_days
        if role.substitute_job_id:
            out['substitute_job'] = _clean(role.substitute_job_id.name)
        out['sequence'] = role.sequence
        return out

    def _where(self, act, where):
        out = {'channel': act.exec_channel}
        if act.odoo_menu_id:
            xmlid = self.xmlid(act.odoo_menu_id)
            if xmlid:
                out['menu'] = xmlid
            else:
                self.note(where, "el menú %s no tiene XML ID: no se exporta" % act.odoo_menu_id.id)
        if act.external_system:
            out['external_system'] = act.external_system
        if act.location_id:
            out['location'] = act.location_id.complete_name
        if act.workcenter_id:
            if act.workcenter_id.code:
                out['workcenter'] = act.workcenter_id.code
            else:
                self.note(where, "el centro de trabajo %s no tiene código: no se exporta"
                          % act.workcenter_id.display_name)
        if act.place_note:
            out['place'] = act.place_note
        return out

    def _measure(self, act):
        out = {'method': act.measure_method}
        if act.measure_method == 'entregable' and act.measure_deliverable_id:
            out['deliverable'] = act.measure_deliverable_id.code
        elif act.measure_method == 'consecuencia' and act.measure_proxy_activity_id:
            proxy = act.measure_proxy_activity_id
            out['proxy'] = proxy.number if proxy.process_id == act.process_id else \
                "%s:%s" % (proxy.process_id.code, proxy.number)
        elif act.measure_method == 'no_aplica':
            out['justification'] = act.measure_justification or None
        elif act.measure_method == 'muestreo':
            out['sample_cadence'] = act.sample_cadence
        return out

    def _objective(self, objective):
        return {'name': objective.name, 'description': objective.description or None,
                'target_year': objective.target_year or None}

    def _control_plan(self, plan):
        out = {'name': plan.name, 'phase': plan.phase, 'revision': plan.revision or None,
               'notes': plan.notes or None}
        out['document'] = self._doc_code(plan.document_id, "plan de control %s" % plan.name)
        out['partner'] = self.ref(plan.partner_id) if plan.partner_id else None
        if plan.point_ids:
            self.note("plan de control %s" % plan.name,
                      "%d punto(s) de control no viajan en el mapa" % len(plan.point_ids))
        return out

    def _indicator(self, indicator):
        out = {'code': indicator.code, 'name': indicator.name}
        for name in _INDICATOR_FIELDS:
            if name == 'name' or name not in indicator._fields:
                continue
            value = indicator[name]
            if isinstance(value, (datetime.date, datetime.datetime)):
                value = fields.Date.to_string(value) if not isinstance(value, datetime.datetime) \
                    else fields.Datetime.to_string(value)
            if value in (False, None, ''):
                continue
            out[name] = value
        out['process'] = indicator.process_id.code or None
        if indicator.activity_id:
            out['activity'] = "%s:%s" % (indicator.activity_id.process_id.code,
                                         indicator.activity_id.number)
        if indicator.deliverable_id:
            out['deliverable'] = indicator.deliverable_id.code
        if indicator.objective_id:
            out['objective'] = indicator.objective_id.name
        terms = []
        for term in indicator.term_ids.sorted(lambda t: (t.role, t.id)):
            marks = _Refs(self)
            entry = {'role': term.role, 'model': term.model_id.model,
                     'domain': self.domain(term.domain, term.model_id.model, marks,
                                           "indicador %s" % indicator.code)}
            for name in _TERM_FIELDS:
                if name == 'role':
                    continue
                entry[name] = term[name] if term[name] is not False else None
            if marks.refs:
                entry['refs'] = marks.refs
            terms.append(entry)
        out['terms'] = terms
        return out


class SgiProcessExport(models.Model):
    _inherit = 'sgi.process'

    @api.model
    def export_payload(self, process_codes=None, company_id=None):
        """El mapa de la empresa en el formato de ``load_payload`` (su inverso
        exacto). ``process_codes`` limita a esos procesos (y a los
        entregables, familias e indicadores que usan). Solo Administrador SGI."""
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_admin')):
            raise AccessError("Solo un Administrador SGI puede exportar el mapa.")
        company = self.env['res.company'].browse(company_id).exists() if company_id \
            else self.env.company
        exporter = _SgiExporter(self.env, company, process_codes)
        return exporter.run()

    @api.model
    def export_payload_json(self, process_codes=None, company_id=None):
        """Lo mismo, como texto JSON estable (para guardar en un archivo)."""
        return json.dumps(self.export_payload(process_codes, company_id),
                          ensure_ascii=False, indent=1, sort_keys=False)
