#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Genera ``data/mapa.json`` desde una lectura de producción por MCP.

El camino normal para regenerar el mapa es ``sgi.process.export_payload()``
en la base (botón «Descargar el mapa de esta base» del asistente, o por MCP /
shell). Este script existe porque el mapa inicial (2026-09-29) se armó solo
leyendo producción por MCP (sin escribir nada): toma las lecturas guardadas y
aplica las MISMAS reglas que ``export_payload`` (quimibond_sgi/models/
sgi_export.py), con la misma plantilla de dominios (sgi_domain_refs.py).

Uso::

    python3 tools/generar_mapa.py DIRECTORIO_DE_LECTURAS > data/mapa.json

Lecturas (JSON tal como las devuelve el MCP de Odoo, ``search_records``;
empresa 1, 2026-09-29):

- ``act_*.json``   sgi.process.activity [active=True] con los campos de
  ``ACT_FIELDS`` (310).
- ``role_*.json``  sgi.activity.role [activity_id.active=True], __all__ (1,072).
- ``inp_*.json``   sgi.activity.input [activity_id.active=True], __all__ (246).
- ``del_*.json``   sgi.deliverable [active in (True, False)] (320).
- ``proc.json``    sgi.process [active=True], __all__ (14).
- ``docs_*.json``  documents.document de los formatos, instructivos y
  procedimientos que citan las actividades: solo id y ``sgi_code`` (357).
- ``ind.json``     sgi.indicator [active in (True, False)], __all__ (96).
- ``jobs.json``    {id: {"name", "company", "active"}} de hr.job (129; solo
  el nombre: nada de sueldos ni descripciones).
- ``families.json`` [{code, name, job_ids}] de sgi.job.family (14).
- ``owners.json``  {hr.employee id del dueño: hr.job id} (11).
- ``menus.json``   {ir.ui.menu id: XML ID} de los menús que usan (98).
- ``manual.json``  términos de fórmula (50), objetivos (10) y encabezados de
  planes de control (10), transcritos de su lectura.
- ``refs_catalogo.json`` {"modelo:id": referencia} de cada registro que
  aparece con id fijo en un filtro, resuelto con la regla de
  ``export_payload``: XML ID de un módulo (no ``__export__`` ni
  ``__import__``), si no la llave natural (``_REF_KEYS``) comprobada única por
  MCP, y si no id + nombre (solo se resuelve en una copia de producción).

No se leen usuarios, empleados (salvo el puesto del dueño), sueldos ni el
contenido de ningún documento.

Ojo (quimibond_sgi_mapa 1.1.0): ``data/mapa.json`` se editó a mano después
de generarlo (E1-02 en ``acuerdos_rxd`` y sin la familia OP-PTAR; ver
``meta.review``). Volver a correr este script con las lecturas del
2026-09-29 regresaría esas dos entradas; regenerar mejor con
``export_payload`` en una base con quimibond_sgi 57.7.0 o posterior.
"""
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', 'quimibond_sgi', 'models'))
import sgi_domain_refs  # noqa: E402  (módulo puro del núcleo)

ACT_FIELDS = (
    'process_id', 'step', 'number', 'sequence', 'name', 'description', 'odoo_ref', 'note',
    'responsible_role', 'stage_id', 'block', 'value_class', 'measure_cadence', 'role_ids',
    'input_ids', 'output_deliverable_ids', 'instruction_id', 'related_procedure_id',
    'format_document_ids', 'measure_method', 'measure_proxy_activity_id',
    'measure_deliverable_id', 'measure_justification', 'sample_cadence', 'measure_model_name',
    'measure_domain', 'measure_date_field', 'measure_user_field', 'automation_level_current',
    'automation_level_target', 'automation_method', 'check_against', 'how_steps',
    'done_criteria', 'on_fail', 'exec_channel', 'odoo_menu_id', 'external_system',
    'location_id', 'workcenter_id', 'place_note', 'due_weekday', 'due_business_day',
    'due_month', 'due_day', 'norm_clause_ids', 'fiscal_position_ids', 'sale_team_ids',
    'legacy_number', 'company_id')
ACTIVITY_TEXT = ('name', 'description', 'odoo_ref', 'note', 'responsible_role',
                 'check_against', 'how_steps', 'done_criteria', 'on_fail')
INDICATOR_FIELDS = (
    'name', 'uom', 'direction', 'target_objective', 'target_acceptable',
    'frequency', 'calc_mode', 'monthly_budget', 'nc_on_red', 'formula', 'source',
    'baseline_value', 'target_date', 'status', 'measure_from', 'critical',
    'baseline_date', 'range_min', 'range_max', 'range_tolerance')
TERM_FIELDS = ('date_field', 'aggregation', 'field_name', 'field_name_2',
               'delta_unit', 'delta_op', 'delta_value', 'factor', 'window')

# Etiqueta de los puntos de la norma (sgi.norm.clause.short_label), por id.
_CLAUSES = {}
for base, norm, codes in (
        (1, '9001', ['4.1', '4.2', '4.3', '4.4', '5.1', '5.2', '5.3', '6.1', '6.2', '6.3',
                     '7.1', '7.2', '7.3', '7.4', '7.5', '8.1', '8.2', '8.3', '8.4', '8.5',
                     '8.6', '8.7', '9.1', '9.2', '9.3', '10.1', '10.2', '10.3']),
        (29, '14001', ['4.1', '4.2', '4.3', '4.4', '5.1', '5.2', '5.3', '6.1', '6.2', '7.1',
                       '7.2', '7.3', '7.4', '7.5', '8.1', '8.2', '9.1', '9.2', '9.3', '10.1',
                       '10.2', '10.3']),
        (51, '45001', ['4.1', '4.2', '4.3', '4.4', '5.1', '5.2', '5.3', '5.4', '6.1', '6.2',
                       '7.1', '7.2', '7.3', '7.4', '7.5', '8.1', '8.2', '9.1', '9.2', '9.3',
                       '10.1', '10.2', '10.3'])):
    for offset, code in enumerate(codes):
        _CLAUSES[base + offset] = "%s %s" % (norm, code)
for n in range(1, 11):
    _CLAUSES[73 + n] = "CLI-%02d" % n

# Relación a la que apunta cada campo de los filtros del mapa (última parte
# de la ruta). Un campo con entero que no esté aquí ni en _NOT_RELATIONAL
# detiene el script: hay que decidir.
_COMODEL = {
    'company_id': 'res.company', 'company_ids': 'res.company',
    'journal_id': 'account.journal', 'partner_id': 'res.partner',
    'commercial_partner_id': 'res.partner', 'categ_id': 'product.category',
    'service_type_id': 'fleet.service.type', 'maintenance_team_id': 'maintenance.team',
    'sgi_checklist_template_id': 'sgi.checklist.template',
    'picking_type_id': 'stock.picking.type', 'create_uid': 'res.users', 'user_id': 'res.users',
    'sgi_source_id': 'sgi.alert.source', 'point_id': 'quality.point',
    'product_uom': 'uom.uom', 'product_uom_id': 'uom.uom',
    'location_dest_id': 'stock.location', 'location_id': 'stock.location',
    'survey_id': 'survey.survey',
}
_BY_MODEL = {
    ('approval.request', 'category_id'): 'approval.category',
    ('maintenance.equipment', 'category_id'): 'maintenance.equipment.category',
    ('helpdesk.ticket', 'team_id'): 'helpdesk.team',
    ('quality.alert', 'team_id'): 'quality.alert.team',
    ('quality.check', 'team_id'): 'quality.alert.team',
    ('sale.order', 'team_id'): 'crm.team',
    ('helpdesk.ticket', 'stage_id'): 'helpdesk.stage',
    ('quality.alert', 'stage_id'): 'quality.alert.stage',
}
_NOT_RELATIONAL = {'amount', 'amount_total', 'close_hours', 'file_size', 'duration', 'credit',
                   'credit_limit', 'customer_rank', 'supplier_rank', 'qty_produced', 'quantity',
                   'product_min_qty'}


def _load(folder, pattern):
    records = {}
    for path in sorted(glob.glob(os.path.join(folder, pattern))):
        for record in json.load(open(path, encoding='utf-8'))['records']:
            records[record['id']] = record
    return records


def _clean(text):
    return ' '.join((text or '').split())


class Generator:
    def __init__(self, folder):
        self.folder = folder
        self.acts = _load(folder, 'act_*.json')
        self.roles = _load(folder, 'role_*.json')
        self.inputs = _load(folder, 'inp_*.json')
        self.dels = _load(folder, 'del_*.json')
        self.procs = _load(folder, 'proc.json')
        self.docs = _load(folder, 'docs_*.json')
        self.inds = _load(folder, 'ind.json')
        read = lambda name: json.load(open(os.path.join(folder, name), encoding='utf-8'))  # noqa: E731
        self.jobs = {int(k): v for k, v in read('jobs.json').items()}
        self.families = read('families.json')
        self.owners = {int(k): v for k, v in read('owners.json').items()}
        self.menus = {int(k): v for k, v in read('menus.json').items()}
        self.manual = read('manual.json')
        self.catalog = read('refs_catalogo.json')
        self.review = []

    # ------------------------------------------------------------------
    def note(self, where, message):
        entry = "%s: %s" % (where, message)
        if entry not in self.review:
            self.review.append(entry)

    def ref(self, model, rec_id):
        ref = self.catalog.get("%s:%d" % (model, rec_id))
        if ref is None:
            raise SystemExit("Falta en refs_catalogo.json: %s:%d" % (model, rec_id))
        if 'id' in ref and 'xmlid' not in ref and 'key' not in ref:
            self.note(model, "%s (id %d) sin llave única: se exporta por id + nombre y solo "
                             "se resuelve en una copia de producción" % (ref['name'], rec_id))
        return dict(ref)

    def domain(self, text, model, marks):
        if not text or not model:
            return text or False

        def decide(path, value):
            last = path.split('.')[-1]
            if last in _NOT_RELATIONAL:
                return None
            comodel = _BY_MODEL.get((model, last)) or _COMODEL.get(last)
            if not comodel:
                raise SystemExit("¿%s.%s (%d) es relación? Agrégalo a _COMODEL o a "
                                 "_NOT_RELATIONAL." % (model, path, value))
            key = (comodel, value)
            if key not in marks['by']:
                name = 'r%d' % (len(marks['by']) + 1)
                marks['by'][key] = name
                marks['refs'][name] = self.ref(comodel, value)
            return marks['by'][key]
        return sgi_domain_refs.templatize(text, decide)[0]

    def doc(self, value, where):
        if not value:
            return None
        doc_id = value[0] if isinstance(value, list) else value
        doc = self.docs.get(doc_id)
        if not doc or not doc.get('sgi_code'):
            self.note(where, "documento %s sin clave: no se exporta" % doc_id)
            return None
        return doc['sgi_code']

    def job(self, job_id):
        return _clean(self.jobs[job_id]['name'])

    # ------------------------------------------------------------------
    def run(self):
        acts = [a for a in self.acts.values() if a.get('company_id', [1])[0] == 1]
        deliverables = sorted((d for d in self.dels.values() if d['active']),
                              key=lambda d: d['code'])
        families = sorted(self.families, key=lambda f: f['code'])
        procs = sorted(self.procs.values(), key=lambda p: p['code'])
        objectives = sorted(self.manual['objectives'], key=lambda o: o['id'])
        plans = sorted(self.manual['control_plans'], key=lambda c: c['id'])
        inds = sorted((i for i in self.inds.values() if i['active']), key=lambda i: i['code'])
        payload = {
            'meta': {},
            'tolerant': True,
            'archive_missing': True,
            'families': [{'code': f['code'], 'name': f['name'],
                          'jobs': sorted(self.job(j) for j in f['job_ids'])} for f in families],
            'deliverables': [self.deliverable(d) for d in deliverables],
            'objectives': [{'name': o['name'], 'description': o['description'] or None,
                            'target_year': o['target_year'] or None} for o in objectives],
            'processes': [self.process(p, [a for a in acts if a['process_id'][0] == p['id']])
                          for p in procs],
            'control_plans': [{'name': c['name'], 'phase': c['phase'],
                               'revision': c['revision'] or None, 'notes': c['notes'] or None,
                               'document': None, 'partner': None} for c in plans],
            'indicators': [self.indicator(i, objectives) for i in inds],
        }
        stages = {a['stage_id'][0] for a in acts if a['stage_id']}
        payload['meta'] = {
            'format': 'quimibond_sgi.payload',
            'version': 1,
            'generated': '2026-09-29 (lectura de producción por MCP)',
            'generator': 'quimibond_sgi_mapa/tools/generar_mapa.py',
            'company': 'PRODUCTORA DE NO TEJIDOS QUIMIBOND',
            'counts': {
                'processes': len(procs), 'stages': len(stages), 'activities': len(acts),
                'roles': sum(len(a['role_ids']) for a in acts),
                'inputs': sum(len(a['input_ids']) for a in acts),
                'deliverables': len(deliverables), 'families': len(families),
                'indicators': len(inds),
                'terms': sum(len(i['terms']) for i in payload['indicators']),
                'objectives': len(objectives), 'control_plans': len(plans),
            },
            'review': self.review,
        }
        return payload

    def deliverable(self, d):
        model = d['odoo_model_name'] or None
        marks = {'by': {}, 'refs': {}}
        out = {'code': d['code'], 'name': d['name']}
        out['document'] = self.doc(d['document_id'], "entregable %s" % d['code'])
        out['model'] = model
        out['domain'] = self.domain(d['measure_domain'], model, marks) or '[]'
        out['date_field'] = d['measure_date_field'] or None
        out['user_field'] = d['measure_user_field'] or None
        out['complete_domain'] = self.domain(d['complete_domain'], model, marks) or None
        out['complete_criteria'] = d['complete_criteria'] or None
        out['acceptance_criteria'] = d['acceptance_criteria'] or None
        out['require_signed'] = bool(d.get('require_signed'))
        survey = d.get('survey_id')
        out['survey'] = self.ref('survey.survey', survey[0]) if survey else None
        out['boundary'] = d.get('boundary') or None
        if marks['refs']:
            out['refs'] = marks['refs']
        return out

    def process(self, p, acts):
        out = {'code': p['code'], 'name': p['name']}
        for name in ('purpose', 'scope', 'env_aspects'):
            if p.get(name):
                out[name] = p[name]
        out['type'] = p['process_type']
        owner = p['owner_id'] and self.owners.get(p['owner_id'][0])
        if owner:
            out['owner_job'] = self.job(owner)
        out['parent'] = None
        if p['parent_id']:
            out['parent'] = self.procs[p['parent_id'][0]]['code']
        if p['state'] == 'vigente':
            out['publish'] = True
        elif p['state'] in ('borrador', 'piloto'):
            out['state'] = p['state']
        acts = sorted(acts, key=lambda a: (a['stage_id'][1] if a['stage_id'] else '',
                                           a['stage_id'][0] if a['stage_id'] else 0,
                                           a['sequence'], a['step'], a['id']))
        out['activities'] = [self.activity(a) for a in acts]
        return out

    def activity(self, a):
        where = "actividad %s" % a['number']
        out = {'number': a['number'], 'sequence': a['sequence']}
        if a['stage_id']:
            out['stage'] = a['stage_id'][1]
        for name in ACTIVITY_TEXT:
            if a.get(name):
                out[name] = a[name]
        if a['block']:
            out['block'] = a['block']
        if a['value_class']:
            out['value_class'] = a['value_class']
        if a['measure_cadence']:
            out['cadence'] = a['measure_cadence']
        roles = sorted((self.roles[r] for r in a['role_ids']), key=lambda r: (r['sequence'], r['id']))
        out['roles'] = [self.role(r) for r in roles]
        inputs = []
        for input_id in a['input_ids']:
            line = self.inputs[input_id]
            deliverable = self.dels[line['deliverable_id'][0]]
            marks = {'by': {}, 'refs': {}}
            entry = {'code': deliverable['code'], 'days': line['max_days']}
            if line['applies_domain']:
                entry['applies_domain'] = self.domain(
                    line['applies_domain'], deliverable['odoo_model_name'] or None, marks)
            for src, dst in (('applies_note', 'applies_note'), ('match_path', 'match'),
                             ('due_field', 'due_field'), ('offset_days', 'offset_days')):
                if line[src]:
                    entry[dst] = line[src]
            if marks['refs']:
                entry['refs'] = marks['refs']
            inputs.append(entry)
        out['inputs'] = inputs
        out['outputs'] = sorted(self.dels[d]['code'] for d in a['output_deliverable_ids'])
        if a['exec_channel']:
            out['where'] = self.where(a, where)
        due = {}
        if a['due_weekday']:
            due['weekday'] = int(a['due_weekday'])
        if a['due_business_day']:
            due['business_day'] = a['due_business_day']
        if a['due_month'] and a['due_day']:
            due.update(month=int(a['due_month']), day=a['due_day'])
        if due:
            out['due'] = due
        out['measure'] = self.measure(a)
        if a['measure_method'] == 'odoo':
            raise SystemExit("%s: medición «odoo» (evidencia) no prevista en el generador." % where)
        out['automation'] = {'current': a['automation_level_current'] or None,
                             'target': a['automation_level_target'] or None,
                             'method': a['automation_method'] or None}
        instruction = self.doc(a['instruction_id'], where)
        if instruction:
            out['instruction'] = instruction
        procedure = self.doc(a['related_procedure_id'], where)
        if procedure:
            out['related_procedure'] = procedure
        formats = [self.doc(d, where) for d in a['format_document_ids']]
        if formats:
            out['formats'] = sorted(code for code in formats if code)
        if a['norm_clause_ids']:
            out['complies_with'] = sorted(_CLAUSES[c] for c in a['norm_clause_ids'])
        if a['fiscal_position_ids']:
            out['markets'] = [self.ref('account.fiscal.position', i)
                              for i in sorted(a['fiscal_position_ids'])]
        if a['sale_team_ids']:
            out['teams'] = [self.ref('crm.team', i) for i in sorted(a['sale_team_ids'])]
        return out

    def role(self, r):
        out = {'role': r['role']}
        if r['target_type'] == 'family':
            out['family'] = next(f['code'] for f in self.families if f['name'] == r['family_id'][1]
                                 or ("%s - %s" % (f['code'], f['name'])) == r['family_id'][1])
        elif r['target_type'] == 'relative':
            out['relative'] = r['relative_role']
        else:
            out['job'] = self.job(r['job_id'][0])
        if r['condition']:
            out['condition'] = r['condition']
        if r['after_days']:
            out['after_days'] = r['after_days']
        out['sequence'] = r['sequence']
        return out

    def where(self, a, where):
        out = {'channel': a['exec_channel']}
        if a['odoo_menu_id']:
            xmlid = self.menus.get(a['odoo_menu_id'][0])
            if xmlid:
                out['menu'] = xmlid
            else:
                self.note(where, "el menú %s no tiene XML ID: no se exporta" % a['odoo_menu_id'][0])
        if a['external_system']:
            out['external_system'] = a['external_system']
        if a['location_id']:
            out['location'] = a['location_id'][1]
        if a['workcenter_id']:
            raise SystemExit("%s: centro de trabajo no previsto en el generador." % where)
        if a['place_note']:
            out['place'] = a['place_note']
        return out

    def measure(self, a):
        out = {'method': a['measure_method']}
        if a['measure_method'] == 'entregable' and a['measure_deliverable_id']:
            out['deliverable'] = self.dels[a['measure_deliverable_id'][0]]['code']
        elif a['measure_method'] == 'consecuencia' and a['measure_proxy_activity_id']:
            proxy = self.acts[a['measure_proxy_activity_id'][0]]
            out['proxy'] = proxy['number'] if proxy['process_id'][0] == a['process_id'][0] else \
                "%s:%s" % (self.procs[proxy['process_id'][0]]['code'], proxy['number'])
        elif a['measure_method'] == 'no_aplica':
            out['justification'] = a['measure_justification'] or None
        elif a['measure_method'] == 'muestreo':
            out['sample_cadence'] = a['sample_cadence']
        return out

    def indicator(self, i, objectives):
        out = {'code': i['code'], 'name': i['name']}
        for name in INDICATOR_FIELDS:
            if name == 'name':
                continue
            value = i.get(name)
            if value in (False, None, '') or (isinstance(value, (int, float))
                                              and not isinstance(value, bool) and value == 0):
                continue
            out[name] = value
        out['process'] = self.procs[i['process_id'][0]]['code'] if i['process_id'] else None
        if i['activity_id']:
            act = self.acts[i['activity_id'][0]]
            out['activity'] = "%s:%s" % (self.procs[act['process_id'][0]]['code'], act['number'])
        if i['deliverable_id']:
            out['deliverable'] = self.dels[i['deliverable_id'][0]]['code']
        if i['objective_id']:
            out['objective'] = next(o['name'] for o in objectives if o['id'] == i['objective_id'][0])
        terms = []
        for t in sorted((t for t in self.manual['terms'] if t['indicator_id'] == i['id']),
                        key=lambda t: (t['role'], t['id'])):
            marks = {'by': {}, 'refs': {}}
            entry = {'role': t['role'], 'model': t['model'],
                     'domain': self.domain(t['domain'], t['model'], marks)}
            values = dict(date_field=t['date_field'] or None, aggregation=t['aggregation'],
                          field_name=t['field_name'] or None, field_name_2=None,
                          delta_unit='days', delta_op='<=', delta_value=0, factor=t['factor'],
                          window=t['window'])
            for name in TERM_FIELDS:
                entry[name] = values[name]
            if marks['refs']:
                entry['refs'] = marks['refs']
            terms.append(entry)
        out['terms'] = terms
        return out


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    payload = Generator(sys.argv[1]).run()
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=1))
    sys.stdout.write('\n')


if __name__ == '__main__':
    main()
