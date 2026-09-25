# -*- coding: utf-8 -*-
"""Diagramas del SGI en HTML (componente OWL `sgi_diagram`, en
static/src/diagram). Un solo motor dibuja bandas, carriles o una matriz con
cajas y flechas; cada diagrama es solo un método de aquí que devuelve los
datos. Formato:

    {'kind', 'title', 'subtitle', 'layout': 'bands' | 'columns' | 'matrix',
     'lanes': [{'key', 'label', 'items': [{'key', 'model', 'res_id', 'code',
                'name', 'subtitle', 'color', 'avatar', 'meta'}]}],
     'edges': [{'from', 'to', 'label'}],
     'matrix': {'rows', 'cols', 'cells'},  # solo layout matrix
     'legend': [{'color', 'label'}],
     'per_process': bool, 'nav': [{'kind', 'label'}]}

`key` es «modelo,id» y es lo que une cajas y flechas. `color` es un color de
Bootstrap (success, warning, danger, info, muted)."""
from odoo import api, models

DOC_GROUPS = [
    ('gobierno', "Manual y procedimientos", ('miid', 'procedimiento', 'reglamento')),
    ('como', "Instructivos, protocolos y diagramas",
     ('instructivo', 'diagrama', 'protocolo', 'control_operacional', 'metodo_anexo', 'descripcion_puesto')),
    ('registros', "Formatos y registros", ('formato', 'formato_it', 'dat', 'anexo', 'formulario_odoo')),
    ('otros', "Otros", ()),
]

PROCESS_KINDS = [
    ('process_map', "Mapa de procesos"),
    ('process_flow', "Flujo del proceso"),
    ('sipoc', "Tortuga (SIPOC)"),
    ('pdca', "PDCA"),
    ('doc_tree', "Árbol documental"),
    ('kpi_tree', "Indicadores"),
    ('risk_matrix', "Riesgos"),
    ('nc_flow', "No conformidades"),
]

# Etiqueta de cada diagrama (pestañas del componente y validación de la
# vista sgi_diagram). Los de sgi_diagram_iso.py se agregan abajo.
KIND_LABELS = {
    'process_map': "Mapa de procesos",
    'process_flow': "Flujo del proceso",
    'sipoc': "Tortuga (SIPOC)",
    'pdca': "PDCA",
    'doc_tree': "Árbol documental",
    'kpi_tree': "Indicadores",
    'who_does_what': "Quién hace qué",
    'interaction_matrix': "Interacción (4.4)",
    'context_map': "Contexto (4.1 / 4.2)",
    'roles_map': "Roles (5.3)",
    'risk_matrix': "Riesgos (6.1)",
    'legal_matrix': "Cumplimiento legal",
    'calibration_map': "Calibración",
    'competence_matrix': "Competencias",
    'doc_pyramid': "Pirámide documental",
    'emergency_map': "Emergencias",
    'audit_program': "Programa de auditorías",
    'management_review': "Revisión por la dirección",
    'nc_flow': "No conformidades",
}

SEMAPHORE_COLOR = {'verde': 'success', 'amarillo': 'warning', 'rojo': 'danger'}
STATE_COLOR = {'vigente': 'success', 'piloto': 'warning', 'borrador': 'muted', 'obsoleto': 'danger'}


def _key(record):
    return "%s,%d" % (record._name, record.id)


class SgiDiagram(models.AbstractModel):
    _name = 'sgi.diagram'
    _description = "Diagramas del SGI (datos para el componente sgi_diagram)"

    @api.model
    def data(self, kind, res_id=None, params=None):
        method = getattr(self, '_data_%s' % kind, None)
        if not method:
            raise ValueError("Diagrama desconocido: %s" % kind)
        self = self.with_context(sgi_diagram_params=dict(params or {}))
        result = method(int(res_id) if res_id else None)
        result.setdefault('kind', kind)
        result.setdefault('edges', [])
        result.setdefault('legend', [])
        result.setdefault('per_process', False)
        result.setdefault('nav', [])
        return result

    @api.model
    def _kinds(self):
        """Diagramas disponibles: todo método `_data_<kind>`."""
        return {name[6:] for name in dir(self) if name.startswith('_data_')}

    @api.model
    def catalog(self):
        """Etiqueta de cada diagrama, para las pestañas del componente."""
        return [{'kind': k, 'label': KIND_LABELS.get(k, k)} for k in sorted(self._kinds())]

    def _param(self, name, default=None):
        return self.env.context.get('sgi_diagram_params', {}).get(name, default) or default

    @api.model
    def processes(self):
        """Procesos activos para el selector del diagrama."""
        return [{'id': p.id, 'code': p.code or '', 'name': p.name or ''}
                for p in self.env['sgi.process'].search([('active', '=', True)], order='process_type, code')]

    # ------------------------------------------------------------------
    def _process_item(self, process, subtitle=None, meta=None):
        return {
            'key': _key(process), 'model': 'sgi.process', 'res_id': process.id,
            'code': process.code or '', 'name': process.name or '',
            'subtitle': subtitle if subtitle is not None else (process.owner_id.name or ''),
            'color': SEMAPHORE_COLOR.get(process.health, 'warning'),
            'avatar': '/web/image/hr.employee/%d/avatar_128' % process.owner_id.id if process.owner_id else '',
            'meta': meta or [],
        }

    def _activity_item(self, activity, with_process=False):
        jobs = ", ".join(activity.responsible_job_ids.mapped('name'))
        code = activity.number or activity.legacy_number or ''
        if with_process:
            code = "%s · %s" % (activity.process_id.code or '', code)
        return {
            'key': _key(activity), 'model': 'sgi.process.activity', 'res_id': activity.id,
            'code': code, 'name': activity.name or '', 'subtitle': jobs,
            'color': SEMAPHORE_COLOR.get(activity.measure_state, 'muted'),
            'avatar': '', 'meta': [],
        }

    def _process_nav(self, process):
        return {
            'per_process': True,
            'process_id': process.id,
            'nav': [{'kind': k, 'label': label} for k, label in PROCESS_KINDS],
        }

    def _process(self, res_id):
        process = self.env['sgi.process'].browse(res_id).exists() if res_id else self.env['sgi.process']
        if not process:
            process = self.env['sgi.process'].search([('active', '=', True)], order='process_type, code', limit=1)
        return process

    # ------------------------------------------------------------------
    def _data_process_map(self, res_id=None):
        Process = self.env['sgi.process']
        labels = dict(Process._fields['process_type'].selection)
        order = ['estrategico', 'cop', 'soporte']
        processes = Process.search([('active', '=', True)], order='process_type, code')
        flows = self.env['sgi.process.flow'].search([
            ('from_process_id', 'in', processes.ids), ('to_process_id', 'in', processes.ids)])
        in_count, out_count = {}, {}
        for flow in flows:
            out_count[flow.from_process_id.id] = out_count.get(flow.from_process_id.id, 0) + 1
            in_count[flow.to_process_id.id] = in_count.get(flow.to_process_id.id, 0) + 1
        by_type = {}
        for process in processes:
            item = self._process_item(process, meta=[
                {'icon': 'fa-sign-in', 'label': "Entradas", 'value': in_count.get(process.id, 0)},
                {'icon': 'fa-sign-out', 'label': "Salidas", 'value': out_count.get(process.id, 0)}])
            by_type.setdefault(process.process_type or 'soporte', []).append(item)
        keys = [k for k in order if k in by_type] + sorted(k for k in by_type if k not in order)
        return {
            'title': "Mapa de procesos",
            'subtitle': "Pasa el mouse o da clic en un proceso para ver con quién se conecta · doble clic abre la ficha",
            'layout': 'bands',
            'lanes': [{'key': k, 'label': labels.get(k, k), 'items': by_type[k]} for k in keys],
            'edges': [{'from': _key(f.from_process_id), 'to': _key(f.to_process_id), 'label': f.name or ''}
                      for f in flows],
            'legend': [{'color': 'success', 'label': "en cumplimiento"},
                       {'color': 'warning', 'label': "con pendientes"},
                       {'color': 'danger', 'label': "con problemas"}],
            'selected': "sgi.process,%d" % res_id if res_id else None,
        }

    def _data_process_flow(self, res_id=None):
        process = self._process(res_id)
        if not process:
            return {'title': "Flujo del proceso", 'layout': 'columns', 'lanes': []}
        Activity = self.env['sgi.process.activity']
        activities = Activity.search([('process_id', '=', process.id), ('active', '=', True)],
                                     order='sequence, id')
        stages = process.stage_ids.sorted(lambda s: (s.sequence, s.id)) if 'stage_ids' in process._fields \
            else activities.stage_id.sorted(lambda s: (s.sequence, s.id))
        lanes, by_stage = [], {}
        for activity in activities:
            by_stage.setdefault(activity.stage_id.id, []).append(self._activity_item(activity))
        links = self.env['sgi.activity.link'].search([
            '|', ('from_activity_id', 'in', activities.ids), ('to_activity_id', 'in', activities.ids)])
        incoming = links.filtered(lambda l: l.to_activity_id in activities and l.from_activity_id not in activities)
        outgoing = links.filtered(lambda l: l.from_activity_id in activities and l.to_activity_id not in activities)
        if incoming:
            lanes.append({'key': 'in', 'label': "Recibe de otros procesos",
                          'items': [self._activity_item(a, with_process=True)
                                    for a in incoming.from_activity_id.sorted(lambda a: (a.process_id.code or '', a.sequence))]})
        for stage in stages:
            if by_stage.get(stage.id):
                lanes.append({'key': 'stage_%d' % stage.id,
                              'label': "%s %s" % (stage.code or '', stage.name or '') if stage.code else stage.name,
                              'items': by_stage.pop(stage.id)})
        if by_stage.get(False):
            lanes.append({'key': 'stage_none', 'label': "Sin etapa", 'items': by_stage.pop(False)})
        for stage_id, items in by_stage.items():  # etapas de otro proceso (dato raro): que no se pierdan
            lanes.append({'key': 'stage_%s' % stage_id, 'label': "Otra etapa", 'items': items})
        if outgoing:
            lanes.append({'key': 'out', 'label': "Entrega a otros procesos",
                          'items': [self._activity_item(a, with_process=True)
                                    for a in outgoing.to_activity_id.sorted(lambda a: (a.process_id.code or '', a.sequence))]})
        result = {
            'title': "Flujo del proceso — %s %s" % (process.code or '', process.name or ''),
            'subtitle': "Una columna por etapa; las flechas son los eslabones con su entregable",
            'layout': 'columns',
            'lanes': lanes,
            'edges': [{'from': _key(l.from_activity_id), 'to': _key(l.to_activity_id), 'label': l.name or ''}
                      for l in links],
            'legend': [{'color': 'success', 'label': "en cumplimiento"},
                       {'color': 'danger', 'label': "sin evidencia"},
                       {'color': 'muted', 'label': "sin medición automática"}],
        }
        result.update(self._process_nav(process))
        return result

    def _data_sipoc(self, res_id=None):
        process = self._process(res_id)
        if not process:
            return {'title': "Tortuga (SIPOC)", 'layout': 'columns', 'lanes': []}
        Flow = self.env['sgi.process.flow']
        flows_in = Flow.search([('to_process_id', '=', process.id)])
        flows_out = Flow.search([('from_process_id', '=', process.id)])

        def flow_item(flow):
            return {'key': _key(flow), 'model': 'sgi.process.flow', 'res_id': flow.id,
                    'code': '', 'name': flow.name or '', 'color': 'info', 'avatar': '', 'meta': [],
                    'subtitle': flow.document_id.sgi_code or flow.odoo_model_id.name or ''}

        activities = self.env['sgi.process.activity'].search_count(
            [('process_id', '=', process.id), ('active', '=', True)])
        indicators = self.env['sgi.indicator'].search_count([('process_id', '=', process.id)])
        jobs = self.env['sgi.activity.role'].search([('process_id', '=', process.id)]).job_id
        center = self._process_item(process, meta=[
            {'icon': 'fa-list-ol', 'label': "Actividades", 'value': activities},
            {'icon': 'fa-tachometer', 'label': "Indicadores", 'value': indicators},
            {'icon': 'fa-users', 'label': "Puestos", 'value': len(jobs)}])
        edges = []
        for flow in flows_in:
            edges.append({'from': _key(flow.from_process_id), 'to': _key(flow), 'label': ''})
            edges.append({'from': _key(flow), 'to': _key(process), 'label': ''})
        for flow in flows_out:
            edges.append({'from': _key(process), 'to': _key(flow), 'label': ''})
            edges.append({'from': _key(flow), 'to': _key(flow.to_process_id), 'label': ''})
        result = {
            'title': "Tortuga — %s %s" % (process.code or '', process.name or ''),
            'subtitle': "Proveedores → entradas → proceso → salidas → clientes (flujos del mapa)",
            'layout': 'columns',
            'lanes': [
                {'key': 'suppliers', 'label': "Proveedores (procesos)",
                 'items': [self._process_item(p) for p in flows_in.from_process_id.sorted('code')]},
                {'key': 'inputs', 'label': "Entradas", 'items': [flow_item(f) for f in flows_in]},
                {'key': 'process', 'label': "Proceso", 'items': [center]},
                {'key': 'outputs', 'label': "Salidas", 'items': [flow_item(f) for f in flows_out]},
                {'key': 'customers', 'label': "Clientes (procesos)",
                 'items': [self._process_item(p) for p in flows_out.to_process_id.sorted('code')]},
            ],
            'edges': edges,
            'show_all_edges': True,
        }
        result.update(self._process_nav(process))
        return result

    def _data_doc_tree(self, res_id=None):
        process = self._process(res_id)
        if not process:
            return {'title': "Árbol documental", 'layout': 'columns', 'lanes': []}
        Doc = self.env['documents.document']
        docs = Doc.search([('sgi_process_id', '=', process.id), ('sgi_is_controlled', '=', True),
                           ('sgi_state', 'in', ('borrador', 'piloto', 'vigente'))],
                          order='sgi_doc_type, sgi_code')
        pending = {}
        if docs:
            for doc, count in self.env['sgi.document.ack']._read_group(
                    [('document_id', 'in', docs.ids), ('state', '=', 'pendiente')],
                    ['document_id'], ['__count']):
                pending[doc.id] = count
        state_labels = dict(Doc._fields['sgi_state'].selection)
        groups = {key: [] for key, _label, _types in DOC_GROUPS}
        type_of = {}
        for key, _label, types in DOC_GROUPS:
            for t in types:
                type_of[t] = key
        for doc in docs:
            meta = [{'icon': 'fa-pencil-square-o', 'label': "Acuses pendientes", 'value': pending[doc.id]}] \
                if pending.get(doc.id) else []
            groups[type_of.get(doc.sgi_doc_type, 'otros')].append({
                'key': _key(doc), 'model': 'documents.document', 'res_id': doc.id,
                'code': doc.sgi_code or '', 'name': doc.name or '',
                'subtitle': "Rev. %02d · %s" % (doc.sgi_revision or 0, state_labels.get(doc.sgi_state, '')),
                'color': STATE_COLOR.get(doc.sgi_state, 'muted'), 'avatar': '', 'meta': meta})
        keys = set(_key(d) for d in docs)
        result = {
            'title': "Árbol documental — %s %s" % (process.code or '', process.name or ''),
            'subtitle': "Procedimiento → instructivos → formatos; la flecha une cada documento con su padre",
            'layout': 'columns',
            'lanes': [{'key': key, 'label': label, 'items': groups[key]}
                      for key, label, _types in DOC_GROUPS if groups[key]],
            'edges': [{'from': _key(d.sgi_parent_document_id), 'to': _key(d), 'label': ''}
                      for d in docs if d.sgi_parent_document_id and _key(d.sgi_parent_document_id) in keys],
            'legend': [{'color': 'success', 'label': "vigente"}, {'color': 'warning', 'label': "piloto"},
                       {'color': 'muted', 'label': "borrador"}],
            'show_all_edges': True,
        }
        result.update(self._process_nav(process))
        return result

    def _data_kpi_tree(self, res_id=None):
        Indicator = self.env['sgi.indicator']
        domain = [('active', '=', True)]
        process = self._process(res_id) if res_id else self.env['sgi.process']
        if process:
            domain.append(('process_id', '=', process.id))
        indicators = Indicator.search(domain, order='process_id, code')
        objectives = indicators.objective_id.sorted('name')
        processes = indicators.process_id.sorted('code')
        status_labels = dict(Indicator._fields['status'].selection) if 'status' in Indicator._fields else {}
        items = []
        for ind in indicators:
            value = ("%s %s" % (('%g' % ind.last_value) if ind.last_value else '—', ind.uom or '')).strip()
            status = status_labels.get(getattr(ind, 'status', None), '')
            items.append({
                'key': _key(ind), 'model': 'sgi.indicator', 'res_id': ind.id,
                'code': ind.code or '', 'name': ind.name or '',
                'subtitle': " · ".join(x for x in (status, "Último: %s" % value) if x),
                'color': SEMAPHORE_COLOR.get(ind.last_semaphore, 'muted'), 'avatar': '', 'meta': []})
        edges = [{'from': _key(i.objective_id), 'to': _key(i), 'label': ''} for i in indicators if i.objective_id]
        edges += [{'from': _key(i), 'to': _key(i.process_id), 'label': ''} for i in indicators if i.process_id]
        result = {
            'title': "Indicadores — %s %s" % (process.code, process.name) if process else "Árbol de indicadores",
            'subtitle': "Objetivo → indicador → proceso; el color es el último semáforo",
            'layout': 'columns',
            'lanes': [
                {'key': 'objectives', 'label': "Objetivos", 'items': [{
                    'key': _key(o), 'model': 'sgi.objective', 'res_id': o.id, 'code': '', 'name': o.name or '',
                    'subtitle': o.policy_id.name or '' if 'policy_id' in o._fields else '',
                    'color': 'info', 'avatar': '', 'meta': []} for o in objectives]},
                {'key': 'indicators', 'label': "Indicadores", 'items': items},
                {'key': 'processes', 'label': "Procesos", 'items': [self._process_item(p) for p in processes]},
            ],
            'edges': edges,
            'legend': [{'color': 'success', 'label': "verde"}, {'color': 'warning', 'label': "amarillo"},
                       {'color': 'danger', 'label': "rojo"}, {'color': 'muted', 'label': "sin medición"}],
            'show_all_edges': True,
        }
        if process:
            result.update(self._process_nav(process))
        return result

    def _data_who_does_what(self, res_id=None):
        Role = self.env['sgi.activity.role']
        processes = self.env['sgi.process'].search([('active', '=', True)], order='process_type, code')
        roles = Role.search([('process_id', 'in', processes.ids), ('activity_id.active', '=', True)])
        weight = {'ejecuta': 3, 'aprueba': 2, 'participa': 1, 'informa': 1, 'escala': 1}
        cells, jobs = {}, self.env['hr.job']
        for role in roles:
            targets = role.job_id or role.family_id.job_ids
            for job in targets:
                jobs |= job
                cell = cells.setdefault(job.id, {}).setdefault(role.process_id.id, {'n': 0, 'w': 0})
                cell['n'] += 1
                cell['w'] = max(cell['w'], weight.get(role.role, 1))
        rows = [{'key': _key(j), 'model': 'hr.job', 'res_id': j.id, 'label': j.name or ''}
                for j in jobs.sorted('name')]
        cols = [{'key': _key(p), 'model': 'sgi.process', 'res_id': p.id, 'label': p.code or '',
                 'title': p.name or ''} for p in processes]
        matrix_cells = {}
        for job in jobs:
            for process in processes:
                cell = cells.get(job.id, {}).get(process.id)
                if cell:
                    matrix_cells.setdefault(_key(job), {})[_key(process)] = {
                        'value': cell['n'], 'level': cell['w'],
                        'label': "%d actividad(es)" % cell['n']}
        return {
            'title': "Quién hace qué",
            'subtitle': "Puestos contra procesos: número de actividades; el color, el rol más fuerte (ejecuta > aprueba > participa)",
            'layout': 'matrix',
            'lanes': [],
            'matrix': {'rows': rows, 'cols': cols, 'cells': matrix_cells},
            'legend': [{'color': 'success', 'label': "ejecuta"}, {'color': 'warning', 'label': "aprueba"},
                       {'color': 'info', 'label': "participa / se entera / escala"}],
        }
