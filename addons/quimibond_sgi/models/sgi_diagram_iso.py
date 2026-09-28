# -*- coding: utf-8 -*-
"""Diagramas que pide o supone la norma (ISO 9001 / 14001 / 45001, SGI
integrado), sobre el mismo motor de sgi_diagram.py. Uno por cláusula:

  4.1 / 4.2  context_map       partes interesadas → procesos, riesgos, requisitos
  4.4        interaction_matrix procesos × procesos (flujos entre ellos)
  4.4 (PDCA) pdca               planear · hacer · verificar · actuar de un proceso
  5.2 / 6.2  kpi_tree           política → objetivos → indicadores → procesos
  5.3        roles_map          dueños → procesos → puestos que ejecutan
  6.1        risk_matrix        probabilidad × impacto por instrumento (RyO, IPER,
                                ambiental, patrimonial)
  6.1.3/9.1  legal_matrix       requisitos legales por sistema × cumplimiento
  7.1.5      calibration_map    equipos de medición por vigencia de calibración
  7.2        competence_matrix  puestos × tipos de competencia (brechas)
  7.5        doc_pyramid        pirámide documental por proceso
  8.2 (45001) emergency_map     escenarios → simulacros → acciones
  9.2        audit_program      programa anual de auditorías por mes
  9.3        management_review  entradas → revisión → acuerdos
  10.2       nc_flow            no conformidades por fase del CAPA
"""
from datetime import timedelta

from odoo import fields, models

from .sgi_diagram import SEMAPHORE_COLOR, _key

RISK_INSTRUMENTS = [('ryo', "Riesgos y oportunidades"), ('iper', "IPER (SST)"),
                    ('ambiental', "Aspectos ambientales"), ('patrimonial', "Patrimonial")]
DOC_LEVELS = [
    ('nivel1', "Nivel 1 · Manual, política y reglamentos", ('miid', 'reglamento')),
    ('nivel2', "Nivel 2 · Procedimientos", ('procedimiento',)),
    ('nivel3', "Nivel 3 · Instructivos, protocolos y diagramas",
     ('instructivo', 'diagrama', 'protocolo', 'control_operacional', 'metodo_anexo', 'descripcion_puesto')),
    ('nivel4', "Nivel 4 · Formatos y registros", ('formato', 'formato_it', 'dat', 'anexo', 'formulario_odoo')),
]
MONTHS = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto",
          "Septiembre", "Octubre", "Noviembre", "Diciembre"]
STATE_COLOR = {'pendiente': 'muted', 'creada': 'warning', 'cerrada': 'success',
               'borrador': 'muted', 'planificada': 'info', 'en_ejecucion': 'warning', 'informe': 'warning'}


def _short(text, n=110):
    text = (text or '').strip().replace('\n', ' ')
    return text if len(text) <= n else text[:n - 1] + '…'


class SgiDiagramIso(models.AbstractModel):
    _inherit = 'sgi.diagram'

    # ---- helpers -------------------------------------------------------
    def _item(self, record, code='', name=None, subtitle='', color='muted', meta=None, action=None):
        item = {
            'key': _key(record), 'model': record._name, 'res_id': record.id,
            'code': code or '', 'name': name if name is not None else (record.display_name or ''),
            'subtitle': subtitle or '', 'color': color, 'avatar': '', 'meta': meta or [],
        }
        if action:
            item['action'] = action
        return item

    def _virtual(self, key, name, subtitle='', color='muted', meta=None, action=None, code=''):
        """Caja sin registro propio (un conteo, un resumen): abre `action`."""
        item = {'key': key, 'model': '', 'res_id': 0, 'code': code, 'name': name,
                'subtitle': subtitle, 'color': color, 'avatar': '', 'meta': meta or []}
        if action:
            item['action'] = action
        return item

    @staticmethod
    def _list_action(name, model, domain, views=('list', 'form')):
        return {'type': 'ir.actions.act_window', 'name': name, 'res_model': model,
                'domain': domain, 'views': [[False, v] for v in views], 'target': 'current'}

    @staticmethod
    def _cell(value, level, label, action=None):
        cell = {'value': value, 'level': level, 'label': label}
        if action:
            cell['action'] = action
        return cell

    # ---- 4.4 interacción de procesos ---------------------------------
    def _data_interaction_matrix(self, res_id=None):
        processes = self.env['sgi.process'].search([('active', '=', True)], order='process_type, code')
        flows = self.env['sgi.process.flow'].search([
            ('from_process_id', 'in', processes.ids), ('to_process_id', 'in', processes.ids)])
        counts = {}
        for flow in flows:
            counts.setdefault(flow.from_process_id.id, {}).setdefault(flow.to_process_id.id, []).append(flow)
        cols = [{'key': _key(p), 'model': 'sgi.process', 'res_id': p.id, 'label': p.code or '',
                 'title': "Recibe: %s" % (p.name or '')} for p in processes]
        rows = [{'key': _key(p), 'model': 'sgi.process', 'res_id': p.id,
                 'label': "%s %s" % (p.code or '', p.name or '')} for p in processes]
        cells = {}
        for src in processes:
            for dst in processes:
                items = counts.get(src.id, {}).get(dst.id)
                if not items:
                    continue
                cells.setdefault(_key(src), {})[_key(dst)] = self._cell(
                    len(items), min(3, len(items)),
                    "%s → %s: %s" % (src.code, dst.code, "; ".join(f.name or '' for f in items)),
                    self._list_action("Flujos %s → %s" % (src.code, dst.code), 'sgi.process.flow',
                                      [('id', 'in', [f.id for f in items])]))
        return {
            'title': "Interacción de procesos (ISO 9001 4.4)",
            'subtitle': "Filas: quien entrega · columnas: quien recibe · celda: cuántos entregables cruzan",
            'layout': 'matrix', 'lanes': [],
            'matrix': {'rows': rows, 'cols': cols, 'cells': cells},
            'legend': [{'color': 'info', 'label': "1 entregable"}, {'color': 'warning', 'label': "2"},
                       {'color': 'success', 'label': "3 o más"}],
        }

    # ---- 4.1 / 4.2 contexto y partes interesadas ---------------------
    def _data_context_map(self, res_id=None):
        Party = self.env['sgi.interested.party']
        parties = Party.search([('active', '=', True)], order='party_type, category, name')
        labels = dict(Party._fields['category'].selection)
        today = fields.Date.context_today(self)
        items, edges = [], []
        for party in parties:
            overdue = party.next_review_date and party.next_review_date < today
            items.append(self._item(
                party, code=labels.get(party.category, ''), name=party.name,
                subtitle=_short(party.needs), color='danger' if overdue else (
                    'success' if party.becomes_requirement else 'info')))
            for process in party.process_ids:
                edges.append({'from': _key(party), 'to': _key(process), 'label': "Proceso relacionado"})
            for risk in party.risk_ids:
                edges.append({'from': _key(party), 'to': _key(risk), 'label': "Riesgo u oportunidad"})
            for legal in party.legal_ids:
                edges.append({'from': _key(party), 'to': _key(legal), 'label': "Requisito"})
        processes = parties.process_ids.sorted('code')
        risks = parties.risk_ids.sorted(lambda r: (r.attention_level or '', r.name or ''))
        legals = parties.legal_ids.sorted('name')
        return {
            'title': "Contexto y partes interesadas (ISO 9001 4.1 / 4.2)",
            'subtitle': "Cada parte, lo que necesita, y a qué procesos, riesgos y requisitos toca · rojo = revisión vencida",
            'layout': 'columns',
            'lanes': [
                {'key': 'parties', 'label': "Partes interesadas", 'items': items},
                {'key': 'processes', 'label': "Procesos", 'items': [self._process_item(p) for p in processes]},
                {'key': 'risks', 'label': "Riesgos y oportunidades", 'items': [
                    self._item(r, code=r.folio or '', name=r.name,
                               subtitle=dict(r._fields['kind'].selection).get(r.kind, ''),
                               color=SEMAPHORE_COLOR.get(r.semaphore, 'muted')) for r in risks]},
                {'key': 'legal', 'label': "Requisitos legales y otros", 'items': [
                    self._item(l, code=l.reference or '', name=l.name,
                               color={'cumple': 'success', 'parcial': 'warning', 'no_cumple': 'danger'}.get(
                                   l.compliance_state, 'muted')) for l in legals]},
            ],
            'edges': edges,
            'legend': [{'color': 'success', 'label': "se vuelve requisito"}, {'color': 'info', 'label': "expectativa"},
                       {'color': 'danger', 'label': "revisión vencida"}],
        }

    # ---- 5.3 roles y responsabilidades --------------------------------
    def _data_roles_map(self, res_id=None):
        processes = self.env['sgi.process'].search([('active', '=', True)], order='process_type, code')
        roles = self.env['sgi.activity.role'].search([
            ('process_id', 'in', processes.ids), ('role', 'in', ('ejecuta', 'aprueba')),
            ('activity_id.active', '=', True)])
        owners = processes.owner_id.sorted('name')
        jobs, edges, seen = self.env['hr.job'], [], set()
        for process in processes:
            if process.owner_id:
                edges.append({'from': _key(process.owner_id), 'to': _key(process), 'label': "Dueño"})
        for role in roles:
            targets = role.job_id or role.family_id.job_ids
            for job in targets:
                jobs |= job
                pair = (role.process_id.id, job.id, role.role)
                if pair in seen:
                    continue
                seen.add(pair)
                edges.append({'from': _key(role.process_id), 'to': _key(job),
                              'label': "Ejecuta" if role.role == 'ejecuta' else "Aprueba"})
        return {
            'title': "Roles, responsabilidades y autoridades (ISO 9001 5.3)",
            'subtitle': "Dueño → proceso → puestos que ejecutan o aprueban · pasa el mouse por un proceso",
            'layout': 'columns',
            'lanes': [
                {'key': 'owners', 'label': "Dueños de proceso", 'items': [
                    {'key': _key(e), 'model': 'hr.employee', 'res_id': e.id, 'code': '', 'name': e.name or '',
                     'subtitle': e.job_id.name or '', 'color': 'info',
                     'avatar': '/web/image/hr.employee/%d/avatar_128' % e.id, 'meta': []} for e in owners]},
                {'key': 'processes', 'label': "Procesos", 'items': [self._process_item(p) for p in processes]},
                {'key': 'jobs', 'label': "Puestos", 'items': [
                    self._item(j, name=j.name or '', subtitle=j.sgi_family_id.name or '',
                               color='success' if j.sgi_execute_count else 'muted')
                    for j in jobs.sorted('name')]},
            ],
            'edges': edges,
        }

    # ---- 6.1 matriz de riesgos ---------------------------------------
    def _data_risk_matrix(self, res_id=None):
        Risk = self.env['sgi.risk']
        instrument = self._param('instrument', 'ryo')
        if instrument not in dict(RISK_INSTRUMENTS):
            instrument = 'ryo'
        scale = ['1', '2', '3'] if instrument == 'iper' else ['1', '2', '3', '4', '5']
        domain = [('instrument', '=', instrument), ('state', '!=', 'cerrado'),
                  ('eval_probability', '!=', False), ('eval_impact', '!=', False)]
        process = self._process(res_id) if res_id else self.env['sgi.process']
        if process:
            domain.append(('process_id', '=', process.id))
        risks = Risk.search(domain)
        cells = {}
        for risk in risks:
            row, col = 'i%s' % risk.eval_impact, 'p%s' % risk.eval_probability
            cells.setdefault(row, {}).setdefault(col, []).append(risk)
        rows = [{'key': 'i%s' % i, 'model': '', 'res_id': 0, 'label': "Impacto %s" % i} for i in reversed(scale)]
        cols = [{'key': 'p%s' % p, 'model': '', 'res_id': 0, 'label': "P%s" % p,
                 'title': "Probabilidad %s" % p} for p in scale]
        matrix = {}
        top = len(scale) * len(scale)
        for row in rows:
            for col in cols:
                group = cells.get(row['key'], {}).get(col['key'], [])
                score = int(row['key'][1:]) * int(col['key'][1:])
                level = 3 if score >= top * 0.6 else (2 if score >= top * 0.35 else 1)
                if not group:
                    continue
                matrix.setdefault(row['key'], {})[col['key']] = self._cell(
                    len(group), level, "; ".join((r.folio or '') + ' ' + (r.name or '') for r in group[:6]),
                    self._list_action("Riesgos %s × %s" % (row['label'], col['label']), 'sgi.risk',
                                      [('id', 'in', [r.id for r in group])]))
        title = "Matriz de riesgos — %s" % dict(RISK_INSTRUMENTS)[instrument]
        result = {
            'title': title + (" — %s" % process.code if process else ''),
            'subtitle': "Probabilidad × impacto de los riesgos abiertos; clic en la celda abre la lista (ISO 9001 6.1, 14001 6.1.2, 45001 6.1.2)",
            'layout': 'matrix', 'lanes': [],
            'matrix': {'rows': rows, 'cols': cols, 'cells': matrix},
            'legend': [{'color': 'success', 'label': "atención inmediata / alta"},
                       {'color': 'warning', 'label': "media"}, {'color': 'info', 'label': "baja"}],
            'param_options': [{'name': 'instrument', 'default': 'ryo',
                               'values': [{'value': k, 'label': v} for k, v in RISK_INSTRUMENTS]}],
        }
        if process:
            result.update(self._process_nav(process))
        return result

    # ---- 6.1.3 / 9.1 cumplimiento legal ------------------------------
    def _data_legal_matrix(self, res_id=None):
        Req = self.env['sgi.legal.requirement']
        systems = Req._fields['system'].selection
        states = Req._fields['compliance_state'].selection
        reqs = Req.search([])
        options = [{'name': 'vista', 'default': 'requisitos',
                    'values': [{'value': 'requisitos', 'label': "Requisitos por sistema"},
                               {'value': 'resumen', 'label': "Resumen sistema × estado"}]}]
        if self._param('vista', 'requisitos') == 'requisitos':
            # Tablero: una banda por sistema y una tarjeta por requisito con su
            # estado de cumplimiento; el resumen numérico queda en la matriz.
            state_labels = dict(states)
            color = {'cumple': 'success', 'parcial': 'warning', 'no_cumple': 'danger'}
            lanes = []
            for key_r, label_r in systems:
                group = reqs.filtered(lambda r: (r.system or 'varios') == key_r)
                if not group:
                    continue
                lanes.append({'key': key_r, 'label': "%s (%d)" % (label_r, len(group)), 'items': [
                    self._item(r, code=r.reference or '', name=r.name or '',
                               subtitle=state_labels.get(r.compliance_state, ''),
                               color=color.get(r.compliance_state, 'muted'))
                    for r in group.sorted(lambda r: (r.compliance_state or '', r.name or ''))]})
            return {
                'title': "Requisitos legales y otros (ISO 9001 6.1.3 · 14001 9.1.2 · 45001 9.1.2)",
                'subtitle': "Una banda por sistema, una tarjeta por requisito; color = cumplimiento",
                'layout': 'bands', 'lanes': lanes, 'param_options': options,
                'legend': [{'color': 'success', 'label': "cumple"}, {'color': 'warning', 'label': "parcial"},
                           {'color': 'danger', 'label': "no cumple"}, {'color': 'muted', 'label': "sin evaluar / no aplica"}],
            }
        level_by_state = {'cumple': 3, 'parcial': 2, 'no_cumple': 1, 'pendiente': 0, 'no_aplica': 0}
        cells = {}
        for req in reqs:
            key_r, key_c = req.system or 'varios', req.compliance_state or 'pendiente'
            cells.setdefault(key_r, {}).setdefault(key_c, []).append(req)
        matrix = {}
        for key_r, _lr in systems:
            for key_c, lc in states:
                group = cells.get(key_r, {}).get(key_c)
                if group:
                    matrix.setdefault(key_r, {})[key_c] = self._cell(
                        len(group), level_by_state.get(key_c, 0), "%d requisito(s) %s" % (len(group), lc.lower()),
                        self._list_action("Requisitos %s" % lc.lower(), 'sgi.legal.requirement',
                                          [('id', 'in', [r.id for r in group])]))
        return {
            'title': "Cumplimiento de requisitos legales (ISO 9001 6.1.3 · 14001 9.1.2 · 45001 9.1.2)",
            'subtitle': "Sistema × estado de cumplimiento; clic abre los requisitos",
            'layout': 'matrix', 'lanes': [],
            'matrix': {'rows': [{'key': k, 'model': '', 'res_id': 0, 'label': v} for k, v in systems],
                       'cols': [{'key': k, 'model': '', 'res_id': 0, 'label': v, 'title': v} for k, v in states],
                       'cells': matrix},
            'legend': [{'color': 'success', 'label': "cumple"}, {'color': 'warning', 'label': "parcial"},
                       {'color': 'info', 'label': "no cumple"}],
            'param_options': options,
        }

    # ---- 7.1.5 calibración ------------------------------------------
    def _data_calibration_map(self, res_id=None):
        Equipment = self.env['maintenance.equipment']
        if 'sgi_calibration_state' not in Equipment._fields:
            return {'title': "Calibración", 'layout': 'columns', 'lanes': []}
        equipments = Equipment.search([('sgi_is_measuring', '=', True)], order='sgi_next_calibration_date')
        state_color = {'vencido': 'danger', 'por_vencer': 'warning', 'vigente': 'success'}

        def item(e):
            return self._item(e, code=e.sgi_magnitude or '', name=e.name or '',
                              subtitle="Próxima: %s" % (e.sgi_next_calibration_date or '—'),
                              color=state_color.get(e.sgi_calibration_state, 'muted'))
        options = [{'name': 'agrupar', 'default': 'mes',
                    'values': [{'value': 'mes', 'label': "Programa por mes"},
                               {'value': 'estado', 'label': "Por vigencia"}]}]
        lanes = []
        if self._param('agrupar', 'mes') == 'estado':
            for state, label in (('vencido', "Vencidos"), ('por_vencer', "Por vencer (30 días)"),
                                 ('vigente', "Vigentes")):
                group = equipments.filtered(lambda e: e.sgi_calibration_state == state)
                lanes.append({'key': state, 'label': "%s (%d)" % (label, len(group)),
                              'items': [item(e) for e in group[:60]]})
            subtitle = "Por vigencia de la calibración; doble clic abre el equipo"
        else:
            # Programa de calibración: los 12 meses que vienen, los vencidos
            # primero y los que no tienen fecha al final.
            today = fields.Date.context_today(self)
            overdue = equipments.filtered(lambda e: e.sgi_next_calibration_date and e.sgi_next_calibration_date < today)
            if overdue:
                lanes.append({'key': 'vencidos', 'label': "Vencidos (%d)" % len(overdue), 'items': [item(e) for e in overdue[:60]]})
            for n in range(12):
                month = (today.month - 1 + n) % 12 + 1
                year = today.year + (today.month - 1 + n) // 12
                group = equipments.filtered(lambda e, m=month, y=year: e.sgi_next_calibration_date
                                            and e.sgi_next_calibration_date >= today
                                            and e.sgi_next_calibration_date.month == m
                                            and e.sgi_next_calibration_date.year == y)
                lanes.append({'key': 'm%d%02d' % (year, month), 'label': "%s %d" % (MONTHS[month - 1][:3], year),
                              'items': [item(e) for e in group[:60]]})
            later = equipments.filtered(lambda e: not e.sgi_next_calibration_date)
            if later:
                lanes.append({'key': 'sin_fecha', 'label': "Sin fecha (%d)" % len(later), 'items': [item(e) for e in later[:60]]})
            subtitle = "Programa de calibración: una columna por mes según la próxima calibración; rojo = vencido"
        return {
            'title': "Equipos de medición y calibración (ISO 9001 7.1.5)",
            'subtitle': subtitle,
            'layout': 'columns', 'lanes': lanes, 'param_options': options,
            'legend': [{'color': 'danger', 'label': "vencido"}, {'color': 'warning', 'label': "por vencer"},
                       {'color': 'success', 'label': "vigente"}],
        }

    # ---- 7.2 competencias -------------------------------------------
    def _data_competence_matrix(self, res_id=None):
        Gap = self.env['sgi.competence.gap']
        gaps = Gap.search([])
        jobs = gaps.job_id.sorted('name')
        types = gaps.skill_type_id.sorted('name')
        cells = {}
        for gap in gaps:
            cells.setdefault(gap.job_id.id, {}).setdefault(gap.skill_type_id.id, []).append(gap)
        matrix = {}
        for job in jobs:
            for stype in types:
                group = cells.get(job.id, {}).get(stype.id)
                if group:
                    worst = max(g.gap for g in group)
                    matrix.setdefault(_key(job), {})[_key(stype)] = self._cell(
                        len(group), 1 if worst >= 50 else (2 if worst >= 25 else 3),
                        "%d brecha(s); la mayor %d%%" % (len(group), worst),
                        self._list_action("Brechas %s / %s" % (job.name, stype.name), 'sgi.competence.gap',
                                          [('id', 'in', [g.id for g in group])]))
        return {
            'title': "Matriz de competencias: brechas por puesto (ISO 9001 7.2)",
            'subtitle': "Puestos × tipo de competencia; celda = personas por debajo del nivel requerido",
            'layout': 'matrix', 'lanes': [],
            'matrix': {'rows': [{'key': _key(j), 'model': 'hr.job', 'res_id': j.id, 'label': j.name or ''} for j in jobs],
                       'cols': [{'key': _key(t), 'model': 'hr.skill.type', 'res_id': t.id, 'label': t.name or '',
                                 'title': t.name or ''} for t in types],
                       'cells': matrix},
            'legend': [{'color': 'info', 'label': "brecha ≥ 50 %"}, {'color': 'warning', 'label': "25–49 %"},
                       {'color': 'success', 'label': "< 25 %"}],
        }

    # ---- 7.5 pirámide documental ------------------------------------
    def _data_doc_pyramid(self, res_id=None):
        Doc = self.env['documents.document']
        processes = self.env['sgi.process'].search([('active', '=', True)], order='process_type, code')
        docs = Doc.search([('sgi_is_controlled', '=', True), ('sgi_state', 'in', ('vigente', 'piloto')),
                           ('sgi_process_id', 'in', processes.ids)])
        level_of = {}
        for key, _label, types in DOC_LEVELS:
            for t in types:
                level_of[t] = key
        counts = {}
        for doc in docs:
            counts.setdefault(level_of.get(doc.sgi_doc_type, 'nivel4'), {}).setdefault(doc.sgi_process_id.id, []).append(doc)
        lanes = []
        for key, label, types in DOC_LEVELS:
            items = []
            for process in processes:
                group = counts.get(key, {}).get(process.id)
                if not group:
                    continue
                items.append(self._virtual(
                    "%s,%d" % (key, process.id), process.name or '', code=process.code or '',
                    subtitle="%d documento(s)" % len(group), color=SEMAPHORE_COLOR.get(process.health, 'muted'),
                    action=self._list_action("%s — %s" % (label, process.code), 'documents.document',
                                             [('id', 'in', [d.id for d in group])])))
            lanes.append({'key': key, 'label': "%s (%d)" % (label, sum(len(v) for v in counts.get(key, {}).values())),
                          'items': items})
        return {
            'title': "Pirámide documental (ISO 9001 7.5)",
            'subtitle': "Documentos controlados en vigor por nivel y proceso; doble clic abre la lista",
            'layout': 'bands', 'shape': 'pyramid', 'lanes': lanes,
        }

    # ---- 8.2 (45001) emergencias ------------------------------------
    def _data_emergency_map(self, res_id=None):
        Plan = self.env['sgi.emergency.plan']
        today = fields.Date.context_today(self)
        plans = Plan.search([('state', '!=', 'obsoleto')], order='plan_type, name')
        drills = self.env['sgi.emergency.drill'].search([
            ('plan_id', 'in', plans.ids), ('state', '!=', 'cancelado'),
            '|', ('date_done', '>=', today - timedelta(days=365)), ('state', '=', 'programado')], order='date_planned')
        type_labels = dict(Plan._fields['plan_type'].selection)
        result_color = {'satisfactorio': 'success', 'con_observaciones': 'warning', 'no_satisfactorio': 'danger'}
        edges = []
        for drill in drills:
            edges.append({'from': _key(drill.plan_id), 'to': _key(drill), 'label': ''})
            for action in drill.action_line_ids:
                edges.append({'from': _key(drill), 'to': _key(action), 'label': ''})
        actions = drills.action_line_ids
        return {
            'title': "Preparación y respuesta ante emergencias (ISO 45001 8.2 · 14001 8.2)",
            'subtitle': "Escenario → simulacros de los últimos 12 meses → acciones; rojo = simulacro vencido",
            'layout': 'columns',
            'lanes': [
                {'key': 'plans', 'label': "Escenarios", 'items': [
                    self._item(p, code=type_labels.get(p.plan_type, ''), name=p.name,
                               subtitle="Próximo simulacro: %s" % (p.next_drill_date or '—'),
                               color='danger' if p.next_drill_date and p.next_drill_date < today else (
                                   'success' if p.state == 'vigente' else 'muted')) for p in plans]},
                {'key': 'drills', 'label': "Simulacros", 'items': [
                    self._item(d, code=d.folio or '', name=d.plan_id.name or '',
                               subtitle="%s · %s" % (d.date_done or d.date_planned or '', d.result or d.state),
                               color=result_color.get(d.result, 'info')) for d in drills]},
                {'key': 'actions', 'label': "Acciones", 'items': [
                    self._item(a, name=a.name or '', subtitle="%s · %s" % (a.responsible_id.name or '', a.date_commit or ''),
                               color={'terminada': 'success', 'vencida': 'danger'}.get(a.state, 'warning'))
                    for a in actions]},
            ],
            'edges': edges, 'show_all_edges': True,
        }

    # ---- 9.2 programa de auditorías ---------------------------------
    def _data_audit_program(self, res_id=None):
        Program = self.env['sgi.audit.program']
        year = int(self._param('year', fields.Date.context_today(self).year))
        program = Program.search([('year', '=', year)], limit=1)
        years = sorted(set(Program.search([]).mapped('year')) | {year}, reverse=True)
        type_labels = dict(self.env['sgi.audit.program.line']._fields['audit_type'].selection)
        lanes = []
        for month in range(1, 13):
            lines = program.line_ids.filtered(lambda l: l.planned_month == str(month)) if program else []
            lanes.append({'key': 'm%02d' % month, 'label': MONTHS[month - 1], 'items': [
                self._item(l, code=l.process_id.code or type_labels.get(l.audit_type, ''),
                           name=l.process_id.name or l.partner_id.name or type_labels.get(l.audit_type, ''),
                           subtitle=" · ".join(x for x in (type_labels.get(l.audit_type, ''), l.audit_id.folio or '',
                                                           l.lead_auditor_id.name or '') if x),
                           color=STATE_COLOR.get(l.state, 'muted')) for l in lines]})
        return {
            'title': "Programa de auditorías %d (ISO 9001 9.2)" % year + ("" if program else " — sin programa"),
            'subtitle': "Una columna por mes; gris = pendiente, ámbar = auditoría creada, verde = cerrada",
            'layout': 'columns', 'lanes': lanes,
            'param_options': [{'name': 'year', 'default': year,
                               'values': [{'value': y, 'label': str(y)} for y in years]}],
        }

    # ---- 9.3 revisión por la dirección ------------------------------
    def _data_management_review(self, res_id=None):
        Review = self.env['sgi.management.review']
        reviews = Review.search([], order='date desc, id desc')
        review = Review.browse(int(self._param('review', 0) or 0)).exists() or reviews[:1]
        if not review:
            return {'title': "Revisión por la dirección (ISO 9001 9.3)", 'layout': 'columns', 'lanes': []}
        inputs = [
            ('prev_agreements_summary', "Acuerdos previos"), ('nc_summary', "No conformidades"),
            ('complaints_summary', "Reclamaciones"), ('audit_summary', "Auditorías"),
            ('supplier_summary', "Proveedores"), ('env_summary', "Ambiental y SST"),
            ('doc_changes_summary', "Cambios documentales"), ('legal_summary', "Requisitos legales"),
            ('participation_summary', "Participación"), ('objectives_summary', "Objetivos"),
            ('satisfaction_summary', "Satisfacción del cliente"), ('resources_note', "Recursos"),
        ]
        open_action = {'type': 'ir.actions.act_window', 'res_model': 'sgi.management.review',
                       'res_id': review.id, 'views': [[False, 'form']], 'target': 'current'}
        in_items, edges = [], []
        for field_name, label in inputs:
            text = review[field_name] if field_name in review._fields else ''
            key = "rev_in,%s" % field_name
            in_items.append(self._virtual(key, label, subtitle=_short(text, 140) or "— sin dato —",
                                          color='info' if text else 'muted', action=open_action))
            edges.append({'from': key, 'to': _key(review), 'label': ''})
        red = review.kpi_red_measure_ids if 'kpi_red_measure_ids' in review._fields else []
        if red:
            in_items.append(self._virtual("rev_in,kpi", "Indicadores en rojo", subtitle="%d medición(es)" % len(red),
                                          color='danger', action=self._list_action(
                                              "Indicadores en rojo", 'sgi.indicator.measure', [('id', 'in', red.ids)])))
            edges.append({'from': "rev_in,kpi", 'to': _key(review), 'label': ''})
        if review.risk_high_ids:
            in_items.append(self._virtual("rev_in,risk", "Riesgos altos", subtitle="%d riesgo(s)" % len(review.risk_high_ids),
                                          color='danger', action=self._list_action(
                                              "Riesgos altos", 'sgi.risk', [('id', 'in', review.risk_high_ids.ids)])))
            edges.append({'from': "rev_in,risk", 'to': _key(review), 'label': ''})
        agreements = []
        for agr in review.agreement_ids:
            agreements.append(self._item(agr, name=agr.name or '', subtitle="%s · %s · %s" % (
                agr.responsible_id.name or '', agr.deadline or '', agr.status_label or ''),
                color='success' if agr.is_done else ('danger' if agr.status_label == 'Vencida' else 'warning')))
            edges.append({'from': _key(review), 'to': _key(agr), 'label': ''})
        center = self._item(review, code=review.folio or '', name=review.name or "Revisión",
                            subtitle="%s · %s" % (review.date or '', dict(review._fields['state'].selection).get(review.state, '')),
                            color={'cerrada': 'success', 'realizada': 'info'}.get(review.state, 'warning'),
                            meta=[{'icon': 'fa-users', 'label': "Asistentes", 'value': len(review.attendee_ids)}])
        return {
            'title': "Revisión por la dirección (ISO 9001 9.3)",
            'subtitle': "Entradas de la norma → la revisión → acuerdos y su estado",
            'layout': 'columns',
            'lanes': [{'key': 'inputs', 'label': "Entradas", 'items': in_items},
                      {'key': 'review', 'label': "Revisión", 'items': [center]},
                      {'key': 'outputs', 'label': "Acuerdos (salidas)", 'items': agreements}],
            'edges': edges, 'show_all_edges': True,
            'param_options': [{'name': 'review', 'default': review.id,
                               'values': [{'value': r.id, 'label': "%s · %s" % (r.folio or r.name or '', r.date or '')}
                                          for r in reviews]}],
        }

    # ---- 10.2 no conformidades por fase ------------------------------
    def _data_nc_flow(self, res_id=None):
        Alert = self.env['quality.alert']
        today = fields.Date.context_today(self)
        domain = [('sgi_folio', '!=', False)]
        process = self._process(res_id) if res_id else self.env['sgi.process']
        if process:
            domain.append(('sgi_process_id', '=', process.id))
        alerts = Alert.search(domain + ['|', ('sgi_stage_is_closing', '=', False),
                                        ('write_date', '>=', fields.Datetime.to_datetime(today - timedelta(days=90)))],
                              order='create_date desc', limit=400)
        phases = {'contencion': [], 'causa': [], 'plan': [], 'eficacia': [], 'cerradas': [], 'canceladas': []}
        for alert in alerts:
            if alert.sgi_stage_is_cancel:
                phases['canceladas'].append(alert)
            elif alert.sgi_stage_is_closing:
                phases['cerradas'].append(alert)
            elif alert.sgi_containment_state != 'hecha':
                phases['contencion'].append(alert)
            elif alert.sgi_root_cause_state != 'hecha':
                phases['causa'].append(alert)
            elif alert.sgi_plan_state != 'hecha':
                phases['plan'].append(alert)
            else:
                phases['eficacia'].append(alert)

        def color(alert):
            if alert.sgi_stage_is_closing:
                return 'success'
            if alert.sgi_stage_is_cancel:
                return 'muted'
            if 'vencida' in (alert.sgi_containment_state, alert.sgi_root_cause_state, alert.sgi_plan_state):
                return 'danger'
            if alert.sgi_effectiveness_due and alert.sgi_effectiveness_due < today:
                return 'danger'
            return 'warning'

        def items(group):
            return [self._item(a, code=a.sgi_folio or '', name=a.title or a.name or '',
                               subtitle=" · ".join(x for x in (a.sgi_process_id.code or '',
                                                               dict(Alert._fields['sgi_classification'].selection).get(a.sgi_classification, ''),
                                                               a.sgi_supplier_id.name or '') if x),
                               color=color(a)) for a in group[:80]]
        labels = [('contencion', "1 · Contención"), ('causa', "2 · Causa raíz"), ('plan', "3 · Plan de acción"),
                  ('eficacia', "4 · Verificación de eficacia"), ('cerradas', "Cerradas (90 días)"),
                  ('canceladas', "Canceladas (90 días)")]
        result = {
            'title': "No conformidades y acciones correctivas (ISO 9001 10.2)" + (" — %s" % process.code if process else ''),
            'subtitle': "Cada NC en la fase que le falta; rojo = plazo vencido, ámbar = en tiempo, verde = cerrada",
            'layout': 'columns',
            'lanes': [{'key': k, 'label': "%s (%d)" % (label, len(phases[k])), 'items': items(phases[k])}
                      for k, label in labels],
        }
        if process:
            result.update(self._process_nav(process))
        return result

    # ---- PDCA por proceso --------------------------------------------
    def _data_pdca(self, res_id=None):
        process = self._process(res_id)
        if not process:
            return {'title': "PDCA", 'layout': 'columns', 'lanes': []}
        today = fields.Date.context_today(self)
        Doc = self.env['documents.document']
        indicators = self.env['sgi.indicator'].search([('process_id', '=', process.id), ('active', '=', True)])
        objectives = indicators.objective_id
        risks = self.env['sgi.risk'].search([('process_id', '=', process.id), ('state', '!=', 'cerrado')],
                                            order='score desc', limit=15)
        docs = Doc.search([('sgi_process_id', '=', process.id), ('sgi_is_controlled', '=', True),
                           ('sgi_state', 'in', ('vigente', 'piloto'))])
        activities = self.env['sgi.process.activity'].search([('process_id', '=', process.id), ('active', '=', True)])
        audits = self.env['sgi.audit'].search([('process_ids', 'in', process.ids),
                                               ('date_planned', '>=', today.replace(month=1, day=1))])
        alerts = self.env['quality.alert'].search([('sgi_process_id', '=', process.id),
                                                   ('sgi_stage_is_closing', '=', False),
                                                   ('sgi_stage_is_cancel', '=', False)], limit=30)
        actions = self.env['sgi.action.line'].search([('date_done', '=', False), '|', '|',
                                                      ('alert_id.sgi_process_id', '=', process.id),
                                                      ('risk_id.process_id', '=', process.id),
                                                      ('objective_id', 'in', objectives.ids)], limit=30)
        by_stage = {}
        for act in activities:
            by_stage.setdefault(act.stage_id, self.env['sgi.process.activity'])
            by_stage[act.stage_id] |= act
        plan = [self._item(o, name=o.name or '', subtitle="Objetivo", color={'sin_dato': 'muted'}.get(
            o.health, SEMAPHORE_COLOR.get(o.health, 'muted'))) for o in objectives]
        plan += [self._item(r, code=r.folio or '', name=r.name or '', subtitle="Riesgo · %s" % (r.attention_level or ''),
                            color=SEMAPHORE_COLOR.get(r.semaphore, 'muted')) for r in risks]
        do = [self._virtual("pdca_stage,%s" % (stage.id or 0), stage.name if stage else "Sin etapa",
                            code=(stage.code or '') if stage else '', subtitle="%d actividad(es)" % len(acts),
                            color='success' if all(a.measure_state == 'verde' for a in acts if a.measure_state) else 'info',
                            action=self._list_action("Actividades %s" % (stage.name if stage else ''),
                                                     'sgi.process.activity', [('id', 'in', acts.ids)]))
              for stage, acts in sorted(by_stage.items(), key=lambda kv: (kv[0].sequence if kv[0] else 999, kv[0].id if kv[0] else 0))]
        do.append(self._virtual("pdca_docs", "Documentos en vigor", subtitle="%d documento(s)" % len(docs), color='info',
                                action=self._list_action("Documentos %s" % process.code, 'documents.document',
                                                         [('id', 'in', docs.ids)])))
        check = [self._item(i, code=i.code or '', name=i.name or '', subtitle="Indicador",
                            color=SEMAPHORE_COLOR.get(i.last_semaphore, 'muted')) for i in indicators]
        check += [self._item(a, code=a.folio or '', name="Auditoría %s" % (a.date_planned or ''),
                             subtitle=dict(a._fields['state'].selection).get(a.state, ''),
                             color=STATE_COLOR.get(a.state, 'muted')) for a in audits]
        check += [self._item(a, code=a.sgi_folio or '', name=a.title or a.name or '', subtitle="NC abierta",
                             color='danger' if 'vencida' in (a.sgi_containment_state, a.sgi_root_cause_state, a.sgi_plan_state) else 'warning')
                  for a in alerts]
        act = [self._item(a, name=a.name or '', subtitle="%s · %s" % (a.responsible_id.name or '', a.date_commit or ''),
                          color={'vencida': 'danger'}.get(a.state, 'warning')) for a in actions]
        result = {
            'title': "PDCA — %s %s" % (process.code or '', process.name or ''),
            'subtitle': "El ciclo de mejora del proceso: Planear (objetivos y riesgos) → Hacer (etapas y documentos) → Verificar (indicadores, auditorías, NC) → Actuar (acciones abiertas)",
            # Ciclo: cuadrantes en el sentido de las manecillas, con flechas
            # entre cuadrantes (las claves «lane:x» son los propios carriles).
            'layout': 'cycle',
            'lanes': [{'key': 'plan', 'label': "Planear", 'items': plan},
                      {'key': 'do', 'label': "Hacer", 'items': do},
                      {'key': 'check', 'label': "Verificar", 'items': check},
                      {'key': 'act', 'label': "Actuar", 'items': act}],
            'edges': [{'from': 'lane:plan', 'to': 'lane:do', 'label': "Planear → Hacer"},
                      {'from': 'lane:do', 'to': 'lane:check', 'label': "Hacer → Verificar"},
                      {'from': 'lane:check', 'to': 'lane:act', 'label': "Verificar → Actuar"},
                      {'from': 'lane:act', 'to': 'lane:plan', 'label': "Actuar → Planear"}],
            'show_all_edges': True,
        }
        result.update(self._process_nav(process))
        return result

    # ---- 5.2 / 6.2: la política encabeza el árbol de indicadores ------
    def _data_kpi_tree(self, res_id=None):
        result = super()._data_kpi_tree(res_id)
        if res_id or not result.get('lanes'):
            return result
        Policy = self.env['sgi.policy']
        policies = Policy.search([('state', '=', 'vigente')])
        if not policies:
            return result
        objectives = self.env['sgi.objective'].search([('policy_id', 'in', policies.ids)])
        result['lanes'].insert(0, {'key': 'policy', 'label': "Política", 'items': [
            self._item(p, name=p.name or '', subtitle="Vigente desde %s" % (p.issue_date or ''),
                       color=SEMAPHORE_COLOR.get(p.health, 'muted')) for p in policies]})
        keys = {i['key'] for lane in result['lanes'] for i in lane['items']}
        result['edges'] += [{'from': _key(o.policy_id), 'to': _key(o), 'label': ''}
                            for o in objectives if _key(o) in keys]
        result['title'] = "Política → objetivos → indicadores → procesos (ISO 9001 5.2 / 6.2)"
        return result
