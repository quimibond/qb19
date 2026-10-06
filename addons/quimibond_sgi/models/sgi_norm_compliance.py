# -*- coding: utf-8 -*-
"""Actividades ↔ requisitos de la norma (56.23.0).

- ``sgi.process.activity.norm_clause_ids`` («Cumple con»): los puntos de la
  norma que la actividad cumple. Se ve en la ficha de la actividad y en Mi
  procedimiento (pantalla y PDF) como referencia; no entra en la huella de Mi
  procedimiento, así que ligar puntos no obliga a republicar ni a firmar.
- En cada punto de la norma, la pestaña «Evidencia en el SGI»: actividades y
  procesos que lo cumplen, y las NC levantadas contra él.
- «Matriz de cumplimiento» (PDF por norma): puntos contra procesos, con los
  numerales de las actividades en cada cruce y en rojo los puntos sin ninguna
  actividad.
- El checklist de auditoría pone en cada pregunta los puntos que cumple la
  actividad (de las normas de la auditoría, si las tiene), el hallazgo nace
  con su cláusula y, si la auditoría dice qué normas cubre, agrega una
  pregunta por cada punto sin ninguna actividad en el SGI.
"""
from odoo import api, fields, models


class SgiProcessActivityNorm(models.Model):
    _inherit = 'sgi.process.activity'

    norm_clause_ids = fields.Many2many(
        'sgi.norm.clause', 'sgi_activity_norm_clause_rel', 'activity_id', 'clause_id',
        string="Cumple con",
        help="Puntos de la norma (ISO 9001, 14001, 45001…) que esta actividad cumple. "
             "Alimenta la Matriz de cumplimiento y el checklist de auditoría.")

    def _sgi_norm_labels(self, norms=None):
        """«9001 8.5.1, 45001 8.1.2» de los puntos que cumple (opcionalmente
        solo de ciertas normas)."""
        self.ensure_one()
        clauses = self.sudo().norm_clause_ids
        if norms:
            clauses = clauses.filtered(lambda c: c.norm_id in norms)
        return ", ".join(clauses.sorted(lambda c: (c.norm_id.code or '', c._sgi_sort_key()))
                         .mapped('short_label'))


class SgiNormClauseEvidence(models.Model):
    _inherit = 'sgi.norm.clause'

    short_label = fields.Char(string="Punto", compute='_compute_short_label')
    activity_ids = fields.Many2many(
        'sgi.process.activity', 'sgi_activity_norm_clause_rel', 'clause_id', 'activity_id',
        string="Actividades que lo cumplen",
        help="Actividades que cumplen con esta cláusula («Cumple con»).")
    process_ids = fields.Many2many(
        'sgi.process', string="Procesos que lo cumplen", compute='_compute_sgi_evidence',
        help="Procesos cuyas actividades cumplen con la cláusula.")
    activity_count = fields.Integer(string="Actividades", compute='_compute_sgi_evidence')
    alert_count = fields.Integer(string="NC", compute='_compute_sgi_evidence')
    covered = fields.Boolean(string="Con actividad", compute='_compute_sgi_evidence',
                             search='_search_covered',
                             help="Indica si al menos una actividad cumple con la cláusula.")

    @api.depends('code', 'norm_id.code')
    def _compute_short_label(self):
        for clause in self:
            code = clause.code or ''
            # Un numeral (8.5.1) necesita la norma; una clave propia (CLI-01,
            # requisitos de clientes) ya se explica sola.
            if code[:1].isdigit():
                clause.short_label = ("%s %s" % (clause.norm_id._sgi_short_code(), code)).strip()
            else:
                clause.short_label = code or clause.name

    @api.model
    def _sgi_find(self, label):
        """«9001 8.5.1», «ISO 9001:2015 8.5.1» o «ISO 9001 8.5.1» → el punto.
        El numeral es la última palabra; lo de antes es la norma."""
        pieces = (label or '').split()
        if len(pieces) == 1:
            # Clave propia sin norma (CLI-01): vale si es única.
            found = self.search([('code', '=', pieces[0])], limit=2)
            return found if len(found) == 1 else self.browse()
        if len(pieces) < 2:
            return self.browse()
        code, norm_text = pieces[-1], " ".join(pieces[:-1])
        wanted = norm_text.upper().replace('ISO', '').strip().split(':')[0].strip()
        norms = self.env['sgi.norm'].search([]).filtered(lambda n: n._sgi_short_code() == wanted)
        return self.search([('norm_id', 'in', norms.ids), ('code', '=', code)], limit=1)

    def _sgi_sort_key(self):
        """8.10 va después de 8.9: numeral como tupla de enteros."""
        self.ensure_one()
        parts = []
        for piece in (self.code or '').split('.'):
            parts.append((0, int(piece), '') if piece.isdigit() else (1, 0, piece))
        return tuple(parts)

    @api.depends('activity_ids.active', 'activity_ids.process_id')
    def _compute_sgi_evidence(self):
        Alert = self.env['quality.alert'].sudo()
        counts = {}
        if self.ids:
            for group in Alert._read_group([('sgi_norm_clause_id', 'in', self.ids)],
                                           ['sgi_norm_clause_id'], ['__count']):
                counts[group[0].id] = group[1]
        for clause in self:
            activities = clause.activity_ids.filtered('active')
            clause.activity_count = len(activities)
            clause.process_ids = activities.process_id
            clause.covered = bool(activities)
            clause.alert_count = counts.get(clause.id, 0)

    def _search_covered(self, operator, value):
        if operator not in ('=', '!=') or not isinstance(value, bool):
            return NotImplemented
        positive = (operator == '=') == value
        domain = [('activity_ids', 'any', [('active', '=', True)])]
        return domain if positive else ['!'] + domain

    def action_view_alerts(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "NC contra %s" % self.short_label,
                'res_model': 'quality.alert', 'view_mode': 'list,form',
                'domain': [('sgi_norm_clause_id', '=', self.id)]}


class SgiNormMatrix(models.Model):
    _inherit = 'sgi.norm'

    def _sgi_short_code(self):
        """«ISO 9001:2015» → «9001»."""
        self.ensure_one()
        code = (self.code or '').upper().replace('ISO', '').strip()
        return code.split(':')[0].strip() or (self.code or '')

    def _sgi_compliance_matrix(self):
        """Filas = puntos de la norma; columnas = procesos que declaran la
        norma o tienen alguna actividad ligada a ella. Cada celda, los
        numerales de las actividades vivas que cumplen el punto."""
        self.ensure_one()
        clauses = self.clause_ids.sorted(lambda c: c._sgi_sort_key())
        activities = self.env['sgi.process.activity'].search(
            [('norm_clause_ids', 'in', clauses.ids), ('active', '=', True)])
        processes = self.env['sgi.process'].search(
            ['|', ('norm_ids', 'in', self.ids), ('id', 'in', activities.process_id.ids)])
        if not processes:
            # Norma que ningún proceso declara todavía (p. ej. requisitos de
            # clientes recién cargados): todos los procesos, para ver el hueco.
            processes = self.env['sgi.process'].search([])
        processes = processes.sorted(lambda p: (p.code or '', p.name or ''))
        rows, uncovered = [], 0
        for clause in clauses:
            mine = activities.filtered(lambda a: clause in a.norm_clause_ids)
            cells = []
            for process in processes:
                numbers = mine.filtered(lambda a: a.process_id == process).sorted(
                    lambda a: (a.step, a.id)).mapped(lambda a: a.number or a.legacy_number or a.name)
                cells.append(numbers)
            if not mine:
                uncovered += 1
            rows.append({'clause': clause, 'cells': cells, 'total': len(mine)})
        return {'processes': processes, 'rows': rows, 'uncovered': uncovered,
                'covered': len(rows) - uncovered}

    def action_view_uncovered_clauses(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Puntos sin actividad — %s" % self.code,
                'res_model': 'sgi.norm.clause', 'view_mode': 'list,form',
                'domain': [('norm_id', '=', self.id), ('covered', '=', False)]}


class SgiAuditChecklistNorm(models.Model):
    _inherit = 'sgi.audit.checklist.line'

    activity_id = fields.Many2one(required=False)
    norm_clause_ids = fields.Many2many(
        'sgi.norm.clause', 'sgi_audit_checklist_clause_rel', 'line_id', 'clause_id',
        string="Requisitos", compute='_compute_norm_clause_ids', store=True, readonly=False,
        help="Puntos de la norma que se auditan con esta pregunta.")

    @api.depends('activity_id.norm_clause_ids', 'audit_id.norm_ids')
    def _compute_norm_clause_ids(self):
        for line in self:
            if not line.activity_id:
                line.norm_clause_ids = line.norm_clause_ids
                continue
            clauses = line.activity_id.sudo().norm_clause_ids
            if line.audit_id.norm_ids:
                clauses = clauses.filtered(lambda c: c.norm_id in line.audit_id.norm_ids)
            line.norm_clause_ids = clauses

    @api.depends('norm_clause_ids')
    def _compute_question(self):
        with_activity = self.filtered('activity_id')
        super(SgiAuditChecklistNorm, with_activity)._compute_question()
        for line in with_activity.filtered('norm_clause_ids'):
            line.question = "%s — Requisito: %s" % (line.question, ", ".join(
                line.norm_clause_ids.sorted(lambda c: (c.norm_id.code or '', c._sgi_sort_key()))
                .mapped('short_label')))
        for line in self - with_activity:
            clause = line.norm_clause_ids[:1]
            line.question = ("¿Cómo se cumple %s «%s»? En el SGI no hay ninguna actividad ligada a "
                             "este punto." % (clause.short_label, clause.name)) if clause else False
            line.executor = False
            line.deliverables = False

    def _sgi_sync_finding(self):
        res = super()._sgi_sync_finding()
        for line in self.filtered(lambda l: l.finding_id and l.norm_clause_ids
                                  and not l.finding_id.norm_clause_id):
            line.finding_id.norm_clause_id = line.norm_clause_ids[:1]
        return res


class SgiAuditChecklistByClause(models.Model):
    _inherit = 'sgi.audit'

    def action_generate_checklist(self):
        """Además de una pregunta por actividad, si la auditoría dice qué
        normas cubre: una pregunta por cada punto de esas normas que no tiene
        ninguna actividad en el SGI (idempotente)."""
        res = super().action_generate_checklist()
        Line = self.env['sgi.audit.checklist.line']
        Clause = self.env['sgi.norm.clause']
        for audit in self.filtered('norm_ids'):
            asked = audit.checklist_line_ids.filtered(lambda l: not l.activity_id).norm_clause_ids
            gaps = Clause.search([('norm_id', 'in', audit.norm_ids.ids), ('covered', '=', False)])
            gaps = (gaps - asked).sorted(lambda c: (c.norm_id.code or '', c._sgi_sort_key()))
            seq = max(audit.checklist_line_ids.mapped('sequence') or [0])
            for clause in gaps:
                seq += 10
                Line.create({'audit_id': audit.id, 'sequence': seq,
                             'norm_clause_ids': [(6, 0, clause.ids)]})
            if gaps:
                audit.message_post(body="Checklist: %d pregunta(s) por puntos de la norma sin actividad "
                                        "en el SGI." % len(gaps))
        return res
