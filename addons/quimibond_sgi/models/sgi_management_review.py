# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

from .sgi_risk import SGI_HIGH_ATTENTION


class SgiManagementReview(models.Model):
    _name = 'sgi.management.review'
    _description = "Revisión por la Dirección (IT-P-A10-01)"
    _inherit = ['sgi.base.mixin']
    _order = 'date desc, folio desc'
    _sgi_sequence_code = 'sgi.management.review'
    _sgi_locked_states = ('cerrada',)

    _folio_uniq = models.Constraint(
        'unique(folio)',
        "Ya existe una revisión por la dirección con ese folio.",
    )

    name = fields.Char(string="Nombre", compute='_compute_name', store=True)
    date = fields.Date(string="Fecha", default=fields.Date.context_today, required=True)
    period_from = fields.Date(string="Periodo desde", required=True)
    period_to = fields.Date(string="Periodo hasta", required=True)
    attendee_ids = fields.Many2many('hr.employee', 'sgi_review_attendee_rel',
                                    'review_id', 'employee_id', string="Asistentes")
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('realizada', "Realizada"),
        ('cerrada', "Cerrada"),
    ], string="Estado", default='borrador', required=True, tracking=True)

    # Entradas 9.3.2 (snapshot readonly)
    prev_agreements_summary = fields.Text(string="1. Acuerdos previos", readonly=True)
    nc_summary = fields.Text(string="2. No conformidades", readonly=True)
    complaints_summary = fields.Text(string="3. Reclamaciones de clientes", readonly=True)
    audit_summary = fields.Text(string="4. Auditorías", readonly=True)
    # P-3: las auditorías que cubre la revisión, como registros y no solo
    # como texto. «Cargar entradas» las toma del periodo; se pueden ajustar a
    # mano. Es la liga con la que E2.14 se mide (match: audit_ids).
    audit_ids = fields.Many2many(
        'sgi.audit', 'sgi_review_audit_rel', 'review_id', 'audit_id',
        string="Auditorías del periodo")
    kpi_red_measure_ids = fields.Many2many('sgi.indicator.measure', 'sgi_review_kpi_rel',
                                           'review_id', 'measure_id',
                                           string="5. Indicadores en rojo", readonly=True)
    supplier_summary = fields.Text(string="6. Proveedores", readonly=True)
    risk_high_ids = fields.Many2many('sgi.risk', 'sgi_review_risk_rel',
                                     'review_id', 'risk_id',
                                     string="7. Riesgos de atención inmediata/alta", readonly=True)
    env_summary = fields.Text(string="8. Desempeño ambiental (scrap)", readonly=True)
    resources_note = fields.Text(string="9. Recursos (calibraciones/capacitación)")
    doc_changes_summary = fields.Text(string="10. Cambios documentales", readonly=True)
    legal_summary = fields.Text(
        string="11. Cumplimiento legal", readonly=True,
        help="14001/45001 9.3: estado de la evaluación del cumplimiento de "
             "requisitos legales y permisos.")
    participation_summary = fields.Text(
        string="12. Consulta y participación", readonly=True,
        help="45001 5.4/9.3: respuestas de la encuesta de consulta y "
             "participación y quejas del canal interno en el periodo.")
    # DIR-3 (52.0.0): las entradas que faltaban de ISO 9.3.2 tomadas de Odoo.
    objectives_summary = fields.Text(
        string="13. Objetivos e indicadores", readonly=True,
        help="Objetivos integrales con sus indicadores oficiales: último valor, "
             "semáforo y rojos sin plan.")
    satisfaction_summary = fields.Text(
        string="14. Satisfacción del cliente", readonly=True,
        help="Indicador CA-02 y reclamaciones del periodo.")
    agreement_action_ids = fields.One2many(
        'sgi.action.line', 'review_id', string="Acuerdos (acciones)",
        help="Los acuerdos de la revisión son acciones con responsable y fecha; "
             "miden E1-02 (acuerdos cumplidos a tiempo).")

    # Salidas
    agreement_ids = fields.One2many('sgi.management.review.agreement', 'review_id',
                                    string="Acuerdos")


    @api.depends('folio', 'date')
    def _compute_name(self):
        for review in self:
            review.name = "Revisión por la Dirección %s" % (review.folio or '')

    def _sgi_bounds(self):
        self.ensure_one()
        dt_from = fields.Datetime.to_datetime(self.period_from)
        dt_to = fields.Datetime.to_datetime(self.period_to) + relativedelta(days=1)
        return dt_from, dt_to

    # ------------------------------------------------------------------
    # Cargar entradas (snapshot)
    # ------------------------------------------------------------------
    def action_load_inputs(self):
        for review in self:
            if review.state != 'borrador':
                raise UserError("Solo se pueden recargar las entradas en borrador.")
            review.write({
                'prev_agreements_summary': review._sgi_load_prev_agreements(),
                'nc_summary': review._sgi_load_nc(),
                'complaints_summary': review._sgi_load_complaints(),
                'audit_summary': review._sgi_load_audits(),
                'audit_ids': [(6, 0, review._sgi_period_audits().ids)],
                'kpi_red_measure_ids': [(6, 0, review._sgi_load_red_measures().ids)],
                'supplier_summary': review._sgi_load_suppliers(),
                'risk_high_ids': [(6, 0, review._sgi_load_high_risks().ids)],
                'env_summary': review._sgi_load_env(),
                'doc_changes_summary': review._sgi_load_doc_changes(),
                'legal_summary': review._sgi_load_legal(),
                'participation_summary': review._sgi_load_participation(),
                'objectives_summary': review._sgi_load_objectives(),
                'satisfaction_summary': review._sgi_load_satisfaction(),
            })
        return True

    def _sgi_load_objectives(self):
        """Entrada 13 (9.3.2 c): objetivos integrales e indicadores oficiales."""
        self.ensure_one()
        Indicator = self.env['sgi.indicator']
        lines = []
        for objective in self.env['sgi.objective'].search([]):
            indicators = objective.indicator_ids.filtered(lambda i: i.status == 'oficial') \
                if 'status' in Indicator._fields else objective.indicator_ids
            lines.append("• %s (%s): %d indicador(es) oficial(es)." % (
                objective.name, dict(objective._fields['health'].selection).get(objective.health, '-')
                if 'health' in objective._fields else '-', len(indicators)))
            for ind in indicators:
                lines.append("    - %s %s: %s %s (%s)" % (
                    ind.code, ind.name, ind.last_value, ind.uom or '',
                    ind.last_semaphore or 'sin dato'))
        reds = self._sgi_load_red_measures().filtered(lambda m: m.plan_required and not m.plan_done)
        if reds:
            lines.append("Rojos del periodo SIN causa ni plan: %s." % ", ".join(
                sorted(set(reds.mapped('indicator_id.code')))))
        return "\n".join(lines) or "Sin objetivos integrales registrados."

    def _sgi_load_satisfaction(self):
        """Entrada 14 (9.3.2 c1): satisfacción del cliente (CA-02) y reclamaciones."""
        self.ensure_one()
        parts = []
        ca02 = self.env['sgi.indicator'].search([('code', '=', 'CA-02')], limit=1)
        if ca02:
            measures = ca02.measure_ids.filtered(
                lambda m: self.period_from <= m.period_date <= self.period_to and m.state != 'pendiente')
            if measures:
                values = ", ".join("%s: %s" % (m.period_date, m.value) for m in measures.sorted('period_date'))
                parts.append("CA-02 satisfacción del cliente en el periodo → %s." % values)
            else:
                parts.append("CA-02 sin mediciones en el periodo.")
        else:
            parts.append("No existe el indicador CA-02 (satisfacción del cliente).")
        parts.append(self._sgi_load_complaints())
        return "\n".join(parts)

    def _sgi_load_legal(self):
        """Entrada 11 (14001/45001 9.3): foto del cumplimiento legal."""
        self.ensure_one()
        Requirement = self.env['sgi.legal.requirement']
        total = Requirement.search_count([])
        if not total:
            return ("Sin requisitos legales registrados: capture la matriz "
                    "legal (SGI → Riesgos y auditorías → Requisitos legales).")
        today = fields.Date.context_today(self)
        parts = ["%d requisito(s) registrados." % total]
        labels = dict(Requirement._fields['compliance_state'].selection)
        for state, count in Requirement._read_group(
                [], ['compliance_state'], ['__count']):
            parts.append("%s: %d" % (labels.get(state, state), count))
        overdue = Requirement.search_count([
            ('next_eval_date', '!=', False), ('next_eval_date', '<=', today)])
        if overdue:
            parts.append("Evaluaciones vencidas: %d." % overdue)
        expiring = Requirement.search_count([
            ('expiry_date', '!=', False),
            ('expiry_date', '<=', today + relativedelta(days=90))])
        if expiring:
            parts.append("Permisos que vencen en ≤90 días: %d." % expiring)
        return "\n".join(parts)

    def _sgi_load_participation(self):
        """Entrada 12 (45001 5.4): consulta y participación de trabajadores."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        parts = []
        survey = self.env['sgi.cron']._sgi_participation_survey()
        if survey:
            answered = self.env['survey.user_input'].sudo().search_count([
                ('survey_id', '=', survey.id), ('state', '=', 'done'),
                ('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
            parts.append("Encuesta de consulta y participación (F-P-A10-05): "
                         "%d respuesta(s) en el periodo." % answered)
        else:
            parts.append("La encuesta de consulta y participación "
                         "(F-P-A10-05) no está disponible en Encuestas.")
        team = self.env.ref('quimibond_sgi.sgi_helpdesk_team_voice',
                            raise_if_not_found=False)
        if team:
            Ticket = self.env['helpdesk.ticket']
            received = Ticket.search_count([
                ('team_id', '=', team.id),
                ('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
            open_now = Ticket.search_count([
                ('team_id', '=', team.id), ('stage_id.fold', '=', False)])
            parts.append("Quejas y sugerencias internas: %d recibidas en el "
                         "periodo, %d abiertas hoy." % (received, open_now))
        return "\n".join(parts)

    def _sgi_load_prev_agreements(self):
        self.ensure_one()
        prev = self.search([
            ('id', '!=', self.id),
            ('date', '<', self.date),
        ], order='date desc', limit=1)
        if not prev or not prev.agreement_ids:
            return "Sin acuerdos de la revisión anterior."
        total = len(prev.agreement_ids)
        closed = prev.agreement_ids.filtered(lambda a: a.is_done)
        pct = round(len(closed) / total * 100.0, 1) if total else 0.0
        lines = ["Acuerdos de la revisión %s — %s%% cerrados (%d/%d):" % (
            prev.folio, pct, len(closed), total)]
        for agr in prev.agreement_ids:
            lines.append("• %s (resp. %s, límite %s) — %s" % (
                agr.name, agr.responsible_id.name or '-',
                agr.deadline or '-', agr.status_label))
        return "\n".join(lines)

    def _sgi_load_nc(self):
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        Alert = self.env['quality.alert']
        result = []
        # La etapa "Seguimiento" es una sola compartida por ambos equipos NC;
        # el conteo por equipo lo hace el dominio team_id.
        teams = [
            ('Internas', 'sgi_quality_team_internal', 'sgi_nc_int_stage_followup'),
            ('Externas', 'sgi_quality_team_external', 'sgi_nc_int_stage_followup'),
        ]
        for label, team_xmlid, followup_xmlid in teams:
            team = self.env.ref('quimibond_sgi.%s' % team_xmlid, raise_if_not_found=False)
            if not team:
                continue
            followup = self.env.ref('quimibond_sgi.%s' % followup_xmlid, raise_if_not_found=False)
            base = [('team_id', '=', team.id)]
            abiertas = Alert.search_count(base + [
                ('stage_id.sgi_is_closing_stage', '=', False),
                ('stage_id.sgi_is_cancel_stage', '=', False),
            ])
            seguimiento = Alert.search_count(base + [
                ('stage_id', '=', followup.id),
            ]) if followup else 0
            cerradas = Alert.search_count(base + [
                ('date_close', '>=', dt_from), ('date_close', '<', dt_to),
            ])
            result.append("%s: %d abiertas, %d en seguimiento, %d cerradas en el periodo." % (
                label, abiertas, seguimiento, cerradas))
        return "\n".join(result) or "Sin No Conformidades."

    def _sgi_load_complaints(self):
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        team = self.env.ref('quimibond_sgi.sgi_helpdesk_team_complaints', raise_if_not_found=False)
        if not team:
            return "Sin equipo de reclamaciones configurado."
        tickets = self.env['helpdesk.ticket'].search([
            ('team_id', '=', team.id),
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
        ])
        total = len(tickets)
        sla_ok = len(tickets.filtered(
            lambda t: 'sla_reached_late' in t._fields and not t.sla_reached_late))
        pct = round(sla_ok / total * 100.0, 1) if total else 0.0
        return "%d reclamaciones en el periodo. Cumplimiento SLA aprox.: %s%%." % (total, pct)

    def _sgi_period_audits(self):
        self.ensure_one()
        return self.env['sgi.audit'].search([
            ('date_start', '>=', self.period_from),
            ('date_start', '<=', self.period_to),
        ])

    def _sgi_load_audits(self):
        self.ensure_one()
        audits = self._sgi_period_audits()
        if not audits:
            return "Sin auditorías en el periodo."
        findings = audits.mapped('finding_ids')
        by_type = {}
        for finding in findings:
            by_type[finding.finding_type] = by_type.get(finding.finding_type, 0) + 1
        labels = dict(self.env['sgi.audit.finding']._fields['finding_type'].selection)
        detail = ", ".join("%s: %d" % (labels.get(k, k), v) for k, v in by_type.items())
        return "%d auditoría(s). Hallazgos → %s." % (len(audits), detail or "sin hallazgos")

    def _sgi_load_red_measures(self):
        self.ensure_one()
        return self.env['sgi.indicator.measure'].search([
            ('semaphore', '=', 'rojo'),
            ('period_date', '>=', self.period_from),
            ('period_date', '<=', self.period_to),
        ])

    def _sgi_load_suppliers(self):
        self.ensure_one()
        Partner = self.env['res.partner']
        counts = []
        for key, label in (('acreditado', "Acreditados"), ('condicionado', "Condicionados"),
                           ('baja', "Baja")):
            counts.append("%s: %d" % (label, Partner.search_count([
                ('sgi_supplier_class', '=', key)])))
        return "Proveedores por clase — %s." % ", ".join(counts)

    def _sgi_load_high_risks(self):
        self.ensure_one()
        return self.env['sgi.risk'].search([
            ('attention_level', 'in', list(SGI_HIGH_ATTENTION)),
            ('state', '!=', 'cerrado'),
        ])

    def _sgi_load_env(self):
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        scraps = self.env['stock.scrap'].search([
            ('state', '=', 'done'),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to),
        ])
        if not scraps:
            return "Sin registros de scrap en el periodo."
        by_reason = {}
        for scrap in scraps:
            reason = ", ".join(scrap.scrap_reason_tag_ids.mapped('name')) or "Sin motivo"
            by_reason[reason] = by_reason.get(reason, 0.0) + scrap.scrap_qty
        lines = ["Scrap por motivo (%d movimientos):" % len(scraps)]
        for reason, qty in by_reason.items():
            lines.append("• %s: %s" % (reason, round(qty, 2)))
        return "\n".join(lines)

    def _sgi_load_doc_changes(self):
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        requests = self.env['approval.request'].search([
            ('sgi_is_doc_change', '=', True),
            ('request_status', '=', 'approved'),
            ('write_date', '>=', dt_from), ('write_date', '<', dt_to),
        ])
        if not requests:
            return "Sin cambios documentales aprobados en el periodo."
        lines = ["%d solicitud(es) de cambio aprobadas:" % len(requests)]
        for req in requests:
            lines.append("• %s — %s" % (req.name, req.sgi_reason or req.reason or ''))
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Salidas
    # ------------------------------------------------------------------
    def action_mark_done(self):
        for review in self:
            if not review.agreement_ids:
                raise UserError(
                    "No se puede marcar como Realizada sin al menos un acuerdo "
                    "con responsable y fecha límite.")
            incomplete = review.agreement_ids.filtered(
                lambda a: not a.responsible_id or not a.deadline)
            if incomplete:
                raise UserError(
                    "Todo acuerdo de la Revisión por la Dirección debe tener "
                    "responsable y fecha límite (ISO 9.3.3: las salidas son "
                    "accionables). Completa: %s" % ", ".join(
                        incomplete.mapped('name')))
            # DIR-3 (52.0.0): cada acuerdo es una ACCIÓN del SGI (sgi.action.line)
            # con responsable y compromiso: actividad nativa al responsable,
            # escalamiento del cron de acciones vencidas y medición de E1-02.
            Line = self.env['sgi.action.line']
            for agr in review.agreement_ids:
                if agr.action_line_id:
                    continue
                agr.action_line_id = Line.create({
                    'review_id': review.id,
                    'action_type': 'correctiva',
                    'name': agr.name,
                    'responsible_id': agr.responsible_id.id,
                    'date_commit': agr.deadline,
                }).id
            review.state = 'realizada'
        return True

    def action_close(self):
        self.write({'state': 'cerrada'})

    def action_draft(self):
        self.write({'state': 'borrador'})


class SgiManagementReviewAgreement(models.Model):
    _name = 'sgi.management.review.agreement'
    _description = "Acuerdo de Revisión por la Dirección"
    _order = 'deadline, id'

    review_id = fields.Many2one('sgi.management.review', string="Revisión",
                                required=True, ondelete='cascade')
    name = fields.Char(string="Acuerdo", required=True)
    responsible_id = fields.Many2one('res.users', string="Responsable")
    deadline = fields.Date(string="Fecha límite")
    task_id = fields.Many2one('project.task', string="Tarea (anterior a 52.0.0)", readonly=True)
    action_line_id = fields.Many2one('sgi.action.line', string="Acción", readonly=True, copy=False)
    action_state = fields.Selection(related='action_line_id.state', string="Estado de la acción")
    is_done = fields.Boolean(compute='_compute_status')
    status_label = fields.Char(compute='_compute_status')

    @api.depends('action_line_id.state', 'action_line_id.date_done', 'task_id.stage_id.fold')
    def _compute_status(self):
        labels = dict(self.env['sgi.action.line']._fields['state'].selection)
        for agr in self:
            if agr.action_line_id:
                agr.is_done = bool(agr.action_line_id.date_done)
                agr.status_label = labels.get(agr.action_line_id.state, agr.action_line_id.state)
            elif agr.task_id:
                agr.is_done = bool(agr.task_id.stage_id.fold)
                agr.status_label = "cerrado" if agr.is_done else "abierto"
            else:
                agr.is_done = False
                agr.status_label = "sin acción"


class SgiActionLineReview(models.Model):
    """Origen «acuerdo de la Revisión por la Dirección» de una acción (DIR-3)."""
    _inherit = 'sgi.action.line'

    review_id = fields.Many2one('sgi.management.review', string="Revisión por la Dirección",
                                ondelete='cascade', index=True)

    @api.depends('review_id')
    def _compute_origin_display(self):
        with_review = self.filtered('review_id')
        for line in with_review:
            line.origin_display = line.review_id.display_name
        super(SgiActionLineReview, self - with_review)._compute_origin_display()

    def _sgi_origin(self):
        self.ensure_one()
        if self.review_id:
            return self.review_id
        return super()._sgi_origin()

    @api.constrains('alert_id', 'risk_id', 'fmea_line_id', 'incident_id',
                    'drill_id', 'objective_id', 'review_id', 'name')
    def _check_parent_xor(self):
        with_review = self.filtered('review_id')
        for line in with_review:
            others = [line.alert_id, line.risk_id, line.fmea_line_id,
                      line.incident_id, line.drill_id, line.objective_id]
            if any(others):
                raise ValidationError("Un acuerdo de la Revisión por la Dirección no puede tener otro origen.")
        return super(SgiActionLineReview, self - with_review)._check_parent_xor()
