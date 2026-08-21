# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import UserError


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
                'kpi_red_measure_ids': [(6, 0, review._sgi_load_red_measures().ids)],
                'supplier_summary': review._sgi_load_suppliers(),
                'risk_high_ids': [(6, 0, review._sgi_load_high_risks().ids)],
                'env_summary': review._sgi_load_env(),
                'doc_changes_summary': review._sgi_load_doc_changes(),
            })
        return True

    def _sgi_load_prev_agreements(self):
        self.ensure_one()
        prev = self.search([
            ('id', '!=', self.id),
            ('date', '<', self.date),
        ], order='date desc', limit=1)
        if not prev or not prev.agreement_ids:
            return "Sin acuerdos de la revisión anterior."
        tasks = prev.agreement_ids.mapped('task_id')
        total = len(prev.agreement_ids)
        closed = tasks.filtered(lambda t: t.stage_id.fold)
        pct = round(len(closed) / total * 100.0, 1) if total else 0.0
        lines = ["Acuerdos de la revisión %s — %s%% cerrados (%d/%d):" % (
            prev.folio, pct, len(closed), total)]
        for agr in prev.agreement_ids:
            status = "cerrado" if agr.task_id.stage_id.fold else "abierto"
            lines.append("• %s (resp. %s, límite %s) — %s" % (
                agr.name, agr.responsible_id.name or '-',
                agr.deadline or '-', status))
        return "\n".join(lines)

    def _sgi_load_nc(self):
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        Alert = self.env['quality.alert']
        result = []
        teams = [
            ('Internas', 'sgi_quality_team_internal', 'sgi_nc_int_stage_followup'),
            ('Externas', 'sgi_quality_team_external', 'sgi_nc_ext_stage_followup'),
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

    def _sgi_load_audits(self):
        self.ensure_one()
        audits = self.env['sgi.audit'].search([
            ('date_start', '>=', self.period_from),
            ('date_start', '<=', self.period_to),
        ])
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
            ('attention_level', 'in', ['inmediata', 'alto']),
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
            project = self.env.ref('quimibond_sgi.sgi_project_agreements',
                                   raise_if_not_found=False)
            for agr in review.agreement_ids:
                if agr.task_id:
                    continue
                task_vals = {
                    'name': agr.name,
                    'date_deadline': agr.deadline,
                    'description': "Acuerdo de la Revisión por la Dirección %s." % review.folio,
                }
                if project:
                    task_vals['project_id'] = project.id
                if agr.responsible_id:
                    task_vals['user_ids'] = [(6, 0, agr.responsible_id.ids)]
                agr.task_id = self.env['project.task'].create(task_vals).id
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
    task_id = fields.Many2one('project.task', string="Tarea", readonly=True)
