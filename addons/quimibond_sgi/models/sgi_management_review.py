# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

from .sgi_health_const import HEALTH_MODES
from .sgi_risk import SGI_HIGH_ATTENTION
from .sgi_menu_paths import sgi_menu_path

# 57.97.0 (N-09, ISO 9001 9.3.3; 14001 y 45001 9.3): sin estas salidas la
# revisión no se marca realizada.
SGI_REVIEW_CONCLUSIONS = ('conclusion_suitability', 'conclusion_adequacy',
                          'conclusion_effectiveness', 'output_needs')


class SgiManagementReview(models.Model):
    """Revisión por la dirección: carga de entradas (auditorías, NC, indicadores, quejas, riesgos…),
    acuerdos y cierre."""
    _name = 'sgi.management.review'
    _description = "Revisión por la Dirección (IT-P-A10-01)"
    # 57.67.0: ``hr.mixin``: asistentes (Many2many a hr.employee) sin ser de
    # RH (Odoo 19).
    _inherit = ['sgi.base.mixin', 'hr.mixin']
    _order = 'date desc, folio desc'
    _sgi_sequence_code = 'sgi.management.review'
    _sgi_locked_states = ('cerrada',)

    _folio_uniq = models.Constraint(
        'unique(folio)',
        "Ya existe una revisión por la dirección con ese folio.",
    )

    name = fields.Char(string="Nombre", compute='_compute_name', store=True)
    date = fields.Date(string="Fecha", default=fields.Date.context_today, required=True,
                       help="Fecha de la reunión.")
    period_from = fields.Date(string="Periodo desde", required=True, help="Inicio del periodo que se revisa.")
    period_to = fields.Date(string="Periodo hasta", required=True, help="Fin del periodo que se revisa.")
    attendee_ids = fields.Many2many('hr.employee', 'sgi_review_attendee_rel',
                                    'review_id', 'employee_id', string="Asistentes",
                                    help="Personas que asistieron a la revisión.")
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('realizada', "Realizada"),
        ('cerrada', "Cerrada"),
    ], string="Estado", default='borrador', required=True, tracking=True,
        help="Borrador mientras se prepara; realizada al marcarla hecha (sus acuerdos pasan a acciones); "
             "cerrada por el Jefe MAST y SGI.")

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
        string="Auditorías del periodo",
        help="Auditorías que cubre la revisión. «Cargar entradas» las toma del periodo; se pueden ajustar.")
    kpi_red_measure_ids = fields.Many2many('sgi.indicator.measure', 'sgi_review_kpi_rel',
                                           'review_id', 'measure_id',
                                           string="5. Indicadores en rojo", readonly=True,
                                           help="Mediciones en rojo del periodo. Se llenan con «Cargar "
                                                "entradas».")
    supplier_summary = fields.Text(string="6. Proveedores", readonly=True)
    risk_high_ids = fields.Many2many('sgi.risk', 'sgi_review_risk_rel',
                                     'review_id', 'risk_id',
                                     string="7. Riesgos de atención inmediata/alta", readonly=True,
                                     help="Riesgos de atención inmediata o alta. Se llenan con «Cargar "
                                          "entradas».")
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
    # 57.97.0 (N-09): las entradas que faltaban (45001 9.3 d, 9.3.2 b,
    # 14001 9.3, 9.3.2 f). Snapshot de «Cargar entradas».
    incidents_summary = fields.Text(
        string="15. Incidentes y desempeño de SST", readonly=True,
        help="45001 9.3: incidentes del periodo por tipo y severidad, días perdidos, abiertos, IPER de "
             "riesgo alto sin acción y permisos de trabajo vencidos. Solo conteos, sin nombres.")
    context_summary = fields.Text(
        string="16. Cambios en el contexto y las partes interesadas", readonly=True,
        help="9.3.2 b: partes interesadas nuevas y revisadas, revisiones vencidas y cuestiones FODA "
             "nuevas o evaluadas en el periodo.")
    env_aspects_summary = fields.Text(
        string="17. Aspectos ambientales significativos", readonly=True,
        help="14001 9.3: aspectos significativos de la matriz, sin control operacional o en evaluación.")
    improvement_summary = fields.Text(
        string="18. Oportunidades de mejora", readonly=True,
        help="9.3.2 f: propuestas de Mejora Continua, oportunidades de la matriz de riesgos y de las "
             "auditorías del periodo.")

    # Salidas
    agreement_ids = fields.One2many('sgi.management.review.agreement', 'review_id',
                                    string="Acuerdos")
    # 57.97.0 (N-09): los acuerdos abiertos de revisiones anteriores pasan a
    # esta. Siguen siendo de su revisión (E1-02 los mide en su fecha límite).
    carried_agreement_ids = fields.Many2many(
        'sgi.management.review.agreement', 'sgi_review_carried_agreement_rel',
        'review_id', 'agreement_id', string="Acuerdos abiertos de revisiones anteriores",
        readonly=True,
        help="Acuerdos de revisiones ya realizadas o cerradas que siguen sin cumplirse. Se cargan con "
             "«Cargar entradas»; cada uno sigue siendo de su revisión.")
    # 57.97.0 (N-09): conclusiones y salidas 9.3.3.
    conclusion_suitability = fields.Text(
        string="Conveniencia",
        help="¿El SGI sigue siendo conveniente para la empresa y su contexto? Obligatoria para marcar la "
             "revisión como Realizada; si no hay cambios, escríbalo.")
    conclusion_adequacy = fields.Text(
        string="Adecuación",
        help="¿El SGI cubre lo que la empresa necesita (procesos, requisitos, partes interesadas)? "
             "Obligatoria para marcar la revisión como Realizada.")
    conclusion_effectiveness = fields.Text(
        string="Eficacia",
        help="¿El SGI logra los resultados previstos (objetivos, indicadores, NC, incidentes)? "
             "Obligatoria para marcar la revisión como Realizada.")
    output_needs = fields.Text(
        string="Mejora, cambios y recursos",
        help="Decisiones sobre oportunidades de mejora, cambios al SGI y recursos que se necesitan "
             "(9.3.3 a, b y c). Obligatoria para marcar la revisión como Realizada.")


    # V-M07 (57.42.0): título legible de la ficha (el folio va debajo).
    sgi_heading = fields.Char(string="Título", compute='_compute_sgi_heading')

    @api.depends('period_from', 'period_to')
    def _compute_sgi_heading(self):
        for review in self:
            if review.period_from and review.period_to:
                review.sgi_heading = "Revisión por la dirección · %s a %s" % (
                    review.period_from.strftime('%d/%m/%Y'), review.period_to.strftime('%d/%m/%Y'))
            else:
                review.sgi_heading = "Revisión por la dirección"

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
                'incidents_summary': review._sgi_load_incidents(),
                'context_summary': review._sgi_load_context(),
                'env_aspects_summary': review._sgi_load_env_aspects(),
                'improvement_summary': review._sgi_load_improvements(),
                'carried_agreement_ids': [(6, 0, review._sgi_open_previous_agreements().ids)],
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

    @staticmethod
    def _sgi_count_by(records, field_name):
        """«Etiqueta: n, …» en el orden de la lista de selección."""
        counts = {}
        for value in records.mapped(field_name):
            counts[value] = counts.get(value, 0) + 1
        return ", ".join("%s: %d" % (label, counts[key])
                         for key, label in records._fields[field_name].selection if counts.get(key))

    @staticmethod
    def _sgi_names(records, limit=10):
        """Los nombres de los más nuevos primero, «… y N más». Solo para
        registros que no son personas."""
        names = records.sorted('id', reverse=True).mapped('display_name')
        if len(names) > limit:
            return "%s y %d más" % (", ".join(names[:limit]), len(names) - limit)
        return ", ".join(names)

    def _sgi_load_incidents(self):
        """Entrada 15 (45001 9.3 d): incidentes y desempeño de SST. Solo
        conteos: el incidente lo leen quien lo reportó, el Jefe MAST, Salud
        ocupacional y el Auditor; se cuenta con sudo y no se nombra nada."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        Incident = self.env['sgi.incident'].sudo()
        incidents = Incident.search([('date', '>=', dt_from), ('date', '<', dt_to)])
        parts = []
        if incidents:
            parts.append("Incidentes en el periodo: %d (%s)." % (
                len(incidents), self._sgi_count_by(incidents, 'incident_type')))
            parts.append("Por severidad: %s." % self._sgi_count_by(incidents, 'severity'))
            parts.append("Días perdidos: %d." % sum(incidents.mapped('days_lost')))
        else:
            parts.append("Sin incidentes registrados en el periodo.")
        parts.append("Incidentes abiertos hoy: %d." % Incident.search_count([('state', '!=', 'cerrado')]))
        parts.append("Verificados como eficaces en el periodo: %d." % Incident.search_count([
            ('sgi_effective', '=', 'eficaz'), ('sgi_effectiveness_date', '>=', self.period_from),
            ('sgi_effectiveness_date', '<=', self.period_to)]))
        parts.append("IPER de riesgo alto sin acción abierta: %d." % self.env['sgi.risk'].sudo().search_count([
            ('instrument', '=', 'iper'), ('high_without_action', '=', True)]))
        # D-03: el permiso sí tiene empresa (regla multiempresa); con sudo, explícita.
        parts.append("Permisos de trabajo vencidos sin cerrar: %d." % self.env['sgi.work.permit'].sudo()
                     .search_count([('expired', '=', True),
                                    ('company_id', '=', self.env['sgi.config']._sgi_company().id)]))
        return "\n".join(parts)

    def _sgi_load_context(self):
        """Entrada 16 (9.3.2 b): cambios en las cuestiones externas e internas
        (FODA) y en las partes interesadas."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        today = fields.Date.context_today(self)
        Party = self.env['sgi.interested.party'].sudo()
        parts = []
        total = Party.search_count([])
        if not total:
            parts.append("Sin partes interesadas registradas (%s)." % sgi_menu_path('partes_interesadas'))
        else:
            reviewed = Party.search_count([('last_review_date', '>=', self.period_from),
                                           ('last_review_date', '<=', self.period_to)])
            overdue = Party.search_count([('next_review_date', '!=', False),
                                          ('next_review_date', '<', today)])
            parts.append("Partes interesadas: %d; revisadas en el periodo: %d; con revisión vencida "
                         "hoy: %d." % (total, reviewed, overdue))
            new = Party.search([('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
            if new:
                parts.append("Nuevas en el periodo: %s." % self._sgi_names(new))
        Risk = self.env['sgi.risk'].sudo()
        foda = Risk.search([
            ('instrument', '=', 'foda'), '|',
            '&', ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
            '&', ('last_eval_date', '>=', self.period_from), ('last_eval_date', '<=', self.period_to)])
        parts.append("FODA: %d cuestión(es); nuevas o evaluadas en el periodo: %s." % (
            Risk.search_count([('instrument', '=', 'foda')]),
            self._sgi_names(foda) if foda else "ninguna"))
        return "\n".join(parts)

    def _sgi_load_env_aspects(self):
        """Entrada 17 (14001 9.3 y 6.1.2): aspectos significativos de la
        matriz de la empresa del SGI (D-03)."""
        self.ensure_one()
        company = self.env['sgi.config']._sgi_company()
        Aspect = self.env['sgi.env.aspect'].sudo()
        domain = [('company_id', '=', company.id), ('state', '!=', 'obsoleto')]
        total = Aspect.search_count(domain)
        if not total:
            parts = ["Sin aspectos ambientales en la matriz (%s)." % sgi_menu_path('aspectos_ambientales')]
        else:
            significant = Aspect.search(domain + [('significant', '=', True)])
            levels = dict(Aspect._fields['level'].selection)
            no_control = significant.filtered(
                lambda a: not a.operational_control_id and not (a.control_description or '').strip())
            parts = [
                "Aspectos en la matriz: %d; significativos: %d (%s)." % (
                    total, len(significant), self._sgi_count_by(significant, 'level') or "-"),
                "Significativos sin control operacional: %d. En evaluación: %d." % (
                    len(no_control), Aspect.search_count(domain + [('state', '=', 'borrador')])),
            ]
            for aspect in significant[:15]:
                parts.append("• %s %s (%s)" % (aspect.folio or '', aspect.name,
                                               levels.get(aspect.level, '-')))
            if len(significant) > 15:
                parts.append("… y %d más." % (len(significant) - 15))
        # Activos o archivados, como los cuenta el «Traspaso de riesgos ambientales» (57.96.0).
        legacy = self.env['sgi.risk'].sudo().with_context(active_test=False).search_count([
            ('instrument', '=', 'ambiental'), ('sgi_env_aspect_ids', '=', False)])
        if legacy:
            parts.append("Riesgos ambientales que aún no pasan a la matriz: %d (Traspaso de riesgos "
                         "ambientales)." % legacy)
        return "\n".join(parts)

    def _sgi_load_improvements(self):
        """Entrada 18 (9.3.2 f, 10.3): oportunidades de mejora. Solo el
        proyecto «Mejora Continua SGI» (el de Diseño y Desarrollo también
        trae ``sgi_is_improvement``)."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        parts = []
        project = self.env.ref('quimibond_sgi.sgi_project_improvement', raise_if_not_found=False)
        if project:
            Task = self.env['project.task'].sudo()
            base = [('project_id', '=', project.id)]
            new = Task.search(base + [('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
            done = Task.search_count(base + [('stage_id.sgi_is_done_stage', '=', True),
                                             ('date_last_stage_update', '>=', dt_from),
                                             ('date_last_stage_update', '<', dt_to)])
            open_now = Task.search_count(base + [('stage_id.sgi_is_done_stage', '=', False)])
            parts.append("Mejora Continua SGI: %d propuesta(s) nuevas, %d terminadas en el periodo, "
                         "%d abiertas hoy." % (len(new), done, open_now))
            if new:
                parts.append("Nuevas: %s." % self._sgi_names(new))
        opportunities = self.env['sgi.risk'].sudo().search([
            ('kind', '=', 'oportunidad'), ('state', '!=', 'cerrado')])
        parts.append("Oportunidades abiertas en la matriz de riesgos: %d%s" % (
            len(opportunities), (": %s." % self._sgi_names(opportunities)) if opportunities else "."))
        findings = self._sgi_period_audits().sudo().finding_ids.filtered(
            lambda f: f.finding_type == 'oportunidad')
        parts.append("Oportunidades de mejora de las auditorías del periodo: %d." % len(findings))
        return "\n".join(parts)

    def _sgi_load_legal(self):
        """Entrada 11 (14001/45001 9.3): foto del cumplimiento legal."""
        self.ensure_one()
        Requirement = self.env['sgi.legal.requirement']
        total = Requirement.search_count([])
        if not total:
            return ("Sin requisitos legales registrados: capture la matriz "
                    "legal (%s)." % sgi_menu_path('requisitos_legales'))
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

    def _sgi_open_previous_agreements(self):
        """57.97.0 (N-09): acuerdos sin cumplir de cualquier revisión anterior
        ya realizada o cerrada. «Cumplido» como en E1-02: su acción terminada
        o la fecha de cumplimiento capturada a mano."""
        self.ensure_one()
        agreements = self.env['sgi.management.review.agreement'].search([
            ('review_id', '!=', self.id),
            ('review_id.state', 'in', ('realizada', 'cerrada')),
            ('review_id.date', '<=', self.date)])
        return agreements.filtered(lambda a: not a.is_done and not a.done_date)

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
        return "\n".join(result) or "Sin no conformidades."

    def _sgi_load_complaints(self):
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        if not self.env['helpdesk.team'].search_count([('sgi_is_complaint', '=', True)]):
            return "Sin equipo de reclamaciones configurado."
        tickets = self.env['helpdesk.ticket'].search(
            self.env['helpdesk.team']._sgi_complaint_domain() + [
                ('create_date', '>=', dt_from), ('create_date', '<', dt_to)])
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
        # 57.99.0: los rojos de salud del SGI no entran a la revisión.
        return self.env['sgi.indicator.measure'].search([
            ('semaphore', '=', 'rojo'),
            ('indicator_id.calc_mode', 'not in', HEALTH_MODES),
            ('period_date', '>=', self.period_from),
            ('period_date', '<=', self.period_to),
        ])

    def _sgi_load_suppliers(self):
        self.ensure_one()
        Partner = self.env['res.partner']
        counts = []
        for key, label in (('acreditado', "Acreditados"), ('condicionado', "Condicionados"),
                           ('baja', "Baja"), ('sin_datos', "Sin datos")):
            counts.append("%s: %d" % (label, Partner.search_count([
                ('sgi_supplier_class', '=', key)])))
        return "Proveedores por clase — %s." % ", ".join(counts)

    def _sgi_load_high_risks(self):
        self.ensure_one()
        return self.env['sgi.risk'].search([
            ('attention_level', 'in', list(SGI_HIGH_ATTENTION)),
            ('state', '!=', 'cerrado'),
        ])

    def _sgi_scrap_domain(self):
        """57.97.0 (N-09, D-03): el scrap del periodo de la empresa del SGI.
        Antes se buscaba sin empresa y podía sumar el de otras razones
        sociales activas en la sesión."""
        self.ensure_one()
        dt_from, dt_to = self._sgi_bounds()
        company = self.env['sgi.config']._sgi_company()
        return [('company_id', '=', company.id), ('state', '=', 'done'),
                ('date_done', '>=', dt_from), ('date_done', '<', dt_to)]

    def _sgi_load_env(self):
        self.ensure_one()
        company = self.env['sgi.config']._sgi_company()
        # sudo: Dirección no siempre tiene Inventario; la empresa va explícita.
        scraps = self.env['stock.scrap'].sudo().search(self._sgi_scrap_domain())
        if not scraps:
            return "Sin registros de scrap de %s en el periodo." % company.name
        by_reason = {}
        for scrap in scraps:
            reason = ", ".join(scrap.scrap_reason_tag_ids.mapped('name')) or "Sin motivo"
            by_reason[reason] = by_reason.get(reason, 0.0) + scrap.scrap_qty
        lines = ["Scrap de %s por motivo (%d movimientos):" % (company.name, len(scraps))]
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
    def _sgi_check_conclusions(self):
        """57.97.0 (N-09): las conclusiones 9.3.3 no vacías (ni solo espacios).
        Solo al marcar realizada (el cambio de estado), no en write."""
        for review in self:
            missing = [review._fields[name].string for name in SGI_REVIEW_CONCLUSIONS
                       if not (review[name] or '').strip()]
            if missing:
                raise UserError(
                    "No se puede marcar como Realizada la revisión %s sin las conclusiones (ISO 9.3.3): "
                    "%s. Captúrelas en la pestaña «Conclusiones (9.3.3)»; si no hay cambios, "
                    "escríbalo («Sin cambios»)." % (review.folio or review.name, ", ".join(missing)))

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
                    "accionables). Complete: %s" % ", ".join(
                        incomplete.mapped('name')))
            review._sgi_check_conclusions()
            # DIR-3 (52.0.0): cada acuerdo es una ACCIÓN del SGI (sgi.action.line)
            # con responsable y compromiso: actividad nativa al responsable,
            # escalamiento del cron de acciones vencidas y medición de E1-02.
            # 57.97.0 (N-09): del tipo «acuerdo» (antes «correctiva»): no infla
            # las acciones correctivas ni pide su evidencia.
            Line = self.env['sgi.action.line']
            for agr in review.agreement_ids:
                if agr.action_line_id:
                    continue
                agr.action_line_id = Line.create({
                    'review_id': review.id,
                    'action_type': 'acuerdo',
                    'name': agr.name,
                    'responsible_id': agr.responsible_id.id,
                    'date_commit': agr.deadline,
                }).id
            review.state = 'realizada'
        return True

    def _sgi_check_mast(self):
        """D-009 (entrega 4): cerrar y reabrir la revisión es del Jefe MAST.
        Dirección la prepara, la marca realizada y la consulta."""
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise UserError("Solo el Jefe MAST cierra o regresa a borrador una revisión por la dirección.")

    def action_close(self):
        self._sgi_check_mast()
        self.write({'state': 'cerrada'})
        # 57.97.0 (N-09): cerrar no se bloquea; los acuerdos abiertos quedan en
        # el historial y la siguiente revisión los carga («Cargar entradas»).
        for review in self:
            pending = review.agreement_ids.filtered(lambda a: not a.is_done and not a.done_date)
            if not pending:
                continue
            items = Markup("<br/>").join(
                Markup("• %s (responsable: %s, límite: %s)") % (
                    agr.name, agr.responsible_id.name or '-', agr.deadline or '-')
                for agr in pending)
            review.message_post(body=Markup(
                "<b>Acuerdos abiertos al cerrar:</b> pasan a la siguiente revisión por la "
                "dirección (se cargan con «Cargar entradas»).<br/>%s") % items)

    def sgi_minutes_filename(self):
        """57.101.0 (B3): nombre de la copia guardada del acta cerrada (la
        expresión ``attachment`` del reporte la llama). Es el mismo nombre que
        el de la descarga (``print_report_name``) más «.pdf»."""
        self.ensure_one()
        return "Acta de revisión por la dirección - %s.pdf" % (self.folio or self.name)

    def _sgi_retire_stored_minutes(self):
        """57.101.0 (B3): al reabrir un acta cerrada, su copia guardada se
        renombra (no se borra) para que al cerrarla otra vez se guarde la
        nueva; la anterior queda como evidencia."""
        Attachment = self.env['ir.attachment'].sudo()
        stamp = fields.Date.context_today(self).strftime('%d-%m-%Y')
        for review in self.filtered(lambda r: r.state == 'cerrada'):
            name = review.sgi_minutes_filename()
            stored = Attachment.search([('res_model', '=', review._name),
                                        ('res_id', '=', review.id), ('name', '=', name)])
            if not stored:
                continue
            new_name = "%s (reabierta el %s).pdf" % (name[:-4], stamp)
            stored.write({'name': new_name})
            review.message_post(body="Se reabrió el acta cerrada: su copia guardada queda como "
                                     "«%s». Al cerrarla de nuevo se guarda la nueva." % new_name)

    def action_draft(self):
        self._sgi_check_mast()
        self._sgi_retire_stored_minutes()
        self.write({'state': 'borrador'})


class SgiManagementReviewAgreement(models.Model):
    """Acuerdo de una revisión por la dirección con responsable y fecha; se sigue como acción o
    tarea."""
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
    is_done = fields.Boolean(string="Cumplido", compute='_compute_status')
    status_label = fields.Char(string="Situación", compute='_compute_status')

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

    def _sgi_origin_closed(self):
        """57.93.0 (K-03): un acuerdo terminado de una revisión cerrada es evidencia."""
        self.ensure_one()
        if self.review_id:
            return self.sudo().review_id.state == 'cerrada'
        return super()._sgi_origin_closed()

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

    @api.constrains('action_type', 'review_id')
    def _check_acuerdo_only_in_review(self):
        """57.97.0 (N-09): «Acuerdo» es solo para los acuerdos de una revisión
        por la dirección."""
        wrong = self.filtered(lambda l: l.action_type == 'acuerdo' and not l.review_id)
        if wrong:
            raise ValidationError(
                "El tipo «Acuerdo de la revisión por la dirección» es solo para los acuerdos de una "
                "revisión por la dirección. Elija otro tipo para: %s" % ", ".join(wrong.mapped('name')))
