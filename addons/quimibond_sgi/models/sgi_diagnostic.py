# -*- coding: utf-8 -*-
"""Diagnóstico del SGI: el módulo se audita a sí mismo.

Una corrida del Jefe MAST que ejecuta las verificaciones de configuración y
adopción que de otro modo requieren revisar modelo por modelo: dueños de
proceso, responsables de KPI, validación de mediciones, difusión documental,
política, riesgos, CAPA, NCs fuera del flujo, presupuestos sin aprobar,
fuentes apagadas, embudo de reclamaciones y ajustes clave. Solo lectura:
reporta y dice dónde se arregla cada cosa.

Desde 19.0.47.1.0 cada hallazgo es una fila de `sgi.diagnostic.line` y el
menú abre la lista nativa (buscar, filtrar por nivel, agrupar por sección);
antes era un HTML armado a mano en un wizard.
"""
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api

from .sgi_menu_paths import sgi_menu_path

LEVELS = [('bad', 'Falla'), ('warn', 'Aviso'), ('ok', 'Bien')]
_LEVEL_ORDER = {'bad': 0, 'warn': 1, 'ok': 2}


class SgiDiagnosticLine(models.TransientModel):
    """Hallazgo de una corrida del diagnóstico, con cómo corregirlo."""
    _name = 'sgi.diagnostic.line'
    _description = "Hallazgo del diagnóstico del SGI"
    _order = 'sequence, id'

    diagnostic_id = fields.Many2one(
        'sgi.diagnostic', string="Diagnóstico", required=True, ondelete='cascade', index=True,
        help="Corrida del diagnóstico a la que pertenece el hallazgo.")
    sequence = fields.Integer(default=10)
    section = fields.Char(string="Sección", required=True)
    level = fields.Selection(LEVELS, string="Nivel", required=True, help="Qué tan grave es el hallazgo.")
    text = fields.Text(string="Hallazgo", required=True)
    fix = fields.Char(string="Dónde se arregla")


class SgiDiagnostic(models.TransientModel):
    """Diagnóstico de configuración y adopción del SGI: una corrida con hallazgos por sección (bien,
    aviso, mal). Pantalla, no historia."""
    _name = 'sgi.diagnostic'
    _description = "Diagnóstico de configuración y adopción del SGI"

    date = fields.Date(string="Fecha", default=fields.Date.context_today, readonly=True)
    line_ids = fields.One2many('sgi.diagnostic.line', 'diagnostic_id', string="Hallazgos")
    bad_count = fields.Integer(string="Fallas", compute='_compute_counts')
    warn_count = fields.Integer(string="Avisos", compute='_compute_counts')
    ok_count = fields.Integer(string="En orden", compute='_compute_counts')
    summary = fields.Text(string="Resumen", compute='_compute_summary')

    @api.depends('line_ids.level')
    def _compute_counts(self):
        for rec in self:
            rec.bad_count = len(rec.line_ids.filtered(lambda l: l.level == 'bad'))
            rec.warn_count = len(rec.line_ids.filtered(lambda l: l.level == 'warn'))
            rec.ok_count = len(rec.line_ids.filtered(lambda l: l.level == 'ok'))

    @api.depends('date', 'line_ids.section', 'line_ids.text', 'line_ids.fix')
    def _compute_summary(self):
        """Texto plano con lo mismo que la lista (para el chatter, el log y las
        pruebas): «Diagnóstico del <fecha>» y una línea por hallazgo."""
        for rec in self:
            parts = ["Diagnóstico del %s. Solo lectura: nada se modifica; "
                     "cada punto dice dónde se arregla." % rec.date]
            section = None
            for line in rec.line_ids:
                if line.section != section:
                    section = line.section
                    parts.append("== %s ==" % section)
                parts.append("[%s] %s%s" % (
                    dict(LEVELS)[line.level], line.text,
                    (" — %s" % line.fix) if line.fix else ""))
            rec.summary = "\n".join(parts)

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        for rec in records:
            if not rec.line_ids:
                rec._sgi_fill()
        return records

    def _sgi_fill(self):
        """Corre las verificaciones y deja una fila por hallazgo."""
        self.ensure_one()
        self.line_ids.unlink()
        # D-18 (entrega 4): el Auditor y Dirección leen el Diagnóstico. Las
        # verificaciones solo cuentan y nombran (no escriben nada); para quien
        # no es Jefe MAST corren con sudo para no depender de las ACL de cada
        # app. Las filas se crean con el usuario (son suyas).
        builder = self if self.env.user.has_group('quimibond_sgi.group_sgi_manager') else self.sudo()
        rows = builder._sgi_build_report()
        self.env['sgi.diagnostic.line'].create([
            dict(row, diagnostic_id=self.id, sequence=seq * 10)
            for seq, row in enumerate(rows, start=1)])
        return self.line_ids

    @api.model
    def action_run(self):
        """Menú Diagnóstico del SGI: corre el diagnóstico y abre la lista
        nativa de hallazgos (fallas primero, agrupadas por sección)."""
        diag = self.create({})
        action = self.env['ir.actions.act_window']._for_xml_id(
            'quimibond_sgi.sgi_diagnostic_line_action')
        action['domain'] = [('diagnostic_id', '=', diag.id)]
        action['context'] = {
            'search_default_group_section': 1,
            'default_diagnostic_id': diag.id,
        }
        action['name'] = action['display_name'] = (
            "Diagnóstico del SGI (%s: %d fallas, %d avisos)" % (
                diag.date, diag.bad_count, diag.warn_count))
        return action

    def action_refresh(self):
        self.ensure_one()
        self._sgi_fill()
        return True

    # ------------------------------------------------------------------
    # Construcción del reporte
    # ------------------------------------------------------------------
    def _sgi_line(self, level, text, fix=None):
        """Un hallazgo: nivel (bad / warn / ok), texto y dónde se arregla."""
        assert level in _LEVEL_ORDER
        return {'level': level, 'text': text, 'fix': fix or False}

    @api.model
    def _sgi_key_settings_checks(self):
        """[(clave de parámetro, aviso si vale 0)] de «Ajustes clave» que
        agregan otros módulos (la lista presupuestal, quimibond_ventas_presupuesto)."""
        return []

    @api.model
    def _sgi_floor_quality_lines(self, floor_alerts):
        """Hallazgos de «Calidad preventiva y piso» que vienen de un satélite
        (el revisado de tela, quimibond_sgi_revisado). ``floor_alerts``:
        alertas de calidad de piso (sin folio de NC)."""
        return []

    @api.model
    def _sgi_build_report(self):
        """Lista de dicts (section, level, text, fix) en el orden del reporte."""
        env = self.env
        today = fields.Date.context_today(self)
        sections = []

        def section(title, lines):
            for line in lines:
                sections.append(dict(line, section=title))

        # ---- 1. Procesos -------------------------------------------------
        lines = []
        no_owner = env['sgi.process'].search(
            [('parent_id', '!=', False), ('owner_id', '=', False), ('active', '=', True)])
        if no_owner:
            lines.append(self._sgi_line(
                'bad', "%d proceso(s) sin dueño: %s" % (
                    len(no_owner), ", ".join(no_owner.mapped('code'))),
                sgi_menu_path('mapa_procesos', "campo Dueño (el dueño recibe los avisos de riesgos y salud)")))
        else:
            lines.append(self._sgi_line('ok', "Todos los procesos tienen dueño."))
        no_activities = env['sgi.process'].search(
            [('parent_id', '!=', False), ('active', '=', True)]).filtered(
            lambda p: not p.procedure_activity_ids)
        if no_activities:
            lines.append(self._sgi_line(
                'warn', "%d proceso(s) sin procedimiento vivo (0 actividades): %s" % (
                    len(no_activities), ", ".join(no_activities.mapped('code'))),
                "pestaña Procedimiento del proceso — su procedimiento sigue siendo solo PDF"))
        section("Procesos", lines)

        # ---- 2. Indicadores y mediciones --------------------------------
        lines = []
        Ind = env['sgi.indicator']
        no_resp = Ind.search_count([('responsible_id', '=', False)])
        no_proc = Ind.search_count([('process_id', '=', False)])
        if no_resp:
            lines.append(self._sgi_line(
                'bad', "%d indicador(es) sin responsable: sus capturas y validaciones caen todas en MAST." % no_resp,
                sgi_menu_path('indicadores')))
        if no_proc:
            lines.append(self._sgi_line(
                'bad', "%d indicador(es) sin proceso: sus rojos NO cuentan en la salud del Panel." % no_proc,
                sgi_menu_path('indicadores')))
        Measure = env['sgi.indicator.measure']
        pend = Measure.search_count([('state', '=', 'pendiente')])
        capt = Measure.search_count([('state', '=', 'capturado')])
        valid = Measure.search_count([('state', '=', 'validado')])
        if capt and not valid:
            lines.append(self._sgi_line(
                'bad', "Hay %d medición(es) capturadas y NINGUNA validada: sin validación no hay NC automática, ni rojos en la salud, ni entrada 5 de la RxD." % capt,
                "botón Validar en cada medición (responsable del KPI o MAST)"))
        if pend:
            lines.append(self._sgi_line(
                'warn', "%d medición(es) pendientes de captura manual." % pend,
                sgi_menu_path('mediciones', "filtro Pendientes")))
        groups = Measure._read_group(
            [('state', '=', 'pendiente')], ['indicator_id'], ['__count'])
        chronic = [ind.code for ind, count in groups if count >= 3]
        if chronic:
            lines.append(self._sgi_line(
                'warn', "KPIs con 3+ periodos sin capturar (automatizar, reasignar o eliminar): %s" % ", ".join(chronic)))
        if not lines:
            lines.append(self._sgi_line('ok', "Indicadores configurados y mediciones al día."))
        section("Indicadores y mediciones", lines)

        # ---- 3. Documental ----------------------------------------------
        lines = []
        Doc = env['documents.document']
        vigentes = Doc.search_count(
            [('sgi_is_controlled', '=', True), ('sgi_state', '=', 'vigente')])
        sin_puestos = Doc.search_count(
            [('sgi_is_controlled', '=', True), ('sgi_state', '=', 'vigente'),
             ('sgi_job_ids', '=', False)])
        if sin_puestos:
            lines.append(self._sgi_line(
                'warn', "%d de %d documentos vigentes sin puestos asignados: sin puestos no hay acuses ni «Mis procedimientos»." % (sin_puestos, vigentes),
                "pestaña Puestos que aplican del documento"))
        acks_pend = env['sgi.document.ack'].search_count([('state', '=', 'pendiente')])
        acks_total = env['sgi.document.ack'].search_count([])
        if not acks_total:
            lines.append(self._sgi_line(
                'bad', "Cero acuses de lectura generados: la difusión documental no ha iniciado.",
                "asignar puestos y botón Generar acuses"))
        elif acks_pend:
            lines.append(self._sgi_line(
                'warn', "%d acuse(s) de lectura pendientes de firma." % acks_pend))
        doc_changes = env['approval.request'].search_count(
            [('sgi_is_doc_change', '=', True)])
        if not doc_changes:
            lines.append(self._sgi_line(
                'warn', "Ninguna revisión documental ha pasado por el flujo de Aprobaciones (F-P-G01-06).",
                sgi_menu_path('solicitudes_cambio')))
        if not lines:
            lines.append(self._sgi_line('ok', "Difusión documental operando."))
        section("Documental", lines)

        # ---- 4. Estrategia y planificación -------------------------------
        lines = []
        if not env['sgi.policy'].search_count([('state', '=', 'vigente')]):
            lines.append(self._sgi_line(
                'bad', "No hay Política Integral vigente (la cascada Política → Objetivos → KPIs arranca ahí).",
                sgi_menu_path('politica')))
        if not env['sgi.risk'].search_count([]):
            lines.append(self._sgi_line(
                'bad', "Cero riesgos/oportunidades registrados (6.1 sin evidencia operativa).",
                sgi_menu_path('riesgos')))
        if 'sgi.emergency.plan' in env and not env['sgi.emergency.plan'].search_count(
                [('state', '=', 'vigente')]):
            lines.append(self._sgi_line(
                'warn', "No hay planes de emergencia vigentes (14001/45001 8.2).",
                sgi_menu_path('planes_emergencia')))
        # 57.11.0 (A-016): el presupuesto es de quimibond_ventas_presupuesto.
        budgets_draft = env['sgi.sales.budget'].search_count(
            [('kind', '=', 'presupuesto'), ('state', '=', 'borrador'),
             ('year', '=', today.year)]) if 'sgi.sales.budget' in env else 0
        if budgets_draft:
            lines.append(self._sgi_line(
                'warn', "%d presupuesto(s) de ventas %d en borrador: el KPI VE-02 y el cierre de mes solo miden presupuestos APROBADOS." % (budgets_draft, today.year),
                "revisar precios de lista y aprobar (Dirección)"))
        if not lines:
            lines.append(self._sgi_line('ok', "Estrategia y planificación en orden."))
        section("Estrategia y planificación", lines)

        # ---- 4b. Calidad preventiva y piso --------------------------------
        lines = []
        empty_plans = env['sgi.control.plan'].search(
            [('state', '=', 'vigente')]).filtered(lambda p: not p.point_ids)
        if empty_plans:
            lines.append(self._sgi_line(
                'bad', "%d plan(es) de control vigentes con 0 puntos: %s — sin puntos ligados no hay CoA ni cadena IATF." % (
                    len(empty_plans), ", ".join(empty_plans.mapped('folio'))),
                "botón «Ligar puntos sueltos» en el plan"))
        orphan_points = env['quality.point'].search_count(
            [('sgi_control_plan_id', '=', False)])
        if orphan_points:
            lines.append(self._sgi_line(
                'warn', "%d punto(s) de calidad del piso sin plan de control." % orphan_points,
                "campo «SGI - Plan de control» del punto"))
        measuring = env['maintenance.equipment'].search_count(
            [('sgi_is_measuring', '=', True)])
        if not measuring:
            lines.append(self._sgi_line(
                'bad', "Cero equipos marcados como «de medición»: metrología vacía — sin calibraciones vigiladas, la constraint IATF no protege ninguna inspección y los MSA no tienen equipo.",
                "marcar «Equipo de medición» en Mantenimiento → Equipos"))
        Maint = env['maintenance.request']
        since90 = fields.Datetime.now() - relativedelta(days=90)
        corr_total = Maint.search_count(
            [('maintenance_type', '=', 'corrective'), ('create_date', '>=', since90)])
        corr_no_eq = Maint.search_count(
            [('maintenance_type', '=', 'corrective'), ('create_date', '>=', since90),
             ('equipment_id', '=', False)])
        if corr_total and corr_no_eq / corr_total > 0.5:
            lines.append(self._sgi_line(
                'warn', "%d de %d correctivas de los últimos 90 días sin equipo asignado: la señal de falla repetitiva no puede detectarlas." % (corr_no_eq, corr_total),
                "capturar el equipo en la solicitud de mantenimiento"))
        floor_alerts = env['quality.alert'].search_count([('sgi_folio', '=', False)])
        # 57.10.0 (A-019): lo del revisado de tela lo agrega quimibond_sgi_revisado.
        lines += self._sgi_floor_quality_lines(floor_alerts)
        if not lines:
            lines.append(self._sgi_line('ok', "Calidad preventiva conectada al piso."))
        section("Calidad preventiva y piso", lines)

        # ---- 5. Mejora continua ------------------------------------------
        lines = []
        if not env['sgi.action.line'].search_count([]):
            lines.append(self._sgi_line(
                'warn', "Cero acciones CAPA registradas: ningún tratamiento en curso.",))
        Alert = env['quality.alert']
        nc_wrong_team = Alert.search_count(
            [('sgi_folio', '!=', False), ('team_id.sgi_sequence_id', '=', False)])
        if nc_wrong_team:
            lines.append(self._sgi_line(
                'bad', "%d NC con folio en equipos fuera del flujo SGI: no pasarán por los candados de cierre." % nc_wrong_team,
                "mover al equipo NC Internas/Externas"))
        month_ago = fields.Datetime.now() - relativedelta(days=30)
        nc_old = Alert.search_count(
            [('sgi_folio', '!=', False),
             ('stage_id.sgi_is_closing_stage', '=', False),
             ('stage_id.sgi_is_cancel_stage', '=', False),
             ('create_date', '<', month_ago)])
        if nc_old:
            lines.append(self._sgi_line(
                'warn', "%d NC abiertas hace más de 30 días." % nc_old,
                sgi_menu_path('no_conformidades')))
        sources_off = env['sgi.alert.source'].search(
            [('enabled', '=', False), ('suppressed_count', '>', 0)])
        for source in sources_off:
            lines.append(self._sgi_line(
                'warn', "Fuente «%s» apagada con %d NC omitidas: confirmar que sigue siendo intencional." % (
                    source.name, source.suppressed_count),
                sgi_menu_path('fuentes_nc')))
        # D-006: las reclamaciones son los tickets de los equipos marcados
        # «Equipo de reclamaciones (SGI)», no los de un XML ID.
        Ticket = env['helpdesk.ticket']
        complaint_domain = env['helpdesk.team']._sgi_complaint_domain()
        sgi_tickets = Ticket.search_count(complaint_domain)
        others = Ticket.search_count(
            [('team_id.sgi_is_complaint', '=', False),
             ('team_id.name', 'ilike', 'reclama')])
        if not sgi_tickets and others:
            lines.append(self._sgi_line(
                'bad', "Los equipos de reclamaciones del SGI tienen 0 tickets mientras otros equipos con «reclama» en el nombre acumulan %d: el embudo (SLA, Generar NC, KPI CA-01) está desviado." % others,
                "marcar el equipo como «Equipo de reclamaciones (SGI)» en Servicio de asistencia"))
        if not lines:
            lines.append(self._sgi_line('ok', "Mejora continua fluyendo."))
        section("Mejora continua", lines)

        # ---- 5b. Contexto y cumplimiento (4.1/4.2 · 6.1.3 · 7.2) ----------
        lines = []
        if not env['sgi.interested.party'].search_count([]):
            lines.append(self._sgi_line(
                'bad', "Cero partes interesadas registradas (4.2 sin evidencia: es de lo primero que pregunta un auditor de certificación).",
                sgi_menu_path('partes_interesadas')))
        Legal = env['sgi.legal.requirement']
        legal_total = Legal.search_count([])
        if not legal_total:
            lines.append(self._sgi_line(
                'bad', "Cero requisitos legales registrados (14001/45001 6.1.3): sin matriz legal no hay evaluación del cumplimiento.",
                sgi_menu_path('requisitos_legales')))
        else:
            broken = Legal.search_count(
                [('compliance_state', 'in', ('no_cumple', 'parcial'))])
            if broken:
                lines.append(self._sgi_line(
                    'bad', "%d requisito(s) legales en incumplimiento (total o parcial)." % broken,
                    "trate la NC ligada y re-evalúe"))
            overdue = Legal.search_count([
                ('next_eval_date', '!=', False), ('next_eval_date', '<=', today)])
            if overdue:
                lines.append(self._sgi_line(
                    'warn', "%d evaluación(es) de cumplimiento legal vencidas." % overdue))
        if not env['hr.job.skill'].search_count([]):
            lines.append(self._sgi_line(
                'bad', "Cero competencias requeridas por puesto (hr.job.skill): la DNC y el KPI de capacitación no tienen materia prima (7.2).",
                "Empleados → Puestos → pestaña Habilidades"))
        suppliers_pending = env['res.partner'].search_count(
            [('supplier_rank', '>', 0), ('sgi_supplier_status', '=', False)])
        if suppliers_pending:
            lines.append(self._sgi_line(
                'warn', "%d proveedor(es) sin aprobación 8.4.1 (el bloqueo de OC no aplica a nadie): arranque por los que reciben compras hoy." % suppliers_pending,
                "ficha del proveedor → pestaña SGI"))
        # Encuesta de satisfacción divergente: hay respuestas reales en OTRA
        # encuesta de satisfacción distinta de la que alimenta CA-02.
        survey = env['sgi.indicator']._sgi_satisfaction_survey()
        if survey:
            configured_count = env['survey.user_input'].sudo().search_count(
                [('survey_id', '=', survey.id), ('state', '=', 'done')])
            others = env['survey.survey'].sudo().with_context(
                active_test=False).search(
                [('id', '!=', survey.id), ('title', 'ilike', 'satisf')])
            other_count = env['survey.user_input'].sudo().search_count(
                [('survey_id', 'in', others.ids), ('state', '=', 'done')]) \
                if others else 0
            if other_count and not configured_count:
                lines.append(self._sgi_line(
                    'warn', "El KPI CA-02 lee una encuesta con 0 respuestas mientras otra encuesta de satisfacción acumula %d: re-apunte la fuente en Ajustes o declare el corte." % other_count,
                    sgi_menu_path('ajustes', "Encuesta de satisfacción (CA-02)")))
        if not lines:
            lines.append(self._sgi_line('ok', "Contexto, matriz legal y competencias con base capturada."))
        section("Contexto y cumplimiento", lines)

        # ---- 6. Ajustes clave ---------------------------------------------
        lines = []
        Param = env['ir.config_parameter'].sudo()
        checks = [
            ('quimibond_sgi.rh_user_id',
             "Usuario de RH sin configurar: los crons de DNC y certificaciones caen en MAST."),
            ('quimibond_sgi.energy_partner_id',
             "Proveedor de energía sin configurar: el KPI TR-03 queda pendiente."),
            ('quimibond_sgi.production_monthly_capacity',
             "Capacidad instalada sin configurar: el KPI MA-02 queda pendiente."),
        ] + self._sgi_key_settings_checks()
        for key, msg in checks:
            try:
                value = int(float(Param.get_param(key, 0) or 0))
            except (TypeError, ValueError):
                value = 0
            if not value:
                lines.append(self._sgi_line('warn', msg, sgi_menu_path('ajustes')))
        if not lines:
            lines.append(self._sgi_line('ok', "Ajustes clave configurados."))
        section("Ajustes clave", lines)

        return sections
