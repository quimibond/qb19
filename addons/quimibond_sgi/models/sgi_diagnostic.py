# -*- coding: utf-8 -*-
"""Diagnóstico del SGI: el módulo se audita a sí mismo.

Un wizard del Jefe MAST que corre las verificaciones de configuración y
adopción que de otro modo requieren revisar modelo por modelo: dueños de
proceso, responsables de KPI, validación de mediciones, difusión documental,
política, riesgos, CAPA, NCs fuera del flujo, presupuestos sin aprobar,
fuentes apagadas, embudo de reclamaciones y ajustes clave. Solo lectura:
reporta y dice dónde se arregla cada cosa.
"""
from dateutil.relativedelta import relativedelta
from markupsafe import Markup, escape

from odoo import models, fields, api


class SgiDiagnostic(models.TransientModel):
    _name = 'sgi.diagnostic'
    _description = "Diagnóstico de configuración y adopción del SGI"

    result = fields.Html(string="Resultado", readonly=True, sanitize=False)

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        if 'result' in fields_list:
            res['result'] = self._sgi_build_report()
        return res

    def action_refresh(self):
        self.ensure_one()
        self.result = self._sgi_build_report()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'sgi.diagnostic',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    # ------------------------------------------------------------------
    # Construcción del reporte
    # ------------------------------------------------------------------
    _ICONS = {'ok': '✔', 'warn': '⚠', 'bad': '✖'}
    _COLORS = {'ok': '#1e7d3c', 'warn': '#9a6a00', 'bad': '#b3372b'}

    def _sgi_line(self, level, text, fix=None):
        icon = self._ICONS[level]
        color = self._COLORS[level]
        fix_html = (
            Markup(' <span style="color:#888">— %s</span>') % fix if fix else Markup(''))
        return Markup(
            '<li style="margin:3px 0"><span style="color:%s;font-weight:bold">%s</span> %s%s</li>'
        ) % (color, icon, escape(text), fix_html)

    @api.model
    def _sgi_build_report(self):
        env = self.env
        today = fields.Date.context_today(self)
        sections = []

        def section(title, lines):
            if lines:
                sections.append(
                    Markup('<h4 style="margin:14px 0 4px">%s</h4><ul style="margin:0;padding-left:6px;list-style:none">%s</ul>')
                    % (escape(title), Markup('').join(lines)))

        # ---- 1. Procesos -------------------------------------------------
        lines = []
        no_owner = env['sgi.process'].search(
            [('parent_id', '!=', False), ('owner_id', '=', False), ('active', '=', True)])
        if no_owner:
            lines.append(self._sgi_line(
                'bad', "%d proceso(s) sin dueño: %s" % (
                    len(no_owner), ", ".join(no_owner.mapped('code'))),
                "SGI → Procesos → campo Dueño (el dueño recibe los avisos de riesgos y salud)"))
        else:
            lines.append(self._sgi_line('ok', "Todos los procesos tienen dueño."))
        no_activities = env['sgi.process'].search(
            [('parent_id', '!=', False), ('active', '=', True)]).filtered(
            lambda p: not p.activity_ids)
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
                "SGI → Medición → Indicadores"))
        if no_proc:
            lines.append(self._sgi_line(
                'bad', "%d indicador(es) sin proceso: sus rojos NO cuentan en la salud del Panel." % no_proc,
                "SGI → Medición → Indicadores"))
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
                "SGI → Medición → Mediciones, filtro Pendientes"))
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
                "SGI → Documental → Cambios documentales"))
        if not lines:
            lines.append(self._sgi_line('ok', "Difusión documental operando."))
        section("Documental", lines)

        # ---- 4. Estrategia y planificación -------------------------------
        lines = []
        if not env['sgi.policy'].search_count([('state', '=', 'vigente')]):
            lines.append(self._sgi_line(
                'bad', "No hay Política Integral vigente (la cascada Política → Objetivos → KPIs arranca ahí).",
                "SGI → Panel → Política Integral"))
        if not env['sgi.risk'].search_count([]):
            lines.append(self._sgi_line(
                'bad', "Cero riesgos/oportunidades registrados (6.1 sin evidencia operativa).",
                "SGI → Riesgos y auditorías → Riesgos y oportunidades"))
        if 'sgi.emergency.plan' in env and not env['sgi.emergency.plan'].search_count(
                [('state', '=', 'vigente')]):
            lines.append(self._sgi_line(
                'warn', "No hay planes de emergencia vigentes (14001/45001 8.2).",
                "SGI → Riesgos y auditorías → Emergencias"))
        budgets_draft = env['sgi.sales.budget'].search_count(
            [('kind', '=', 'presupuesto'), ('state', '=', 'borrador'),
             ('year', '=', today.year)])
        if budgets_draft:
            lines.append(self._sgi_line(
                'warn', "%d presupuesto(s) de ventas %d en borrador: el KPI VE-02 y el cierre de mes solo miden presupuestos APROBADOS." % (budgets_draft, today.year),
                "revisar precios de lista y aprobar (Dirección)"))
        if not lines:
            lines.append(self._sgi_line('ok', "Estrategia y planificación en orden."))
        section("Estrategia y planificación", lines)

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
                "SGI → Mejora continua → No Conformidades"))
        sources_off = env['sgi.alert.source'].search(
            [('enabled', '=', False), ('suppressed_count', '>', 0)])
        for source in sources_off:
            lines.append(self._sgi_line(
                'warn', "Fuente «%s» apagada con %d NC omitidas: confirmar que sigue siendo intencional." % (
                    source.name, source.suppressed_count),
                "SGI → Configuración → Fuentes de NC automáticas"))
        team = env.ref('quimibond_sgi.sgi_helpdesk_team_complaints',
                       raise_if_not_found=False)
        if team:
            Ticket = env['helpdesk.ticket']
            sgi_tickets = Ticket.search_count([('team_id', '=', team.id)])
            others = Ticket.search_count(
                [('team_id', '!=', team.id), ('team_id.name', 'ilike', 'reclama')])
            if not sgi_tickets and others:
                lines.append(self._sgi_line(
                    'bad', "El equipo SGI de reclamaciones tiene 0 tickets mientras otros equipos de reclamación acumulan %d: el embudo (SLA, Generar NC, KPI CA-01) está desviado." % others,
                    "canalizar las reclamaciones al equipo del SGI"))
        if not lines:
            lines.append(self._sgi_line('ok', "Mejora continua fluyendo."))
        section("Mejora continua", lines)

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
            ('quimibond_sgi.budget_pricelist_id',
             "Lista de precios presupuestal sin configurar: las líneas globales del presupuesto quedan sin precio."),
        ]
        for key, msg in checks:
            try:
                value = int(float(Param.get_param(key, 0) or 0))
            except (TypeError, ValueError):
                value = 0
            if not value:
                lines.append(self._sgi_line('warn', msg, "SGI → Configuración → Ajustes"))
        if not lines:
            lines.append(self._sgi_line('ok', "Ajustes clave configurados."))
        section("Ajustes clave", lines)

        header = Markup(
            '<p style="color:#666;margin:0 0 6px">Diagnóstico del %s. Solo lectura: '
            'nada se modifica; cada punto dice dónde se arregla.</p>') % today
        return header + Markup('').join(sections)
