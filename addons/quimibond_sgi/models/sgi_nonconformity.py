# -*- coding: utf-8 -*-
import logging

from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

from .sgi_base import SGI_FREE_PREFIXES, sgi_bypass_allowed
from .sgi_calendar import sgi_add_business_days, sgi_business_days
from .sgi_control_hierarchy import CONTROL_HIERARCHY, CONTROL_HIERARCHY_HELP
from .sgi_menu_paths import sgi_menu_path

_logger = logging.getLogger(__name__)


class MailActivity(models.Model):
    _inherit = 'mail.activity'

    def _action_done(self, feedback=False, attachment_ids=None):
        """Cierre bidireccional: completar desde el chatter la actividad espejo
        de una acción del SGI marca terminada la acción (date_done).

        sudo() obligatorio: este override corre para CUALQUIER usuario que
        complete CUALQUIER actividad de CUALQUIER app, y el ACL de
        sgi.action.line solo da lectura al grupo SGI — sin sudo, un usuario
        interno fuera del grupo no podía cerrar ni sus propias actividades
        (AccessError). Es contabilidad interna del sistema, no un acceso que
        el usuario pida."""
        lines = self.env['sgi.action.line'].sudo().search([
            ('activity_id', 'in', self.ids), ('date_done', '=', False),
        ])
        # 57.93.0 (N-02): la actividad espejo de una correctiva sin evidencia
        # solo se marca hecha con archivos; esos archivos quedan como evidencia.
        # El candado vale para el usuario real aunque aquí se escriba con sudo.
        if lines and not self.env.user._is_superuser():
            missing = lines.filtered(lambda l: l._sgi_needs_evidence() and not l._sgi_has_evidence())
            if missing and not attachment_ids:
                raise UserError(
                    "Una acción correctiva no se da por terminada sin evidencia. Abra la acción "
                    "«%s» y capture la evidencia (nota o archivo); después marque hecha la actividad."
                    % "», «".join(missing.mapped('name')))
            if missing:
                missing.write({'evidence_attachment_ids': [(4, att_id) for att_id in attachment_ids]})
        res = super()._action_done(feedback=feedback, attachment_ids=attachment_ids)
        if lines:
            lines.with_context(sgi_activity_done=True).write(
                {'date_done': fields.Date.context_today(self)})
        return res


class QualityAlertStage(models.Model):
    _inherit = 'quality.alert.stage'

    sgi_is_closing_stage = fields.Boolean(string="Etapa de cierre SGI")
    sgi_is_cancel_stage = fields.Boolean(string="Etapa de cancelación SGI")


class QualityAlertTeam(models.Model):
    _inherit = 'quality.alert.team'

    sgi_sequence_id = fields.Many2one('ir.sequence', string="Secuencia de folio SGI",
                                      help="Secuencia anual para el folio de las NC de este equipo.")


# 57.93.0 (N-02): campos de la eficacia que solo escribe el sistema (con sudo)
# o el Jefe MAST; de cualquier otro cliente se ignoran.
_SGI_SYSTEM_FIELDS = ('sgi_ineffective_count', 'sgi_effectiveness_due')

_SGI_DEADLINE_STATES = [
    ('pendiente', "Pendiente"),
    ('vencida', "Vencida"),
    ('hecha', "Hecha"),
]

# 57.93.0 (K-03): campos de una NC cerrada que se escriben solos y no cuentan
# como edición, además de los del chatter (SGI_FREE_PREFIXES). ``date_close``
# lo escribe Calidad (``quality``) en un segundo write justo después de mover
# la NC a una etapa «hecha»: sin esta excepción, el dueño del proceso no
# podría cerrar. ``sgi_action_line_ids``: la lista editable «Correcciones y
# acciones» de la ficha escribe las acciones a través de la NC; cada acción
# se protege sola (SgiActionLine._sgi_check_closed_origin: lo terminado no se
# cambia y no se agregan ni borran acciones), y las pendientes se terminan.
_SGI_CLOSED_FREE_FIELDS = frozenset({'date_close', 'sgi_action_line_ids'})


class QualityAlert(models.Model):
    _inherit = 'quality.alert'

    sgi_folio = fields.Char(string="Folio SGI", readonly=True, copy=False, index=True, tracking=True)
    sgi_stage_is_closing = fields.Boolean(related='stage_id.sgi_is_closing_stage',
                                          help="Indica si la etapa actual es de cierre.")
    sgi_stage_is_cancel = fields.Boolean(related='stage_id.sgi_is_cancel_stage',
                                         help="Indica si la etapa actual es de cancelación.")
    sgi_origin_type = fields.Selection([
        ('proceso', "Proceso"),
        ('auditoria_interna', "Auditoría interna"),
        ('auditoria_externa', "Auditoría externa"),
        ('reclamacion', "Reclamación de cliente"),
        ('indicador', "Indicador incumplido"),
        # 56.20.0: E2.27, E2.20, C5.28, E2.29 y E2.39.
        ('incidente_sst', "Incidente/accidente SST"),
        ('emergencia', "Emergencia real"),
        ('scorecard', "Scorecard de cliente"),
        ('recorrido_csh', "Recorrido de la Comisión de Seguridad e Higiene"),
        ('riesgo', "Riesgo u oportunidad"),
    ], string="Origen", default='proceso', tracking=True,
        help="De dónde viene la NC: proceso, auditoría, reclamación, indicador, incidente u otra fuente.")
    sgi_source_id = fields.Many2one(
        'sgi.alert.source', string="Fuente automática", readonly=True, copy=False,
        index=True, ondelete='set null',
        help="Automatismo que levantó esta NC. Vacío si se capturó a mano.")
    sgi_classification = fields.Selection([
        ('mayor', "Mayor"),
        ('menor', "Menor"),
        ('observacion', "Observación"),
    ], string="Clasificación", tracking=True,
        help="Mayor, menor u observación. Obligatoria para pasar la NC de Abierta a Seguimiento. Una NC "
             "mayor manda un correo crítico al abrirse y exige aplicar la lección aprendida antes de cerrar.")
    sgi_norm_clause_id = fields.Many2one('sgi.norm.clause', string="Requisito (cláusula)",
                                         ondelete='restrict',
                                         help="Cláusula de la norma que se incumplió (también las de "
                                              "requisitos de clientes). Obligatoria para pasar la NC de "
                                              "Abierta a Seguimiento.")
    sgi_requester_id = fields.Many2one('res.users', string="Solicitante",
                                       help="Persona que levanta la no conformidad.")
    sgi_requester_job = fields.Char(related='sgi_requester_id.employee_id.job_title',
                                    string="Cargo del solicitante", readonly=True)
    sgi_lead_auditor_id = fields.Many2one('res.users', string="Auditor líder",
                                          help="Auditor líder de la auditoría que detectó la NC.")
    sgi_process_id = fields.Many2one('sgi.process', string="Proceso detectado", ondelete='restrict',
                                     help="Proceso en el que se detectó la NC. Su dueño recibe los "
                                          "escalamientos.")
    sgi_responsible_ids = fields.Many2many('res.users', 'sgi_alert_responsible_rel',
                                           'alert_id', 'user_id', string="Responsables a contestar",
                                           help="Personas que deben contestar la NC. La ven en Mis "
                                                "pendientes hasta que se cierre.")
    sgi_deviation = fields.Text(string="Desviación detectada")
    sgi_why_1 = fields.Char(string="¿Por qué? 1")
    sgi_why_2 = fields.Char(string="¿Por qué? 2")
    sgi_why_3 = fields.Char(string="¿Por qué? 3")
    sgi_why_4 = fields.Char(string="¿Por qué? 4")
    sgi_why_5 = fields.Char(string="¿Por qué? 5")
    sgi_root_cause = fields.Text(string="Causa raíz")
    sgi_ishikawa_notes = fields.Text(string="Notas Ishikawa (5-6M)")
    sgi_effectiveness_note = fields.Text(string="Verificación de eficacia")
    sgi_effectiveness_date = fields.Date(string="Fecha de eficacia",
                                         help="Fecha de la verificación de eficacia. Se pide cuando terminan "
                                              "todas las acciones.")
    sgi_effectiveness_by = fields.Many2one('res.users', string="Eficacia verificada por",
                                           help="Persona que verificó que las acciones fueron eficaces.")
    # 57.93.0 (N-02): la verificación de eficacia tiene resultado. Solo
    # «Eficaz» deja cerrar; «No eficaz» regresa la NC a Seguimiento y pide
    # una acción correctiva nueva (_sgi_on_ineffective).
    sgi_effective = fields.Selection([
        ('eficaz', "Eficaz"),
        ('no_eficaz', "No eficaz"),
    ], string="Resultado de la eficacia", tracking=True, copy=False,
        help="Resultado de la verificación de eficacia. La NC solo cierra con «Eficaz». «No eficaz» la "
             "regresa a Seguimiento y pide una acción correctiva nueva.")
    sgi_ineffective_count = fields.Integer(
        string="Verificaciones no eficaces", readonly=True, copy=False,
        help="Veces que la verificación de eficacia salió «No eficaz». Cero al cerrar = eficaz a la primera.")
    # Último eslabón de la línea dorada (IATF 10.2.3): la lección de una NC mayor
    # se lleva al AMEF / plan de control / documento. Se atestigua explícitamente
    # (queda en el chatter por tracking) y es requisito para cerrar la NC mayor.
    sgi_lesson_captured = fields.Boolean(
        string="Lección aplicada a AMEF / plan de control / documento",
        tracking=True,
        help="Confírmelo cuando la lección aprendida de esta NC mayor ya se "
             "reflejó en el AMEF, el plan de control y/o el documento controlado "
             "correspondiente.")
    sgi_followup_comments = fields.Text(string="Comentarios de seguimiento")
    sgi_required_capa = fields.Boolean(string="¿Requirió acción correctiva?",
                                       help="Marque si la NC requirió acción correctiva además de la "
                                            "corrección inmediata.")
    sgi_followup_action = fields.Selection([
        ('exhorto', "Exhorto"),
        ('administrativa', "Acción administrativa"),
        ('na', "N/A"),
    ], string="Acción a seguir",
        help="Consecuencia para los responsables. «Acción administrativa» pide al Coordinador de RH levantar "
             "el acta.")
    sgi_verified_by = fields.Many2one('res.users', string="Verificó",
                                      help="Persona que verifica la NC antes de su aprobación.")
    sgi_verified_date = fields.Date(string="Fecha de verificación", help="Fecha de la verificación.")
    sgi_approved_by = fields.Many2one('res.users', string="Aprobó",
                                      help="Persona que aprueba el cierre de la no conformidad.")
    sgi_approved_date = fields.Date(string="Fecha de aprobación",
                                    help="Fecha en que se aprobó el cierre de la no conformidad.")
    sgi_complaint_ticket_id = fields.Many2one('helpdesk.ticket', string="Reclamación ligada", readonly=True,
                                              help="Reclamación de cliente de la que nació esta no "
                                                   "conformidad.")
    sgi_external_ref = fields.Char(string="N° NCR externo")

    sgi_action_line_ids = fields.One2many('sgi.action.line', 'alert_id', string="Correcciones y acciones")

    # --- NC-1 (49.0.0): plazos por etapa. Se calculan al abrir la NC (días
    # hábiles desde la fecha de creación, parámetros ajustables) y quedan
    # fijos; el cron avisa el día que vence cada uno y escala al dueño del
    # proceso y luego a MAST. Cada plazo se da por cumplido con un hecho, no
    # con una fecha capturada: contención = acción de contención registrada;
    # causa raíz = campo capturado; plan = acción correctiva/preventiva con
    # responsable y compromiso.
    sgi_due_containment = fields.Date(string="Contención vence", readonly=True, copy=False,
                                      help="Fecha límite para registrar la contención, en días hábiles desde "
                                           "que se abrió la NC. La pone el sistema.")
    sgi_due_root_cause = fields.Date(string="Causa raíz vence", readonly=True, copy=False,
                                     help="Fecha límite para capturar la causa raíz, en días hábiles desde "
                                          "que se abrió la NC. La pone el sistema.")
    sgi_due_plan = fields.Date(string="Plan de acción vence", readonly=True, copy=False,
                               help="Fecha límite para registrar el plan de acción (acción correctiva o "
                                    "preventiva con responsable y compromiso). La pone el sistema.")
    sgi_containment_state = fields.Selection(
        _SGI_DEADLINE_STATES, string="Contención", compute='_compute_sgi_deadline_states',
        help="Plazo de la contención: hecha cuando hay una acción de contención; vencida si pasó la fecha "
             "sin ella. Se calcula al mostrarlo.")
    sgi_root_cause_state = fields.Selection(
        _SGI_DEADLINE_STATES, string="Causa raíz (plazo)", compute='_compute_sgi_deadline_states',
        help="Plazo de la causa raíz: hecha cuando está capturada; vencida si pasó la fecha. Se calcula al "
             "mostrarlo.")
    sgi_plan_state = fields.Selection(
        _SGI_DEADLINE_STATES, string="Plan de acción", compute='_compute_sgi_deadline_states',
        help="Plazo del plan de acción: hecha cuando hay acción correctiva o preventiva con responsable y "
             "compromiso. Se calcula al mostrarlo.")
    sgi_containment_done = fields.Boolean(compute='_compute_sgi_deadline_states',
                                          help="Se marca sola cuando la NC ya tiene al menos una acción de "
                                               "contención registrada.")
    # 57.40.0 (V-A02): semáforo de la lista de NC y filtro «Plazo vencido».
    sgi_deadline_overdue = fields.Boolean(
        string="Plazo vencido", compute='_compute_sgi_deadline_states',
        search='_search_sgi_deadline_overdue',
        help="Algún plazo de la NC (contención, causa raíz o plan de acción) "
             "ya venció sin cumplirse.")
    # --- NC-3: eficacia programada a N días de la última acción correctiva.
    sgi_effectiveness_due = fields.Date(
        string="Verificar eficacia el", readonly=True, copy=False,
        help="Se fija al terminar la última acción correctiva (90 días por "
             "omisión) y agenda la verificación al dueño del proceso (o al Jefe "
             "MAST si no hay).")
    # --- NC-4: cancelación con motivo aprobado por el Jefe MAST.
    sgi_cancel_reason = fields.Text(string="Motivo de cancelación", readonly=True, copy=False)
    sgi_cancel_requested_by = fields.Many2one(
        'res.users', string="Cancelación solicitada por", readonly=True, copy=False,
        help="Quién pidió cancelar la no conformidad. La cancelación la confirma el Jefe MAST y SGI.")

    # Ligas reales del SGI (H7): trazabilidad NC <-> riesgo <-> AMEF <-> documento.
    sgi_risk_ids = fields.Many2many(
        'sgi.risk', 'sgi_alert_risk_rel', 'alert_id', 'risk_id',
        string="Riesgos ligados",
        help="Riesgos del SGI relacionados con esta NC.")
    sgi_fmea_id = fields.Many2one('sgi.fmea', string="AMEF ligado",
                                  help="AMEF relacionado. Al cerrar una NC mayor, el aviso para actualizarlo "
                                       "se agenda sobre este AMEF.")
    sgi_document_id = fields.Many2one(
        'documents.document', string="Documento ligado",
        domain=[('sgi_is_controlled', '=', True)],
        help="Documento controlado relacionado con la NC (el que se incumplió o el que hay que cambiar).")

    # Detector de reincidencia (H2): NCs previas del mismo proceso en la ventana
    # de reincidencia; una misma cláusula pesa doble. Se congela al crear/
    # clasificar (snapshot), no se recalcula por el paso del tiempo.
    sgi_recurrence_count = fields.Integer(
        string="Reincidencias", compute='_compute_sgi_recurrence', store=True,
        help="Casos previos del mismo proceso en la ventana de reincidencia "
             "(misma cláusula cuenta doble).")
    sgi_is_recurrent = fields.Boolean(
        string="Reincidente", compute='_compute_sgi_recurrence', store=True,
        help="Se marca sola si el mismo proceso tuvo otra NC en los últimos meses (parámetro "
             "quimibond_sgi.nc_recurrence_months, 12 de fábrica).")

    def copy_data(self, default=None):
        """57.93.0 (FUNC-C13): una NC duplicada nace abierta: la copia toma
        la primera etapa de su equipo (la de ``create``), no la del original.
        Copiar una NC cerrada o cancelada no crea otra cerrada o cancelada."""
        default = dict(default or {})
        vals_list = super().copy_data(default=default)
        if 'stage_id' in default:
            return vals_list
        Stage = self.env['quality.alert.stage'].sudo()
        for alert, vals in zip(self, vals_list):
            first = Stage.search([('team_ids', 'in', alert.team_id.ids)],
                                 order='sequence, id', limit=1) if alert.team_id else Stage
            if first:
                vals['stage_id'] = first.id
            else:
                vals.pop('stage_id', None)
        return vals_list

    @api.model_create_multi
    def create(self, vals_list):
        # 57.93.0 (FUNC-C13 al crear): una NC nace abierta. Crearla en
        # Cerrada se brincaba los candados de cierre y quién cierra; en
        # Cancelada, el motivo aprobado (NC-4). Se revisa ANTES de crear para
        # no gastar folio (la secuencia no regresa números). La etapa puede
        # venir en los valores o en ``default_stage_id`` del contexto (alta
        # rápida en la columna «Cerrada» del kanban). El Jefe MAST y el
        # sistema (superusuario) sí pueden (cargas históricas). Un sudo() no
        # exime: los caminos con sudo conservan el contexto del cliente
        # (``default_stage_id``).
        user = self.env.user
        if not (user._is_superuser() or user.has_group('quimibond_sgi.group_sgi_manager')):
            ctx_stage = self.env.context.get('default_stage_id')
            default_stage = ctx_stage if isinstance(ctx_stage, int) and not isinstance(ctx_stage, bool) \
                else False
            stage_ids = {vals.get('stage_id') or default_stage for vals in vals_list} - {False, None}
            stage_ids = {sid for sid in stage_ids if isinstance(sid, int) and not isinstance(sid, bool)}
            bad = self.env['quality.alert.stage'].sudo().browse(stage_ids).exists().filtered(
                lambda s: s.sgi_is_closing_stage or s.sgi_is_cancel_stage)
            if bad:
                raise UserError(
                    "Una NC nace abierta: no se crea directamente en «%s». Créela, registre sus "
                    "acciones y ciérrela (o pida su cancelación) desde la ficha."
                    % ", ".join(bad.mapped('name')))
        # 57.97.0 (N-05): nacer en «Seguimiento» (alta rápida en esa columna
        # del kanban, RPC) es pasar a Seguimiento sin pasar por el write: pide
        # clasificación y cláusula. Antes de crear (no gasta folio). Solo el
        # sistema queda exento; el Jefe MAST no (como en el write).
        followup = self.env.ref('quimibond_sgi.sgi_nc_int_stage_followup', raise_if_not_found=False)
        if followup and not user._is_superuser():
            ctx_stage = self.env.context.get('default_stage_id')
            for vals in vals_list:
                if (vals.get('stage_id') or ctx_stage) == followup.id and not (
                        vals.get('sgi_classification') and vals.get('sgi_norm_clause_id')):
                    raise UserError(
                        "Una NC no nace en «Seguimiento» sin la clasificación (mayor, menor u "
                        "observación) y el requisito (cláusula) que se incumplió. Créela en "
                        "«Abierta» o capture los dos en el alta.")
        # 57.93.0 (N-02): el contador de «No eficaz» y la fecha programada de
        # la eficacia los pone el sistema; no se aceptan del cliente.
        if not sgi_bypass_allowed(self.env):
            vals_list = [{k: v for k, v in vals.items() if k not in _SGI_SYSTEM_FIELDS}
                         for vals in vals_list]
        alerts = super().create(vals_list)
        Stage = self.env['quality.alert.stage'].sudo()
        for alert in alerts:
            if not alert.sgi_folio and alert.team_id.sgi_sequence_id:
                alert.sgi_folio = alert.team_id.sgi_sequence_id.next_by_id()
            # La etapa por defecto de quality no mira el equipo: en producción
            # las NC del SGI nacían sin etapa (fuera del kanban). Primera etapa
            # del equipo.
            if alert.sgi_folio and not alert.stage_id:
                first = Stage.search([('team_ids', 'in', alert.team_id.ids)],
                                     order='sequence, id', limit=1)
                if first:
                    alert.stage_id = first
            if alert.sgi_folio:
                alert._sgi_set_deadlines()
        # Una NC MAYOR del SGI avisa por correo además de la actividad:
        # Dirección no vive dentro de Odoo.
        Cron = self.env['sgi.cron']
        for alert in alerts:
            if alert.sgi_folio and alert.sgi_classification == 'mayor':
                Cron._sgi_send_critical_mail(
                    'quimibond_sgi.mail_template_sgi_nc_mayor', alert)
        alerts._sgi_request_admin_record()
        return alerts

    # ------------------------------------------------------------------
    # NC-1: plazos por etapa
    # ------------------------------------------------------------------
    @api.model
    def _sgi_deadline_days(self):
        Param = self.env['ir.config_parameter'].sudo()

        def _int(key, default):
            try:
                return int(Param.get_param(key, default) or default)
            except (TypeError, ValueError):
                return default
        return {
            'containment': _int('quimibond_sgi.nc_days_containment', 1),
            'root_cause': _int('quimibond_sgi.nc_days_root_cause', 10),
            'plan': _int('quimibond_sgi.nc_days_plan', 15),
        }

    def _sgi_set_deadlines(self, force=False):
        """Fija los tres plazos (días hábiles desde la creación). Solo donde
        falten, salvo force."""
        days = self._sgi_deadline_days()
        for alert in self:
            if alert.sgi_due_plan and not force:
                continue
            start = fields.Datetime.context_timestamp(
                alert, alert.create_date or fields.Datetime.now()).date()
            alert.write({
                'sgi_due_containment': sgi_add_business_days(self.env, start, days['containment']),
                'sgi_due_root_cause': sgi_add_business_days(self.env, start, days['root_cause']),
                'sgi_due_plan': sgi_add_business_days(self.env, start, days['plan']),
            })

    @api.depends('sgi_action_line_ids.action_type', 'sgi_action_line_ids.responsible_id',
                 'sgi_action_line_ids.date_commit', 'sgi_root_cause',
                 'sgi_due_containment', 'sgi_due_root_cause', 'sgi_due_plan')
    def _compute_sgi_deadline_states(self):
        today = fields.Date.context_today(self)

        def state(done, due):
            if done:
                return 'hecha'
            return 'vencida' if due and today > due else 'pendiente'
        for alert in self:
            lines = alert.sgi_action_line_ids
            containment = bool(lines.filtered(lambda l: l.action_type == 'contencion'))
            plan = bool(lines.filtered(
                lambda l: l.action_type in ('correctiva', 'preventiva')
                and l.responsible_id and l.date_commit))
            alert.sgi_containment_done = containment
            alert.sgi_containment_state = state(containment, alert.sgi_due_containment)
            alert.sgi_root_cause_state = state(bool(alert.sgi_root_cause), alert.sgi_due_root_cause)
            alert.sgi_plan_state = state(plan, alert.sgi_due_plan)
            alert.sgi_deadline_overdue = 'vencida' in (
                alert.sgi_containment_state, alert.sgi_root_cause_state, alert.sgi_plan_state)

    @api.model
    def _search_sgi_deadline_overdue(self, operator, value):
        """Los estados de plazo no se guardan (dependen de hoy): se calculan
        sobre las NC con algún plazo ya pasado, que son pocas."""
        if operator not in ('=', '!=', 'in', 'not in'):
            raise UserError("Búsqueda no soportada en «Plazo vencido».")
        if operator in ('in', 'not in'):
            wanted = any(value or [])
            positive = wanted if operator == 'in' else not wanted
        else:
            positive = bool(value) if operator == '=' else not value
        today = fields.Date.context_today(self)
        candidates = self.sudo().search([
            ('sgi_folio', '!=', False), '|', '|',
            ('sgi_due_containment', '<', today),
            ('sgi_due_root_cause', '<', today),
            ('sgi_due_plan', '<', today)])
        ids = candidates.filtered('sgi_deadline_overdue').ids
        return [('id', 'in' if positive else 'not in', ids)]

    def _sgi_deadline_owner_user_id(self):
        """Quién responde el plazo: el primer responsable a contestar, si no
        el responsable de la alerta, si no el Jefe MAST."""
        self.ensure_one()
        Cron = self.env['sgi.cron']
        return (self.sgi_responsible_ids[:1].id or self.user_id.id
                or Cron._sgi_manager_user_id())

    def _sgi_deadline_escalation(self, today):
        """Avisos y escalamientos de los tres plazos para una NC abierta.
        Idempotente: cada aviso tiene un resumen propio y `_sgi_schedule` no
        lo duplica. Devuelve los resúmenes agendados (para las pruebas)."""
        self.ensure_one()
        Cron = self.env['sgi.cron']
        Param = self.env['ir.config_parameter'].sudo()
        try:
            mast_after = int(Param.get_param('quimibond_sgi.nc_escalation_mast_days', 3) or 3)
        except (TypeError, ValueError):
            mast_after = 3
        owner_id = self._sgi_deadline_owner_user_id()
        process_owner_id = self.sgi_process_id.owner_id.user_id.id
        manager_id = Cron._sgi_manager_user_id()
        folio = self.sgi_folio or self.name
        labels = {
            'containment': ("Contención", self.sgi_due_containment, self.sgi_containment_state,
                            "registre al menos una acción de contención"),
            'root_cause': ("Causa raíz", self.sgi_due_root_cause, self.sgi_root_cause_state,
                           "capture la causa raíz (5 porqués / Ishikawa)"),
            'plan': ("Plan de acción", self.sgi_due_plan, self.sgi_plan_state,
                     "registre las acciones correctivas con responsable y compromiso"),
        }
        scheduled = []
        for key, (label, due, state, what) in labels.items():
            if not due or state == 'hecha':
                continue
            if today >= due:
                summary = "NC %s: %s vence el %s" % (folio, label.lower(), due)
                Cron._sgi_schedule(
                    self, summary,
                    "Plazo de %s de la NC %s: %s. Vence el %s." % (label.lower(), folio, what, due),
                    owner_id)
                scheduled.append(summary)
            if today > due and process_owner_id and process_owner_id != owner_id:
                summary = "NC %s: %s vencida, escalada al dueño del proceso" % (folio, label.lower())
                Cron._sgi_schedule(
                    self, summary,
                    "El plazo de %s de la NC %s venció el %s y sigue pendiente (%s)." % (
                        label.lower(), folio, due, what),
                    process_owner_id)
                scheduled.append(summary)
            # 57.15.0 (G-009): el umbral de MAST se cuenta en días hábiles.
            if sgi_business_days(self.env, due, today) > mast_after and manager_id:
                summary = "NC %s: %s vencida hace más de %d días, escalada a MAST" % (
                    folio, label.lower(), mast_after)
                Cron._sgi_schedule(
                    self, summary,
                    "El plazo de %s de la NC %s venció el %s y nadie lo ha cerrado (%s)." % (
                        label.lower(), folio, due, what),
                    manager_id)
                scheduled.append(summary)
        return scheduled

    # ------------------------------------------------------------------
    # NC-3: eficacia programada
    # ------------------------------------------------------------------
    def _sgi_effectiveness_user_id(self):
        """57.93.0 (FUNC-C13): la eficacia la verifica quien puede cerrar la
        NC: el usuario activo del dueño del proceso; sin él, el Jefe MAST."""
        self.ensure_one()
        owner_user = self.sudo().sgi_process_id.owner_id.user_id
        if owner_user and owner_user.active:
            return owner_user.id
        return self.env['sgi.cron']._sgi_manager_user_id()

    def _sgi_needs_new_corrective(self):
        """57.93.0 (N-02): tras un «No eficaz», ¿falta la acción correctiva
        nueva terminada? Nueva = registrada en la ronda actual
        (effectiveness_round >= sgi_ineffective_count)."""
        self.ensure_one()
        if not self.sgi_ineffective_count:
            return False
        return not self.sgi_action_line_ids.filtered(
            lambda l: l.action_type == 'correctiva' and l.date_done
            and l.effectiveness_round >= self.sgi_ineffective_count)

    def _sgi_on_ineffective(self):
        """57.93.0 (N-02): la verificación salió «No eficaz». Deja la
        verificación en el historial (chatter y contador), la limpia para la
        siguiente (el resultado también, para que otro «No eficaz» vuelva a
        contar), regresa la NC a Seguimiento y pide la acción correctiva nueva.

        La etapa: una NC en Abierta se queda ahí (todavía no la trabaja nadie
        y moverla chocaría con la contención obligatoria de las
        reclamaciones); en Seguimiento no cambia; desde cualquier otra
        (Cerrada) regresa a Seguimiento. Una NC cancelada no entra aquí."""
        followup = self.env.ref('quimibond_sgi.sgi_nc_int_stage_followup', raise_if_not_found=False)
        open_stage = self.env.ref('quimibond_sgi.sgi_nc_int_stage_open', raise_if_not_found=False)
        Cron = self.env['sgi.cron']
        for alert in self:
            if alert.stage_id.sgi_is_cancel_stage:
                continue
            folio = alert.sgi_folio or alert.name
            alert.message_post(body=Markup(
                "<b>Verificación de eficacia: no eficaz</b> (%s).<br/>%s") % (
                    alert.sgi_effectiveness_date or fields.Date.context_today(alert),
                    alert.sgi_effectiveness_note or ''))
            alert.activity_ids.sudo().filtered(
                lambda a: (a.summary or '').startswith("Verificar eficacia")).action_feedback(
                feedback="No eficaz: se pidió una acción correctiva nueva.")
            # Campos del sistema: con sudo (el cliente no los escribe).
            alert.sudo().write({
                'sgi_ineffective_count': alert.sgi_ineffective_count + 1,
                'sgi_effectiveness_due': False,
            })
            vals = {
                'sgi_effective': False,
                'sgi_effectiveness_note': False,
                'sgi_effectiveness_date': False,
            }
            if followup and alert.stage_id != followup and alert.stage_id != open_stage:
                vals['stage_id'] = followup.id
            alert.write(vals)
            Cron._sgi_schedule(
                alert, "Registrar acción correctiva nueva de la NC %s (no eficaz)" % folio,
                "La verificación de eficacia salió «No eficaz». Revise la causa raíz y registre "
                "una acción correctiva nueva; la NC no cierra sin ella.",
                alert._sgi_effectiveness_user_id())

    def _sgi_schedule_effectiveness(self):
        """Al terminar la última acción correctiva: fecha de verificación a N
        días (parámetro) y actividad a quien puede cerrar la NC (57.93.0:
        dueño del proceso o Jefe MAST, antes «verificada por»)."""
        Param = self.env['ir.config_parameter'].sudo()
        try:
            days = int(Param.get_param('quimibond_sgi.nc_effectiveness_days', 90) or 90)
        except (TypeError, ValueError):
            days = 90
        Cron = self.env['sgi.cron']
        for alert in self:
            corrective = alert.sgi_action_line_ids.filtered(lambda l: l.action_type == 'correctiva')
            if not corrective or any(not l.date_done for l in corrective):
                continue
            if alert.sgi_effectiveness_due or alert.sgi_effectiveness_date:
                continue
            last = max(corrective.mapped('date_done'))
            due = last + relativedelta(days=days)
            alert.sudo().sgi_effectiveness_due = due
            user_id = alert._sgi_effectiveness_user_id()
            summary = "Verificar eficacia de la NC %s (a %d días)" % (alert.sgi_folio or alert.name, days)
            if user_id and not Cron._sgi_activity_exists(alert, summary, user_id):
                alert.activity_schedule(
                    'mail.mail_activity_data_todo', summary=summary,
                    note="La última acción correctiva terminó el %s. Verifique la eficacia el %s o "
                         "después y registre el resultado («Eficaz» o «No eficaz»), la nota y la "
                         "fecha en la pestaña Verificación y cierre; la NC solo cierra con "
                         "«Eficaz»." % (last, due),
                    user_id=user_id, date_deadline=due)
            alert.message_post(body=Markup(
                "Verificación de eficacia programada para el <b>%s</b>.") % due)

    # ------------------------------------------------------------------
    # NC-2 / NC-4: candados de etapa
    # ------------------------------------------------------------------
    def _sgi_check_stage_move(self, new_stage):
        """Antes de cambiar de etapa una NC con folio: una reclamación no sale
        de Abierta sin contención (NC-2); a Cancelada solo se llega con motivo
        aprobado por el Jefe MAST, vía el asistente (NC-4); y una NC del SGI
        no se va a una etapa ajena a los equipos del SGI (NC-5)."""
        ctx = self.env.context
        cancel_ok = ctx.get('sgi_cancel_approved') and sgi_bypass_allowed(self.env)
        force = ctx.get('sgi_force_close') and sgi_bypass_allowed(self.env)
        open_stage = self.env.ref('quimibond_sgi.sgi_nc_int_stage_open', raise_if_not_found=False)
        for alert in self:
            if not alert.sgi_folio or alert.stage_id == new_stage:
                continue
            if new_stage.sgi_is_cancel_stage and not cancel_ok and not force:
                raise UserError(
                    "La NC %s no se cancela arrastrándola: use el botón «Cancelar NC», "
                    "capture el motivo y el Jefe MAST la aprueba." % (alert.sgi_folio))
            # 57.91.0 (FUNC-C13): cerrar es de quien responde por el proceso.
            if new_stage.sgi_is_closing_stage and not force \
                    and not alert._sgi_user_can_close():
                raise UserError(
                    "La NC %s solo la cierra el Jefe MAST o el dueño del proceso (%s). "
                    "Pídale que revise la eficacia y la cierre."
                    % (alert.sgi_folio, alert.sudo().sgi_process_id.owner_id.name or "sin dueño"))
            if (alert.sgi_origin_type == 'reclamacion' and not new_stage.sgi_is_cancel_stage
                    and alert.stage_id == open_stage and not alert.sgi_containment_done
                    and not force):
                raise UserError(
                    "La NC %s viene de una reclamación de cliente: no avanza de «%s» sin al "
                    "menos una acción de CONTENCIÓN registrada (pestaña Correcciones y "
                    "acciones, tipo Contención)." % (alert.sgi_folio, alert.stage_id.name or ''))
            if alert.team_id.sgi_sequence_id and new_stage.team_ids and \
                    alert.team_id not in new_stage.team_ids:
                raise UserError(
                    "La etapa «%s» no es del flujo del SGI; las NC con folio solo viven en "
                    "Abierta, Seguimiento, Cerrada y Cancelada." % new_stage.name)

    def _sgi_user_can_close(self):
        """57.91.0 (FUNC-C13): cierra una NC el Jefe MAST (o código de sistema)
        o el usuario del dueño de su proceso."""
        self.ensure_one()
        if sgi_bypass_allowed(self.env):
            return True
        owner_user = self.sudo().sgi_process_id.owner_id.user_id
        return bool(owner_user) and owner_user == self.env.user

    def _sgi_leaving_open(self, new_stage):
        """57.97.0 (N-05): las NC con folio que salen de Abierta (o de ninguna
        etapa, o de una etapa genérica) hacia Seguimiento o hacia una etapa de
        cierre. Se arma con la etapa de ANTES del write. Reabrir una cerrada,
        el «No eficaz» y lo que ya está en Seguimiento no entran: el candado no
        es retroactivo."""
        followup = self.env.ref('quimibond_sgi.sgi_nc_int_stage_followup', raise_if_not_found=False)
        if not ((followup and new_stage == followup) or new_stage.sgi_is_closing_stage):
            return self.browse()
        # «Antes de Seguimiento»: sin etapa, Abierta o una etapa genérica de
        # Calidad (base nueva); no Seguimiento, cierre ni cancelación.
        return self.filtered(lambda a: a.sgi_folio and a.stage_id != new_stage and not (
            (followup and a.stage_id == followup) or a.stage_id.sgi_is_closing_stage
            or a.stage_id.sgi_is_cancel_stage))

    def _sgi_check_classified(self):
        """57.97.0 (N-05, ISO 10.2): una NC no se trabaja sin decir qué
        requisito se incumplió y qué tan grave es. Se revisa DESPUÉS del
        write (cuenta lo que el formulario manda junto con la etapa)."""
        for alert in self:
            missing = []
            if not alert.sgi_classification:
                missing.append("• la clasificación (mayor, menor u observación)")
            if not alert.sgi_norm_clause_id:
                missing.append("• el requisito (cláusula de la norma) que se incumplió")
            if missing:
                raise UserError(
                    "La NC %s no pasa a «%s» sin:\n%s\nCaptúrelos en «Datos de la NC» y vuelva a "
                    "moverla. (ISO 10.2: qué requisito se incumplió y qué tan grave es.)"
                    % (alert.sgi_folio or alert.name, alert.stage_id.name or '', "\n".join(missing)))

    def _sgi_check_closed_edit(self, vals):
        """57.93.0 (K-03): una NC cerrada es evidencia. Quien no es Jefe MAST
        (ni código de sistema) solo escribe en el chatter y las actividades
        y, si es el dueño del proceso, la reabre cambiando solo la etapa
        (D-009, el mismo criterio de quién cierra)."""
        if sgi_bypass_allowed(self.env):
            return
        touched = {k for k in vals
                   if not k.startswith(SGI_FREE_PREFIXES) and k not in _SGI_CLOSED_FREE_FIELDS}
        if not touched:
            return
        closed = self.sudo().filtered(
            lambda a: a.sgi_folio and a.stage_id.sgi_is_closing_stage).with_env(self.env)
        if not closed:
            return
        if touched == {'stage_id'}:
            cannot = closed.filtered(lambda a: not a._sgi_user_can_close())
            if cannot:
                raise UserError(
                    "La NC %s está cerrada: solo la reabren el Jefe MAST o el dueño del proceso."
                    % ", ".join(cannot.sudo().mapped('sgi_folio')))
            return
        if all(a._sgi_user_can_close() for a in closed):
            raise UserError(
                "La NC %s está cerrada y es evidencia: no se modifica cerrada. Usted es el dueño "
                "del proceso: reábrala (cambie solo la etapa) y después capture el cambio."
                % ", ".join(closed.sudo().mapped('sgi_folio')))
        raise UserError(
            "La NC %s está cerrada y es evidencia: solo el Jefe MAST la modifica. Si hay un "
            "error real, pida al dueño del proceso o al Jefe MAST que la reabra."
            % ", ".join(closed.sudo().mapped('sgi_folio')))

    def action_sgi_cancel(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Cancelar no conformidad",
            'res_model': 'sgi.nc.cancel',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_alert_id': self.id,
                        'default_reason': self.sgi_cancel_reason or False},
        }

    @api.depends('sgi_process_id', 'sgi_norm_clause_id', 'sgi_folio')
    def _compute_sgi_recurrence(self):
        months = int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.nc_recurrence_months', 12))
        for alert in self:
            if not alert.sgi_folio or not alert.sgi_process_id:
                alert.sgi_recurrence_count = 0
                alert.sgi_is_recurrent = False
                continue
            ref_date = alert.create_date or fields.Datetime.now()
            since = ref_date - relativedelta(months=months)
            # "Previa" = id menor (monotónico y determinista, también cuando dos
            # NCs comparten create_date por caer en la misma transacción). Se usa
            # sudo() porque la reincidencia es un hecho del sistema, no depende de
            # las reglas de registro del usuario en turno; y se excluyen las NCs
            # canceladas (una falsa alarma cancelada no marca a la siguiente).
            # En el formulario (onchange) el registro es un NewId: se usa el
            # id real; sin él (NC nueva sin guardar) todas cuentan como previas.
            real_id = alert._origin.id or (alert.id if isinstance(alert.id, int) else False)
            prior = self.sudo().search(([('id', '<', real_id)] if real_id else []) + [
                ('sgi_folio', '!=', False),
                ('sgi_process_id', '=', alert.sgi_process_id.id),
                ('create_date', '>=', since),
                '|', ('stage_id', '=', False),
                ('stage_id.sgi_is_cancel_stage', '=', False),
            ])
            count = 0
            for other in prior:
                count += 2 if (alert.sgi_norm_clause_id
                               and other.sgi_norm_clause_id == alert.sgi_norm_clause_id) else 1
            alert.sgi_recurrence_count = count
            alert.sgi_is_recurrent = count >= 1

    def _sgi_recompute_later_recurrence(self, processes):
        """Marca para recalcular la reincidencia de las NC con folio de estos
        procesos creadas después de la primera de ``self`` (G-026)."""
        if not processes or not self.ids:
            return
        later = self.sudo().with_context(active_test=False).search([
            ('sgi_process_id', 'in', processes.ids), ('sgi_folio', '!=', False),
            ('id', '>', min(self.ids)), ('id', 'not in', self.ids)])
        if later:
            for fname in ('sgi_recurrence_count', 'sgi_is_recurrent'):
                self.env.add_to_compute(self._fields[fname], later)

    def _sgi_read_across(self):
        """H2: al cerrar una NC reincidente ligada a un AMEF, agenda revisión en
        los AMEF del mismo proceso (posible modo de falla análogo)."""
        self.ensure_one()
        if not self.sgi_is_recurrent or not self.sgi_fmea_id:
            return
        process = self.sgi_fmea_id.process_id
        if not process:
            return
        manager_id = self.env['sgi.cron']._sgi_manager_user_id()
        if not manager_id:
            return
        peers = self.env['sgi.fmea'].search([
            ('process_id', '=', process.id),
            ('id', '!=', self.sgi_fmea_id.id),
            ('state', '!=', 'obsoleto'),
        ])
        summary = "Read-across NC reincidente %s: revisar AMEF del mismo proceso" % (
            self.sgi_folio or self.name)
        note = ("Una NC reincidente cerrada afecta un AMEF de este proceso. "
                "Revise si aplica el mismo modo de falla en este AMEF.")
        for fmea in peers:
            fmea._sgi_schedule_activity(manager_id, summary, note)

    @api.model
    def sgi_auto_create(self, source_code, vals, count_suppression=True):
        """Punto ÚNICO de entrada de las NC que levanta el sistema.

        Todo automatismo del SGI debe crear su NC por aquí en vez de llamar a
        `create()` directo: así el Jefe de MAST puede apagar la fuente desde
        Configuración → Fuentes de NC, y así queda estampado de dónde salió cada
        NC (`sgi_source_id`).

        Devuelve la NC creada, o un recordset VACÍO si la fuente está apagada —
        el llamador debe contemplarlo (`if not alert: return`).

        Si la clave no está declarada en el registro se crea igual y se avisa al
        log: en un sistema de calidad es peor perder una NC por un dato faltante
        que registrar una de más.

        `count_suppression=False` lo usan los llamadores re-entrantes (un cron
        que reevalúa el mismo hecho en cada corrida) para no contar veinte veces
        una sola omisión. La fecha de última omisión sí se actualiza siempre.
        """
        source = self.env['sgi.alert.source']._get_by_code(source_code)
        if not source:
            _logger.warning(
                "SGI: la fuente de NC «%s» no está declarada en sgi.alert.source; "
                "se crea la NC de todos modos.", source_code)
        elif not source.enabled:
            source._register_suppression(count=count_suppression)
            if source.trigger_type == 'manual':
                # Hay una persona esperando respuesta del botón: avisarle en vez
                # de no hacer nada y dejarla adivinando.
                raise UserError(
                    "La generación de no conformidades desde «%s» está "
                    "desactivada.\n\nSi debe volver a generarse, actívela en "
                    "%s." % (source.name, sgi_menu_path('fuentes_nc')))
            return self.browse()
        return self.create(dict(vals, sgi_source_id=source.id if source else False))

    def _sgi_check_can_close(self):
        """Valida los candados de cierre de una NC."""
        for alert in self:
            problems = []
            if not alert.sgi_root_cause:
                problems.append("• Falta la causa raíz.")
            if not alert.sgi_action_line_ids:
                problems.append("• La NC no tiene NINGUNA corrección/acción registrada "
                                "(ISO 10.2: sin acción no hay tratamiento).")
            pending = alert.sgi_action_line_ids.filtered(lambda l: not l.date_done)
            if pending:
                problems.append("• Hay %d acción(es) sin fecha de terminación." % len(pending))
            if not alert.sgi_effectiveness_note or not alert.sgi_effectiveness_date:
                problems.append("• Falta la verificación de eficacia (nota y fecha).")
            # 57.93.0 (N-02): resultado y fecha de la eficacia.
            if alert.sgi_effective != 'eficaz':
                problems.append("• Falta el resultado de la verificación de eficacia: «Eficaz» "
                                "(pestaña Verificación y cierre).")
            eff_date = alert.sgi_effectiveness_date
            if eff_date:
                if eff_date > fields.Date.context_today(alert):
                    problems.append("• La fecha de eficacia (%s) no puede ser futura." % eff_date)
                due = alert.sgi_effectiveness_due
                if due and eff_date < due:
                    problems.append(
                        "• La eficacia se programó para el %s y se registró el %s: verifíquela en "
                        "esa fecha o después, o pida al Jefe MAST un cierre forzado con motivo."
                        % (due, eff_date))
                done_dates = [d for d in alert.sgi_action_line_ids.mapped('date_done') if d]
                if done_dates and eff_date < max(done_dates):
                    problems.append(
                        "• La eficacia (%s) se registró antes de que terminara la última acción (%s)."
                        % (eff_date, max(done_dates)))
            if alert._sgi_needs_new_corrective():
                problems.append("• La verificación anterior salió «No eficaz»: registre y termine "
                                "una ACCIÓN CORRECTIVA nueva.")
            # NC mayor (refinamiento H1): exige el análisis de causa completo.
            if alert.sgi_classification == 'mayor':
                if not all((alert.sgi_why_1, alert.sgi_why_2, alert.sgi_why_3,
                            alert.sgi_why_4, alert.sgi_why_5)):
                    problems.append(
                        "• NC mayor: falta completar los 5 porqués del análisis "
                        "de causa.")
                # Último eslabón (IATF 10.2.3): la lección debe llevarse al AMEF /
                # plan de control / documento y atestiguarse antes de cerrar.
                if not alert.sgi_lesson_captured:
                    problems.append(
                        "• NC mayor: confirme que la lección se aplicó al AMEF / "
                        "plan de control / documento («Lección aplicada...»).")
            # Acción CORRECTIVA real terminada (no basta una corrección/contención)
            # cuando la NC es mayor (H1) o reincidente (H2: lo puntual se vuelve
            # sistémico).
            if alert.sgi_classification == 'mayor' or alert.sgi_is_recurrent:
                if not alert.sgi_action_line_ids.filtered(
                        lambda l: l.action_type == 'correctiva' and l.date_done):
                    reason = "NC mayor" if alert.sgi_classification == 'mayor' \
                        else "NC reincidente"
                    problems.append(
                        "• %s: se requiere al menos una ACCIÓN CORRECTIVA "
                        "terminada (una corrección inmediata no basta)." % reason)
            if problems:
                raise UserError(
                    "No se puede cerrar la NC %s:\n%s" % (
                        alert.sgi_folio or alert.name, "\n".join(problems)))

    def write(self, vals):
        # 57.93.0 (N-02): el contador y la fecha programada son del sistema.
        if not sgi_bypass_allowed(self.env) and any(f in vals for f in _SGI_SYSTEM_FIELDS):
            vals = {k: v for k, v in vals.items() if k not in _SGI_SYSTEM_FIELDS}
            if not vals:
                return True
        # 57.93.0 (K-03): lo cerrado solo lo modifica el Jefe MAST; el dueño
        # del proceso la reabre. Se revisa con la etapa de ANTES del write:
        # cerrar (entrar a la etapa) no se detiene aquí.
        self._sgi_check_closed_edit(vals)
        # Reclasificar a MAYOR una NC con folio también dispara el correo
        # crítico (solo la transición: no re-avisa a las que ya eran mayores).
        newly_mayor = self.browse()
        if vals.get('sgi_classification') == 'mayor':
            newly_mayor = self.filtered(
                lambda a: a.sgi_folio and a.sgi_classification != 'mayor')
        # 57.93.0 (N-02): solo la transición a «No eficaz» dispara el flujo.
        newly_ineffective = self.browse()
        if vals.get('sgi_effective') == 'no_eficaz':
            newly_ineffective = self.filtered(
                lambda a: a.sgi_folio and a.sgi_effective != 'no_eficaz'
                and not a.stage_id.sgi_is_cancel_stage)
        newly_closed = self.env['quality.alert']
        to_check = self.env['quality.alert']
        to_classify = self.env['quality.alert']
        new_stage = self.env['quality.alert.stage']
        if 'stage_id' in vals:
            new_stage = self.env['quality.alert.stage'].browse(vals['stage_id'])
            self._sgi_check_stage_move(new_stage)
            # El cierre forzado solo cuenta desde el wizard de MAST (o código de
            # sistema): el contexto lo controla el cliente RPC y no debe bastar
            # para brincarse los candados de cierre.
            force = (self.env.context.get('sgi_force_close')
                     and sgi_bypass_allowed(self.env))
            # 57.97.0 (N-05): clasificación y cláusula al salir de Abierta (a
            # Seguimiento o directo a Cerrada). El cierre forzado del Jefe MAST
            # y el sistema quedan exentos; se revisa después de escribir.
            if not force and not self.env.su:
                to_classify = self._sgi_leaving_open(new_stage)
            # 57.93.0 (N-02): los candados de cierre se revisan DESPUÉS de
            # escribir, con lo que la persona capturó: el formulario manda los
            # campos sin guardar y la etapa en un solo write. Quién mueve y a
            # qué etapa sí se revisa antes (_sgi_check_stage_move).
            if new_stage.sgi_is_closing_stage and not force:
                to_check = self.filtered(lambda a: a.stage_id != new_stage)
            if new_stage.sgi_is_closing_stage:
                newly_closed = self.filtered(
                    lambda a: a.stage_id != new_stage and a.sgi_folio)
        # 57.16.0 (G-026): cancelar (o reabrir) una NC o cambiarla de proceso
        # cambia la reincidencia de las NC posteriores de ese proceso.
        recurrence_scope = self.env['sgi.process']
        if 'sgi_process_id' in vals:
            recurrence_scope = self.sgi_process_id
        elif 'stage_id' in vals:
            target = self.env['quality.alert.stage'].browse(vals['stage_id'])
            if target.sgi_is_cancel_stage or any(self.stage_id.mapped('sgi_is_cancel_stage')):
                recurrence_scope = self.sgi_process_id
        res = super().write(vals)
        # Un UserError aquí deshace el write completo (misma transacción).
        to_classify._sgi_check_classified()
        to_check._sgi_check_can_close()
        if newly_ineffective:
            newly_ineffective._sgi_on_ineffective()
        # Solo cuenta como cerrada la NC que de verdad quedó en la etapa de cierre.
        newly_closed = newly_closed.filtered(lambda a: a.stage_id == new_stage)
        if 'sgi_process_id' in vals or recurrence_scope:
            self._sgi_recompute_later_recurrence(recurrence_scope | self.sgi_process_id)
        Cron = self.env['sgi.cron']
        if vals.get('sgi_followup_action') == 'administrativa':
            self._sgi_request_admin_record()
        for alert in newly_mayor:
            Cron._sgi_send_critical_mail(
                'quimibond_sgi.mail_template_sgi_nc_mayor', alert)
        for alert in newly_closed:
            if alert.sgi_classification == 'mayor':
                alert._sgi_notify_mayor_closed()
            alert._sgi_read_across()
        return res

    def unlink(self):
        """57.91.0 (K-02): una NC con folio es evidencia (ISO 10.2) y su folio
        no puede dejar hueco. Se cancela con «Cancelar NC»."""
        if not sgi_bypass_allowed(self.env):
            with_folio = self.filtered('sgi_folio')
            if with_folio:
                raise UserError(
                    "Una NC con folio no se borra: use «Cancelar NC» y capture el motivo. (%s)"
                    % ", ".join(with_folio.mapped('sgi_folio')))
        return super().unlink()

    def _sgi_request_admin_record(self):
        """S4.32 (56.20.0): «Acción administrativa» pide el acta al
        Coordinador de RH (parámetro quimibond_sgi.rh_user_id; si no, Jefe
        MAST). Una sola actividad por NC."""
        Cron = self.env['sgi.cron']
        rh_id = Cron._sgi_rh_user_id()
        for alert in self.filtered(lambda a: a.sgi_followup_action == 'administrativa'):
            Cron._sgi_schedule(
                alert, "Levantar acta administrativa (S4.32)",
                "La NC %s pide acción administrativa. Levante el acta con los responsables "
                "y adjúntela a la NC." % (alert.sgi_folio or alert.name), rh_id)

    def _sgi_notify_mayor_closed(self):
        """PROT-05/D7: al cerrar una NC mayor, recordar actualizar AMEF y plan de
        control (lecciones aprendidas).

        H7: si la NC tiene un AMEF ligado, la actividad se agenda sobre ese AMEF
        concreto (no genérica al Jefe MAST) usando el helper del cimiento.
        """
        self.ensure_one()
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        if not manager_id:
            return
        summary = "NC mayor cerrada: actualizar AMEF y plan de control (%s)" % (
            self.sgi_folio or self.name)
        note = ("Se cerró una no conformidad mayor. Revise si el AMEF y el plan "
                "de control del proceso/producto deben actualizarse con la "
                "lección aprendida.")
        if self.sgi_fmea_id:
            self.sgi_fmea_id._sgi_schedule_activity(manager_id, summary, note)
            # Con la liga AMEF → plan de control (C.25), el aviso de lecciones
            # aprendidas también llega al plan que materializa los controles.
            plan = self.sgi_fmea_id.control_plan_id
            if plan:
                plan._sgi_schedule_activity(manager_id, summary, note)
        else:
            Cron._sgi_schedule(self, summary, note, manager_id)
        return True

    def action_sgi_force_close(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Cierre forzado (Jefe MAST)",
            'res_model': 'sgi.nc.force.close',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_alert_id': self.id},
        }

    def action_sgi_escalate_to_nc(self):
        """Escala una alerta operativa de piso a una No Conformidad sistémica del
        SGI: la mueve al equipo NC Internas, le asigna folio y origen 'proceso',
        conservando producto/orden/picking. Las alertas rutinarias de los equipos
        de piso siguen su flujo normal; solo lo sistémico se escala (así el
        concentrado F-P-G05-02 no se contamina)."""
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                            raise_if_not_found=False)
        if not team or not team.sgi_sequence_id:
            raise UserError(
                "No está configurado el equipo «NC Internas» del SGI.")
        # Etapa "Abierta" del equipo NC Internas: las etapas de quality.alert son por
        # equipo, así que al cambiar de equipo hay que moverla a una etapa propia.
        open_stage = self.env.ref('quimibond_sgi.sgi_nc_int_stage_open',
                                  raise_if_not_found=False)
        for alert in self:
            if alert.sgi_folio:
                raise UserError(
                    "La alerta «%s» ya es una NC del SGI (%s)." % (
                        alert.name or alert.title, alert.sgi_folio))
            vals = {
                'team_id': team.id,
                'sgi_origin_type': 'proceso',
                'sgi_folio': team.sgi_sequence_id.next_by_id(),
            }
            if open_stage:
                vals['stage_id'] = open_stage.id
            alert.write(vals)
            alert.message_post(
                body=Markup("Alerta escalada a no conformidad del SGI: <b>%s</b>.") % alert.sgi_folio)
        return True


class SgiActionLine(models.Model):
    """Acción o corrección con responsable y fecha compromiso. Cuelga de una NC, riesgo, AMEF,
    incidente, simulacro, medición en rojo, objetivo o acuerdo de la revisión por la dirección;
    se cierra con «Marcar hecha»."""
    _name = 'sgi.action.line'
    # D-007 (57.21.0): historial y conversación como en NC, incidente y
    # auditoría. Sin mail.activity.mixin: la actividad accionable vive en el
    # registro origen (``activity_id``).
    _inherit = ['mail.thread']
    _description = "Acción / corrección de no conformidad"
    _order = 'date_commit, id'

    alert_id = fields.Many2one('quality.alert', string="No conformidad", ondelete='restrict',
                               help="No conformidad a la que pertenece la acción.")
    risk_id = fields.Many2one('sgi.risk', string="Riesgo / Oportunidad", ondelete='cascade',
                              help="Riesgo u oportunidad al que pertenece la acción.")
    fmea_line_id = fields.Many2one('sgi.fmea.line', string="Modo de falla (AMEF)",
                                   ondelete='cascade',
                                   help="Modo de falla del AMEF al que pertenece la acción.")
    incident_id = fields.Many2one('sgi.incident', string="Incidente SST", ondelete='cascade',
                                  help="Incidente o accidente de seguridad al que pertenece la acción.")
    drill_id = fields.Many2one('sgi.emergency.drill', string="Simulacro", ondelete='cascade',
                               help="Simulacro al que pertenece la acción.")
    objective_id = fields.Many2one('sgi.objective', string="Objetivo integral",
                                   ondelete='cascade',
                                   help="Plan de acción del objetivo (ISO 6.2.2).")
    action_type = fields.Selection([
        ('contencion', "Contención"),
        ('correccion', "Corrección"),
        ('correctiva', "Acción correctiva"),
        ('preventiva', "Acción preventiva"),
    ], string="Tipo", default='correccion', required=True,
        help="Contención y corrección atienden el efecto; la acción correctiva ataca la causa; la "
             "preventiva, una causa potencial.")
    # 57.96.0 (N-06): jerarquía del control que aplica la acción (riesgos e
    # incidentes). Cuenta para el candado de los IPER de riesgo alto.
    control_hierarchy = fields.Selection(
        CONTROL_HIERARCHY, string="Jerarquía del control", tracking=True,
        help=CONTROL_HIERARCHY_HELP)
    name = fields.Char(string="Descripción", required=True)
    responsible_id = fields.Many2one('res.users', string="Responsable", required=True,
                                     tracking=True,
                                     help="Persona que ejecuta la acción. La ve en Mis pendientes y recibe "
                                          "los avisos de vencimiento.")
    date_commit = fields.Date(string="Compromiso", required=True, tracking=True,
                              help="Fecha en que el responsable se compromete a terminar la acción. Pasada "
                                   "esta fecha, la acción se marca vencida y escala.")
    date_done = fields.Date(string="Terminada el", tracking=True,
                            help="Fecha en que se terminó la acción. Al capturarla, la acción queda "
                                 "terminada.")
    progress = fields.Selection([
        ('0', "0%"),
        ('50', "50%"),
        ('100', "100%"),
    ], string="Avance", default='0',
        help="Avance de la acción según el responsable.")
    # 57.93.0 (N-02, H-B1.4): una correctiva se termina con evidencia.
    evidence_note = fields.Text(
        string="Evidencia",
        help="Qué demuestra que la acción se hizo: número de orden, documento, registro o foto. Una "
             "acción correctiva no se termina sin evidencia (esta nota o un archivo).")
    evidence_attachment_ids = fields.Many2many(
        'ir.attachment', 'sgi_action_line_evidence_rel', 'line_id', 'attachment_id',
        string="Archivos de evidencia",
        help="Fotos, registros o documentos que demuestran la acción.")
    state = fields.Selection([
        ('abierta', "Abierta"),
        ('vencida', "Vencida"),
        ('terminada', "Terminada"),
    ], string="Estado", compute='_compute_state', store=True,
        help="Abierta, vencida (pasó el compromiso) o terminada (tiene fecha de término). Se calcula sola.")
    # Actividad nativa que hace accionable la acción en el registro origen.
    activity_id = fields.Many2one('mail.activity', string="Actividad",
                                  readonly=True, copy=False, index=True)
    # 57.93.0 (N-02): ronda de verificación de eficacia de su NC al crearse.
    effectiveness_round = fields.Integer(
        string="Ronda de eficacia", readonly=True, copy=False, default=0,
        help="Veces que la eficacia de la NC había salido «No eficaz» cuando se registró la acción. "
             "Tras un «No eficaz» la NC pide una correctiva de la ronda nueva.")
    origin_display = fields.Char(string="Origen", compute='_compute_origin_display')

    @api.depends('alert_id', 'risk_id', 'incident_id', 'drill_id',
                 'fmea_line_id', 'objective_id')
    def _compute_origin_display(self):
        for line in self:
            origin = line._sgi_origin()
            # Entrega 4: el origen puede ser un incidente que el usuario no lee
            # (solo lo ven quien lo reportó, MAST, Salud y Auditor): se dice
            # qué es, sin abrirlo.
            if origin and not origin._filtered_access('read'):
                line.origin_display = "Incidente o accidente" if origin._name == 'sgi.incident' \
                    else origin._description
            else:
                line.origin_display = origin.display_name if origin else ''

    def action_mark_done(self):
        """El click más usado del empleado: terminar su acción. Sella la fecha
        de hoy y el avance al 100%; el write cierra la actividad espejo y
        revalida el candado del riesgo si aplica."""
        today = fields.Date.context_today(self)
        self.filtered(lambda l: not l.date_done).write(
            {'date_done': today, 'progress': '100'})
        return True

    def action_open_origin(self):
        """Abre el registro que originó la acción (NC, riesgo, incidente,
        simulacro o AMEF): el contexto completo de QUÉ hay que resolver."""
        self.ensure_one()
        origin = self._sgi_origin()
        if not origin:
            raise UserError("Esta acción no tiene un registro origen ligado.")
        return {
            'type': 'ir.actions.act_window',
            'res_model': origin._name,
            'res_id': origin.id,
            'view_mode': 'form',
        }

    @api.constrains('alert_id', 'risk_id', 'fmea_line_id', 'incident_id',
                    'drill_id', 'objective_id', 'name')
    def _check_parent_xor(self):
        for line in self:
            parents = [line.alert_id, line.risk_id, line.fmea_line_id,
                       line.incident_id, line.drill_id, line.objective_id]
            if sum(1 for p in parents if p) != 1:
                raise ValidationError(
                    "Una acción debe pertenecer exactamente a un origen: una No "
                    "Conformidad, un Riesgo, un modo de falla de AMEF, un incidente "
                    "SST, un simulacro o un objetivo integral (exactamente uno, "
                    "no varios ni ninguno).")

    @api.constrains('date_done', 'progress')
    def _sgi_check_done(self):
        """3.6: una acción terminada tiene 100 % de avance y una fecha de
        término que ya pasó (hoy o antes). Antes había una «terminada» con 0 %
        y fecha futura."""
        today = fields.Date.context_today(self)
        for line in self.filtered('date_done'):
            if line.date_done > today:
                raise ValidationError(
                    "La acción «%s» no puede terminar en el futuro (%s). Capture la fecha "
                    "en que realmente se terminó." % (line.name, line.date_done))
            if line.progress != '100':
                raise ValidationError(
                    "La acción «%s» está al %s%%: para terminarla, el avance debe ser 100%%."
                    % (line.name, line.progress or '0'))

    def _sgi_needs_evidence(self):
        """57.93.0 (N-02): las correctivas piden evidencia al terminarse."""
        self.ensure_one()
        return self.action_type == 'correctiva'

    def _sgi_has_evidence(self):
        """Nota, archivo de evidencia o archivo en el chatter de la acción."""
        self.ensure_one()
        return bool((self.evidence_note or '').strip() or self.evidence_attachment_ids
                    or self.sudo().message_attachment_count)

    def _sgi_check_evidence(self):
        """57.93.0 (N-02, H-B1.4): una acción correctiva no se da por
        terminada sin evidencia. Solo el sistema (superusuario: OdooBot,
        crons, migraciones) queda exento; un sudo() que conserva al usuario
        no (Mis pendientes, el espejo del chatter)."""
        if self.env.user._is_superuser():
            return
        missing = self.filtered(
            lambda l: l.date_done and l._sgi_needs_evidence() and not l._sgi_has_evidence())
        if missing:
            raise UserError(
                "Una acción correctiva no se da por terminada sin evidencia. Abra la acción "
                "«%s» y capture la evidencia (nota o archivo: orden, documento, registro, foto); "
                "después termínela." % "», «".join(missing.mapped('name')))

    @staticmethod
    def _sgi_done_vals(vals):
        """Terminar (poner fecha) sin decir el avance lo sube a 100 %;
        reabrir (quitar la fecha) sin decir el avance lo regresa a 50 %."""
        if 'date_done' in vals and 'progress' not in vals:
            vals = dict(vals, progress='100' if vals['date_done'] else '50')
        return vals

    @api.constrains('action_type', 'alert_id')
    def _sgi_check_root_cause_before_capa(self):
        """H8: sin causa raíz no hay acción correctiva/preventiva.

        ISO 10.2 distingue la corrección/contención inmediata (permitida antes de
        conocer la causa) de la acción correctiva/preventiva, que ataca la causa y
        por tanto exige haberla identificado primero.
        """
        for line in self:
            alert = line.alert_id
            if (alert and alert.sgi_folio
                    and line.action_type in ('correctiva', 'preventiva')
                    and not alert.sgi_root_cause):
                raise ValidationError(
                    "No se puede registrar una acción %s en la NC %s sin la causa "
                    "raíz. Primero investigue y capture la causa raíz; la corrección "
                    "(contención inmediata) sí puede registrarse antes." % (
                        dict(self._fields['action_type'].selection)[line.action_type],
                        alert.sgi_folio or alert.name))

    @api.depends('date_commit', 'date_done')
    def _compute_state(self):
        today = fields.Date.context_today(self)
        for line in self:
            line.state = line._sgi_state_on(today)

    def _sgi_state_on(self, today):
        """Estado de la acción en la fecha ``today`` (56.37.0: el cron lo usa
        para escribir solo las que cambian, G-024)."""
        self.ensure_one()
        if self.date_done:
            return 'terminada'
        if self.date_commit and self.date_commit < today:
            return 'vencida'
        return 'abierta'

    # ------------------------------------------------------------------
    # Acciones como actividades nativas (corazón accionable del SGI)
    # ------------------------------------------------------------------
    def _sgi_origin(self):
        """Registro origen (con chatter) al que se cuelga la actividad."""
        self.ensure_one()
        if self.alert_id:
            return self.alert_id
        if self.risk_id:
            return self.risk_id
        if self.incident_id:
            return self.incident_id
        if self.drill_id:
            return self.drill_id
        if self.objective_id:
            return self.objective_id
        if self.fmea_line_id:
            return self.fmea_line_id.fmea_id
        return self.env['sgi.action.line'].browse()

    def _sgi_origin_closed(self):
        """57.93.0 (K-03): ¿ya cerró el registro dueño de la acción? NC en
        etapa de cierre o incidente cerrado; la revisión por la dirección se
        agrega en sgi_management_review.py."""
        self.ensure_one()
        line = self.sudo()
        if line.alert_id:
            return bool(line.alert_id.stage_id.sgi_is_closing_stage)
        if line.incident_id:
            return line.incident_id.state == 'cerrado'
        return False

    def _sgi_check_closed_origin(self, vals=None, any_line=False):
        """57.93.0 (K-03): con el origen cerrado, quien no es Jefe MAST (ni
        código de sistema) no cambia una acción terminada; con ``any_line``
        tampoco agrega ni borra ninguna. Las pendientes sí se trabajan y se
        terminan (cierre forzado de una NC, acuerdos de una revisión cerrada).
        Chatter y actividad espejo (``activity_id``) se mueven solos."""
        if sgi_bypass_allowed(self.env):
            return
        if vals is not None and not any(not k.startswith(SGI_FREE_PREFIXES) for k in vals):
            return
        locked = self.sudo().filtered(
            lambda l: (any_line or l.date_done) and l._sgi_origin_closed())
        if locked:
            raise UserError(
                "La acción «%s» pertenece a un registro cerrado y es evidencia: solo el Jefe MAST "
                "la modifica. Si hay un error real, pida que reabran el registro."
                % "», «".join(locked.mapped('name')))

    def _sgi_activity_note(self):
        self.ensure_one()
        label = dict(self._fields['action_type'].selection).get(
            self.action_type, self.action_type)
        return "%s del SGI. Responsable: %s. Compromiso: %s." % (
            label, self.responsible_id.display_name or '-',
            self.date_commit or '-')

    def _sgi_sync_activity(self):
        """Crea/actualiza la actividad ligada a la acción (idempotente)."""
        Todo = 'mail.mail_activity_data_todo'
        for line in self:
            if line.date_done or not line.responsible_id or not line.date_commit:
                continue
            origin = line._sgi_origin()
            if not origin:
                continue
            # 57.13.0: la actividad espejo es contabilidad del sistema, con
            # sudo. El responsable de una acción no siempre puede editar el
            # registro de origen (un riesgo de un proceso ajeno) y Odoo exige
            # escritura en él para agendar: reabrir su propia acción tronaba.
            if line.activity_id:
                line.activity_id.sudo().write({
                    'user_id': line.responsible_id.id,
                    'date_deadline': line.date_commit,
                    'summary': line.name,
                })
            else:
                act = origin.sudo().activity_schedule(
                    Todo,
                    summary=line.name,
                    note=line._sgi_activity_note(),
                    user_id=line.responsible_id.id,
                    date_deadline=line.date_commit)
                line.activity_id = act.id

    def _sgi_close_activity(self):
        """Marca hecha la actividad cuando la acción se termina.

        En Odoo 19 action_feedback archiva la actividad (conserva historia en
        el chatter). Soltamos el enlace para que, si se reabre la acción, se
        genere una actividad nueva en lugar de reactivar una archivada.
        """
        for line in self:
            if line.activity_id:
                # 57.13.0: con sudo (ver _sgi_sync_activity); la nota queda a
                # nombre de quien terminó la acción (sudo conserva el usuario).
                line.activity_id.sudo().action_feedback(
                    feedback="Acción terminada el %s." % (line.date_done or ''))
                line.activity_id = False

    @api.model_create_multi
    def create(self, vals_list):
        Alert = self.env['quality.alert']
        prepared = []
        for vals in vals_list:
            vals = self._sgi_done_vals(vals)
            # La ronda la pone el sistema (no se acepta del cliente): así un
            # «No eficaz» no se atiende con una acción de una ronda inventada.
            if vals.get('alert_id'):
                vals = dict(vals, effectiveness_round=Alert.browse(vals['alert_id']).sudo()
                            .sgi_ineffective_count)
            elif vals.get('incident_id'):
                # 57.96.0 (N-06): la ronda de eficacia también para incidentes.
                vals = dict(vals, effectiveness_round=self.env['sgi.incident'].browse(
                    vals['incident_id']).sudo().sgi_ineffective_count)
            prepared.append(vals)
        lines = super().create(prepared)
        # 57.93.0 (K-03): a un registro cerrado no se le agregan acciones.
        lines._sgi_check_closed_origin(any_line=True)
        # 57.93.0 (N-02): una correctiva que nace terminada también trae evidencia.
        lines.filtered('date_done')._sgi_check_evidence()
        lines._sgi_sync_activity()
        # 57.93.0 (N-02): la correctiva nueva atiende el aviso del «No eficaz».
        for line in lines.filtered(lambda l: l.action_type == 'correctiva' and l.alert_id
                                   and l.effectiveness_round
                                   and l.effectiveness_round >= l.alert_id.sudo().sgi_ineffective_count):
            line.alert_id.sudo().activity_ids.filtered(
                lambda a: (a.summary or '').startswith("Registrar acción correctiva nueva")
            ).action_feedback(feedback="Se registró la acción correctiva «%s»." % line.name)
        # 57.96.0 (N-06): la acción nueva atiende el aviso del «No eficaz» del incidente.
        for line in lines.filtered(lambda l: l.incident_id and l.effectiveness_round
                                   and l.effectiveness_round >= l.incident_id.sudo().sgi_ineffective_count):
            line.incident_id.sudo().activity_ids.filtered(
                lambda a: (a.summary or '').startswith("Registrar acción nueva del incidente")
            ).action_feedback(feedback="Se registró la acción «%s»." % line.name)
        return lines

    def write(self, vals):
        # 57.93.0 (K-03): la acción terminada de un registro cerrado es evidencia.
        self._sgi_check_closed_origin(vals)
        vals = self._sgi_done_vals(vals)
        # 57.93.0 (N-02): la ronda de eficacia solo la cambia el sistema.
        if 'effectiveness_round' in vals and not sgi_bypass_allowed(self.env):
            vals = {k: v for k, v in vals.items() if k != 'effectiveness_round'}
        # 57.93.0 (N-02): el candado de evidencia corre al pasar a terminada,
        # por cualquier vía (botón, lista editable de la NC, chatter). Lo ya
        # terminado no se vuelve a revisar.
        finishing = self.filtered(lambda l: not l.date_done) if vals.get('date_done') \
            else self.browse()
        res = super().write(vals)
        # 57.93.0 (K-03): mover una acción a otro origen se revisa contra el
        # origen nuevo (el chequeo previo solo ve el viejo).
        if {'alert_id', 'incident_id', 'review_id'} & set(vals):
            self._sgi_check_closed_origin(any_line=True)
        finishing._sgi_check_evidence()
        resync = bool({'responsible_id', 'date_commit', 'name'} & set(vals))
        if 'date_done' in vals:
            done = self.filtered('date_done')
            if self.env.context.get('sgi_activity_done'):
                # El cierre vino de completar la actividad espejo en el chatter:
                # la actividad ya se marcó hecha, sólo soltamos el enlace para no
                # volver a cerrarla (evita recursión).
                done.activity_id = False
            else:
                done._sgi_close_activity()
            # Reabrir una acción (borrar la fecha de terminación) vuelve a
            # agendar la actividad en el responsable.
            resync = True
        if resync:
            self.filtered(lambda l: not l.date_done)._sgi_sync_activity()
        # Reabrir la acción de un riesgo ya controlado/cerrado puede dejarlo
        # sin tratamiento terminado: se revalida el candado (H11). Solo aplica
        # a riesgos de atención alta (el check se auto-filtra).
        if 'date_done' in vals and not vals.get('date_done'):
            self.mapped('risk_id').filtered(
                lambda r: r.state in ('controlado', 'cerrado')
            )._sgi_check_can_close()
        # NC-3: terminar la última correctiva programa la eficacia a 90 días.
        # 57.93.0 (K-03): no en una NC ya cerrada (cierre forzado con acciones
        # pendientes): la NC cerrada no se modifica ni se reprograma.
        if vals.get('date_done'):
            self.mapped('alert_id').filtered(
                lambda a: a.sgi_folio and not a.stage_id.sgi_is_closing_stage
            )._sgi_schedule_effectiveness()
        return res

    def unlink(self):
        # 57.93.0 (K-03): antes que el ACL, para que hable el candado.
        self._sgi_check_closed_origin(any_line=True)
        risks = self.mapped('risk_id').filtered(
            lambda r: r.state in ('controlado', 'cerrado'))
        res = super().unlink()
        # Borrar la última acción terminada de un riesgo controlado/cerrado de
        # atención alta invalida su cierre: el candado lo detecta aquí mismo.
        risks._sgi_check_can_close()
        return res


class SgiNcForceClose(models.TransientModel):
    """Asistente de cierre forzado de una NC con motivo; solo Jefe MAST."""
    _name = 'sgi.nc.force.close'
    _description = "Cierre forzado de no conformidad"

    alert_id = fields.Many2one('quality.alert', string="No conformidad", required=True,
                               help="No conformidad que se va a cerrar sin cumplir los candados.")
    reason = fields.Text(string="Motivo del cierre forzado", required=True)

    def action_confirm(self):
        self.ensure_one()
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe MAST y SGI puede realizar un cierre forzado.")
        alert = self.alert_id
        closing_stage = self.env['quality.alert.stage'].search([
            ('sgi_is_closing_stage', '=', True),
            '|', ('team_ids', '=', False), ('team_ids', 'in', alert.team_id.id),
        ], limit=1)
        if not closing_stage:
            raise UserError("No hay una etapa de cierre configurada para este equipo.")
        # 57.91.0 (K-07): nombre y motivo escapados.
        alert.message_post(body=Markup(
            "<b>Cierre forzado</b> por %s.<br/>Motivo: %s") % (self.env.user.name, self.reason))
        alert.with_context(sgi_force_close=True).write({'stage_id': closing_stage.id})
        return {'type': 'ir.actions.act_window_close'}


class SgiNcCancel(models.TransientModel):
    """NC-4: cancelar solo con motivo y aprobación del Jefe MAST. Cualquier
    usuario del SGI pide la cancelación con su motivo (queda en el chatter y
    agenda la aprobación a MAST); el Jefe MAST la aprueba con el mismo
    asistente. Nunca se llega a Cancelada arrastrando la tarjeta."""
    _name = 'sgi.nc.cancel'
    _description = "Cancelación de no conformidad"

    alert_id = fields.Many2one('quality.alert', string="No conformidad", required=True,
                               help="No conformidad que se cancela.")
    reason = fields.Text(string="Motivo de la cancelación", required=True)
    is_manager = fields.Boolean(compute='_compute_is_manager', help="Indica si usted es Jefe MAST y SGI.")

    @api.depends_context('uid')
    def _compute_is_manager(self):
        manager = self.env.user.has_group('quimibond_sgi.group_sgi_manager')
        for wiz in self:
            wiz.is_manager = manager

    def action_confirm(self):
        self.ensure_one()
        alert = self.alert_id
        if alert.stage_id.sgi_is_cancel_stage:
            raise UserError("La NC %s ya está cancelada." % (alert.sgi_folio or alert.name))
        reason = (self.reason or '').strip()
        if not reason:
            raise UserError("Capture el motivo de la cancelación.")
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            # Solicitud: motivo al historial y actividad al Jefe MAST.
            alert.write({'sgi_cancel_reason': reason,
                         'sgi_cancel_requested_by': self.env.user.id})
            alert.message_post(body=Markup(
                "<b>Solicitud de cancelación</b> de %s.<br/>Motivo: %s") % (self.env.user.name, reason))
            Cron = self.env['sgi.cron']
            Cron._sgi_schedule(
                alert, "Aprobar cancelación de la NC %s" % (alert.sgi_folio or alert.name),
                "%s pide cancelar la NC. Motivo: %s. Apruébela con «Cancelar NC» o "
                "conteste en el chatter." % (self.env.user.name, reason),
                Cron._sgi_manager_user_id())
            return {'type': 'ir.actions.act_window_close'}
        cancel_stage = self.env['quality.alert.stage'].search([
            ('sgi_is_cancel_stage', '=', True),
            '|', ('team_ids', '=', False), ('team_ids', 'in', alert.team_id.id),
        ], limit=1)
        if not cancel_stage:
            raise UserError("No hay una etapa de cancelación configurada para este equipo.")
        requested_by = alert.sgi_cancel_requested_by
        requested_note = (Markup("<br/>Solicitada por %s.") % requested_by.name
                          if requested_by else Markup())
        alert.message_post(body=Markup(
            "<b>NC cancelada</b> por %s (Jefe MAST).<br/>Motivo: %s%s") % (
                self.env.user.name, reason, requested_note))
        alert.with_context(sgi_cancel_approved=True).write({
            'stage_id': cancel_stage.id, 'sgi_cancel_reason': reason})
        # Cierra la actividad de aprobación, si la había.
        alert.activity_ids.sudo().filtered(
            lambda a: (a.summary or '').startswith("Aprobar cancelación")).action_feedback(
            feedback="Cancelación aprobada.")
        return {'type': 'ir.actions.act_window_close'}
