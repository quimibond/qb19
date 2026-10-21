# -*- coding: utf-8 -*-
"""Mis pendientes: la bandeja de cada persona del SGI.

57.145.0 (rediseño, auditoría 2026-10-13: «le doy Hecho y la sigo viendo»):

- **Renglones persistentes.** ``sgi.my.pending`` deja de ser transitorio. El
  autovacuum diario de Odoo borraba los renglones con más de una hora y un
  botón sobre un renglón borrado fallaba sin dejar rastro. Ahora cada persona
  tiene una fila por pendiente (clave: persona, tipo, modelo e id del
  registro); abrir la lista o el respaldo nocturno la actualizan en su lugar
  (``_sgi_store``: se escribe lo que cambió, se crea lo nuevo y se quita lo
  resuelto), así que los ids son estables y «Hechas» ya no reaparecen.
- **Un solo «Hecho» que sabe qué hacer** (``action_done``): la actividad con
  registro de cumplimiento abre su asistente (evidencia si es de registro
  manual); la actividad atrasada por semáforo que lleva registro lo crea al
  vuelo para el periodo en curso y abre el mismo asistente; el aviso se marca
  hecho en Odoo; la acción de NC se termina; el acuse se firma y la medición
  se valida. Lo que se resuelve en su propio registro (NC, requisito legal,
  documento, aprobación, firma, revisión) no tiene «Hecho»: tiene «Ir» y la
  ficha lo explica.
- **Escalamiento es un tipo propio** (antes se mezclaba con «Actividad
  atrasada»): quien lo recibe no la ejecuta; solo «Ir».
- **Ficha del pendiente** (vista formulario): qué hay que hacer (criterio de
  terminado, pasos, instructivo, nota del aviso, plazos de la NC…), sobre qué
  registro, cuándo vence y los botones grandes. En la lista, «Vence» usa
  ``remaining_days`` («Hace 3 días», «Hoy», «En 5 días»).
- **Recarga estándar de Odoo 19.** Ningún botón devuelve ``soft_reload``: el
  renglón se actualiza o se quita en el servidor y la acción devuelve una
  notificación con ``next: act_window_close``, que cierra el asistente y
  recarga la lista por el ``onClose`` de la vista.
- Más formas de actuar: «En proceso», «No aplica este periodo» y «Posponer
  una semana» (avisos propios).

Historia anterior (se conserva la lógica de las fuentes):

56.3.0: una sola lista con semáforo (atrasada: vence antes de hoy; por
vencer: en 7 días o menos; al día). 56.36.0 (entrega 8a, D-04): «Mis
pendientes con todo adentro»: capturar y validar mediciones con plazos en
días hábiles, actividades atrasadas y acuses por persona (con o sin usuario),
firmas, escalamientos pasados sus días hábiles. 56.38.1 (decisiones de Jose):
capturar 5 días hábiles, validar 3, acuse 5; el aprobador solo ve lo que ya
le toca. 57.92.0 (U-02/U-03/U-05): avisos de los crons, «Ir» a donde se hace,
«Leído y entendido» desde el renglón, validar en lote. 57.95.0 (K-08):
resumen guardado por persona para los filtros de Mi equipo. 57.103.0:
registro de cumplimiento por periodo con «En proceso» y «Hecha».
"""
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta
from markupsafe import Markup, escape

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError

# sgi_calendar no define modelos: importarlo aquí no cambia el registro.
from .sgi_calendar import sgi_add_business_days, sgi_nth_business_day
from .sgi_health_const import HEALTH_MODES
from .sgi_menu_paths import SgiNoAccess

PENDING_KINDS = [
    ('accion', "Acción"),
    ('nc', "No conformidad"),
    ('medicion', "Capturar medición"),
    ('validacion', "Validar medición"),
    ('legal', "Requisito legal"),
    ('documento', "Revisión de documento"),
    ('aprobacion', "Aprobación"),
    ('solicitud', "Solicitud por aprobar"),
    ('actividad', "Actividad atrasada"),
    ('escalamiento', "Escalamiento"),
    ('acuse', "Acuse de lectura"),
    ('firma', "Firma"),
    ('aviso', "Aviso"),
    ('revision', "Revisar medición"),
    # 57.168.0: el flujo atrasado de un proceso (entradas con plazo vencido
    # sin salida) le llega una vez a su dueño, no a cada persona del puesto.
    ('flujo', "Flujo atrasado"),
]
# Los tipos que salen de la persona (hr.employee) y no del usuario: la gente
# de planta sin usuario también los tiene.
EMPLOYEE_KINDS = ('actividad', 'escalamiento', 'acuse', 'flujo')
PENDING_STATES = [
    ('atrasada', "Atrasada"),
    ('por_vencer', "Por vencer"),
    ('al_dia', "Al día"),
]
STATE_RANK = {'atrasada': 0, 'por_vencer': 1, 'al_dia': 2}
SOON_DAYS = 7
HORIZON_DAYS = 60
# Días hábiles que «Posponer una semana» mueve un aviso propio.
SNOOZE_BUSINESS_DAYS = 5
# 57.92.0 (U-03): actividades nativas (avisos de los crons) de estas apps que
# se muestran en Mis pendientes, además de las de los modelos ``sgi.*``.
NOTICE_MODELS = ('quality.alert', 'documents.document', 'maintenance.request',
                 'helpdesk.ticket', 'project.task',
                 # 57.94.0 (U-08): aviso semanal de RH por departamento.
                 'hr.department',
                 # 57.100.0 (N-14): salida validada sin CoA.
                 'stock.picking',
                 # 57.105.0: aprobación del MIID retenida por candados.
                 'approval.request')
# Plazos en días hábiles (parámetros del sistema; default entre paréntesis).
CAPTURE_DAYS_PARAM = 'quimibond_sgi.measure_capture_business_days'   # (5)
VALIDATE_DAYS_PARAM = 'quimibond_sgi.measure_validate_business_days'  # (3)
ACK_DAYS_PARAM = 'quimibond_sgi.ack_business_days'                    # (5)
# Indicadores automáticos cuya medición sí tiene que capturar alguien.
_CALC_NEEDS_PERSON = ('error', 'sin_formula', 'manual')
# Campos del renglón que forman su clave (una fila por persona y registro).
_ROW_KEY = ('employee_id', 'user_id', 'kind', 'res_model', 'res_id')
# 57.150.0: tipos que tienen su acción real desde el renglón (además de
# acción, aviso, acuse, validación y actividad): NC (paso que toca), requisito
# legal (evaluar), documento (revisar), solicitud y aprobación (aprobar o
# rechazar) y revisión de medición (contestar).
ROW_ACTION_KINDS = ('nc', 'legal', 'documento', 'solicitud', 'aprobacion', 'revision')
# Estados abiertos del registro de cumplimiento (EXEC_OPEN de
# sgi_activity_execution; aquí sin importar el módulo al cargar).
EXEC_OPEN_STATES = ('pendiente', 'en_proceso')
# Campos que se escriben al actualizar un renglón que ya existe.
_ROW_FIELDS = ('name', 'process_id', 'date_due', 'state', 'state_rank', 'detail',
               'source_name', 'exec_state', 'exec_note', 'exec_manual', 'exec_date_estimated',
               'group_count', 'group_res_ids')
# 57.168.0: campos de Odoo que ligan una solicitud de Firma electrónica a un
# registro del SGI. Las demás firmas (órdenes de compra, recibos…) se juntan
# en un renglón por persona.
SGI_SIGN_FIELDS = (
    ('approval.request', 'sgi_sign_request_id'),
    ('documents.document', 'sgi_publish_sign_request_id'),
    ('sgi.document.ack', 'sign_request_id'),
    ('sgi.epp.delivery', 'sign_request_id'),
)
# 57.168.0: cuántos renglones del grupo se listan en la ficha.
GROUP_DETAIL_MAX = 30


def pending_state(due, today):
    if not due:
        return 'al_dia'
    if due < today:
        return 'atrasada'
    if due <= today + timedelta(days=SOON_DAYS):
        return 'por_vencer'
    return 'al_dia'


def _int_param(env, key, default):
    raw = env['ir.config_parameter'].sudo().get_param(key)
    try:
        return int(raw) if raw not in (None, False, '') else default
    except (TypeError, ValueError):
        return default


def _fmt(value):
    """Fecha como dd/mm/aaaa; lo demás como texto."""
    if isinstance(value, date):
        return value.strftime('%d/%m/%Y')
    return str(value)


def sgi_detail_html(items):
    """[(etiqueta, valor)] → HTML corto y escapado para la ficha del
    pendiente; los vacíos se omiten. Un valor ``Markup`` (nota de un aviso)
    entra tal cual; el texto con saltos de línea los conserva."""
    lines = []
    for label, value in items:
        if value in (False, None, ''):
            continue
        if isinstance(value, Markup):
            body = value
        else:
            body = escape(_fmt(value)).replace('\n', Markup('<br/>'))
        if label:
            lines.append(Markup('<p><b>%s:</b> %s</p>') % (label, body))
        else:
            lines.append(Markup('<p>%s</p>') % body)
    return Markup('').join(lines) if lines else False


class SgiIndicatorMeasureDue(models.Model):
    """G-001 / I-006: una sola regla de vencimiento de la medición."""
    _inherit = 'sgi.indicator.measure'

    captured_date = fields.Date(
        string="Capturada el", readonly=True, copy=False,
        help="Día en que la medición pasó a «Capturado» (a mano o por el cálculo "
             "automático). El dueño del indicador tiene 3 días hábiles desde aquí "
             "para validarla.")

    @api.model_create_multi
    def create(self, vals_list):
        today = fields.Date.context_today(self)
        for vals in vals_list:
            if vals.get('state') == 'capturado' and not vals.get('captured_date'):
                vals['captured_date'] = today
        return super().create(vals_list)

    def write(self, vals):
        newly = self.browse()
        if vals.get('state') == 'capturado' and 'captured_date' not in vals:
            # Recalcular una ya capturada no mueve su plazo.
            newly = self.filtered(lambda m: m.state != 'capturado')
        res = super().write(vals)
        if newly:
            newly.write({'captured_date': fields.Date.context_today(self)})
        return res

    def _sgi_run_day(self):
        """Día hábil en que se mide el periodo: el N-ésimo del mes siguiente
        (``quimibond_sgi.monthly_measure_business_day``, 3) o el primero
        después del domingo de la semana medida."""
        self.ensure_one()
        period = self.period_date
        if self.indicator_id.frequency == 'weekly':
            sunday = period - timedelta(days=period.weekday()) + timedelta(days=6)
            return sgi_add_business_days(self.env, sunday, 1)
        nth = _int_param(self.env, 'quimibond_sgi.monthly_measure_business_day', 3) or 3
        following = period.replace(day=1) + relativedelta(months=1)
        return sgi_nth_business_day(self.env, following.year, following.month, nth)

    def _sgi_capture_due(self):
        """«Capturar»: el día en que se mide más el plazo de captura (5 días
        hábiles desde 56.38.1, decisión de Jose). Es la única regla: la usan
        Mis pendientes y el aviso «Capturar indicador» del cron."""
        self.ensure_one()
        return sgi_add_business_days(self.env, self._sgi_run_day(),
                                     _int_param(self.env, CAPTURE_DAYS_PARAM, 5))

    def _sgi_validate_due(self):
        """«Validar»: 3 días hábiles desde la captura (decisión 3, tanda 2).
        Sin fecha de captura (mediciones anteriores a 56.36.0), desde que se
        creó: el cálculo automático la crea ya capturada."""
        self.ensure_one()
        start = self.captured_date or (self.create_date and self.create_date.date()) \
            or fields.Date.context_today(self)
        return sgi_add_business_days(self.env, start, _int_param(self.env, VALIDATE_DAYS_PARAM, 3))

    def _sgi_period_label(self):
        """«semana del 14/09/2026» o «09/2026» (I-007)."""
        self.ensure_one()
        if not self.period_date:
            return ''
        if self.indicator_id.frequency == 'weekly':
            monday = self.period_date - timedelta(days=self.period_date.weekday())
            return "semana del %s" % monday.strftime('%d/%m/%Y')
        return self.period_date.strftime('%m/%Y')

    def _sgi_needs_capture(self):
        """Una medición pendiente es de alguien solo si el indicador se captura
        a mano o si su cálculo automático no pudo dar el dato."""
        self.ensure_one()
        indicator = self.indicator_id
        if indicator.calc_mode == 'manual':
            return True
        return bool('calc_status' in indicator._fields and indicator.calc_status in _CALC_NEEDS_PERSON)


class SgiMyPending(models.Model):
    """Mis pendientes: bandeja de cada persona con actividades atrasadas,
    escalamientos, firmas, acuses, capturas, validaciones, aprobaciones y
    avisos. Una fila por persona y registro pendiente; se actualiza al abrir
    la lista y cada noche (``_sgi_store``). No guarda historia: lo resuelto
    se quita."""
    _name = 'sgi.my.pending'
    _description = "Mis pendientes (SGI)"
    _order = 'state_rank, date_due, id'
    _rec_name = 'name'
    _SGI_REQUEST_DAYS = 3  # días para contestar una solicitud de Aprobaciones

    user_id = fields.Many2one('res.users', string="Usuario", readonly=True, index=True)
    employee_id = fields.Many2one('hr.employee.public', string="Persona", readonly=True, index=True,
                                  help="Persona a la que le toca el pendiente.")
    kind = fields.Selection(PENDING_KINDS, string="Tipo", required=True, readonly=True,
                            help="Qué hay que hacer: acción, NC, medición, validación, acuse, firma, "
                                 "aprobación, actividad, escalamiento, aviso…")
    name = fields.Char(string="Qué", readonly=True)
    process_id = fields.Many2one('sgi.process', string="Proceso", readonly=True, ondelete='restrict',
                                 help="Proceso del pendiente.")
    date_due = fields.Date(string="Vence", readonly=True, help="Fecha en que vence el pendiente.")
    state = fields.Selection(PENDING_STATES, string="Estado", readonly=True,
                             help="Atrasado, por vencer o al día.")
    state_rank = fields.Integer(readonly=True, help="Orden para mostrar primero lo atrasado.")
    res_model = fields.Char(readonly=True)
    res_id = fields.Integer(readonly=True)
    # 57.145.0: la ficha del pendiente.
    detail = fields.Html(string="Qué hay que hacer", readonly=True, sanitize=True,
                         help="Criterio de terminado, pasos, instructivo, nota del aviso, plazos "
                              "de la NC… lo que hace falta saber para resolverlo sin salir de aquí.")
    source_name = fields.Char(string="Registro", readonly=True,
                              help="El registro de Odoo sobre el que está el pendiente; «Ir» lo abre.")
    # 57.92.0 (U-05): «Hecho» y «Leído y entendido» solo salen en los
    # renglones de quien abre la lista (no en los de Mi equipo).
    is_mine = fields.Boolean(compute='_compute_is_mine',
                             help="El renglón es de quien abre la lista.")
    # 57.103.0: renglón del registro de cumplimiento (sgi.activity.execution).
    exec_state = fields.Selection([
        ('pendiente', "Pendiente"),
        ('en_proceso', "En proceso"),
    ], string="Avance", readonly=True,
        help="Actividades del periodo: pendiente o en proceso (con nota de avance). «En proceso» "
             "no quita el atraso.")
    exec_note = fields.Char(string="Nota de avance", readonly=True)
    exec_date_estimated = fields.Date(string="Fecha estimada", readonly=True,
                                      help="Cuándo espera terminarla quien la marcó «En proceso».")
    exec_manual = fields.Boolean(readonly=True,
                                 help="Actividad de registro manual: «Ir» abre su registro.")
    can_mark_exec = fields.Boolean(compute='_compute_can_act',
                                   help="Quien abre la lista puede marcar el avance del registro de "
                                        "cumplimiento (la persona, su jefe o el Jefe MAST).")
    can_done = fields.Boolean(compute='_compute_can_act',
                              help="«Hecho» aplica a este renglón para quien abre la lista: la "
                                   "actividad con registro de cumplimiento (o que lo lleva), el "
                                   "aviso propio y la acción propia.")
    can_snooze = fields.Boolean(compute='_compute_can_act',
                                help="Aviso propio: se puede posponer una semana.")
    # 57.168.0: un renglón junta lo repetido (las mediciones de un indicador,
    # las firmas que no son del SGI, los avisos de una misma clase, las
    # revisiones de medición, los requisitos legales). «Ir» abre la lista.
    group_count = fields.Integer(string="Cuántos", readonly=True,
                                 help="Cuántos registros junta este renglón (vacío: uno solo).")
    group_res_ids = fields.Char(readonly=True,
                                help="Ids de los registros que junta, «,4,34,64,» (para buscarlos).")
    # 57.174.0 (interfaz, entrega 5): el renglón dice qué sigue, con palabras;
    # la lista deja dos botones (el del tipo y «Abrir»).
    next_step = fields.Char(string="Qué sigue", compute='_compute_next_step',
                            help="Lo que toca hacer con este pendiente.")
    # 57.165.0: los pasos de la NC, campos con su estado (no un párrafo).
    # 57.166.0: «Dónde se hace» es «Ir a hacerlo» (action_open), un solo botón.
    nc_id = fields.Many2one('quality.alert', string="NC", compute='_compute_links', compute_sudo=True)
    nc_due_containment = fields.Date(related='nc_id.sgi_due_containment', string="Contención vence")
    nc_containment_state = fields.Selection(related='nc_id.sgi_containment_state', string="Contención")
    nc_due_root_cause = fields.Date(related='nc_id.sgi_due_root_cause', string="Causa raíz vence")
    nc_root_cause_state = fields.Selection(related='nc_id.sgi_root_cause_state', string="Causa raíz")
    nc_due_plan = fields.Date(related='nc_id.sgi_due_plan', string="Plan de acción vence")
    nc_plan_state = fields.Selection(related='nc_id.sgi_plan_state', string="Plan de acción")
    nc_effectiveness_due = fields.Date(related='nc_id.sgi_effectiveness_due', string="Eficacia vence")

    @api.depends('kind', 'group_count', 'exec_state', 'exec_manual', 'res_model', 'res_id')
    @api.depends_context('uid')
    def _compute_next_step(self):
        steps = {
            'accion': "Terminar la acción",
            'medicion': "Capturar el dato del periodo",
            'validacion': "Revisar el dato y validarlo",
            'legal': "Evaluar el cumplimiento",
            'documento': "Revisar si sigue vigente o cambia",
            'aprobacion': "Aprobar o rechazar",
            'solicitud': "Aprobar o rechazar",
            'escalamiento': "Dar seguimiento: la marca hecha quien la ejecuta",
            'acuse': "Leer el documento y firmar «Leído y entendido»",
            'firma': "Firmar en Firma electrónica",
            'aviso': "Atenderlo o posponerlo una semana",
            'revision': "Abrir los tres registros y contestar",
            'flujo': "Que llegue la entrega atrasada",
        }
        nc_steps = (('nc_containment_state', "Registrar la contención"),
                    ('nc_root_cause_state', "Registrar la causa raíz"),
                    ('nc_plan_state', "Registrar el plan de acción"))
        for row in self:
            if row.group_count > 1:
                row.next_step = "Abrir la lista de %d y atenderlos" % row.group_count
            elif row.kind == 'actividad':
                if row.exec_state == 'en_proceso':
                    row.next_step = "Terminarla (va en proceso)"
                elif row.exec_manual or row.can_done:
                    row.next_step = "Hacerla y marcarla hecha con su evidencia"
                else:
                    row.next_step = "Hacerla: se marca sola al registrarla en Odoo"
            elif row.kind == 'nc':
                todo = [label for fname, label in nc_steps if row.nc_id and row[fname] not in ('hecha', False)]
                row.next_step = todo[0] if todo else "Seguir la NC hasta su eficacia"
            else:
                row.next_step = steps.get(row.kind, "")

    @api.depends('res_model', 'res_id')
    def _compute_links(self):
        for row in self:
            nc = False
            if row.res_id and row.res_model == 'quality.alert':
                nc = self.env['quality.alert'].browse(row.res_id).exists()
            row.nc_id = nc or False

    @api.depends_context('uid')
    def _compute_is_mine(self):
        for row in self:
            row.is_mine = row.user_id == self.env.user

    @api.depends('kind', 'res_model', 'res_id', 'employee_id', 'user_id', 'group_count')
    @api.depends_context('uid')
    def _compute_can_act(self):
        Exec = self.env['sgi.activity.execution'].sudo()
        Activity = self.env['sgi.process.activity'].sudo()
        Employee = self.env['hr.employee'].sudo()
        for row in self:
            mine = row.user_id == self.env.user
            can_exec = can_done = False
            if row.res_model == 'sgi.activity.execution' and row.res_id:
                # 57.149.0: también el escalamiento que apunta a la ejecución
                # atrasada; quien escala la actividad puede marcarla.
                execution = Exec.browse(row.res_id).exists()
                can_exec = bool(execution) and execution.state in EXEC_OPEN_STATES \
                    and Exec._sgi_user_can_mark(execution.employee_id, execution.activity_id)
                can_done = can_exec
            elif row.kind == 'actividad' and row.res_model == 'sgi.process.activity' and row.res_id:
                # Atrasada por semáforo: «Hecho» crea su registro del periodo
                # si la actividad lo lleva (cadencia y método que se mide).
                activity = Activity.browse(row.res_id).exists()
                employee = Employee.browse(row.employee_id.id) if row.employee_id else Employee
                can_done = bool(activity and employee and activity._sgi_exec_applies()
                                and Exec._sgi_user_can_mark(employee, activity))
            elif row.kind in ('aviso', 'accion'):
                can_done = mine
            # 57.150.0 (auditoría UX, fase 2): lo que se resuelve en su
            # registro también se resuelve desde aquí, solo para quien es.
            elif mine and row.kind in ROW_ACTION_KINDS and row.res_id:
                can_done = row._sgi_kind_can_done()
            # 57.168.0: un grupo se atiende en su lista («Ir»); solo el de
            # validaciones valida todas desde el renglón.
            grouped = row.group_count > 1
            row.can_mark_exec = can_exec and not grouped
            row.can_done = can_done and not grouped
            row.can_snooze = row.kind == 'aviso' and mine and not grouped

    # ------------------------------------------------------------------
    # Fuentes: una búsqueda por tipo para todos los usuarios a la vez.
    # ------------------------------------------------------------------
    @api.model
    def _sgi_pending_records(self, users):
        """{tipo: registros (sudo)} de los usuarios dados; mismos criterios
        que tenía cada botón de la pantalla. Los tipos de la persona
        (EMPLOYEE_KINDS) salen de ``_sgi_employee_rows``."""
        env = self.env
        today = fields.Date.context_today(self)
        ids = users.ids
        Alert = env['quality.alert'].sudo()
        domains = self._sgi_kind_domains(ids, today)
        measures = env['sgi.indicator.measure'].sudo().search(
            [('indicator_id.responsible_id', 'in', ids), ('indicator_id.active', '=', True),
             ('state', 'in', ('pendiente', 'capturado')), ('period_date', '<=', today)],
            order='period_date')
        records = {
            'accion': env['sgi.action.line'].sudo().search(domains['accion'][1], order='date_commit, id'),
            'nc': Alert.search(domains['nc'][1], order='create_date')
            if 'nc' in domains else Alert,
            # G-001: «Capturar» solo lo que alguien tiene que capturar.
            'medicion': measures.filtered(lambda m: m.state == 'pendiente' and m._sgi_needs_capture()),
            # I-006: lo ya calculado lo valida el dueño. 57.99.0: los de salud
            # del SGI no se validan (su respuesta es la revisión semanal).
            'validacion': measures.filtered(
                lambda m: m.state == 'capturado' and m.indicator_id.calc_mode not in HEALTH_MODES),
            'legal': env['sgi.legal.requirement'].sudo().search(domains['legal'][1], order='next_eval_date, id'),
            'documento': env['documents.document'].sudo().search(
                domains['documento'][1], order='sgi_next_review_date, sgi_code'),
        }
        # 56.5.0: aprobaciones nativas por dar (reglas de aprobación de Odoo).
        if 'studio.approval.request' in env:
            records['aprobacion'] = env['studio.approval.request'].sudo().search(
                [('mail_activity_id.user_id', 'in', ids)], order='create_date')
        # 56.6.0: solicitudes de Aprobaciones esperando a la persona.
        records['solicitud'] = env['approval.approver'].sudo().search(
            [('user_id', 'in', ids), ('status', '=', 'pending'),
             ('request_id.request_status', '=', 'pending')], order='create_date')
        # 56.36.0 (D-04): firmas de Firma electrónica por hacer.
        if 'sign.request.item' in env:
            records['firma'] = env['sign.request.item'].sudo().search(
                [('partner_id', 'in', users.partner_id.ids), ('state', '=', 'sent'),
                 ('sign_request_id.state', '=', 'sent')], order='create_date')
        # 57.107.0: revisión mensual de la medición (dueño del proceso).
        records['revision'] = env['sgi.measure.review'].sudo().search(
            [('user_id', 'in', ids), ('state', '=', 'pendiente')], order='date_due, id')
        # 57.92.0 (U-03, D-04): avisos de los crons del SGI y actividades de
        # los modelos ``sgi.*``, vencidos o de los próximos 7 días. Las
        # actividades nativas siguen existiendo; aquí solo se muestran. De las
        # apps de NOTICE_MODELS solo los avisos de los crons (con clave).
        Activity = env['mail.activity'].sudo()
        notices = Activity.search(
            [('user_id', 'in', ids), ('date_deadline', '<=', today + timedelta(days=SOON_DAYS)),
             '|', ('res_model', '=like', 'sgi.%'),
             '&', ('res_model', 'in', NOTICE_MODELS), ('sgi_cron_key', '!=', False)],
            order='date_deadline, id')
        if notices:
            covered = self._sgi_notice_covered(records, notices)
            notices = notices.filtered(lambda a: a.id not in covered)
        records['aviso'] = notices
        return records

    @api.model
    def _sgi_kind_domains(self, user_ids, today):
        """57.150.0: {tipo: (modelo, dominio)} de los tipos que salen de un
        registro con responsable (acción, NC, requisito legal, documento por
        revisar). Una sola definición para la bandeja y para saber si un aviso
        de cron ya lo muestra la bandeja (``_sgi_pending_covers``)."""
        soon = today + timedelta(days=HORIZON_DAYS)
        ids = list(user_ids)
        out = {
            'accion': ('sgi.action.line', [('responsible_id', 'in', ids), ('state', 'in', ('abierta', 'vencida'))]),
            'legal': ('sgi.legal.requirement', [
                ('responsible_id', 'in', ids),
                '|', ('next_eval_date', '<=', soon), ('expiry_date', '<=', soon)]),
            'documento': ('documents.document', [
                ('sgi_owner_id', 'in', ids), ('sgi_is_controlled', '=', True),
                ('sgi_state', 'in', ('vigente', 'piloto')),
                ('sgi_next_review_date', '!=', False), ('sgi_next_review_date', '<=', soon)]),
        }
        if 'sgi_responsible_ids' in self.env['quality.alert']._fields:
            out['nc'] = ('quality.alert', [
                ('sgi_responsible_ids', 'in', ids), ('sgi_stage_is_closing', '=', False),
                ('sgi_stage_is_cancel', '=', False)])
        return out

    @api.model
    def _sgi_pending_covers(self, record, user_id):
        """57.150.0 (auditoría UX, fase 2): Mis pendientes de ``user_id`` ya
        muestra ``record`` con su propio renglón (NC de la que es responsable,
        documento suyo por revisar, requisito legal, acción)."""
        if not record or not user_id:
            return False
        today = fields.Date.context_today(self)
        for model, domain in self._sgi_kind_domains([user_id], today).values():
            if model == record._name:
                return bool(self.env[model].sudo().search_count(domain + [('id', '=', record.id)], limit=1))
        return False

    @api.model
    def _sgi_notice_covered(self, records, notices):
        """57.92.0 (U-03): ids de los avisos que ya tienen renglón propio para
        la misma persona (se compara por destinatario, no por clave)."""
        env = self.env
        covered = set()
        # Aprobación nativa (Studio): su renglón es «aprobacion».
        if records.get('aprobacion'):
            covered |= set(records['aprobacion'].mapped('mail_activity_id').ids)
        # Espejo de una acción: su renglón es «accion».
        covered |= set(env['sgi.action.line'].sudo().search(
            [('activity_id', 'in', notices.ids)]).mapped('activity_id').ids)
        # Mismo registro y misma persona que un renglón de otro tipo (NC,
        # requisito legal, documento por revisar…).
        own = set()
        for kind, recs in records.items():
            if kind in ('aprobacion', 'solicitud', 'aviso'):
                continue
            for rec in recs:
                for user in self._sgi_row_users(kind, rec):
                    own.add((user.id, rec._name, rec.id))
        # Acuse pendiente: el renglón «acuse» es del empleado del acuse.
        ack_ids = {}
        # Capturar indicador: el renglón «medicion» es del responsable.
        indicator_acts = notices.browse()
        # 57.95.0 (K-08): «Documentos por leer y firmar» de la persona: sus
        # renglones son los «acuse» (la clave lleva su empleado; el ancla es
        # el documento de su acuse más viejo, que puede ya estar firmado).
        own_ack_acts = {}
        for act in notices:
            if (act.user_id.id, act.res_model, act.res_id) in own:
                covered.add(act.id)
                continue
            key = act.sgi_cron_key or ''
            if key.startswith('acuse_pendiente:'):
                try:
                    ack_ids[act.id] = int(key.split(':', 1)[1])
                except ValueError:
                    pass
            elif key.startswith('acuses_propios:'):
                try:
                    own_ack_acts[act.id] = int(key.split(':', 1)[1])
                except ValueError:
                    pass
            elif key.startswith('capturar_indicador:') and act.res_model == 'sgi.indicator':
                indicator_acts |= act
        if ack_ids:
            acks = env['sgi.document.ack'].sudo().browse(set(ack_ids.values())).exists()
            ack_user = {ack.id: ack.employee_id.user_id.id for ack in acks}
            for act in notices.browse(list(ack_ids)):
                if ack_user.get(ack_ids[act.id]) == act.user_id.id:
                    covered.add(act.id)
        if own_ack_acts:
            emps = env['hr.employee'].sudo().browse(set(own_ack_acts.values())).exists()
            emp_user = {emp.id: emp.user_id.id for emp in emps}
            for act in notices.browse(list(own_ack_acts)):
                if emp_user.get(own_ack_acts[act.id]) == act.user_id.id:
                    covered.add(act.id)
        if indicator_acts:
            indicators = env['sgi.indicator'].sudo().browse(
                set(indicator_acts.mapped('res_id'))).exists()
            responsible = {ind.id: ind.responsible_id.id for ind in indicators}
            for act in indicator_acts:
                if responsible.get(act.res_id) == act.user_id.id:
                    covered.add(act.id)
        return covered

    @api.model
    def _sgi_row(self, kind, rec):
        """Valores de un renglón: qué, proceso y fecha de vencimiento."""
        if kind == 'accion':
            return {'name': rec.name, 'date_due': rec.date_commit,
                    'process_id': rec.alert_id.sgi_process_id.id if rec.alert_id else False}
        if kind == 'nc':
            dues = [due for due, st in ((rec.sgi_due_containment, rec.sgi_containment_state),
                                        (rec.sgi_due_root_cause, rec.sgi_root_cause_state),
                                        (rec.sgi_due_plan, rec.sgi_plan_state))
                    if due and st != 'hecha']
            due = min(dues) if dues else rec.sgi_effectiveness_due
            label = " ".join(p for p in (rec.name, rec.title if 'title' in rec._fields else '') if p)
            return {'name': "Contestar NC %s" % label, 'date_due': due,
                    'process_id': rec.sgi_process_id.id}
        if kind in ('medicion', 'validacion'):
            indicator = rec.indicator_id
            verb = "Capturar" if kind == 'medicion' else "Validar"
            return {'name': "%s %s %s (%s)" % (verb, indicator.code or '', indicator.name or '',
                                               rec._sgi_period_label()),
                    'date_due': rec._sgi_capture_due() if kind == 'medicion' else rec._sgi_validate_due(),
                    'process_id': indicator.process_id.id}
        if kind == 'legal':
            dates = [d for d in (rec.next_eval_date, rec.expiry_date) if d]
            return {'name': "Evaluar %s" % (rec.display_name or rec.name),
                    'date_due': min(dates) if dates else False,
                    'process_id': rec.process_ids[:1].id}
        if kind == 'solicitud':
            req = rec.request_id
            category = req.category_id
            return {'name': "Aprobar %s" % (req.name or category.name or ''),
                    # Vence 3 días después de enviada: antes vencía el mismo
                    # día del envío y todo salía «Atrasada».
                    'date_due': req.date_confirmed and fields.Date.add(
                        fields.Date.to_date(req.date_confirmed), days=self._SGI_REQUEST_DAYS),
                    'process_id': (category.sgi_role_id.activity_id.process_id.id
                                   if 'sgi_role_id' in category._fields else False)
                    or req.sgi_affected_process_ids[:1].id}
        if kind == 'aprobacion':
            rule = rec.rule_id
            record = self.env[rule.model_name].sudo().browse(rec.res_id).exists() if rule.model_name else None
            return {'name': "Aprobar %s%s" % (rule.message or rule.name or '',
                                                (" — %s" % record.display_name) if record else ''),
                    'date_due': rec.mail_activity_id.date_deadline,
                    'process_id': rule.sgi_role_id.activity_id.process_id.id
                    if 'sgi_role_id' in rule._fields else False}
        if kind == 'firma':
            request = rec.sign_request_id
            due = request.validity if 'validity' in request._fields else False
            if not due and request.create_date:
                due = sgi_add_business_days(self.env, request.create_date, self._SGI_REQUEST_DAYS)
            return {'name': "Firmar %s" % (request.reference or request.display_name or ''),
                    'date_due': due, 'process_id': False}
        if kind == 'revision':
            return {'name': rec.name, 'date_due': rec.date_due, 'process_id': rec.process_id.id}
        if kind == 'aviso':
            # 57.92.0 (U-03): qué dice el aviso y sobre qué registro.
            what = rec.summary or rec.activity_type_id.name or "Aviso"
            return {'name': "%s — %s" % (what, rec.res_name or rec.res_model),
                    'date_due': rec.date_deadline, 'process_id': False}
        # I-007: el documento se nombra por su título limpio, sin la clave vieja.
        title = rec.sgi_title if 'sgi_title' in rec._fields else False
        return {'name': "Revisar %s" % (title or rec.name or ''),
                'date_due': rec.sgi_next_review_date, 'process_id': rec.sgi_process_id.id}

    @api.model
    def _sgi_activity_detail(self, activity, first=()):
        """Qué hay que hacer en una actividad del procedimiento: criterio de
        terminado, pasos, instructivo, dónde se hace y qué hacer si no se
        puede cumplir."""
        items = list(first)
        items.append(("Criterio de terminado", activity.done_criteria))
        items.append(("Cómo", activity.how_steps))
        instruction = activity.instruction_id
        if instruction:
            title = instruction.sgi_title if 'sgi_title' in instruction._fields else False
            items.append(("Instructivo", title or instruction.name))
        # 57.165.0: «Dónde se hace» es el botón «Ir a hacerlo» de la ficha.
        items.append(("Si no se puede cumplir", activity.on_fail))
        return sgi_detail_html(items)

    @api.model
    def _sgi_row_detail(self, kind, rec):
        """57.145.0: ``detail`` (HTML) y ``source_name`` del renglón, para la
        ficha del pendiente. Solo lectura de campos que ya se leen en
        ``_sgi_row``; nunca truena por un campo de otra app que no esté."""
        source = rec.display_name or ''
        detail = False
        if kind == 'accion':
            alert = rec.alert_id
            origin = rec.origin_display if 'origin_display' in rec._fields else ''
            detail = sgi_detail_html([
                ("Compromiso", rec.date_commit),
                ("Origen", origin or (alert.display_name if alert else '')),
                ("Avance", ("%s %%" % rec.progress) if rec.progress else ''),
            ])
        elif kind == 'nc':
            # 57.165.0: los pasos (contención, causa raíz, plan y eficacia)
            # salen en la ficha como campos con su estado.
            detail = sgi_detail_html([
                (None, "Registre el paso que toca con «Registrar paso» o contéstelo en la ficha de la "
                       "NC («Ir»)."),
            ])
        elif kind in ('medicion', 'validacion'):
            indicator = rec.indicator_id
            value = rec.value if 'value' in rec._fields else False
            detail = sgi_detail_html([
                ("Indicador", "%s %s" % (indicator.code or '', indicator.name or '')),
                ("Periodo", rec._sgi_period_label()),
                ("Valor capturado", value if kind == 'validacion' and value not in (False, None) else ''),
                ("Capturada el", rec.captured_date if kind == 'validacion' else False),
                (None, "Capture el valor en la medición: pulse «Capturar»." if kind == 'medicion'
                 else "Revise el valor y su evidencia y pulse «Validar»."),
            ])
        elif kind == 'legal':
            detail = sgi_detail_html([
                ("Próxima evaluación", rec.next_eval_date),
                ("Vigencia", rec.expiry_date),
                (None, "Se evalúa en el requisito legal: pulse «Ir»."),
            ])
        elif kind == 'documento':
            detail = sgi_detail_html([
                ("Clave", rec.sgi_code if 'sgi_code' in rec._fields else ''),
                ("Próxima revisión", rec.sgi_next_review_date),
                (None, "Se revisa en el documento: pulse «Ir»."),
            ])
        elif kind == 'solicitud':
            req = rec.request_id
            source = req.display_name or ''
            detail = sgi_detail_html([
                ("Categoría", req.category_id.name),
                ("Solicita", req.request_owner_id.name if 'request_owner_id' in req._fields else ''),
                ("Enviada", fields.Date.to_date(req.date_confirmed) if req.date_confirmed else False),
                ("Motivo", req.reason if 'reason' in req._fields else ''),
                (None, "Se aprueba o rechaza en Aprobaciones: pulse «Ir»."),
            ])
        elif kind == 'aprobacion':
            rule = rec.rule_id
            detail = sgi_detail_html([
                ("Regla", rule.message or rule.name),
                (None, "Se aprueba en el documento que la espera: pulse «Ir»."),
            ])
        elif kind == 'firma':
            request = rec.sign_request_id
            source = request.reference or request.display_name or ''
            detail = sgi_detail_html([(None, "Se firma en Firma electrónica: pulse «Firmar».")])
        elif kind == 'revision':
            detail = sgi_detail_html([(None, "Conteste la revisión mensual: pulse «Ir».")])
        elif kind == 'aviso':
            source = rec.res_name or rec.res_model or ''
            detail = sgi_detail_html([
                (None, rec.note if rec.note else ''),
                ("Sobre", source),
                (None, "Atiéndalo en su registro («Ir») y luego márquelo «Hecho»; si todavía no "
                       "toca, «Posponer una semana»."),
            ])
        return {'detail': detail, 'source_name': source[:200] if source else False}

    @api.model
    def _sgi_row_users(self, kind, rec):
        if kind == 'accion':
            return rec.responsible_id
        if kind == 'nc':
            return rec.sgi_responsible_ids
        if kind in ('medicion', 'validacion'):
            return rec.indicator_id.responsible_id
        if kind == 'legal':
            return rec.responsible_id
        if kind == 'aprobacion':
            return rec.mail_activity_id.user_id
        if kind == 'solicitud':
            return rec.user_id
        if kind == 'firma':
            return rec.partner_id.user_ids
        if kind in ('aviso', 'revision'):
            return rec.user_id
        return rec.sgi_owner_id

    # ------------------------------------------------------------------
    # Tipos de la persona: actividades atrasadas, escalamientos y acuses.
    # ------------------------------------------------------------------
    @api.model
    def _sgi_activity_late_due(self, activity, today):
        """Fecha en que la actividad quedó atrasada, o False si no se sabe.
        Con vencimiento periódico, el vencimiento que se pasó (el del periodo
        en curso si ya pasó; si no, el del periodo anterior); sin él, la
        última ejecución más la ventana de su cadencia."""
        from .sgi_activity_execution import sgi_exec_start_date
        exec_start = sgi_exec_start_date(self.env)
        due = activity._sgi_periodic_due(today)
        if due is not None:
            if due < today:
                late = due
            else:
                start = activity._sgi_period_start(today)
                late = activity._sgi_periodic_due(start - timedelta(days=1)) if start else None
        else:
            days = activity._SGI_CADENCE_DAYS.get(activity.measure_cadence)
            last = activity.measure_last_date
            late = fields.Datetime.to_datetime(last).date() + timedelta(days=days) if days and last else None
        # 57.149.0: lo que venció antes del arranque no se escala.
        if not late or late < exec_start:
            return False
        return late

    @api.model
    def _sgi_employee_rows(self, employees):
        """{empleado.id: [renglones]} de los tipos de la persona (I-001,
        I-012): actividades que ejecuta y van atrasadas en Mi procedimiento,
        escalamientos cuyo atraso ya pasó sus días hábiles y acuses de
        «leído y entendido» pendientes. Solo empleados de la empresa del SGI
        (D-03).

        56.38.1 (decisión de Jose): el rol «Aprueba» ya no recibe renglón
        por el atraso del ejecutor. La actividad del procedimiento no sabe
        si el ejecutor ya cumplió y falta la aprobación: su semáforo sale de
        la evidencia (``measure_state`` y entradas con plazo vencido), que no
        distingue «no la hizo» de «la hizo y falta aprobar». Lo que sí le
        toca al aprobador llega por Odoo cuando el ejecutor ya actuó: la
        regla de aprobación nativa del botón (tipo ``aprobacion``), la
        solicitud de Aprobaciones (``solicitud``) o la firma (``firma``). El
        atraso del ejecutor le llega a quien tiene «Escala», pasados sus días
        hábiles (57.145.0: tipo ``escalamiento``)."""
        employees = employees.sudo()
        result = {emp.id: [] for emp in employees}
        if not employees:
            return result
        env = self.env
        today = fields.Date.context_today(self)
        company = env['sgi.config']._sgi_company()
        employees = employees.filtered(lambda e: not e.company_id or e.company_id == company)
        roles_by_emp = {}
        # 57.13.0: los escalamientos a un rol relativo («Dueño del proceso»)
        # no son de un puesto: le llegan al dueño del proceso de la actividad,
        # o a su jefe si el dueño también la ejecuta (sgi_relative_roles).
        # 57.159.0: con los escalamientos en pausa no llega ningún renglón
        # «Escalamiento»; quien ejecuta sigue viendo su actividad atrasada.
        escalate = env['sgi.cron']._sgi_escalations_on()
        relative = env['sgi.activity.role'].sudo()._sgi_relative_escalations(employees) \
            if escalate else {}
        for emp in employees:
            detail = emp.sgi_mp_role_ids.filtered(lambda r: r.role == 'ejecuta')
            received = (emp.sgi_mp_received_role_ids if escalate else env['sgi.activity.role']) \
                | relative.get(emp.id, env['sgi.activity.role'])
            roles_by_emp[emp.id] = (detail, received)
        all_roles = env['sgi.activity.role'].sudo()
        for detail, received in roles_by_emp.values():
            all_roles |= detail | received
        all_roles = all_roles.filtered(
            lambda r: r.activity_id.active and r.activity_id.process_id.active
            and (not r.company_id or r.company_id == company))
        activities = all_roles.activity_id
        status = env['hr.job']._sgi_mp_status_map(activities) if activities else {}
        late = activities.filtered(lambda a: status.get(a, ('',))[0] == 'atrasada')
        late_due = {act.id: self._sgi_activity_late_due(act, today) for act in late}
        # 57.168.0 (Jose 2026-10-20): el flujo atrasado (entradas con plazo
        # vencido sin salida) es del proceso, no de cada persona del puesto:
        # no sale a quien la ejecuta ni como escalamiento; le llega a su
        # dueño (``_sgi_flow_rows``).
        flow = self._sgi_flow_late(late)
        # 57.103.0: lo que tiene registro de cumplimiento sale de ahí (un
        # renglón por periodo abierto, con su avance), no del semáforo.
        executions, with_exec = self._sgi_execution_rows(employees, today)
        # 57.149.0: el escalamiento de una actividad con registro de
        # cumplimiento apunta a cada periodo abierto atrasado de quien la
        # ejecuta (accionable: «Ir», «Hecho», «No aplica»); sin registro,
        # como antes, a la actividad.
        late_open, tracked = self._sgi_escalation_executions(
            all_roles.filtered(lambda r: r.role == 'escala').activity_id, today)

        def label(activity):
            number = activity.number or activity.legacy_number or ''
            return ("%s %s" % (number, activity.name or '')).strip()

        escalation_dates = {}
        for emp in employees:
            detail, received = roles_by_emp[emp.id]
            rows = result[emp.id]
            seen = set()
            for role in detail & all_roles:
                activity = role.activity_id
                if activity not in late or activity.id in seen or activity.id in flow \
                        or (emp.id, activity.id) in with_exec:
                    continue
                seen.add(activity.id)
                detail_txt = status[activity][2]
                how = ("Esta actividad lleva registro de cumplimiento: «Hecho» abre el del "
                       "periodo en curso." if activity._sgi_exec_applies()
                       else "Se cumple haciendo su registro en Odoo o por evento: pulse «Ir».")
                rows.append({
                    'kind': 'actividad', 'name': "Hacer %s%s" % (
                        label(activity), (" — %s" % detail_txt) if detail_txt else ''),
                    'date_due': late_due[activity.id], 'process_id': activity.process_id.id,
                    'res_model': 'sgi.process.activity', 'res_id': activity.id,
                    'source_name': label(activity)[:200],
                    'detail': self._sgi_activity_detail(
                        activity, [("Por qué está aquí", detail_txt), (None, how)]),
                    # Van atrasadas por definición, aunque no se sepa desde cuándo.
                    'state': 'atrasada'})
            seen_exec = set()
            for role in received & all_roles:
                activity = role.activity_id
                if activity.id in tracked:
                    for execution in late_open.get(activity.id, []):
                        if execution.id in seen_exec or execution.employee_id == emp:
                            continue
                        key = (execution.date_due, role.after_days or 0)
                        if key not in escalation_dates:
                            escalation_dates[key] = sgi_add_business_days(env, key[0], key[1])
                        if escalation_dates[key] > today:
                            continue
                        seen_exec.add(execution.id)
                        rows.append(self._sgi_escalation_row(execution, role, escalation_dates[key]))
                    continue
                due = late_due.get(activity.id) if activity in late else False
                if not due or activity.id in seen or activity.id in flow:
                    continue
                key = (due, role.after_days or 0)
                if key not in escalation_dates:  # una consulta al calendario por fecha y plazo
                    escalation_dates[key] = sgi_add_business_days(env, due, key[1])
                escalated = escalation_dates[key]
                if escalated > today:
                    continue
                seen.add(activity.id)
                executors = ", ".join(activity.role_ids.filtered(
                    lambda r: r.role == 'ejecuta').mapped('job_id.name')) \
                    if 'job_id' in activity.role_ids._fields else ''
                rows.append({
                    'kind': 'escalamiento', 'name': "Escalamiento: %s (atrasada desde el %s)" % (
                        label(activity), due.strftime('%d/%m/%Y')),
                    'date_due': escalated, 'process_id': activity.process_id.id,
                    'res_model': 'sgi.process.activity', 'res_id': activity.id,
                    'source_name': label(activity)[:200],
                    'detail': self._sgi_activity_detail(activity, [
                        ("Atrasada desde", due),
                        ("La ejecuta", executors),
                        ("Le llega a usted", "pasados %d días hábiles de atraso (rol «Escala»)"
                         % (role.after_days or 0)),
                        (None, "Quien la ejecuta la marca hecha; usted puede ir a la actividad "
                               "y darle seguimiento."),
                    ]),
                    'state': 'atrasada'})
            for execution in executions.get(emp.id, []):
                seen.add(execution.activity_id.id)
                rows.append(self._sgi_execution_row(execution))
        for emp_id, flow_rows in self._sgi_flow_rows(employees, company).items():
            result[emp_id].extend(flow_rows)
        ack_days = _int_param(env, ACK_DAYS_PARAM, 5)
        acks = env['sgi.document.ack'].sudo().search(
            [('employee_id', 'in', employees.ids), ('state', '=', 'pendiente'),
             ('document_id.active', '=', True)], order='create_date')
        ack_dues = {}
        for ack in acks:
            doc = ack.document_id
            title = doc.sgi_title if 'sgi_title' in doc._fields else False
            revision = doc.sgi_revision_label if 'sgi_revision_label' in doc._fields else False
            start = (ack.create_date or fields.Datetime.now()).date()
            if start not in ack_dues:
                ack_dues[start] = sgi_add_business_days(env, start, ack_days)
            due = ack_dues[start]
            doc_name = title or doc.name or ''
            result[ack.employee_id.id].append({
                'kind': 'acuse', 'name': "Leer y firmar %s%s" % (
                    doc_name, (" (rev. %s)" % revision) if revision else ''),
                'date_due': due,
                'process_id': doc.sgi_process_id.id if 'sgi_process_id' in doc._fields else False,
                'res_model': 'sgi.document.ack', 'res_id': ack.id,
                'source_name': doc_name[:200],
                'detail': sgi_detail_html([
                    ("Documento", doc_name), ("Revisión", revision),
                    ("Clave", doc.sgi_code if 'sgi_code' in doc._fields else ''),
                    (None, "«Leer» abre el archivo; «Leído y entendido» deja su acuse con la "
                           "fecha de hoy."),
                ])})
        return result

    @api.model
    def _sgi_flow_late(self, activities):
        """57.168.0: {actividad.id: semana medida} de las actividades cuya
        última semana medida tiene entradas con plazo vencido sin salida (lo
        mismo que pone «Atrasada» en ``hr.job._sgi_mp_status_map``)."""
        if not activities:
            return {}
        latest = {}
        for stat in self.env['sgi.activity.week.stat'].sudo().search(
                [('activity_id', 'in', activities.ids)], order='period_start desc'):
            latest.setdefault(stat.activity_id.id, stat)
        return {aid: stat for aid, stat in latest.items() if stat.late_open_count}

    @api.model
    def _sgi_flow_rows(self, employees, company):
        """57.168.0: {empleado.id: [renglones «Flujo atrasado»]} de los
        procesos que tienen por dueño a esos empleados; los de procesos sin
        dueño, al Jefe MAST. Uno por actividad, con la semana medida como
        fecha."""
        env = self.env
        result = {}
        Process = env['sgi.process'].sudo()
        mast_uid = env['sgi.cron']._sgi_manager_user_id()
        mast = employees.filtered(lambda e: mast_uid and e.user_id.id == mast_uid)[:1]
        processes = Process.search([('owner_id', 'in', employees.ids)])
        if mast:
            processes |= Process.search([('owner_id', '=', False)])
        processes = processes.filtered(lambda p: not p.company_id or p.company_id == company)
        if not processes:
            return result
        activities = env['sgi.process.activity'].sudo().search(
            [('process_id', 'in', processes.ids), ('active', '=', True)])
        flow = self._sgi_flow_late(activities)
        for activity in activities.filtered(lambda a: a.id in flow):
            owner = activity.process_id.owner_id
            emp = owner if owner in employees else mast
            if not emp:
                continue
            stat = flow[activity.id]
            number = activity.number or activity.legacy_number or ''
            label = ("%s %s" % (number, activity.name or '')).strip()
            executors = ", ".join(activity.role_ids.filtered(lambda r: r.role == 'ejecuta').mapped('job_id.name'))
            result.setdefault(emp.id, []).append({
                'kind': 'flujo',
                'name': "Flujo atrasado: %s — %d entrada(s) con plazo vencido sin salida" % (
                    label, stat.late_open_count),
                'date_due': stat.period_start, 'process_id': activity.process_id.id,
                'res_model': 'sgi.process.activity', 'res_id': activity.id,
                'source_name': label[:200],
                'detail': self._sgi_activity_detail(activity, [
                    ("Semana medida", stat.period_start),
                    ("Entradas con plazo vencido sin salida", stat.late_open_count),
                    ("La ejecuta", executors),
                    (None, "Le llega a usted como dueño del proceso: el atraso es del flujo, no de una "
                           "persona. «Ir a hacerlo» abre la pantalla; revíselo con quien la ejecuta."),
                ]),
                'state': 'atrasada'})
        return result

    @api.model
    def _sgi_escalation_executions(self, activities, today):
        """57.149.0: ({actividad.id: [periodos abiertos atrasados]}, {ids de
        actividades con registro de cumplimiento}). Atrasado: ya empezó, ya
        venció y no venció antes del arranque."""
        from .sgi_activity_execution import EXEC_OPEN, sgi_exec_start_date
        Exec = self.env['sgi.activity.execution'].sudo()
        if not activities:
            return {}, set()
        tracked = {act.id for [act] in Exec._read_group(
            [('activity_id', 'in', activities.ids)], ['activity_id'])}
        late = {}
        for execution in Exec.search([
                ('activity_id', 'in', list(tracked)), ('state', 'in', EXEC_OPEN),
                ('period_start', '<=', today), ('date_due', '<', today),
                ('date_due', '>=', sgi_exec_start_date(self.env))], order='date_due, id'):
            late.setdefault(execution.activity_id.id, []).append(execution)
        return late, tracked

    @api.model
    def _sgi_escalation_row(self, execution, role, escalated):
        """Renglón de escalamiento de un periodo atrasado de otra persona."""
        execution = execution.sudo()
        activity = execution.activity_id
        number = activity.number or activity.legacy_number or ''
        label = ("%s %s" % (number, activity.name or '')).strip()
        who = execution.employee_id.name or ''
        note = (execution.progress_note or '').strip() if execution.state == 'en_proceso' else ''
        return {
            'kind': 'escalamiento',
            'name': "Escalamiento: %s — %s (%s, atrasada desde el %s)" % (
                label, who, execution.period_label or '', execution.date_due.strftime('%d/%m/%Y')),
            'date_due': escalated, 'process_id': activity.process_id.id,
            'res_model': 'sgi.activity.execution', 'res_id': execution.id,
            'source_name': (execution.name or '')[:200],
            'detail': self._sgi_activity_detail(activity, [
                ("La ejecuta", who),
                ("Periodo", execution.period_label),
                ("Atrasada desde", execution.date_due),
                ("Avance", note),
                ("Le llega a usted", "pasados %d días hábiles de atraso (rol «Escala»)" % (role.after_days or 0)),
                (None, "«Ir» abre el registro del periodo. Puede marcarlo «Hecho» (con evidencia) o «No "
                       "aplica este periodo» (con motivo) si lo resolvió con la persona."),
            ]),
            'state': 'atrasada',
            'exec_state': execution.state, 'exec_note': note or False,
            'exec_date_estimated': execution.date_estimated if execution.state == 'en_proceso' else False,
            'exec_manual': activity._sgi_exec_is_manual(),
        }

    @api.model
    def _sgi_execution_rows(self, employees, today):
        """57.103.0: ({empleado.id: [renglones abiertos del registro de
        cumplimiento ya empezados]}, {(empleado.id, actividad.id) con algún
        renglón}). Con registro, la actividad ya no sale por el semáforo."""
        from .sgi_activity_execution import EXEC_OPEN, sgi_exec_start_date
        Exec = self.env['sgi.activity.execution'].sudo()
        if not employees:
            return {}, set()
        with_exec = {(emp.id, act.id) for emp, act in Exec._read_group(
            [('employee_id', 'in', employees.ids)], ['employee_id', 'activity_id'])}
        result = {}
        # 57.149.0: un periodo que venció antes del arranque no es pendiente
        # (lo cierra el saneamiento «No aplica: anteriores al arranque»).
        for execution in Exec.search([
                ('employee_id', 'in', employees.ids), ('state', 'in', EXEC_OPEN),
                ('period_start', '<=', today), ('activity_id.active', '=', True),
                ('date_due', '>=', sgi_exec_start_date(self.env))],
                order='date_due, id'):
            result.setdefault(execution.employee_id.id, []).append(execution)
        return result, with_exec

    @api.model
    def _sgi_execution_row(self, execution):
        """Renglón de Mis pendientes de un periodo abierto de una actividad."""
        execution = execution.sudo()
        activity = execution.activity_id
        name = "Hacer %s" % execution.name
        note = False
        if execution.state == 'en_proceso':
            note = (execution.progress_note or '').strip()
            name += " — En proceso%s%s" % (
                (": %s" % note) if note else '',
                (" (estimada %s)" % execution.date_estimated.strftime('%d/%m/%Y'))
                if execution.date_estimated else '')
        manual = activity._sgi_exec_is_manual()
        how = ("Al terminarla pulse «Hecho» y deje la evidencia (nota o archivo)." if manual
               else "Se marca hecha sola cuando aparece su registro en Odoo dentro del periodo; "
                    "si se hizo por otro lado, «Hecho» pide decir dónde.")
        return {
            'kind': 'actividad', 'name': name, 'date_due': execution.date_due,
            'process_id': execution.process_id.id,
            'res_model': 'sgi.activity.execution', 'res_id': execution.id,
            'source_name': (execution.name or '')[:200],
            'detail': self._sgi_activity_detail(activity, [
                ("Periodo", execution.period_label),
                ("Avance", note),
                (None, how),
            ]),
            'exec_state': execution.state, 'exec_note': note or False,
            'exec_date_estimated': execution.date_estimated if execution.state == 'en_proceso' else False,
            'exec_manual': manual,
        }

    @api.model
    def _sgi_finish(self, row, today):
        if not row.get('state'):
            row['state'] = pending_state(row['date_due'], today)
        row['state_rank'] = STATE_RANK[row['state']]
        return row

    @staticmethod
    def _sgi_sort(rows):
        rows.sort(key=lambda r: (r['state_rank'], r['date_due'] or fields.Date.to_date('9999-12-31')))

    @api.model
    def _sgi_user_employees(self, users):
        """{usuario.id: empleado} de la empresa del SGI."""
        company = self.env['sgi.config']._sgi_company()
        found = {}
        for emp in self.env['hr.employee'].sudo().search(
                [('user_id', 'in', users.ids), ('company_id', 'in', (company.id, False))]):
            found.setdefault(emp.user_id.id, emp)
        return found

    @api.model
    def _sgi_sgi_sign_requests(self, items):
        """57.168.0: ids de las solicitudes de firma de ``items`` que son del
        SGI (procedimiento, cambio documental, acuse, EPP)."""
        if not items:
            return set()
        requests = items.sign_request_id
        found = set()
        for model, fname in SGI_SIGN_FIELDS:
            if model in self.env and fname in self.env[model]._fields:
                found |= {req.id for [req] in self.env[model].sudo().with_context(active_test=False)._read_group(
                    [(fname, 'in', requests.ids)], [fname])}
        return found

    @api.model
    def _sgi_group_key(self, kind, rec, sgi_signs=()):
        """57.168.0: (clave, nombre del grupo, sustantivo) del renglón si se
        junta con otros de la misma persona; None si va solo."""
        if kind in ('medicion', 'validacion'):
            indicator = rec.indicator_id
            verb = "Capturar" if kind == 'medicion' else "Validar"
            return ((kind, indicator.id), "%s %s %s" % (verb, indicator.code or '', indicator.name or ''),
                    "mediciones")
        if kind == 'firma' and rec.sign_request_id.id not in sgi_signs:
            return (('firma', 'fuera_sgi'), "Firmar en Firma electrónica", "documentos")
        if kind == 'aviso' and rec.sgi_cron_kind:
            what = (rec.summary or rec.activity_type_id.name or "Aviso").split(':', 1)[0].strip()
            return (('aviso', rec.sgi_cron_kind), what, "avisos")
        if kind == 'revision':
            return (('revision',), "Revisar la medición de sus actividades", "actividades")
        if kind == 'legal':
            return (('legal',), "Evaluar requisitos legales", "requisitos")
        return None

    @api.model
    def _sgi_grouped(self, vals_list):
        """57.168.0: junta en un renglón los de la misma persona y el mismo
        grupo (``_group``). El renglón del grupo toma la clave del más
        urgente, la fecha más vieja y el peor estado; su ficha lista lo que
        junta. Uno solo se queda como está."""
        order, groups = [], {}
        for vals in vals_list:
            group = vals.get('_group')
            if not group:
                order.append(vals)
                continue
            key = (vals.get('employee_id') or False, vals.get('user_id') or False, group[0])
            if key not in groups:
                groups[key] = (group, [])
                order.append(key)
            groups[key][1].append(vals)
        out = []
        for item in order:
            if not isinstance(item, tuple):
                out.append(item)
                continue
            (_key, label, noun), members = groups[item]
            if len(members) == 1:
                out.append(members[0])
                continue
            members = sorted(members, key=lambda r: (
                r.get('state_rank', 2), r.get('date_due') or fields.Date.to_date('9999-12-31')))
            first = members[0]
            dues = [m['date_due'] for m in members if m.get('date_due')]
            items = [(None, m.get('name')) for m in members[:GROUP_DETAIL_MAX]]
            if len(members) > GROUP_DETAIL_MAX:
                items.append((None, "… y %d más" % (len(members) - GROUP_DETAIL_MAX)))
            out.append(dict(
                first, name="%s — %d %s" % (label, len(members), noun),
                date_due=min(dues) if dues else False,
                source_name=label[:200],
                detail=sgi_detail_html([(None, "Junta %d %s; «Ir» abre la lista." % (len(members), noun))]
                                       + items),
                group_count=len(members),
                group_res_ids=",%s," % ",".join(str(m['res_id']) for m in members),
                exec_state=False, exec_note=False, exec_date_estimated=False, exec_manual=False))
        return out

    def _sgi_res_ids(self):
        """57.168.0: los ids que representa el renglón (uno o los del grupo)."""
        self.ensure_one()
        if self.group_count and self.group_res_ids:
            return [int(x) for x in self.group_res_ids.split(',') if x.strip().isdigit()]
        return [self.res_id] if self.res_id else []

    @api.model
    def _sgi_pending_values(self, users):
        """{usuario.id: [valores de renglón]} ya con estado y orden."""
        today = fields.Date.context_today(self)
        result = {uid: [] for uid in users.ids}
        pending = self._sgi_pending_records(users)
        sgi_signs = self._sgi_sgi_sign_requests(pending.get('firma'))
        for kind, records in pending.items():
            for rec in records:
                row = dict(self._sgi_row(kind, rec), kind=kind,
                           res_model=rec._name, res_id=rec.id)
                row.update(self._sgi_row_detail(kind, rec))
                group = self._sgi_group_key(kind, rec, sgi_signs)
                if group:
                    row['_group'] = group
                if kind == 'aprobacion':
                    # «Abrir» lleva al documento que espera la aprobación.
                    row.update(res_model=rec.rule_id.model_name, res_id=rec.res_id)
                elif kind == 'solicitud':
                    row.update(res_model='approval.request', res_id=rec.request_id.id)
                self._sgi_finish(row, today)
                for user in self._sgi_row_users(kind, rec):
                    if user.id in result:
                        result[user.id].append(dict(row, user_id=user.id))
        by_user = self._sgi_user_employees(users)
        if by_user:
            emp_rows = self._sgi_employee_rows(self.env['hr.employee'].sudo().browse(
                [emp.id for emp in by_user.values()]))
            for uid, emp in by_user.items():
                for row in emp_rows.get(emp.id, []):
                    result[uid].append(dict(self._sgi_finish(row, today), user_id=uid))
        for rows in result.values():
            self._sgi_sort(rows)
        return result

    @api.model
    def _sgi_pending_values_employees(self, employees):
        """{empleado.id: [renglones]}: con usuario, lo de su usuario (que ya
        trae lo de la persona); sin usuario, lo de la persona (I-021)."""
        employees = employees.sudo()
        today = fields.Date.context_today(self)
        with_user = employees.filtered('user_id')
        by_user = self._sgi_pending_values(with_user.user_id) if with_user else {}
        without = employees - with_user
        emp_rows = self._sgi_employee_rows(without) if without else {}
        result = {}
        for emp in employees:
            if emp.user_id:
                result[emp.id] = [dict(row) for row in by_user.get(emp.user_id.id, [])]
            else:
                rows = [self._sgi_finish(dict(row), today) for row in emp_rows.get(emp.id, [])]
                self._sgi_sort(rows)
                result[emp.id] = rows
        return result

    @staticmethod
    def _sgi_count(rows):
        late = sum(1 for r in rows if r['state'] == 'atrasada')
        worst = rows[0]['state'] if rows else False
        return (len(rows), late, worst)

    @api.model
    def _sgi_summary(self, users):
        """{usuario.id: (total, atrasadas, peor estado)} para botones y Mi equipo."""
        return {uid: self._sgi_count(rows) for uid, rows in self._sgi_pending_values(users).items()}

    @api.model
    def _sgi_summary_employees(self, employees):
        """{empleado.id: (total, atrasadas, peor estado)}, con o sin usuario."""
        return {eid: self._sgi_count(rows)
                for eid, rows in self._sgi_pending_values_employees(employees).items()}

    # ------------------------------------------------------------------
    # Renglones guardados (57.145.0): una fila por persona y registro.
    # ------------------------------------------------------------------
    @staticmethod
    def _sgi_key_of_vals(vals):
        return tuple((vals.get(f) or False) for f in _ROW_KEY)

    def _sgi_key(self):
        self.ensure_one()
        return (self.employee_id.id or False, self.user_id.id or False, self.kind,
                self.res_model or False, self.res_id or False)

    def _sgi_differs(self, vals):
        """True si algún valor cambia respecto al renglón guardado."""
        self.ensure_one()
        for name in _ROW_FIELDS:
            if name not in vals:
                continue
            current = self[name]
            if isinstance(current, models.BaseModel):
                current = current.id or False
            if (current or False) != (vals[name] or False):
                return True
        return False

    @api.model
    def _sgi_store(self, domain, vals_list):
        """Deja en la tabla exactamente los renglones ``vals_list`` dentro de
        ``domain``: actualiza en su lugar los que ya existen (misma clave:
        persona, tipo, modelo e id), crea los nuevos y quita los que ya no
        están pendientes. Los ids de lo que sigue pendiente no cambian: la
        lista abierta en otra pestaña o en el celular sigue válida. Escribe
        como sistema; devuelve los renglones vigentes en el entorno del
        usuario, en el orden de la lista."""
        Row = self.sudo()
        vals_list = self._sgi_grouped(vals_list)
        existing = Row.search(domain)
        by_key = {}
        for row in existing:
            by_key.setdefault(row._sgi_key(), []).append(row)
        keep = Row.browse()
        to_create = []
        for vals in vals_list:
            rows = by_key.pop(self._sgi_key_of_vals(vals), None)
            if rows:
                row = rows[0]
                if row._sgi_differs(vals):
                    row.write({name: vals.get(name, False) for name in _ROW_FIELDS})
                keep |= row
            else:
                to_create.append({name: vals.get(name, False) for name in _ROW_KEY + _ROW_FIELDS})
        # Lo resuelto y los repetidos (dos listas abiertas a la vez) se van.
        (existing - keep).unlink()
        created = Row.create(to_create) if to_create else Row
        rows = keep | created
        return self.search([('id', 'in', rows.ids)]) if rows else self.browse()

    @api.model
    def _sgi_store_employees(self, employees, values):
        """Guarda los renglones de las personas dadas ({empleado.id: [valores]})."""
        vals_list = []
        for emp in employees.sudo():
            for row in values.get(emp.id, []):
                vals_list.append(dict(row, employee_id=emp.id, user_id=emp.user_id.id or False))
        return self._sgi_store([('employee_id', 'in', employees.ids)], vals_list)

    @api.model
    def _sgi_build(self, employees):
        """Renglones de los empleados dados (hr.employee, sudo), al día, para
        abrirlos en la lista. Desde 56.36.0 también los de quien no tiene
        usuario (I-021). Refresca el resumen guardado que leen los filtros de
        Mi equipo (57.95.0, K-08)."""
        employees = employees.sudo()
        values = self._sgi_pending_values_employees(employees)
        self.env['hr.employee']._sgi_save_pending_summary(
            {emp.id: self._sgi_count(values.get(emp.id, [])) for emp in employees})
        return self._sgi_store_employees(employees, values)

    @api.model
    def _sgi_action(self, rows, name, group_by_person=False):
        context = {'create': False}
        if group_by_person:
            context['search_default_group_employee'] = 1
        else:
            context['search_default_group_state'] = 1
        # 57.92.0 (U-02): abre con lo atrasado y por vencer (Mis pendientes,
        # Mi procedimiento y «Pendientes — persona» de Mi equipo); solo
        # «Pendientes del equipo» (agrupado por persona) muestra todo.
        if not group_by_person:
            context['search_default_actionable'] = 1
        ref = self.env.ref
        return {
            'type': 'ir.actions.act_window', 'name': name, 'res_model': self._name,
            # 57.94.0 (I-05): kanban en el celular y la tableta. 57.145.0: la
            # ficha del pendiente al abrir el renglón.
            'view_mode': 'list,kanban,form', 'mobile_view_mode': 'kanban',
            'domain': [('id', 'in', rows.ids)],
            'views': [(ref('quimibond_sgi.sgi_my_pending_view_list').id, 'list'),
                      (ref('quimibond_sgi.sgi_my_pending_view_kanban').id, 'kanban'),
                      (ref('quimibond_sgi.sgi_my_pending_view_form').id, 'form')],
            'search_view_id': [ref('quimibond_sgi.sgi_my_pending_view_search').id, 'search'],
            'context': context,
            # I-001: sin pendientes de verdad; las actividades sin medición
            # automática no pueden salir aquí, y se dice.
            'help': "<p class='o_view_nocontent_smiling_face'>Sin pendientes</p>"
                    "<p>No tiene nada atrasado ni por vencer. Las actividades «por evento» sin "
                    "medición automática no salen aquí: revíselas en Mi procedimiento.</p>",
        }

    @api.model
    def action_open_mine(self):
        """Inicio → Mis pendientes: los del usuario actual. Sin empleado
        ligado, los renglones van solo con el usuario."""
        employee = self._sgi_user_employees(self.env.user).get(self.env.uid) \
            or self.env.user.employee_id.sudo()
        if employee:
            rows = self._sgi_build(employee)
        else:
            user = self.env.user
            rows = self._sgi_store(
                [('employee_id', '=', False), ('user_id', '=', user.id)],
                [dict(row, user_id=user.id, employee_id=False)
                 for row in self._sgi_pending_values(user).get(user.id, [])])
        return self._sgi_action(rows, "Mis pendientes")

    @api.model
    def action_open_home(self):
        """57.98.0 (U-07, Q20): acción del menú raíz «SGI». Dirección abre en
        el Tablero; los demás, en Mis pendientes (como antes). No toca la
        acción de inicio de ningún usuario."""
        if self.env.user.has_group('quimibond_sgi.group_sgi_director'):
            return self.env['sgi.direction.board'].action_open()
        return self.action_open_mine()

    # 57.98.0 (U-07): SGI → Reportar. tipo: (modelo, título, valores por
    # omisión; ``default_team_id`` es el xmlid del equipo del SGI).
    _SGI_REPORT_KINDS = {
        'nc': ('quality.alert', "Reportar una no conformidad",
               {'default_team_id': 'quimibond_sgi.sgi_quality_team_internal',
                'default_sgi_origin_type': 'proceso'}),
        'incident': ('sgi.incident', "Reportar un casi accidente o incidente",
                     {'default_incident_type': 'casi_accidente'}),
        'voice': ('helpdesk.ticket', "Reportar una queja o sugerencia",
                  {'default_team_id': 'quimibond_sgi.sgi_helpdesk_team_voice'}),
    }

    @api.model
    def action_sgi_report(self, kind):
        """57.98.0 (U-07): SGI → Reportar abre la ficha NUEVA de una NC, un
        incidente o una queja, con su equipo del SGI. Sin el equipo (base sin
        datos) avisa en lugar de abrir Calidad o Soporte completos."""
        model, title, defaults = self._SGI_REPORT_KINDS[kind]
        context = {}
        for key, value in defaults.items():
            if key == 'default_team_id':
                team = self.env.ref(value, raise_if_not_found=False)
                if not team:
                    raise UserError("Falta el equipo del SGI para reportar; avise al Jefe MAST.")
                value = team.id
            context[key] = value
        return {
            'type': 'ir.actions.act_window', 'name': title, 'res_model': model,
            'view_mode': 'form', 'views': [(False, 'form')], 'target': 'current',
            'context': context,
        }

    # ------------------------------------------------------------------
    # Ir: abrir el registro de origen o la pantalla donde se hace.
    # ------------------------------------------------------------------
    def action_open(self):
        """Abre el registro de origen (la acción, la NC, la medición…)."""
        self.ensure_one()
        if self.group_count > 1:
            return self._sgi_open_group()
        # 57.92.0 (U-03): el aviso abre el registro sobre el que está.
        if self.kind == 'aviso' and self.res_model == 'mail.activity':
            act = self.env['mail.activity'].sudo().browse(self.res_id).exists()
            # 57.94.0 (U-08): el aviso de RH abre la lista de faltantes del
            # departamento, no su ficha. Solo con «Empleados / Encargado»: la
            # lista filtra por PIN (campo de RH); sin el grupo (el Jefe MAST
            # mientras quimibond_sgi.hr_user_id esté vacío) abre la ficha.
            if act and act.sgi_cron_key == 'rh_empleados_incompletos' and act.res_model == 'hr.department' \
                    and self.env.user.has_group('hr.group_hr_user'):
                return self.env['hr.employee']._sgi_hr_gaps_action(act.res_id)
            # 57.95.0 (K-08): el aviso de acuses de un equipo abre los acuses
            # pendientes que cuenta (los de esa gente que pasaron la misma
            # fecha límite del cron). Solo con Usuario SGI, que lee acuses;
            # sin el grupo, el registro del aviso, como antes.
            if act and act.sgi_cron_kind == 'acuses_equipo' \
                    and self.env.user.has_group('quimibond_sgi.group_sgi_user'):
                try:
                    owner_id = int((act.sgi_cron_key or '').split(':', 1)[1])
                except (IndexError, ValueError):
                    owner_id = False
                if owner_id:
                    acks = self.env['sgi.cron']._sgi_ack_team_acks(owner_id)
                    return {
                        'type': 'ir.actions.act_window', 'name': "Acuses pendientes del equipo",
                        'res_model': 'sgi.document.ack', 'view_mode': 'list,form',
                        'views': [(self.env.ref('quimibond_sgi.sgi_document_ack_view_list').id, 'list'),
                                  (self.env.ref('quimibond_sgi.sgi_document_ack_view_form').id, 'form')],
                        'domain': [('id', 'in', acks.ids)], 'target': 'current'}
            if act and act.res_model and act.res_id:
                return {'type': 'ir.actions.act_window', 'res_model': act.res_model,
                        'res_id': act.res_id, 'view_mode': 'form', 'target': 'current'}
        if self.kind == 'firma' and self.res_model == 'sign.request.item':
            item = self.env['sign.request.item'].sudo().browse(self.res_id).exists()
            # La liga con token firma A NOMBRE del firmante: solo se le da a
            # él. Un jefe que abre el renglón desde Mi equipo ve la solicitud
            # con sus propios permisos (abajo), nunca el token ajeno.
            if item and item.partner_id == self.env.user.partner_id \
                    and 'access_token' in item._fields and item.access_token:
                # La firma se hace en la página de Firma electrónica.
                return {'type': 'ir.actions.act_url', 'target': 'self',
                        'url': '/sign/document/%d/%s' % (item.sign_request_id.id, item.access_token)}
        # 57.149.0: el escalamiento de un periodo abre su registro (Hecha,
        # No aplica), nunca el catálogo.
        if self.kind == 'escalamiento' and self.res_model == 'sgi.activity.execution':
            execution = self.env['sgi.activity.execution'].sudo().browse(self.res_id).exists()
            if not execution:
                raise UserError("El registro de esa actividad ya no existe.")
            return {'type': 'ir.actions.act_window', 'res_model': 'sgi.activity.execution',
                    'res_id': execution.id, 'view_mode': 'form',
                    'views': [(self.env.ref('quimibond_sgi.sgi_activity_execution_view_form').id,
                               'form')],
                    'target': 'current'}
        # 57.103.0: el periodo de una actividad. Las de registro manual abren
        # su registro (criterio de terminado, instructivo, dónde adjuntar);
        # las que se miden solas, la pantalla donde se hacen, como antes.
        if self.kind == 'actividad' and self.res_model == 'sgi.activity.execution':
            execution = self.env['sgi.activity.execution'].sudo().browse(self.res_id).exists()
            if not execution:
                raise UserError("El registro de esa actividad ya no existe.")
            activity = execution.activity_id
            if not execution.needs_evidence and (
                    activity.odoo_menu_id or activity.odoo_action_id
                    or activity.odoo_ref or activity.measure_model_id):
                try:
                    with self.env.cr.savepoint():
                        return activity.action_open_odoo()
                except SgiNoAccess:
                    raise
                except UserError:
                    pass
            return {'type': 'ir.actions.act_window', 'res_model': 'sgi.activity.execution',
                    'res_id': execution.id, 'view_mode': 'form',
                    'views': [(self.env.ref('quimibond_sgi.sgi_activity_execution_view_form').id,
                               'form')],
                    'target': 'current'}
        # 57.92.0 (U-05): «Ir» lleva a la pantalla de Odoo donde se hace la
        # actividad (su menú o acción) o a su evidencia, como «Ir a hacerlo»
        # de Mi procedimiento (``mp_can_go``); no a la ficha del catálogo.
        # 57.145.0: el escalamiento también.
        if self.kind in ('actividad', 'escalamiento', 'flujo') and self.res_model == 'sgi.process.activity':
            activity = self.env['sgi.process.activity'].sudo().browse(self.res_id).exists()
            if activity and (activity.odoo_menu_id or activity.odoo_action_id
                             or activity.odoo_ref or activity.measure_model_id):
                try:
                    # Savepoint: un dominio de evidencia malo no deja el
                    # cursor abortado al caer a la ficha.
                    with self.env.cr.savepoint():
                        return activity.action_open_odoo()
                except SgiNoAccess:
                    # 57.166.0: sin acceso se dice, no se cae a la ficha.
                    raise
                except UserError:
                    # Texto de «Dónde se ejecuta» que no resuelve a un menú y
                    # sin medición ligada: se abre la ficha, como antes.
                    pass
        # 57.92.0 (U-05): «Leer» abre el archivo del documento del acuse.
        if self.kind == 'acuse' and self.res_model == 'sgi.document.ack':
            # El acuse se lee con sudo solo para saber su documento; el
            # archivo se abre con los permisos de quien abre la lista
            # (PDF en el navegador, URL o la vista real del formulario).
            ack = self.env['sgi.document.ack'].sudo().browse(self.res_id).exists()
            if ack:
                return self.env['documents.document'].browse(
                    ack.document_id.id).action_sgi_view_file()
        return {
            'type': 'ir.actions.act_window', 'res_model': self.res_model, 'res_id': self.res_id,
            'view_mode': 'form', 'target': 'current',
        }

    def _sgi_open_group(self):
        """57.168.0: «Ir» de un renglón que junta varios: la lista de lo que
        junta, con los permisos de quien la abre. Los avisos abren sus
        registros (las salidas sin CoA, las actividades atoradas…); las firmas,
        sus solicitudes de Firma electrónica."""
        self.ensure_one()
        model, ids = self.res_model, self._sgi_res_ids()
        if model == 'mail.activity':
            acts = self.env['mail.activity'].sudo().browse(ids).exists()
            models_ = set(acts.mapped('res_model'))
            if len(models_) == 1:
                model, ids = models_.pop(), acts.mapped('res_id')
        elif model == 'sign.request.item':
            model = 'sign.request'
            ids = self.env['sign.request.item'].sudo().browse(ids).exists().sign_request_id.ids
        return {'type': 'ir.actions.act_window', 'name': self.name, 'res_model': model,
                'view_mode': 'list,form', 'views': [(False, 'list'), (False, 'form')],
                'domain': [('id', 'in', ids)], 'target': 'current', 'context': {'create': False}}

    # ------------------------------------------------------------------
    # Hecho y las demás formas de actuar (57.145.0).
    # ------------------------------------------------------------------
    @api.model
    def _sgi_feedback(self, message, warning=False):
        """Lo que devuelve cada botón: una notificación y, en ``next``, el
        cierre estándar (``act_window_close``). En la lista, el cierre dispara
        el ``onClose`` de la vista y la recarga; en un asistente cierra el
        diálogo y recarga la lista de atrás. Sin ``soft_reload``."""
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {
                'type': 'warning' if warning else 'success', 'message': message,
                'next': {'type': 'ir.actions.act_window_close'},
            },
        }

    def action_done(self):
        """«Hecho»: resuelve el pendiente según su tipo. Lo que se resuelve
        en su propio registro (NC, requisito legal, documento, aprobación,
        firma, revisión) lo dice y no hace nada. 57.149.0: el escalamiento de
        un periodo abre el asistente de «Hecha» de ese periodo; sin registro
        de cumplimiento, se sigue en la actividad."""
        self.ensure_one()
        if self.group_count > 1 and self.kind != 'validacion':
            return self.action_open()
        if self.res_model == 'sgi.activity.execution':
            return self.action_exec_done()
        if self.kind == 'actividad' and self.res_model == 'sgi.process.activity':
            self._sgi_exec_from_activity()
            return self.action_exec_done()
        if self.kind == 'aviso':
            return self.action_done_notice()
        if self.kind == 'accion':
            return self.action_done_action_line()
        if self.kind == 'acuse':
            return self.action_sign_ack()
        if self.kind == 'validacion':
            return self.action_validate_measure()
        # 57.150.0: cada tipo, su acción real.
        if self.kind == 'nc':
            return self.action_nc_step()
        if self.kind == 'legal':
            return self.action_legal_evaluate()
        if self.kind == 'documento':
            return self.action_doc_review()
        if self.kind == 'solicitud':
            return self.action_request_approve()
        if self.kind == 'aprobacion':
            return self.action_rule_approve()
        if self.kind == 'revision':
            return self.action_review_confirm()
        if self.kind in ('medicion', 'firma'):
            # Ya tienen su botón («Capturar», «Firmar»): es el mismo.
            return self.action_open()
        if self.kind == 'escalamiento':
            raise UserError("Esta actividad todavía no lleva registro de cumplimiento: el escalamiento lo "
                            "resuelve quien la ejecuta. Ábrala con «Ir a hacerlo» y dele seguimiento.")
        raise UserError("Este pendiente se resuelve en su propio registro: pulse «Ir».")

    # ------------------------------------------------------------------
    # 57.150.0: acciones reales por tipo y Mis pendientes al día.
    # ------------------------------------------------------------------
    @api.model
    def _sgi_refresh_for(self, res_model, res_ids):
        """Pone al día, en el momento, los renglones guardados de TODOS los
        usuarios que apuntan a esos registros (se rearma la lista de cada
        persona afectada, como al abrir Mis pendientes)."""
        if not res_ids:
            return
        res_ids = list(res_ids)
        # 57.168.0: también los renglones que lo juntan en un grupo.
        domain = [('res_id', 'in', res_ids)]
        for res_id in res_ids[:50]:
            domain = ['|', ('group_res_ids', 'like', ',%d,' % res_id)] + domain
        rows = self.sudo().search([('res_model', '=', res_model)] + domain)
        if not rows:
            return
        employees = rows.employee_id
        if employees:
            self._sgi_build(employees)
        for user in rows.filtered(lambda r: not r.employee_id).user_id:
            self._sgi_store(
                [('employee_id', '=', False), ('user_id', '=', user.id)],
                [dict(row, user_id=user.id, employee_id=False)
                 for row in self._sgi_pending_values(user).get(user.id, [])])

    def _sgi_record(self, model):
        self.ensure_one()
        if self.group_count > 1:
            raise UserError("Este renglón junta %d registros: pulse «Ir» y atiéndalos en la lista." % self.group_count)
        if self.res_model != model:
            raise UserError("Este renglón no es de ese tipo.")
        rec = self.env[model].browse(self.res_id).exists()
        if not rec:
            raise UserError("El registro ya no existe.")
        return rec

    def _sgi_kind_can_done(self):
        """«Hecho» de los tipos de ROW_ACTION_KINDS para quien es el renglón."""
        self.ensure_one()
        env = self.env
        if self.kind == 'nc' and self.res_model == 'quality.alert':
            alert = env['quality.alert'].sudo().browse(self.res_id).exists()
            Step = env['sgi.nc.step']
            return bool(alert) and bool(Step._sgi_next_step(alert)) and Step._sgi_can_register(alert)
        if self.kind == 'documento' and self.res_model == 'documents.document':
            return True
        if self.kind == 'legal' and self.res_model == 'sgi.legal.requirement':
            return True
        if self.kind == 'solicitud' and self.res_model == 'approval.request':
            return True
        if self.kind == 'aprobacion':
            return bool(self._sgi_studio_request())
        if self.kind == 'revision' and self.res_model == 'sgi.measure.review':
            return True
        return False

    def action_nc_step(self):
        """NC: el asistente del paso que toca (contención, causa raíz o plan)."""
        self.ensure_one()
        alert = self._sgi_record('quality.alert')
        Step = self.env['sgi.nc.step']
        step = Step._sgi_next_step(alert)
        if not step:
            return self.action_open()
        if not Step._sgi_can_register(alert):
            raise AccessError("Solo los responsables de la NC, el dueño de su proceso, el Jefe MAST o Dirección "
                              "registran sus pasos.")
        steps = dict(Step._fields['step'].selection)
        return {'type': 'ir.actions.act_window',
                'name': "NC %s: %s" % (alert.sudo().display_name or '', steps[step]),
                'res_model': 'sgi.nc.step', 'view_mode': 'form', 'target': 'new',
                'views': [(self.env.ref('quimibond_sgi.sgi_nc_step_view_form').id, 'form')],
                'context': {'default_alert_id': alert.id, 'default_step': step, 'sgi_pending_row_id': self.id}}

    def action_legal_evaluate(self):
        """Requisito legal: el asistente de evaluación de cumplimiento."""
        self.ensure_one()
        action = self._sgi_record('sgi.legal.requirement').action_evaluate()
        action['context'] = dict(action.get('context') or {}, sgi_pending_row_id=self.id)
        return action

    def action_doc_review(self):
        """Documento por revisar: sigue vigente sin cambios o necesita cambio."""
        self.ensure_one()
        doc = self._sgi_record('documents.document')
        if not self.env['sgi.doc.review']._sgi_can_review(doc):
            raise AccessError("Solo el responsable del documento o el Jefe MAST registran su revisión.")
        return {'type': 'ir.actions.act_window', 'name': "Revisar %s" % (doc.sudo().display_name or ''),
                'res_model': 'sgi.doc.review', 'view_mode': 'form', 'target': 'new',
                'views': [(self.env.ref('quimibond_sgi.sgi_doc_review_view_form').id, 'form')],
                'context': {'default_document_id': doc.id, 'sgi_pending_row_id': self.id}}

    def _sgi_approval_request(self):
        """La solicitud de Aprobaciones del renglón, si es de quien la abre."""
        self.ensure_one()
        request = self._sgi_record('approval.request')
        if self.user_id != self.env.user:
            raise UserError("Solo la persona que aprueba la solicitud la aprueba o la rechaza.")
        return request

    def action_request_approve(self):
        request = self._sgi_approval_request()
        request.action_approve()
        self._sgi_refresh_for('approval.request', request.ids)
        return self._sgi_feedback("Aprobada: %s" % (request.sudo().display_name or ''))

    def action_request_refuse(self):
        request = self._sgi_approval_request()
        request.action_refuse()
        self._sgi_refresh_for('approval.request', request.ids)
        return self._sgi_feedback("Rechazada: %s" % (request.sudo().display_name or ''), warning=True)

    def _sgi_studio_request(self):
        """La aprobación de Studio (studio.approval.request) del renglón: el
        renglón guarda el documento que la espera; se busca la de quien abre
        la lista sobre ese documento. Vacía si Studio no está o si la regla no
        se puede aprobar desde aquí (sin ``set_approval``)."""
        self.ensure_one()
        env = self.env
        if 'studio.approval.request' not in env or not self.res_model or not self.res_id:
            return False
        Rule = env['studio.approval.rule']
        if not hasattr(Rule, 'set_approval'):
            return False
        return env['studio.approval.request'].sudo().search([
            ('mail_activity_id.user_id', '=', self.user_id.id), ('rule_id.model_name', '=', self.res_model),
            ('res_id', '=', self.res_id)], limit=1)

    def _sgi_rule_decide(self, approved):
        self.ensure_one()
        if self.kind != 'aprobacion' or self.user_id != self.env.user:
            raise UserError("Solo la persona que aprueba lo aprueba o lo rechaza.")
        request = self._sgi_studio_request()
        if not request:
            raise UserError("Esta aprobación se da en el documento que la espera: pulse «Ir».")
        name, res_model, res_id = self.name or '', self.res_model, self.res_id
        # Con los permisos de quien aprueba: Studio revisa que pueda.
        self.env['studio.approval.rule'].browse(request.rule_id.id).set_approval(request.res_id, approved)
        self._sgi_refresh_for(res_model, [res_id])
        return self._sgi_feedback("%s: %s" % ("Aprobado" if approved else "Rechazado", name),
                                  warning=not approved)

    def action_rule_approve(self):
        return self._sgi_rule_decide(True)

    def action_rule_refuse(self):
        return self._sgi_rule_decide(False)

    def action_review_confirm(self):
        """Revisión mensual de la medición: «Sí, esto es lo que hago»."""
        self.ensure_one()
        review = self._sgi_record('sgi.measure.review')
        name = self.name or ''
        review._sgi_answer('confirmada')
        return self._sgi_feedback("Revisión contestada: %s" % name)

    def action_review_not_matching(self):
        """Revisión mensual: «No corresponde» (pide la nota)."""
        self.ensure_one()
        return self._sgi_record('sgi.measure.review').action_not_matching()

    def action_sign_ack_selected(self):
        """«Leído y entendido» a todos los acuses seleccionados de quien abre la
        lista (el candado de identidad de sgi.document.ack decide)."""
        rows = self.filtered(lambda r: r.kind == 'acuse' and r.res_model == 'sgi.document.ack')
        if not rows:
            raise UserError("Seleccione al menos un acuse de lectura.")
        acks = self.env['sgi.document.ack'].browse(rows.mapped('res_id')).exists()
        mine = acks.filtered(lambda a: a.state == 'pendiente' and a.sudo().employee_id.user_id == self.env.user)
        if not mine:
            raise UserError("Ninguno de los acuses seleccionados es suyo y está pendiente.")
        mine.action_mark_read()
        rows.filtered(lambda r: r.res_id in mine.ids).sudo().unlink()
        skipped = len(acks) - len(mine)
        return self._sgi_feedback("Acuses registrados: %d.%s" % (
            len(mine), (" %d no son suyos y se dejaron." % skipped) if skipped else ''), warning=bool(skipped))

    def _sgi_exec_from_activity(self):
        """Actividad atrasada por semáforo que lleva registro de cumplimiento
        y todavía no tiene renglón del periodo (el cron nocturno lo crea; aquí
        se adelanta): crea o encuentra el registro del periodo en curso de la
        persona y convierte este renglón en el suyo, sin perder el id."""
        self.ensure_one()
        Exec = self.env['sgi.activity.execution'].sudo()
        activity = self.env['sgi.process.activity'].sudo().browse(self.res_id).exists()
        if not activity:
            raise UserError("La actividad ya no existe.")
        if not activity._sgi_exec_applies():
            raise UserError("«%s» se cumple haciendo su registro en Odoo o por evento; aquí no se "
                            "marca. Pulse «Ir» para hacerla." % (activity.name or ''))
        employee = self.env['hr.employee'].sudo().browse(self.employee_id.id).exists() \
            if self.employee_id else False
        if not employee:
            raise UserError("Este renglón no tiene persona: no se puede abrir su registro.")
        if not Exec._sgi_user_can_mark(employee, activity):
            raise AccessError(Exec._SGI_MARK_DENIED % (activity.name or ''))
        today = fields.Date.context_today(self)
        period = activity._sgi_exec_period(today)
        if not period:
            raise UserError("Hoy no hay periodo abierto para «%s» (día inhábil o actividad sin "
                            "cadencia)." % (activity.name or ''))
        start, end, due = period
        from .sgi_activity_execution import sgi_exec_start_date
        exec_start = sgi_exec_start_date(self.env)
        if due < exec_start:
            # 57.149.0: el periodo venció antes del arranque; no se registra.
            raise UserError("El periodo de «%s» venció el %s, antes del arranque del registro de "
                            "cumplimiento (%s): no se registra." % (
                                activity.name or '', due.strftime('%d/%m/%Y'),
                                exec_start.strftime('%d/%m/%Y')))
        execution = Exec.search([('activity_id', '=', activity.id), ('employee_id', '=', employee.id),
                                 ('period_start', '=', start)], limit=1)
        if not execution:
            execution = Exec.with_context(mail_create_nolog=True, tracking_disable=True).create({
                'activity_id': activity.id, 'employee_id': employee.id,
                'period_start': start, 'period_end': end, 'date_due': due})
        self._sgi_sync_execution(execution)
        return execution

    def _sgi_sync_execution(self, execution):
        """Pone al día este renglón con su registro de cumplimiento: cerrado
        (hecha o no aplica) se quita; abierto, se reescribe (avance, nota,
        vencimiento, estado). Escribe como sistema; lo llaman los botones y
        el asistente de avance."""
        from .sgi_activity_execution import EXEC_OPEN
        execution = execution.sudo()
        rows = self.sudo()
        if not execution.exists() or execution.state not in EXEC_OPEN:
            rows.unlink()
            return False
        today = fields.Date.context_today(self)
        plain = rows.filtered(lambda r: r.kind != 'escalamiento')
        if plain:
            vals = self._sgi_finish(self._sgi_execution_row(execution), today)
            plain.write({name: vals.get(name, False) for name in ('res_model', 'res_id') + _ROW_FIELDS})
        # 57.149.0: el escalamiento abierto sigue (el avance no quita el
        # atraso); se pone al día lo que muestra del avance.
        escalations = rows - plain
        if escalations:
            note = (execution.progress_note or '').strip() if execution.state == 'en_proceso' else ''
            escalations.write({
                'exec_state': execution.state, 'exec_note': note or False,
                'exec_date_estimated': execution.date_estimated if execution.state == 'en_proceso' else False})
        return True

    @api.model
    def _sgi_sync_executions(self, executions):
        """57.149.0 (cierre inmediato): después de marcar periodos, pone al
        día los renglones guardados de TODOS los usuarios (la persona, sus
        jefes, quien recibe el escalamiento), sin esperar al respaldo
        nocturno: los del periodo y sus escalamientos se quitan si quedó
        cerrado y se reescriben si sigue abierto; los que todavía apuntan a la
        actividad (semáforo o escalamiento de antes del registro) se quitan,
        porque la actividad ya lleva registro de cumplimiento."""
        executions = executions.sudo().exists()
        if not executions:
            return
        Row = self.sudo()
        rows = Row.search([('res_model', '=', 'sgi.activity.execution'), ('res_id', 'in', executions.ids)])
        # 57.157.0: se agrupa antes de tocar nada. Al cerrar un periodo se
        # borran sus renglones, y volver a leer ``rows`` para el siguiente
        # periodo leía renglones ya borrados (MissingError: tumbó la
        # migración 57.149.0 en producción con 12 periodos a cerrar).
        by_exec = {}
        for row in rows:
            by_exec.setdefault(row.res_id, []).append(row.id)
        for execution in executions:
            mine = Row.browse(by_exec.get(execution.id, [])).exists()
            if mine:
                mine._sgi_sync_execution(execution)
        pairs = {(e.employee_id.id, e.activity_id.id) for e in executions}
        stale = Row.search([('res_model', '=', 'sgi.process.activity'),
                            ('res_id', 'in', executions.activity_id.ids),
                            ('kind', 'in', ('actividad', 'escalamiento'))])
        stale.filtered(lambda r: r.kind == 'escalamiento' or (r.employee_id.id, r.res_id) in pairs).unlink()

    def _sgi_execution(self):
        self.ensure_one()
        if self.res_model != 'sgi.activity.execution':
            raise UserError("Este renglón no es una actividad con registro de cumplimiento.")
        execution = self.env['sgi.activity.execution'].browse(self.res_id).exists()
        if not execution:
            raise UserError("El registro de esa actividad ya no existe.")
        return execution

    def _sgi_exec_wizard(self, mode):
        action = self._sgi_execution()._sgi_mark_wizard(mode)
        action['context'] = dict(action['context'], sgi_pending_row_id=self.id)
        return action

    def action_exec_progress(self):
        """57.103.0: «En proceso» desde el renglón (nota de avance y fecha
        estimada)."""
        return self._sgi_exec_wizard('progress')

    def action_exec_done(self):
        """57.103.0: «Hecho» desde el renglón (evidencia si es de registro
        manual o si el sistema no vio el registro)."""
        return self._sgi_exec_wizard('done')

    def action_exec_not_applicable(self):
        """57.145.0: «No aplica este periodo» desde el renglón (con motivo)."""
        return self._sgi_exec_wizard('na')

    def action_sign_ack(self):
        """57.92.0 (U-05): «Leído y entendido» desde el renglón. El candado de
        identidad de ``sgi.document.ack`` decide si quien abre la lista puede
        firmar: el propio empleado o el Jefe MAST."""
        self.ensure_one()
        if self.kind != 'acuse' or self.res_model != 'sgi.document.ack':
            raise UserError("Este renglón no es un acuse de lectura.")
        ack = self.env['sgi.document.ack'].browse(self.res_id).exists()
        if not ack:
            raise UserError("El acuse ya no existe.")
        ack.action_mark_read()
        name = self.name
        self.sudo().unlink()
        return self._sgi_feedback("Acuse registrado: %s" % name)

    def action_done_notice(self):
        """57.92.0 (U-03): «Hecho» marca hecho el aviso (la actividad nativa),
        solo si es de quien abre la lista."""
        self.ensure_one()
        act = self.env['mail.activity'].browse(self.res_id).exists() \
            if self.kind == 'aviso' and self.res_model == 'mail.activity' else False
        if not act:
            raise UserError("El aviso ya no existe.")
        # La dueña se lee con sudo: solo la persona del aviso lo cierra.
        if act.sudo().user_id != self.env.user:
            raise UserError("Solo la persona a quien está asignado el aviso puede marcarlo hecho.")
        # Sudo con el mismo uid: la nota queda a nombre de quien lo marcó.
        act.sudo().action_feedback(feedback="Hecho desde Mis pendientes.")
        name = self.name
        self.sudo().unlink()
        return self._sgi_feedback("Aviso hecho: %s" % name)

    def action_snooze_notice(self):
        """57.145.0: «Posponer una semana» un aviso propio: su fecha límite
        pasa a 5 días hábiles desde hoy y el renglón sale de lo urgente."""
        self.ensure_one()
        act = self.env['mail.activity'].browse(self.res_id).exists() \
            if self.kind == 'aviso' and self.res_model == 'mail.activity' else False
        if not act:
            raise UserError("El aviso ya no existe.")
        if act.sudo().user_id != self.env.user:
            raise UserError("Solo la persona a quien está asignado el aviso puede posponerlo.")
        today = fields.Date.context_today(self)
        new_due = sgi_add_business_days(self.env, today, SNOOZE_BUSINESS_DAYS)
        act.sudo().with_context(mail_activity_quick_update=True).write({'date_deadline': new_due})
        self.sudo().write({'date_due': new_due, 'state': pending_state(new_due, today),
                           'state_rank': STATE_RANK[pending_state(new_due, today)]})
        return self._sgi_feedback("Aviso pospuesto al %s: %s" % (new_due.strftime('%d/%m/%Y'), self.name))

    def action_done_action_line(self):
        """57.145.0: «Hecho» termina la acción (de NC, riesgo, incidente…)
        del responsable: fecha de hoy y avance al 100 %, como el botón de la
        acción. Solo el responsable o el Jefe MAST."""
        self.ensure_one()
        line = self.env['sgi.action.line'].browse(self.res_id).exists() \
            if self.kind == 'accion' and self.res_model == 'sgi.action.line' else False
        if not line:
            raise UserError("La acción ya no existe.")
        if line.sudo().responsible_id != self.env.user \
                and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el responsable de la acción o el Jefe MAST pueden terminarla.")
        line.action_mark_done()
        name = self.name
        self.sudo().unlink()
        return self._sgi_feedback("Acción terminada: %s" % name)

    def action_validate_measure(self):
        """«Validar» desde el renglón (I-006). Valida quien abre la lista, con
        sus permisos: solo el dueño del indicador o el Jefe MAST pueden
        (``_sgi_check_validate_access``)."""
        self.ensure_one()
        if self.kind != 'validacion' or self.res_model != 'sgi.indicator.measure':
            raise UserError("Este renglón no es una medición por validar.")
        if self.group_count > 1:
            # 57.168.0: el grupo valida todas las del indicador.
            return self.action_validate_selected()
        measure = self.env['sgi.indicator.measure'].browse(self.res_id).exists()
        if not measure:
            raise UserError("La medición ya no existe.")
        measure.action_validate()
        name = self.name
        self.sudo().unlink()
        return self._sgi_feedback("Medición validada: %s" % name)

    def action_validate_selected(self):
        """57.92.0 (U-02): «Validar seleccionadas». Ignora los renglones que no
        son mediciones. Valida, con los permisos de quien abre la lista, solo
        las mediciones que puede validar (``sgi_can_validate``: dueño del
        indicador o Jefe MAST); las demás se dejan y se avisa cuántas. Quita
        los renglones de lo validado y de las mediciones que ya no existen."""
        rows = self.filtered(lambda r: r.kind == 'validacion'
                             and r.res_model == 'sgi.indicator.measure')
        if not rows:
            raise UserError("Seleccione al menos una medición por validar.")
        Measure = self.env['sgi.indicator.measure']
        existing = Measure.browse([i for row in rows for i in row._sgi_res_ids()]).exists()
        allowed = existing.filtered('sgi_can_validate')
        skipped = existing - allowed
        if existing and not allowed:
            raise UserError("Solo el responsable del indicador o el Jefe MAST puede validar "
                            "estas mediciones.")
        # 57.104.0 (B5): como P-40, las manuales en 0 sin nota no se validan
        # (el write las rechazaría y todo el lote fallaba); se dejan y se avisan.
        empty = allowed._sgi_without_value()
        allowed -= empty
        allowed.action_validate()
        left = skipped | empty
        rows.filtered(lambda r: not set(r._sgi_res_ids()) & set(left.ids)).sudo().unlink()
        if left:
            parts = []
            if skipped:
                parts.append("%d no son de sus indicadores y se dejaron" % len(skipped))
            if empty:
                parts.append("%d manuales no tienen valor capturado y se dejaron (%s): "
                             "capture el valor o, si es 0, escriba en la nota por qué"
                             % (len(empty), ", ".join(empty.mapped('display_name'))))
            return self._sgi_feedback(
                "Se validaron %d mediciones; %s." % (len(allowed), "; ".join(parts)), warning=True)
        return self._sgi_feedback("Se validaron %d mediciones." % len(allowed))


class HrEmployeePendingSaved(models.Model):
    """57.95.0 (K-08): resumen de Mis pendientes guardado por persona. Lo
    leen los filtros de Mi equipo («Con pendientes atrasados», «por
    vencer», «Al día»), que antes armaban Mis pendientes de toda la empresa
    en cada búsqueda. Lo refrescan el respaldo nocturno (sgi.cron,
    cron_nightly_backup) y cada lista de pendientes que se abre
    (sgi.my.pending._sgi_build). Las columnas de Mi equipo siguen en vivo.
    Técnicos: no salen en ninguna vista. 57.145.0: el respaldo nocturno
    también deja guardados los renglones de todos."""
    _inherit = 'hr.employee'

    sgi_pending_saved_total = fields.Integer(
        string="Pendientes (resumen guardado)", readonly=True, copy=False, prefetch=False,
        groups='base.group_system',
        help="Total de Mis pendientes de la persona la última vez que se calculó.")
    sgi_pending_saved_late = fields.Integer(
        string="Pendientes atrasados (resumen guardado)", readonly=True, copy=False,
        prefetch=False, groups='base.group_system')
    sgi_pending_saved_state = fields.Selection(
        PENDING_STATES, string="Semáforo (resumen guardado)", readonly=True, copy=False,
        prefetch=False, groups='base.group_system')

    _SGI_SAVED_FIELDS = ('sgi_pending_saved_total', 'sgi_pending_saved_late',
                         'sgi_pending_saved_state')

    @api.model
    def _sgi_pending_scope_domain(self):
        """Quienes salen en Mi equipo: la empresa del SGI, con usuario o
        puesto (el alcance que usaban los filtros antes de 57.95.0)."""
        # sgi_mp_job_id (guardado) y no job_id: en Odoo 19 job_id pasa por
        # la versión y la búsqueda no encuentra a los empleados
        # (_sgi_mp_ids_where).
        company = self.env['sgi.config']._sgi_company()
        return [('company_id', 'in', (company.id, False)),
                '|', ('user_id', '!=', False), ('sgi_mp_job_id', '!=', False)]

    @api.model
    def _sgi_save_pending_summary(self, summary):
        """{empleado.id: (total, atrasadas, peor estado)} → escribe solo los
        campos que cambiaron, sin seguimiento ni correo. Devuelve los
        empleados escritos (vacío si nada cambió: abrir una lista no escribe
        en hr.employee cuando el resumen ya está al día)."""
        if not summary:
            return self.browse()
        Employee = self.sudo().with_context(tracking_disable=True, mail_notrack=True)
        changed = Employee.browse()
        current = {row['id']: row for row in Employee.browse(list(summary)).exists().read(
            list(self._SGI_SAVED_FIELDS))}
        for emp_id, row in current.items():
            total, late, worst = summary[emp_id]
            new = {'sgi_pending_saved_total': total, 'sgi_pending_saved_late': late,
                   'sgi_pending_saved_state': worst or ('al_dia' if total else False)}
            vals = {k: v for k, v in new.items() if (row[k] or False) != (v or False)}
            if vals:
                emp = Employee.browse(emp_id)
                emp.write(vals)
                changed |= emp
        return changed

    @api.model
    def _sgi_refresh_pending_summary(self, employees=None):
        """Recalcula y guarda el resumen y los renglones. Sin empleados: todo
        el alcance de Mi equipo, y quien salió de él con un resumen viejo
        queda en cero y sin renglones."""
        Pending = self.env['sgi.my.pending']
        if employees:
            scope = employees.sudo()
        else:
            scope = self.sudo().search(self._sgi_pending_scope_domain())
        values = Pending._sgi_pending_values_employees(scope)
        Pending._sgi_store_employees(scope, values)
        summary = {emp.id: Pending._sgi_count(values.get(emp.id, [])) for emp in scope}
        if not employees:
            gone = self.sudo().search([('id', 'not in', scope.ids), '|',
                                       ('sgi_pending_saved_total', '>', 0),
                                       ('sgi_pending_saved_state', '!=', False)])
            summary.update({emp.id: (0, 0, False) for emp in gone})
            stale = Pending.sudo().search([('employee_id', '!=', False),
                                           ('employee_id', 'not in', scope.ids)])
            if stale:
                stale.unlink()
        return self._sgi_save_pending_summary(summary)


class SgiMyProcedurePending(models.TransientModel):
    _inherit = 'sgi.my.procedure'

    pending_total = fields.Integer(string="Mis pendientes", compute='_compute_pending_summary',
                                   help="Total de pendientes de la persona.")
    pending_late = fields.Integer(string="Pendientes atrasados", compute='_compute_pending_summary',
                                  help="Pendientes atrasados de la persona.")

    @api.depends('employee_id')
    @api.depends_context('uid')
    def _compute_pending_summary(self):
        Pending = self.env['sgi.my.pending']
        for wiz in self:
            # 56.36.0 (I-001, I-021): por persona, con o sin usuario.
            emp = wiz._sgi_mp_employee()
            total, late, _worst = Pending._sgi_summary_employees(emp).get(emp.id, (0, 0, False)) \
                if emp else (0, 0, False)
            wiz.pending_total = total
            wiz.pending_late = late

    @api.model
    def _sgi_check_in_scope(self, employees):
        """F-007 (auditoría 2026-09): los pendientes y el procedimiento de otra
        persona solo se ven si está en el alcance de Mi equipo (su gente, sus
        departamentos y los puestos de sus procesos; MAST, administrador y
        Dirección ven a todos). Antes se armaban con sudo() para cualquier id
        mandado por RPC."""
        if self.env.su or self._sgi_mp_is_admin():
            return True
        me = self._sgi_mp_my_employee()
        allowed = (me | me._sgi_mp_team_employees()) if me else me
        if set(employees.ids) - set(allowed.ids):
            raise AccessError("Esa persona no está en su equipo; solo ve a su gente, sus "
                              "departamentos y los puestos de sus procesos.")
        return True

    def action_show_pending(self):
        self.ensure_one()
        self._sgi_check_in_scope(self._sgi_mp_employee())
        rows = self.env['sgi.my.pending']._sgi_build(self._sgi_mp_employee())
        name = "Mis pendientes" if self.is_me else "Pendientes — %s" % (self.employee_id.name or '')
        return self.env['sgi.my.pending']._sgi_action(rows, name)


class HrEmployeePublicPending(models.Model):
    """Mi equipo: el mismo semáforo por persona."""
    _inherit = 'hr.employee.public'

    sgi_mp_pending_total = fields.Integer(
        string="Pendientes", compute='_compute_sgi_mp_pending',
        search='_search_sgi_mp_pending_total',
        help="Total de pendientes de la persona en Mis pendientes. En los filtros se usa el "
             "resumen guardado (de la noche o de la última vez que se abrió su lista).")
    sgi_mp_pending_late = fields.Integer(
        string="Pendientes atrasados", compute='_compute_sgi_mp_pending',
        search='_search_sgi_mp_pending_late',
        help="Pendientes atrasados de la persona en Mis pendientes. En los filtros se usa el "
             "resumen guardado (de la noche o de la última vez que se abrió su lista).")
    sgi_mp_pending_state = fields.Selection(
        PENDING_STATES, string="Semáforo", compute='_compute_sgi_mp_pending',
        search='_search_sgi_mp_pending_state',
        help="El peor estado de sus pendientes: atrasada, por vencer o al día. En los filtros "
             "se usa el resumen guardado (de la noche o de la última vez que se abrió su lista).")

    @api.model
    def _sgi_pending_by_employee(self, employees):
        """56.36.0 (I-021): el semáforo sale de la persona; sin usuario, de
        sus actividades atrasadas y acuses pendientes."""
        summary = self.env['sgi.my.pending']._sgi_summary_employees(employees.sudo())
        return {emp.id: summary.get(emp.id, (0, 0, False)) for emp in employees}

    def _compute_sgi_mp_pending(self):
        values = self._sgi_pending_by_employee(self.env['hr.employee'].sudo().browse(self.ids))
        for rec in self:
            total, late, worst = values.get(rec.id, (0, 0, False))
            rec.sgi_mp_pending_total = total
            rec.sgi_mp_pending_late = late
            rec.sgi_mp_pending_state = worst or ('al_dia' if total else False)

    def _sgi_saved_ids_where(self, predicate):
        """57.95.0 (K-08): los filtros leen el resumen guardado en hr.employee
        (sudo; solo esos tres campos del alcance de Mi equipo, sin calcular
        nada). Ya no arman Mis pendientes de toda la empresa en cada
        búsqueda. Lo que nunca se calculó cuenta como cero."""
        Employee = self.env['hr.employee']
        rows = Employee.sudo().search_read(Employee._sgi_pending_scope_domain(),
                                           list(Employee._SGI_SAVED_FIELDS))
        return [row['id'] for row in rows
                if predicate((row['sgi_pending_saved_total'] or 0, row['sgi_pending_saved_late'] or 0,
                              row['sgi_pending_saved_state'] or False))]

    @api.model
    def _search_sgi_mp_pending_total(self, operator, value):
        if operator not in self._NUMERIC_OPS:
            raise UserError("Filtro no soportado sobre los pendientes.")
        op = self._NUMERIC_OPS[operator]
        return [('id', 'in', self._sgi_saved_ids_where(lambda v: op(v[0], value)))]

    @api.model
    def _search_sgi_mp_pending_late(self, operator, value):
        if operator not in self._NUMERIC_OPS:
            raise UserError("Filtro no soportado sobre los pendientes.")
        op = self._NUMERIC_OPS[operator]
        return [('id', 'in', self._sgi_saved_ids_where(lambda v: op(v[1], value)))]

    @api.model
    def _search_sgi_mp_pending_state(self, operator, value):
        # Import local: cargar el módulo de la pantalla desde aquí a nivel de
        # módulo cambiaría el orden del registro (regla en CLAUDE.md).
        from .sgi_my_procedure_screen import _sgi_as_list
        values = _sgi_as_list(value)
        if operator in ('=', 'in'):
            return [('id', 'in', self._sgi_saved_ids_where(lambda v: v[2] in values))]
        return [('id', 'in', self._sgi_saved_ids_where(lambda v: v[2] not in values))]

    def action_sgi_open_pending(self):
        self.ensure_one()
        employee = self.env['hr.employee'].sudo().browse(self.id)
        self.env['sgi.my.procedure']._sgi_check_in_scope(employee)
        rows = self.env['sgi.my.pending']._sgi_build(employee)
        return self.env['sgi.my.pending']._sgi_action(rows, "Pendientes — %s" % self.name)

    def action_sgi_team_pending(self):
        """«Pendientes del equipo»: una lista, agrupada por persona."""
        team = self.env['hr.employee'].sudo().browse(self.ids) if self else self._sgi_team()
        self.env['sgi.my.procedure']._sgi_check_in_scope(team)
        rows = self.env['sgi.my.pending']._sgi_build(team)
        return self.env['sgi.my.pending']._sgi_action(rows, "Pendientes del equipo", group_by_person=True)
