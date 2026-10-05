# -*- coding: utf-8 -*-
"""Actividades específicas: dónde, cómo, criterio de terminado, plazo, qué
hacer si falla y a quién escalar.

Cada actividad contesta siete preguntas (qué, quién, dónde, cómo, cuándo,
terminado, si falla). Lo que falte queda como renglón en
``sgi.activity.spec.gap`` (se agrupa y pivotea), y publicar el proceso
(``state = vigente``) exige que no haya faltantes de tipo error, ni en sus
actividades ni en sus indicadores. En borrador o piloto todo se puede cargar
incompleto: los faltantes quedan a la vista.

La medición gana lo que antes no se podía ver: cuántas entradas aplicaban,
cuántas salidas llegaron completas y a tiempo, y cuántas siguen abiertas con
el plazo vencido (``sgi.activity.week.stat``).
"""
import calendar
import logging
import unicodedata
from datetime import date, datetime, time, timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError
from odoo.tools.safe_eval import safe_eval

from .sgi_calendar import (
    SgiWorkdays, sgi_add_business_days, sgi_local_date, sgi_local_datetime_utc, sgi_nth_business_day,
    sgi_previous_business_day, sgi_today)
from .sgi_process_procedure import SgiProcessActivity as _BaseActivity

_logger = logging.getLogger(__name__)

SGI_EXEC_CHANNELS = [
    ('odoo', "Odoo"),
    ('correo', "Correo"),
    ('portal_cliente', "Portal del cliente"),
    ('edi', "EDI"),
    ('sistema_externo', "Sistema externo"),
    ('telefono', "Teléfono"),
    ('papel', "Papel"),
    ('fisico', "Trabajo físico (sin registro)"),
]
SGI_EXTERNAL_CHANNELS = ('sistema_externo', 'portal_cliente', 'edi')
SGI_MANUAL_CHANNELS = ('papel', 'correo', 'telefono')

SGI_WEEKDAYS = [
    ('0', "Lunes"), ('1', "Martes"), ('2', "Miércoles"), ('3', "Jueves"),
    ('4', "Viernes"), ('5', "Sábado"), ('6', "Domingo"),
]

SGI_MONTHS = [
    ('1', "Enero"), ('2', "Febrero"), ('3', "Marzo"), ('4', "Abril"), ('5', "Mayo"),
    ('6', "Junio"), ('7', "Julio"), ('8', "Agosto"), ('9', "Septiembre"),
    ('10', "Octubre"), ('11', "Noviembre"), ('12', "Diciembre"),
]
# Meses por periodo de las cadencias largas (56.20.0). Los periodos son
# bloques del año calendario: trimestres, semestres, el año.
SGI_CADENCE_MONTHS = {'trimestral': 3, 'semestral': 6, 'anual': 12}

SGI_SPEC_GAPS = [
    ('no_done', "Sin criterio de terminado"),
    ('no_on_fail', "Sin qué hacer si falla"),
    ('no_timing', "Sin plazo"),
    ('due_mismatch', "Vencimiento que no va con la cadencia"),
    ('multi_exec', "Más de un ejecutor"),
    ('vague_verb', "Verbo vago"),
    ('trigger_as_activity', "«Recibir» como actividad"),
    ('no_check_against', "Sin contra qué comparar"),
    ('no_output', "Sin salida"),
    ('no_escalation', "Sin escalamiento"),
    ('measure_no_complete', "Entregable sin criterio de completo"),
    ('no_channel', "Sin canal"),
    ('odoo_no_menu', "Canal Odoo sin pantalla"),
    ('menu_no_visible', "Pantalla que quien la ejecuta no ve"),
    ('external_no_name', "Sistema externo sin nombre"),
    ('no_how', "Sin cómo"),
    ('odoo_measured_manual', "Se hace en Odoo, se mide a mano"),
    ('paper_channel', "En papel"),
    ('mixed_channel', "Junta trabajo físico y captura"),
    ('no_match', "Entrada que no se liga con la salida"),
    ('menu_model_mismatch', "Pantalla que no va con su medición"),
    ('measure_never', "Evidencia que no aparece"),
    ('weak_attribution', "Atribución débil"),
]
# Severidad por código (error bloquea publicar; warning solo avisa).
SGI_GAP_SEVERITY = {
    'no_done': 'error', 'no_on_fail': 'error', 'no_timing': 'error',
    'due_mismatch': 'error', 'multi_exec': 'error', 'vague_verb': 'error',
    'no_channel': 'error', 'odoo_no_menu': 'error', 'external_no_name': 'error',
    'no_how': 'error',
    'trigger_as_activity': 'warning', 'no_check_against': 'warning',
    'no_output': 'warning', 'no_escalation': 'warning',
    'measure_no_complete': 'warning', 'odoo_measured_manual': 'warning',
    'paper_channel': 'warning', 'mixed_channel': 'warning', 'no_match': 'warning',
    'menu_no_visible': 'warning', 'menu_model_mismatch': 'warning',
    'measure_never': 'warning', 'weak_attribution': 'warning',
}
# 57.104.0: días mínimos sin evidencia para «Evidencia que no aparece» (la
# cadencia larga manda: una anual espera su ventana de 380 días).
MEASURE_NEVER_DAYS = 60
# 57.104.0: adherencia (%) por debajo de la cual la atribución es débil
# cuando hay ejecuciones de otro puesto o de cuentas compartidas.
WEAK_ATTRIBUTION_PCT = 50

VAGUE_VERBS_PARAM = 'quimibond_sgi.vague_verbs'
COMPARE_VERBS_PARAM = 'quimibond_sgi.compare_verbs'
DEFAULT_VAGUE_VERBS = ('dar seguimiento,gestionar,coordinar,apoyar,asegurar,atender,'
                       'ver,checar,manejar,controlar')
DEFAULT_COMPARE_VERBS = 'verificar,comparar,revisar,validar,conciliar,inspeccionar'

# Lo que escribe el cron de medición: no cambia la especificación.
_CRON_FIELDS = set(_BaseActivity._SGI_MEASURE_FIELDS) - {
    'measure_model_id', 'measure_model_name', 'measure_domain', 'measure_date_field',
    'measure_cadence', 'measure_method', 'measure_proxy_activity_id',
    'measure_justification', 'measure_deliverable_id', 'measure_user_field'}


def sgi_plain(text):
    """Minúsculas y sin acentos, para comparar verbos."""
    text = unicodedata.normalize('NFKD', text or '')
    return ''.join(c for c in text if not unicodedata.combining(c)).lower().strip()


def sgi_menu_visible_for(menu, user):
    """True si el usuario ve el menú: cada menú de la ruta sin grupos o con
    alguno de los grupos del usuario (los implicados cuentan). E-010."""
    groups = user.sudo().all_group_ids
    node = menu.sudo()
    while node:
        if node.group_ids and not (node.group_ids & groups):
            return False
        node = node.parent_id
    return True


def sgi_safe_domain(text):
    """Dominio capturado por una persona: safe_eval, nunca eval."""
    domain = safe_eval(text or '[]')
    if not isinstance(domain, (list, tuple)):
        raise ValueError("no es una lista")
    return list(domain)


class SgiActivitySpecGap(models.Model):
    """Faltante de especificación de una actividad (sin ejecutor, sin entregable, verbo vago…). Se
    recalcula; alimenta Diagnóstico → Faltantes de especificación."""
    _name = 'sgi.activity.spec.gap'
    _description = "Faltante de especificación de una actividad SGI"
    _order = 'severity, code, activity_id'

    activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad", required=True,
        ondelete='cascade', index=True,
        help="Actividad a la que le falta especificación.")
    process_id = fields.Many2one(
        related='activity_id.process_id', string="Proceso", store=True, index=True,
        help="Proceso de la actividad.")
    code = fields.Selection(SGI_SPEC_GAPS, string="Faltante", required=True, index=True,
                            help="Qué le falta a la actividad.")
    severity = fields.Selection([
        ('error', "Error (bloquea publicar)"),
        ('warning', "Advertencia"),
    ], string="Severidad", required=True, index=True,
        help="Un error impide publicar el procedimiento; una advertencia no.")
    message = fields.Char(string="Detalle")
    company_id = fields.Many2one(
        related='activity_id.company_id', string="Empresa", store=True, index=True)


class SgiActivitySpec(models.Model):
    _inherit = 'sgi.process.activity'

    # --- Qué ---
    check_against = fields.Char(
        string="Contra qué se compara",
        help="Lista de precios vigente, release anterior, IT-C2-06… Obligatorio "
             "cuando el verbo es de comparación (verificar, revisar, validar…).")
    # --- Terminado y si falla ---
    done_criteria = fields.Text(
        string="Criterio de terminado",
        help="Cómo se sabe que quedó bien hecha, en una frase que se contesta con "
             "sí o no: «El pedido coincide con el release en cantidad, fecha y "
             "planta, y trae número de OC».")
    on_fail = fields.Text(
        string="Si no se puede cumplir",
        help="Qué hace quien ejecuta si no se puede o el resultado no pasa: «Si "
             "cambia cantidad o fecha, avisar a Planeación el mismo día».")
    # --- Cuándo (periódicas) ---
    due_weekday = fields.Selection(
        SGI_WEEKDAYS, string="Vence el (semanal)",
        help="Cadencia semanal: día de la semana en que vence.")
    due_business_day = fields.Integer(
        string="Vence el día hábil (mensual)",
        help="Cadencia mensual: día hábil del mes en que vence (1 a 23).")
    # 56.20.0: trimestral, semestral y anual. El mes es el del primer periodo
    # del año (trimestral: enero, febrero o marzo = 1.º, 2.º o 3.er mes de
    # cada trimestre; semestral: enero a junio); se repite cada 3, 6 o 12 meses.
    due_month = fields.Selection(
        SGI_MONTHS, string="Vence en el mes",
        help="Trimestral, semestral o anual: mes en que vence dentro del periodo. "
             "Trimestral con «Febrero» = febrero, mayo, agosto y noviembre.")
    due_day = fields.Integer(
        string="Vence el día",
        help="Día del mes en que vence (1 a 31; si el mes es más corto, el último día).")
    # --- Dónde ---
    exec_channel = fields.Selection(
        SGI_EXEC_CHANNELS, string="Dónde se hace", index=True,
        help="Dónde se hace el trabajo (no cómo se mide).")
    odoo_action_id = fields.Many2one(
        'ir.actions.act_window', string="Acción de Odoo",
        compute='_compute_odoo_action_id', store=True, readonly=False,
        help="Pantalla que abre «Ir a hacerlo»; sale del menú y se puede cambiar.")
    external_system = fields.Char(
        string="Sistema externo",
        help="Portal proveedores GM, VUCEM, sistema del agente aduanal…")
    location_id = fields.Many2one(
        'stock.location', string="Ubicación",
        help="Ubicación física cuando la actividad mueve o toca material.")
    workcenter_id = fields.Many2one(
        'mrp.workcenter', string="Centro de trabajo",
        help="Centro de trabajo donde se hace la actividad.")
    place_note = fields.Char(
        string="Lugar", help="Andén, laboratorio, oficina de embarques…")
    # --- Cómo ---
    how_steps = fields.Text(
        string="Cómo (pasos)",
        help="De 1 a 5 pasos cortos cuando no amerita un instructivo: «Abrir el "
             "pedido → actualizar cantidad y fecha → capturar la OC → confirmar».")
    # --- Faltantes ---
    spec_gap_ids = fields.One2many(
        'sgi.activity.spec.gap', 'activity_id', string="Faltantes", readonly=True)
    spec_complete = fields.Boolean(
        string="Especificación completa", readonly=True, index=True,
        help="Sin faltantes de tipo error.")
    spec_gap_summary = fields.Text(
        string="Qué le falta", compute='_compute_spec_gap_summary')

    _SGI_MEASURE_FIELDS = _BaseActivity._SGI_MEASURE_FIELDS | {'spec_complete'}

    @api.depends('odoo_menu_id')
    def _compute_odoo_action_id(self):
        for act in self:
            action = act.odoo_menu_id.action
            act.odoo_action_id = action if action and action._name == 'ir.actions.act_window' \
                else False

    @api.depends('spec_gap_ids.message', 'spec_gap_ids.severity')
    def _compute_spec_gap_summary(self):
        for act in self:
            act.spec_gap_summary = "\n".join(
                "%s %s" % ("✖" if gap.severity == 'error' else "⚠", gap.message)
                for gap in act.spec_gap_ids) or False

    @api.constrains('due_business_day')
    def _check_due_business_day(self):
        for act in self:
            if act.due_business_day and not 1 <= act.due_business_day <= 23:
                raise ValidationError("El día hábil de vencimiento va de 1 a 23.")

    @api.constrains('due_day')
    def _check_due_day(self):
        for act in self:
            if act.due_day and not 1 <= act.due_day <= 31:
                raise ValidationError("El día de vencimiento va de 1 a 31.")

    def _sgi_due_months(self):
        """Meses del año en que vence (trimestral, semestral o anual)."""
        self.ensure_one()
        step = SGI_CADENCE_MONTHS.get(self.measure_cadence)
        if not step or not self.due_month:
            return []
        offset = (int(self.due_month) - 1) % step
        return [offset + 1 + step * n for n in range(12 // step)]

    def action_open_odoo(self):
        """«Abrir en Odoo»: la acción configurada manda sobre la del menú."""
        self.ensure_one()
        if self.odoo_action_id:
            return self.odoo_action_id.sudo().read()[0]
        return super().action_open_odoo()

    # ------------------------------------------------------------------
    # Reglas de revisión
    # ------------------------------------------------------------------
    @api.model
    def _sgi_verb_list(self, param, default):
        raw = self.env['ir.config_parameter'].sudo().get_param(param, default)
        return [sgi_plain(v) for v in (raw or '').split(',') if v.strip()]

    def _sgi_spec_problems(self):
        """[(código, mensaje)] de lo que le falta a la actividad."""
        self.ensure_one()
        vague = self._sgi_verb_list(VAGUE_VERBS_PARAM, DEFAULT_VAGUE_VERBS)
        compare = self._sgi_verb_list(COMPARE_VERBS_PARAM, DEFAULT_COMPARE_VERBS)
        name = sgi_plain(self.name)
        words = name.split()
        first = words[0] if words else ''
        first_two = ' '.join(words[:2])
        roles = self.role_ids
        escala = roles.filtered(lambda r: r.role == 'escala')
        executors = roles.filtered(lambda r: r.role == 'ejecuta')
        out = []

        def add(code, message):
            out.append((code, message))

        if not (self.done_criteria or '').strip():
            add('no_done', "Falta el criterio de terminado (una frase de sí o no).")
        if not (self.on_fail or '').strip() and not escala:
            add('no_on_fail', "Falta qué hacer si no se puede cumplir, o un rol «Escala».")
        timed_input = any(line.max_days or line.due_field for line in self.input_ids)
        periodic = bool(self.due_weekday or self.due_business_day
                        or (self.due_month and self.due_day))
        # Arranque externo: actividad de la PRIMERA etapa de su proceso cuyas
        # entradas no las produce ninguna actividad (llegan de fuera: pedido
        # del cliente, requisición…); su plazo lo pone quien la dispara.
        # B-008: antes se leía el bloque fijo «inicial» (campo que se retira);
        # en producción las 15 actividades «inicial» con entradas están en la
        # etapa A de su proceso, así que la regla da lo mismo.
        first_stage = self.process_id.stage_ids[:1]
        external_start = bool(
            first_stage and self.stage_id == first_stage and self.input_ids
            and not any(line.deliverable_id.producer_activity_ids
                        for line in self.input_ids))
        if not timed_input and not periodic and not external_start:
            add('no_timing', "Sin plazo: ponga días a alguna entrada o un vencimiento periódico.")
        elif (self.measure_cadence in SGI_CADENCE_MONTHS
              and not (self.due_month or self.due_day)):
            # 57.15.0 (G-008 a): una trimestral, semestral o anual sin mes ni
            # día no dice «cuándo» en Mi procedimiento aunque una entrada
            # tenga plazo. (Con uno solo de los dos lo marca due_mismatch.)
            add('no_timing', "Cadencia %s sin mes y día de vencimiento." % dict(
                self._fields['measure_cadence'].selection).get(
                    self.measure_cadence, self.measure_cadence).lower())
        if self.due_weekday and self.measure_cadence != 'semanal':
            add('due_mismatch', "Vence un día de la semana pero la cadencia no es semanal.")
        if self.due_business_day and self.measure_cadence != 'mensual':
            add('due_mismatch', "Vence un día hábil del mes pero la cadencia no es mensual.")
        if (self.due_month or self.due_day) and self.measure_cadence not in SGI_CADENCE_MONTHS:
            add('due_mismatch', "Vence en un mes y día pero la cadencia no es trimestral, "
                                "semestral ni anual.")
        elif bool(self.due_month) != bool(self.due_day):
            add('due_mismatch', "Falta el %s del vencimiento." % ("día" if self.due_month else "mes"))
        unconditioned = executors.filtered(lambda r: not (r.condition or '').strip())
        if len(unconditioned) > 1:
            add('multi_exec', "Más de un puesto la ejecuta: divida la actividad.")
        if first in vague or first_two in vague:
            add('vague_verb', "«%s» no se puede observar: use un verbo que diga qué "
                              "se entrega." % (first_two if first_two in vague else first))
        if first == 'recibir':
            add('trigger_as_activity', "«Recibir» es el disparador de la siguiente "
                                       "actividad, no una actividad.")
        if first in compare and not (self.check_against or '').strip():
            add('no_check_against', "«%s» sin decir contra qué se compara." % first)
        if not self.output_deliverable_ids and self.measure_method != 'entregable':
            add('no_output', "Sin salida: únala con la actividad que la usa o declare "
                             "qué entrega.")
        if not escala:
            add('no_escalation', "Sin rol «Escala».")
        if self.measure_method == 'entregable' and self.measure_deliverable_id \
                and not (self.measure_deliverable_id.complete_domain or '').strip():
            add('measure_no_complete', "Se mide con «%s», que no dice cuándo está "
                                       "completo." % self.measure_deliverable_id.name)
        channel = self.exec_channel
        if not channel:
            add('no_channel', "Falta dónde se hace (canal).")
        if channel == 'odoo' and not self.odoo_menu_id:
            add('odoo_no_menu', "Canal Odoo sin la pantalla (menú) donde se hace.")
        if channel == 'odoo' and self.odoo_menu_id:
            # E-010 (entrega 4): Mi procedimiento le dice al ejecutor una ruta
            # que su usuario no puede abrir (p. ej. Administración SGI).
            users = self._sgi_executor_users()
            if users and not any(sgi_menu_visible_for(self.odoo_menu_id, user) for user in users):
                add('menu_no_visible', "Nadie de quien la ejecuta ve «%s»: apunte a una "
                                       "entrada que sí vea (p. ej. Inicio → Mis indicadores) "
                                       "o dele el grupo." % self.odoo_menu_id.sudo().complete_name)
        # 57.103.0: la pantalla (menú o acción) abre un modelo y la evidencia
        # se cuenta en otro: «Ir» manda a un lugar y la medición mira otro.
        mismatch = self._sgi_menu_model_mismatch()
        if mismatch:
            add('menu_model_mismatch', "La pantalla «%s» abre %s, pero la actividad se mide con "
                                       "%s: corrija el menú o el modelo de medición." % mismatch)
        # 57.104.0: la medición no encuentra registros o no sabe de quién son.
        never = self._sgi_measure_never()
        if never:
            add('measure_never', never)
        for message in self._sgi_weak_attribution():
            add('weak_attribution', message)
        if channel in SGI_EXTERNAL_CHANNELS and not (self.external_system or '').strip():
            add('external_no_name', "Falta el nombre del sistema externo.")
        if not self.instruction_id and not (self.how_steps or '').strip():
            add('no_how', "Falta cómo se hace: instructivo o pasos.")
        if channel == 'odoo' and self.measure_method in ('manual', 'correo'):
            add('odoo_measured_manual', "Se hace en Odoo pero se mide a mano: defina "
                                        "el entregable.")
        if channel == 'papel':
            add('paper_channel', "En papel: candidata a pasarse a Odoo.")
        if ' y registrar' in name or ' y capturar' in name:
            add('mixed_channel', "Junta trabajo físico y captura: pártala en dos "
                                 "actividades para medir cada una.")
        output = self._sgi_output_deliverable()
        if output and output.odoo_model_id:
            for line in self.input_ids.filtered(lambda l: l.max_days or l.due_field):
                model = line.deliverable_id.odoo_model_id
                if model and model != output.odoo_model_id and not line.match_path:
                    add('no_match', "«%s» (%s) no se liga con la salida (%s): falta "
                                    "«match»." % (line.deliverable_id.name, model.model,
                                                 output.odoo_model_id.model))
        return out

    def _sgi_measures_itself(self):
        """Se mide sola con un modelo de Odoo (Registro en Odoo, Por su
        entregable o las heredadas sin método y con modelo)."""
        self.ensure_one()
        return bool(self.measure_model_id) and self.measure_method in (False, 'odoo', 'entregable')

    def _sgi_measure_never(self):
        """57.104.0: mensaje si la actividad se mide sola y su evidencia no
        aparece: ningún registro, o el último hace más de 60 días (o de la
        ventana de su cadencia, si es más larga). Las recién creadas esperan
        el mismo plazo. None si va bien."""
        self.ensure_one()
        if not self._sgi_measures_itself() or not self.create_date:
            return None
        days = max(MEASURE_NEVER_DAYS, self._SGI_CADENCE_DAYS.get(self.measure_cadence, 0))
        limit = fields.Datetime.now() - timedelta(days=days)
        last = self.measure_last_date
        if (last or self.create_date) >= limit:
            return None
        if self.measure_warning:
            return ("La medición no cuenta nada: %s" % self.measure_warning.strip().splitlines()[0])
        if not last:
            return ("La medición nunca ha encontrado un registro de %s: revise el filtro de "
                    "evidencia y el campo de fecha, o si de verdad se registra en Odoo."
                    % self.measure_model_id.model)
        return ("El último registro de %s que cuenta la medición es del %s (más de %d días): "
                "revise el filtro de evidencia y el campo de fecha." % (
                    self.measure_model_id.model, fields.Date.to_string(last.date()), days))

    def _sgi_weak_attribution(self):
        """57.104.0: mensajes si la medición no puede decir quién hizo la
        actividad: usa «write_uid» (el último que editó, no quien la hizo) o
        la mayoría de las ejecuciones de 4 semanas son de otro puesto o de
        cuentas compartidas."""
        self.ensure_one()
        out = []
        if not self._sgi_measures_itself():
            return out
        if (self.measure_user_field or '').strip() == 'write_uid':
            out.append("Se atribuye con «write_uid», el último que editó el registro, no quien "
                       "hizo la actividad: use el campo del responsable o de quien valida o cierra.")
        wrong = (self.measure_count_other_job or 0) + (self.measure_count_generic or 0)
        if wrong and (self.measure_adherence_pct or 0.0) < WEAK_ATTRIBUTION_PCT:
            out.append("En 4 semanas, %d ejecuciones son de otro puesto o de cuentas compartidas y "
                       "solo el %.0f %% del puesto asignado: revise el puesto en la Matriz de "
                       "responsabilidades o el campo de usuario." % (
                           wrong, self.measure_adherence_pct or 0.0))
        return out

    def _sgi_screen_model(self):
        """57.104.0: (pantalla, modelo que abre) de la actividad: el de la
        acción de ventana o, si el menú lanza una acción de servidor, el
        modelo sobre el que corre. (pantalla, None) si es una acción de
        cliente (reporte, tablero) o no hay pantalla."""
        self.ensure_one()
        menu = self.sudo().odoo_menu_id
        action = self.sudo().odoo_action_id
        if action and action.res_model:
            return (menu.complete_name or action.name or ''), action.res_model
        menu_action = menu.action
        if menu_action and menu_action._name == 'ir.actions.server' and menu_action.model_id:
            return (menu.complete_name or menu_action.name or ''), menu_action.model_id.model
        return (menu.complete_name or ''), None

    def _sgi_menu_model_mismatch(self):
        """57.103.0: (pantalla, modelo que abre, modelo de medición) si la
        actividad se mide sola con un modelo y su pantalla abre otro; None si
        van juntos o no hay con qué comparar (sin pantalla, acción de cliente,
        sin modelo de medición o medida por consecuencia o a mano). Desde
        57.104.0 también compara los menús con acción de servidor."""
        self.ensure_one()
        if self.measure_method not in (False, 'odoo', 'entregable'):
            return None
        measured = self.measure_model_id
        if not measured and self.measure_method == 'entregable':
            measured = self._sgi_output_deliverable().odoo_model_id
        if not measured:
            return None
        screen, model = self._sgi_screen_model()
        if not model or model == measured.model:
            return None
        return screen, model, measured.model

    def _sgi_executor_users(self):
        """Usuarios activos de los puestos con rol «Ejecuta» (E-010)."""
        self.ensure_one()
        jobs = self.role_ids.filtered(lambda r: r.role == 'ejecuta').job_id
        if not jobs:
            return self.env['res.users']
        employees = self.env['hr.employee'].sudo().search([('job_id', 'in', jobs.ids)])
        return employees.user_id.filtered(lambda u: u.active and not u.share)

    def _sgi_refresh_spec_gaps(self):
        """Reescribe los faltantes solo si cambiaron."""
        Gap = self.env['sgi.activity.spec.gap'].sudo()
        for act in self.exists():
            # Un proceso archivado (o una actividad inactiva) no tiene faltantes
            # que atender: no se le generan y los que tenía se borran.
            live = act.active and act.process_id.active
            wanted = [(code, SGI_GAP_SEVERITY[code], msg) for code, msg in act._sgi_spec_problems()] \
                if live else []
            current = [(g.code, g.severity, g.message) for g in act.sudo().spec_gap_ids]
            if sorted(wanted) != sorted(current):
                act.sudo().spec_gap_ids.unlink()
                if wanted:
                    Gap.create([{'activity_id': act.id, 'code': c, 'severity': s, 'message': m}
                                for c, s, m in wanted])
            complete = not any(s == 'error' for _c, s, _m in wanted)
            if act.spec_complete != complete:
                act.sudo().with_context(sgi_spec_refresh=True).write({'spec_complete': complete})

    @api.model_create_multi
    def create(self, vals_list):
        acts = super().create(vals_list)
        acts._sgi_refresh_spec_gaps()
        return acts

    def write(self, vals):
        res = super().write(vals)
        if not self.env.context.get('sgi_spec_refresh') and set(vals) - _CRON_FIELDS:
            self._sgi_refresh_spec_gaps()
        return res

    def _sgi_sentence_parts(self):
        """Las piezas del procedimiento más dónde, cómo, cuándo está terminada
        y qué hacer si falla."""
        parts = super()._sgi_sentence_parts()
        where = []
        if self.exec_channel:
            where.append(dict(SGI_EXEC_CHANNELS)[self.exec_channel])
        if self.odoo_menu_id:
            where.append(self.odoo_menu_id.complete_name)
        if self.external_system:
            where.append(self.external_system)
        if self.location_id:
            where.append(self.location_id.complete_name)
        if self.workcenter_id:
            where.append(self.workcenter_id.display_name)
        if self.place_note:
            where.append(self.place_note)
        if where:
            parts.append(("Dónde", " — ".join(where)))
        if self.check_against:
            parts.append(("Contra", self.check_against))
        if self.how_steps:
            parts.append(("Cómo", " ".join(self.how_steps.split()).rstrip('.')))
        if self.done_criteria:
            parts.append(("Terminada cuando", self.done_criteria.strip().rstrip('.')))
        if self.due_weekday and self.measure_cadence == 'semanal':
            parts.append((None, "Vence cada %s" % dict(SGI_WEEKDAYS)[self.due_weekday].lower()))
        if self.due_business_day and self.measure_cadence == 'mensual':
            parts.append((None, "Vence el día hábil %d del mes" % self.due_business_day))
        if self.due_day and self._sgi_due_months():
            parts.append((None, "Vence el %s" % self._sgi_due_label()))
        if self.on_fail:
            parts.append(("Si no se puede", self.on_fail.strip().rstrip('.')))
        return parts

    # ------------------------------------------------------------------
    # Medición: aplicables, hechas, completas, a tiempo, vencidas abiertas
    # ------------------------------------------------------------------
    def _sgi_output_deliverable(self):
        """El entregable con el que se mide la salida."""
        self.ensure_one()
        if self.measure_deliverable_id:
            return self.measure_deliverable_id
        return self.output_deliverable_ids.filtered('odoo_model_id')[:1]

    def _sgi_periodic_due(self, day):
        """Fecha de vencimiento del periodo que contiene ``day`` (o None).

        57.15.0 (decisión 4 de la tanda 2, G-008 c): el vencimiento semanal o
        de mes y día que cae en día inhábil (fin de semana o festivo del
        calendario) se adelanta al hábil anterior, sin salirse del periodo."""
        self.ensure_one()
        if self.due_weekday and self.measure_cadence == 'semanal':
            monday = day - timedelta(days=day.weekday())
            return sgi_previous_business_day(
                self.env, monday + timedelta(days=int(self.due_weekday)), floor=monday,
                company=self.company_id)
        if self.due_business_day and self.measure_cadence == 'mensual':
            return sgi_nth_business_day(self.env, day.year, day.month, self.due_business_day,
                                        self.company_id)
        months = self._sgi_due_months() if self.due_day else []
        if months:
            # El periodo es el bloque del año (trimestre, semestre, año) que
            # contiene ``day``; vence en su mes que toca.
            step = SGI_CADENCE_MONTHS[self.measure_cadence]
            block_start = ((day.month - 1) // step) * step + 1
            month = next(m for m in months if block_start <= m < block_start + step)
            last = calendar.monthrange(day.year, month)[1]
            return sgi_previous_business_day(
                self.env, date(day.year, month, min(self.due_day, last)),
                floor=date(day.year, block_start, 1), company=self.company_id)
        return None

    def _sgi_period_start(self, day):
        """Primer día del periodo que contiene ``day`` (lunes, día 1 del mes o
        del bloque del año), o None si la actividad no tiene vencimiento
        periódico. Mismos periodos que ``_sgi_periodic_due``."""
        self.ensure_one()
        if self._sgi_periodic_due(day) is None:
            return None
        if self.measure_cadence == 'semanal':
            return day - timedelta(days=day.weekday())
        if self.measure_cadence == 'mensual':
            return day.replace(day=1)
        step = SGI_CADENCE_MONTHS[self.measure_cadence]
        return date(day.year, ((day.month - 1) // step) * step + 1, 1)

    def _sgi_periodic_state(self, Model, domain, date_field, today):
        """G-017 (56.36.0): semáforo con el vencimiento de la decisión 5, el
        mismo «a tiempo» que ``sgi.activity.week.stat``. None si la actividad
        no tiene vencimiento periódico (entonces manda la ventana de días
        naturales de su cadencia).

        - Hecha en el periodo en curso, a más tardar en su vencimiento: verde.
        - Vencido el periodo en curso sin evidencia a tiempo: rojo.
        - Periodo en curso aún sin vencer: manda el anterior (hecha a tiempo:
          verde; si no: rojo).
        """
        self.ensure_one()
        due = self._sgi_periodic_due(today)
        if due is None:
            return None
        is_date = Model._fields[date_field].type == 'date'

        def done(start, end):
            if end < start:
                return False
            lo, hi = start, end + timedelta(days=1)
            if not is_date:
                lo, hi = datetime.combine(lo, time.min), datetime.combine(hi, time.min)
            return bool(Model.search_count(domain + [(date_field, '>=', lo), (date_field, '<', hi)],
                                           limit=1))

        start = self._sgi_period_start(today)
        if done(start, min(today, due)):
            return 'verde'
        if today > due:
            return 'rojo'
        prev_day = start - timedelta(days=1)
        prev_due = self._sgi_periodic_due(prev_day)
        prev_start = self._sgi_period_start(prev_day)
        if prev_due and prev_start and done(prev_start, prev_due):
            return 'verde'
        return 'rojo'

    def _sgi_due_label(self):
        """«15 de marzo» (anual) o «15 de enero, abril, julio y octubre»."""
        self.ensure_one()
        names = dict(SGI_MONTHS)
        months = [names[str(m)].lower() for m in self._sgi_due_months()]
        text = months[0] if len(months) == 1 else "%s y %s" % (", ".join(months[:-1]), months[-1])
        return "%d de %s" % (self.due_day, text)


class SgiActivityRoleSpec(models.Model):
    _inherit = 'sgi.activity.role'

    @api.model_create_multi
    def create(self, vals_list):
        roles = super().create(vals_list)
        roles.activity_id._sgi_refresh_spec_gaps()
        return roles

    def write(self, vals):
        res = super().write(vals)
        self.activity_id._sgi_refresh_spec_gaps()
        return res

    def unlink(self):
        activities = self.activity_id
        res = super().unlink()
        activities.exists()._sgi_refresh_spec_gaps()
        return res


class SgiActivityInputSpec(models.Model):
    _inherit = 'sgi.activity.input'

    applies_domain = fields.Char(
        string="Aplica cuando",
        help="Dominio sobre el registro del entregable de entrada. Si no se cumple, "
             "la actividad no aplica a ese registro: no vence ni cuenta como atrasada. "
             "Ej. [('partner_id.country_id.code', '!=', 'MX')].")
    applies_note = fields.Char(
        string="Aplica cuando (en palabras)", help="«Solo embarques de exportación».")
    match_path = fields.Char(
        string="Se liga con la salida por",
        help="Campo de la salida que apunta al registro de esta entrada, cuando son "
             "modelos distintos (ej. «sale_id» si la salida es un stock.picking y la "
             "entrada un sale.order). Mismo modelo: no hace falta.")
    # P-4: el vencimiento sale de una fecha del propio registro (la fecha
    # programada de la entrega) y no de «días después de que llegó».
    due_field = fields.Char(
        string="Vence según el campo",
        help="Campo de fecha del registro de esta entrada contra el que vence la "
             "actividad (ej. «scheduled_date» de la entrega). Con esto el plazo "
             "no son días desde que llegó sino esa fecha más el margen.")
    offset_days = fields.Integer(
        string="Margen (días hábiles)",
        help="Días hábiles que se suman a «Vence según el campo». Negativo = antes: "
             "-2 vence dos días hábiles antes de la fecha programada.")

    @api.constrains('due_field', 'deliverable_id')
    def _check_due_field(self):
        for line in self.filtered('due_field'):
            model = line.deliverable_id.odoo_model_id.model
            if not model or model not in self.env:
                raise ValidationError(
                    "«Vence según el campo» de %s necesita que el entregable «%s» "
                    "tenga modelo de Odoo." % (line.activity_id.display_name,
                                                line.deliverable_id.name))
            field = self.env[model]._fields.get(line.due_field.strip())
            if field is None or field.type not in ('date', 'datetime'):
                raise ValidationError(
                    "«Vence según el campo» de %s: «%s» no es un campo de fecha de %s." % (
                        line.activity_id.display_name, line.due_field, model))

    def _sgi_has_deadline(self):
        self.ensure_one()
        return bool(self.max_days or self.due_field)

    def _sgi_due(self, record, in_date, workdays=None):
        """Fecha (date) en que vence la actividad para ``record`` (un registro
        de la entrada): la fecha del campo «vence según» más el margen, o la
        fecha de llegada (``in_date``) más los días hábiles del plazo. None si
        el registro no trae la fecha. ``workdays`` (``SgiWorkdays``, 57.17.0)
        evita consultar el calendario por cada registro."""
        self.ensure_one()
        company = self.activity_id.company_id

        def add(base, days):
            if workdays is not None:
                return workdays.add(base, days)
            return sgi_add_business_days(self.env, base, days, company)
        if self.due_field:
            base = record[self.due_field.strip()]
            if not base:
                return None
            return add(base, self.offset_days)
        base = record[in_date]
        if not base:
            return None
        return add(base, self.max_days)

    @api.constrains('applies_domain', 'deliverable_id')
    def _check_applies_domain(self):
        for line in self.filtered('applies_domain'):
            model = line.deliverable_id.odoo_model_id.model
            try:
                domain = sgi_safe_domain(line.applies_domain)
                if model and model in self.env:
                    self.env[model].sudo().search_count(domain, limit=1)
            except Exception as exc:  # noqa: BLE001 - el mensaje va al usuario
                raise ValidationError("«Aplica cuando» de %s es inválido: %s" % (
                    line.activity_id.display_name, exc))

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines.activity_id._sgi_refresh_spec_gaps()
        return lines

    def write(self, vals):
        res = super().write(vals)
        self.activity_id._sgi_refresh_spec_gaps()
        return res

    def unlink(self):
        activities = self.activity_id
        res = super().unlink()
        activities.exists()._sgi_refresh_spec_gaps()
        return res

    def _sgi_applicable_domain(self):
        """Dominio completo de las entradas que aplican: existe + aplica."""
        self.ensure_one()
        deliverable = self.deliverable_id
        return sgi_safe_domain(deliverable.measure_domain) + sgi_safe_domain(self.applies_domain)


class SgiDeliverableSpec(models.Model):
    _inherit = 'sgi.deliverable'

    complete_domain = fields.Char(
        string="Filtro: está completo",
        help="Además de existir, cuándo cuenta como completo, ej. "
             "[('client_order_ref', '!=', False), ('commitment_date', '!=', False)]. "
             "Vacío: lo que existe cuenta como completo.")
    complete_criteria = fields.Text(
        string="Completo significa",
        help="Lo mismo en palabras; se imprime en el procedimiento.")

    @api.constrains('complete_domain', 'odoo_model_id')
    def _check_complete_domain(self):
        for deliverable in self.filtered('complete_domain'):
            model = deliverable.odoo_model_id.model
            try:
                domain = sgi_safe_domain(deliverable.complete_domain)
                if model and model in self.env:
                    self.env[model].sudo().search_count(domain, limit=1)
            except Exception as exc:  # noqa: BLE001
                raise ValidationError("«Está completo» de %s es inválido: %s" % (
                    deliverable.name, exc))

    def write(self, vals):
        res = super().write(vals)
        if 'complete_domain' in vals:
            self.measured_activity_ids._sgi_refresh_spec_gaps()
        return res


class SgiProcessSpec(models.Model):
    _inherit = 'sgi.process'

    state = fields.Selection([
        ('borrador', "Borrador"),
        ('piloto', "Piloto"),
        ('vigente', "Vigente"),
    ], string="Estado", default='borrador', required=True, tracking=True, index=True,
        help="Borrador y piloto se pueden cargar incompletos (los faltantes se ven). "
             "Vigente exige la especificación completa de actividades e indicadores.")
    spec_error_count = fields.Integer(
        string="Faltantes que bloquean", compute='_compute_spec_counts')
    spec_warning_count = fields.Integer(
        string="Advertencias", compute='_compute_spec_counts')
    channel_odoo_pct = fields.Integer(
        string="% en Odoo", compute='_compute_channel_pct',
        help="Actividades con canal Odoo, sobre las que tienen canal.")
    channel_manual_pct = fields.Integer(
        string="% en papel, correo o teléfono", compute='_compute_channel_pct',
        help="La lista de trabajo de automatización.")
    odoo_measured_pct = fields.Integer(
        string="% de lo que se hace en Odoo que se mide solo", compute='_compute_channel_pct',
        help="Actividades con canal Odoo que se miden por su entregable. Si se hace en "
             "Odoo y no se mide solo, es un hueco del SGI, no del proceso.")

    def _compute_spec_counts(self):
        Gap = self.env['sgi.activity.spec.gap']
        counts = {}
        if self.ids:
            for process, severity, count in Gap._read_group(
                    [('process_id', 'in', self.ids), ('activity_id.active', '=', True)],
                    ['process_id', 'severity'], ['__count']):
                counts[(process.id, severity)] = count
        for process in self:
            process.spec_error_count = counts.get((process.id, 'error'), 0)
            process.spec_warning_count = counts.get((process.id, 'warning'), 0)

    def _compute_channel_pct(self):
        for process in self:
            acts = process.procedure_activity_ids.filtered('exec_channel')
            odoo = acts.filtered(lambda a: a.exec_channel == 'odoo')
            manual = acts.filtered(lambda a: a.exec_channel in SGI_MANUAL_CHANNELS)
            measured = odoo.filtered(lambda a: a.measure_method == 'entregable')
            process.channel_odoo_pct = round(len(odoo) * 100.0 / len(acts)) if acts else 0
            process.channel_manual_pct = round(len(manual) * 100.0 / len(acts)) if acts else 0
            process.odoo_measured_pct = round(len(measured) * 100.0 / len(odoo)) if odoo else 0

    def _sgi_publish_problems(self):
        """(errores, advertencias): [(actividad o indicador, código, mensaje)]."""
        self.ensure_one()
        errors, warnings = [], []
        acts = self.procedure_activity_ids.filtered('active')
        acts._sgi_refresh_spec_gaps()
        for gap in acts.spec_gap_ids:
            item = (gap.activity_id.number or gap.activity_id.name, gap.code, gap.message)
            (errors if gap.severity == 'error' else warnings).append(item)
        for indicator in self.indicator_ids:
            for message in indicator._sgi_spec_problems():
                errors.append((indicator.code, 'indicador', message))
        return errors, warnings

    def action_sgi_set_pilot(self):
        self.write({'state': 'piloto'})

    def action_sgi_set_draft(self):
        self.write({'state': 'borrador'})

    def action_sgi_publish(self):
        """Publicar (vigente) exige la especificación completa."""
        for process in self:
            errors, _warnings = process._sgi_publish_problems()
            if errors:
                raise UserError("No se puede publicar %s: faltan %d cosa(s).\n\n%s" % (
                    process.display_name, len(errors), "\n".join(
                        "• %s · %s · %s" % item for item in errors[:80])))
        self.write({'state': 'vigente'})
        warnings = [w for process in self for w in process._sgi_publish_problems()[1]]
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': "Proceso publicado",
                'message': ("Con %d advertencia(s): %s" % (
                    len(warnings), "; ".join("%s %s" % (w[0], w[1]) for w in warnings[:15])))
                if warnings else "Sin advertencias.",
                'type': 'warning' if warnings else 'success',
                'sticky': bool(warnings),
            },
        }

    def action_view_spec_gaps(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Faltantes — %s" % self.display_name,
            'res_model': 'sgi.activity.spec.gap',
            'view_mode': 'list,pivot',
            'domain': [('process_id', '=', self.id)],
        }


class SgiIndicatorSpec(models.Model):
    _inherit = 'sgi.indicator'

    baseline_value = fields.Float(string="Valor de arranque",
                                  help="El primer mes medido.")
    target_date = fields.Date(
        string="Llegar a la meta el",
        help="Opcional: sin fecha, la meta es permanente.")
    spec_missing = fields.Char(string="Le falta", compute='_compute_spec_missing', store=True)

    # --- Modos genéricos (P-1): el indicador se calcula solo de lo que ya
    # mide el SGI, sin una fórmula fija por indicador.
    activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad medida", ondelete='set null',
        help="Para «% a tiempo»: la actividad cuyo cumplimiento semanal se toma.")
    deliverable_id = fields.Many2one(
        'sgi.deliverable', string="Entregable medido", ondelete='set null',
        help="Para «% completo»: el entregable cuyo filtro «ya está completo» "
             "se compara contra lo entregado. Vacío: el entregable con el que "
             "se mide la actividad.")

    def _sgi_measured_deliverable(self):
        self.ensure_one()
        if self.deliverable_id:
            return self.deliverable_id
        return self.activity_id._sgi_output_deliverable() if self.activity_id \
            else self.env['sgi.deliverable']

    def _detail_actividad_a_tiempo(self, date_from, date_to):
        """% a tiempo de la actividad en las semanas del periodo. Toma las
        filas que ya calculó el cron (sgi.activity.week.stat) y, para las
        semanas que no tenga, las cuenta en el momento. Detalle (I-1): a
        tiempo ÷ con plazo, los casos son los «con plazo» y los registros las
        filas semanales que se usaron."""
        if not self.activity_id:
            return {'value': None}
        monday = date_from - timedelta(days=date_from.weekday())
        weeks = []
        while monday <= date_to:
            weeks.append(monday)
            monday += timedelta(days=7)
        stats = {s.period_start: s for s in self.env['sgi.activity.week.stat'].search([
            ('activity_id', '=', self.activity_id.id), ('period_start', 'in', weeks)])}
        timed = on_time = 0
        for start in weeks:
            stat = stats.get(start)
            counts = ({'timed_count': stat.timed_count, 'on_time_count': stat.on_time_count}
                      if stat else self.activity_id._sgi_week_counts(start))
            timed += counts['timed_count']
            on_time += counts['on_time_count']
        return {
            'value': round(on_time * 100.0 / timed, 2) if timed else None,
            'numerator': on_time, 'denominator': timed, 'sample_size': timed,
            'model': 'sgi.activity.week.stat', 'ids': [s.id for s in stats.values()],
        }

    def _calc_actividad_a_tiempo(self, date_from, date_to):
        return self._detail_actividad_a_tiempo(date_from, date_to)['value']

    def _detail_entregable_completo(self, date_from, date_to):
        """% de lo entregado en el periodo que cumple el filtro «ya está
        completo» del entregable. Detalle (I-1): completos ÷ entregados, con
        los registros entregados."""
        deliverable = self._sgi_measured_deliverable()
        model = deliverable.odoo_model_id.model
        if not model or model not in self.env:
            return {'value': None}
        Model = self.env[model].sudo()
        date_field = deliverable.measure_date_field or 'create_date'
        if date_field not in Model._fields:
            date_field = 'create_date'
        start = datetime.combine(date_from, time.min)
        end = datetime.combine(date_to, time.min) + timedelta(days=1)
        done = Model.search(sgi_safe_domain(deliverable.measure_domain)
                            + [(date_field, '>=', start), (date_field, '<', end)])
        complete_domain = sgi_safe_domain(deliverable.complete_domain)
        complete_ids = Model.search([('id', 'in', done.ids)] + complete_domain).ids \
            if (done and complete_domain) else done.ids
        if deliverable.require_signed and complete_ids:
            # REG-1: completo solo si el registro tiene una firma de Sign ligada.
            signed = set(deliverable._sgi_signed_ids(model, complete_ids))
            complete_ids = [i for i in complete_ids if i in signed]
        complete = len(complete_ids)
        return {
            'value': round(complete * 100.0 / len(done), 2) if done else None,
            'numerator': complete, 'denominator': len(done),
            'model': model, 'ids': done.ids,
        }

    def _calc_entregable_completo(self, date_from, date_to):
        return self._detail_entregable_completo(date_from, date_to)['value']

    def _sgi_spec_problems(self):
        """SMART: meta, fórmula, fuente, responsable y frecuencia."""
        self.ensure_one()
        problems = []
        if self.calc_mode == 'actividad_a_tiempo' and not self.activity_id:
            problems.append("«% a tiempo» sin actividad medida")
        if self.calc_mode == 'entregable_completo':
            deliverable = self._sgi_measured_deliverable()
            if not deliverable.odoo_model_id:
                problems.append("«% completo» sin entregable con modelo de Odoo")
            elif not (deliverable.complete_domain or '').strip():
                problems.append("el entregable %s no dice cuándo está completo" % deliverable.name)
        if not self.target_objective:
            problems.append("sin meta")
        if not (self.formula or '').strip():
            problems.append("sin fórmula")
        if not (self.source or '').strip():
            problems.append("sin fuente del dato")
        if not self.responsible_id:
            problems.append("sin responsable")
        if not self.frequency:
            problems.append("sin frecuencia")
        return problems

    @api.depends('target_objective', 'formula', 'source', 'responsible_id', 'frequency', 'calc_mode',
                 'activity_id', 'deliverable_id.odoo_model_id', 'deliverable_id.complete_domain',
                 'activity_id.output_deliverable_ids.odoo_model_id',
                 'activity_id.output_deliverable_ids.complete_domain')
    def _compute_spec_missing(self):
        for indicator in self:
            indicator.spec_missing = ", ".join(indicator._sgi_spec_problems()) or False


class IrConfigParameterSpec(models.Model):
    _inherit = 'ir.config_parameter'

    def _sgi_verbs_touched(self, keys):
        if {VAGUE_VERBS_PARAM, COMPARE_VERBS_PARAM} & set(keys):
            self.env['sgi.process.activity'].search([])._sgi_refresh_spec_gaps()

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        self._sgi_verbs_touched([v.get('key') for v in vals_list])
        return records

    def write(self, vals):
        keys = self.mapped('key')
        res = super().write(vals)
        self._sgi_verbs_touched(keys)
        return res


class SgiActivityWeekStat(models.Model):
    """Aplicables, hechas, completas, a tiempo y vencidas abiertas, por actividad
    y semana. Va aparte de ``sgi.activity.exec.stat`` (que tiene un renglón por
    usuario): repetir estos totales en cada renglón de usuario los multiplicaría
    al sumar en el pivot."""
    _name = 'sgi.activity.week.stat'
    _description = "Cumplimiento semanal de una actividad SGI"
    _order = 'period_start desc, activity_id'

    activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad", required=True, ondelete='cascade',
        index=True, readonly=True,
        help="Actividad medida.")
    process_id = fields.Many2one(
        related='activity_id.process_id', string="Proceso", store=True, index=True,
        help="Proceso de la actividad.")
    exec_channel = fields.Selection(
        related='activity_id.exec_channel', string="Canal", store=True,
        help="Dónde se hace el trabajo.")
    period_start = fields.Date(string="Semana", required=True, index=True, readonly=True,
                               help="Lunes de la semana medida.")
    applicable_count = fields.Integer(string="Aplicables", readonly=True, aggregator='sum')
    done_count = fields.Integer(string="Hechas", readonly=True, aggregator='sum')
    complete_count = fields.Integer(string="Completas", readonly=True, aggregator='sum')
    timed_count = fields.Integer(
        string="Con plazo medible", readonly=True, aggregator='sum',
        help="Hechas cuya entrada se pudo ligar (o con vencimiento periódico).")
    on_time_count = fields.Integer(string="A tiempo", readonly=True, aggregator='sum')
    late_open_count = fields.Integer(
        string="Vencidas abiertas", readonly=True, aggregator='sum',
        help="Entradas aplicables sin salida y con el plazo vencido al cierre de la semana.")
    completeness_rate = fields.Float(
        string="% completas", compute='_compute_rates', store=True, aggregator='avg',
        digits=(5, 1),
        help="Porcentaje de registros de la semana que cumplen el criterio de completo. Se calcula solo.")
    on_time_rate = fields.Float(
        string="% a tiempo", compute='_compute_rates', store=True, aggregator='avg',
        digits=(5, 1),
        help="Porcentaje de registros de la semana hechos a tiempo. Se calcula solo.")
    company_id = fields.Many2one(
        related='activity_id.company_id', string="Empresa", store=True, index=True)

    _activity_week_uniq = models.Constraint(
        'unique(activity_id, period_start)', "Una fila por actividad y semana.")

    @api.depends('done_count', 'complete_count', 'timed_count', 'on_time_count')
    def _compute_rates(self):
        for stat in self:
            stat.completeness_rate = stat.complete_count * 100.0 / stat.done_count \
                if stat.done_count else 0.0
            stat.on_time_rate = stat.on_time_count * 100.0 / stat.timed_count \
                if stat.timed_count else 0.0

    _LOOKBACK_DAYS = 90

    @api.model
    def _sgi_compute(self, activities, weeks=4):
        """Recalcula las últimas ``weeks`` semanas de cada actividad medible."""
        today = sgi_today(self.env)
        monday = today - timedelta(days=today.weekday())
        periods = [monday - timedelta(weeks=n) for n in range(weeks - 1, -1, -1)]
        failures = 0
        # 57.17.0 (G-016): los días hábiles de toda la corrida se piden al
        # calendario una sola vez (antes, una vez por cada registro de entrada
        # y por cada una de las 4 semanas).
        workdays = SgiWorkdays(
            self.env, periods[0] - timedelta(days=self._LOOKBACK_DAYS + 7), today + timedelta(days=90))
        for act in activities:
            # 57.16.0 (G-002): savepoint por actividad; un dominio que truena en
            # SQL ya no deja abortada la transacción para las demás.
            try:
                with self.env.cr.savepoint():
                    self._sgi_compute_one(act, periods, workdays)
            except Exception:  # noqa: BLE001 - un dominio malo no tumba a las demás
                failures += 1
                _logger.exception("SGI: no se pudo medir la semana de %s", act.display_name)
        return failures

    def _sgi_compute_one(self, act, periods, workdays=None):
        rows = [act._sgi_week_counts(start, workdays) for start in periods]
        existing = {s.period_start: s for s in self.search([
            ('activity_id', '=', act.id), ('period_start', 'in', periods)])}
        for start, counts in zip(periods, rows):
            stat = existing.get(start)
            if stat:
                if any(stat[k] != v for k, v in counts.items()):
                    stat.write(counts)
            else:
                self.create(dict(counts, activity_id=act.id, period_start=start))


class SgiActivityWeekCounts(models.Model):
    _inherit = 'sgi.process.activity'

    def _sgi_week_counts(self, start, workdays=None):
        """Conteos de la semana que empieza el lunes ``start``. ``workdays``
        (57.17.0, G-016): días hábiles de la corrida, calculados una vez."""
        self.ensure_one()
        env = self.env
        # 57.16.0 (G-007 c): la semana se corta a medianoche de México, no UTC.
        week_start = sgi_local_datetime_utc(env, start, 0)
        week_end = sgi_local_datetime_utc(env, start + timedelta(days=7), 0)
        week_end_day = start + timedelta(days=7)
        Activity = env['sgi.process.activity']
        counts = dict(applicable_count=0, done_count=0, complete_count=0,
                      timed_count=0, on_time_count=0, late_open_count=0)
        output = self._sgi_output_deliverable()
        out_model = output.odoo_model_id.model if output else None
        Out = env[out_model].sudo() if out_model and out_model in env else None
        out_date = output.measure_date_field or 'create_date' if output else None
        if Out is not None and out_date not in Out._fields:
            out_date = 'create_date'
        out_domain = sgi_safe_domain(output.measure_domain) if output else []
        if Out is not None:
            # 57.16.0 (H-006): solo la empresa del SGI.
            out_domain = out_domain + Activity._sgi_measure_company_domain(Out)
        # Hechas y completas.
        done = Out.browse()
        if Out is not None:
            done = Out.search(out_domain + [(out_date, '>=', week_start), (out_date, '<', week_end)])
            counts['done_count'] = len(done)
            complete_domain = sgi_safe_domain(output.complete_domain)
            counts['complete_count'] = Out.search_count(
                [('id', 'in', done.ids)] + complete_domain) if complete_domain else len(done)
        # Entradas aplicables, a tiempo y vencidas.
        lines = self.input_ids.filtered(lambda l: l.deliverable_id.odoo_model_id)
        for line in lines:
            deliverable = line.deliverable_id
            In = env[deliverable.odoo_model_id.model].sudo() \
                if deliverable.odoo_model_id.model in env else None
            if In is None:
                continue
            in_date = deliverable.measure_date_field or 'create_date'
            if in_date not in In._fields:
                in_date = 'create_date'
            base = line._sgi_applicable_domain() + Activity._sgi_measure_company_domain(In)
            counts['applicable_count'] += In.search_count(
                base + [(in_date, '>=', week_start), (in_date, '<', week_end)])
            if not line._sgi_has_deadline() or Out is None:
                continue
            same = In._name == Out._name
            if not same and not line.match_path:
                continue
            # Vencidas abiertas: aplicables de los últimos 90 días sin salida
            # y con el plazo vencido al cierre de la semana.
            candidates = In.search(base + [
                (in_date, '>=', week_end - timedelta(days=self.env['sgi.activity.week.stat']._LOOKBACK_DAYS)),
                (in_date, '<', week_end)])
            overdue_ids = []
            for rec in candidates:
                due = line._sgi_due(rec, in_date, workdays)
                if due is not None and due < week_end_day:
                    overdue_ids.append(rec.id)
            if overdue_ids:
                # 57.17.0 (G-016): «tiene salida» en UNA consulta por lote,
                # no una por registro.
                if same:
                    delivered = set(Out.search(out_domain + [
                        ('id', 'in', overdue_ids), (out_date, '<', week_end)]).ids)
                else:
                    delivered = set(Out.search(out_domain + [
                        (line.match_path, 'in', overdue_ids), (out_date, '<', week_end)]
                    ).mapped(line.match_path).ids)
                counts['late_open_count'] += len(set(overdue_ids) - delivered)
            # A tiempo: salidas de la semana contra su entrada. Si la salida
            # apunta a varias entradas (la revisión por la dirección y sus
            # auditorías), manda la última: la salida no podía hacerse antes.
            for rec in done:
                if same:
                    source = rec
                else:
                    sources = rec.mapped(line.match_path).filtered(in_date)
                    source = sources.sorted(in_date)[-1:] if sources else sources
                if not source or not source[in_date]:
                    continue
                due = line._sgi_due(source, in_date, workdays)
                if due is None:
                    continue
                counts['timed_count'] += 1
                if rec[out_date] and sgi_local_date(env, rec[out_date]) <= due:
                    counts['on_time_count'] += 1
            break   # la primera entrada con plazo que se liga es la que manda
        # Periódicas: a tiempo si se hizo antes del vencimiento del periodo.
        if Out is not None and not counts['timed_count'] and (
                self.due_weekday or self.due_business_day or (self.due_month and self.due_day)):
            for rec in done:
                day = sgi_local_date(env, rec[out_date])
                due = self._sgi_periodic_due(day)
                if due is None:
                    continue
                counts['timed_count'] += 1
                if day <= due:
                    counts['on_time_count'] += 1
        return counts

    @api.model
    def cron_measure_activities(self):
        res = super().cron_measure_activities()

        def _weekly():
            measurable = self.search([('measure_method', '=', 'entregable')]) | self.search(
                [('input_ids.deliverable_id.odoo_model_id', '!=', False)])
            self.env['sgi.activity.week.stat']._sgi_compute(measurable)
        # 57.16.0 (G-002): en su savepoint.
        Cron = self.env['sgi.cron']
        Cron._sgi_step("medición semanal de cumplimiento", _weekly)
        # 57.17.0 (G-015): con el semáforo nuevo, las cifras guardadas de
        # «Mi procedimiento» de cada puesto (Mi equipo las lee de ahí).
        Cron._sgi_step("cifras de Mi procedimiento por puesto",
                       lambda: self.env['hr.job']._sgi_mp_refresh_all())
        # 57.104.0: con la medición fresca, los faltantes que dependen de ella
        # («Evidencia que no aparece», «Atribución débil»). Solo escribe los
        # que cambiaron.
        Cron._sgi_step("faltantes de medición",
                       lambda: self.search([('measure_model_id', '!=', False)])._sgi_refresh_spec_gaps())
        return res
