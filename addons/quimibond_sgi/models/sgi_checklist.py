# -*- coding: utf-8 -*-
"""Checklists de planta y de unidades como hojas de mantenimiento (56.21.0).

S5.15 / S5.16 (checklist diario de planta) y S5.20 (checklist semanal de
unidades) dejan el papel: MAST o Mantenimiento define la plantilla (qué se
revisa y en qué equipos o unidades) y el cron diario crea una solicitud de
mantenimiento preventivo por equipo, con los puntos a revisar como hoja
dentro de la solicitud. Cada punto se marca «Bien», «Falla» o «No aplica»;
de las fallas sale una solicitud correctiva con un botón.

56.22.0: los llenan electromecánicos y choferes sin usuario de Odoo, desde una
tableta compartida (un usuario por tableta, responsable de la plantilla). Al
terminar, «Terminar checklist» pide quién lo llenó (empleado) y su PIN de
empleado (el mismo del quiosco de asistencia y del piso de producción): la
hoja guarda el empleado y la hora, y así se mide por persona.

57.15.0 (G-007, G-022, decisión 14 de la tanda 2): las hojas están listas a
las 05:30 de México (el cron se mueve a esa hora) con ``schedule_date`` a las
08:00 hora local (antes 08:00 UTC = 02:00 en México). Los festivos del
calendario del SGI no generan hoja; la semanal se genera el primer día hábil
de la semana en que corra el cron (si el lunes es festivo o el cron no corrió,
el martes), una sola vez por semana. Una plantilla sin equipos avisa al Jefe
MAST en vez de no generar nada en silencio.

57.94.0 (U-01, I-03): la validación de PIN vive en ``sgi.pin`` (la comparte SGI
en planta); la hoja firmada guarda la tableta y la hora, y sus respuestas ya no
se cambian. «Lo llenó» y «Terminado el» solo los escribe el sistema. Botón
«Marcar el resto como Bien» y respuesta de un toque por punto.
"""
import logging
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_is_business_day, sgi_local_datetime_utc, sgi_today
from .sgi_guard import sgi_require_system

_logger = logging.getLogger(__name__)

_ANSWERS = [('ok', "Bien"), ('falla', "Falla"), ('na', "No aplica")]


class SgiChecklistTemplate(models.Model):
    """Plantilla de checklist de planta o de unidades: puntos, equipos, frecuencia y quién la llena.
    El cron diario genera las hojas del día."""
    _name = 'sgi.checklist.template'
    # 57.15.0 (G-022): lleva actividades para el aviso «Checklist sin equipos».
    # 57.67.0: ``hr.mixin``: empleados que la llenan (Many2many a hr.employee)
    # sin ser de RH (Odoo 19).
    _inherit = ['mail.thread', 'mail.activity.mixin', 'hr.mixin']
    _description = "Plantilla de checklist de mantenimiento (planta o unidades)"
    _order = 'code, name'

    name = fields.Char(string="Checklist", required=True)
    code = fields.Char(string="Clave", help="Actividad o formato que sustituye: S5.15, S5.16, S5.20…")
    frequency = fields.Selection([
        ('diaria', "Diaria (lunes a viernes)"),
        ('semanal', "Semanal (lunes)"),
    ], string="Frecuencia", required=True, default='diaria',
        help="Diaria (de lunes a viernes) o semanal (los lunes). El cron genera las hojas a esa frecuencia.")
    equipment_ids = fields.Many2many('maintenance.equipment', string="Equipos o unidades", required=True,
                                     help="Equipos o unidades que se revisan con esta plantilla. Cada uno "
                                          "tiene su hoja.")
    maintenance_team_id = fields.Many2one('maintenance.team', string="Equipo de mantenimiento",
                                          help="Equipo de mantenimiento al que llegan las hojas y las "
                                               "correctivas.")
    user_id = fields.Many2one('res.users', string="Responsable de llenarlo",
                              help="Usuario que ve las hojas: el de la tableta compartida o el jefe del área.")
    employee_ids = fields.Many2many(
        'hr.employee', 'sgi_checklist_template_employee_rel', 'template_id', 'employee_id',
        string="Quién lo llena", help="Electromecánicos o choferes que pueden firmar la hoja. Vacío: cualquiera.")
    item_ids = fields.One2many('sgi.checklist.template.item', 'template_id', string="Puntos a revisar")
    active = fields.Boolean(default=True)
    last_run = fields.Date(string="Última generación", readonly=True,
                           help="Último día en que se generaron hojas de esta plantilla.")
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)

    def _sgi_due_today(self, day):
        self.ensure_one()
        if not sgi_is_business_day(self.env, day, self.company_id):
            return False
        if self.frequency == 'diaria':
            return True
        # Semanal: el primer día hábil de la semana en que corra el cron; una
        # vez por semana (idempotente aunque el lunes haya fallado).
        monday = day - timedelta(days=day.weekday())
        return not (self.last_run and monday <= self.last_run <= day)

    def _sgi_generate(self, day=None):
        """Una solicitud preventiva por equipo, con la hoja de puntos. No
        duplica: si ya existe la del día para ese equipo, no crea otra."""
        day = day or sgi_today(self.env)
        Request = self.env['maintenance.request'].sudo()
        created = Request
        for template in self:
            if not template.item_ids:
                continue
            for equipment in template.equipment_ids:
                exists = Request.search_count([
                    ('sgi_checklist_template_id', '=', template.id), ('equipment_id', '=', equipment.id),
                    ('sgi_checklist_date', '=', day)])
                if exists:
                    continue
                vals = {
                    'name': "%s%s — %s — %s" % (
                        "%s " % template.code if template.code else '', template.name, equipment.name, day),
                    'equipment_id': equipment.id,
                    'maintenance_type': 'preventive',
                    'schedule_date': sgi_local_datetime_utc(self.env, day, 8, 0, template.company_id),
                    'user_id': (template.user_id or equipment.technician_user_id).id,
                    'sgi_checklist_template_id': template.id,
                    'sgi_checklist_date': day,
                    'sgi_checklist_line_ids': [(0, 0, {'sequence': item.sequence, 'name': item.name,
                                                       'hint': item.hint})
                                               for item in template.item_ids],
                }
                # 57.13.0: el equipo de mantenimiento es obligatorio en Odoo 19.
                # Mandar False cuando ni la plantilla ni el equipo lo tienen
                # anulaba el default de Odoo (el primer equipo de la empresa) y
                # la solicitud no se creaba (NOT NULL).
                team = template.maintenance_team_id or equipment.maintenance_team_id
                if team:
                    vals['maintenance_team_id'] = team.id
                created |= Request.create(vals)
            template.last_run = day
        return created

    def action_generate_today(self):
        requests = self._sgi_generate()
        return {'type': 'ir.actions.act_window', 'name': "Checklists generados",
                'res_model': 'maintenance.request', 'view_mode': 'list,form',
                'domain': [('id', 'in', requests.ids)]}

    @api.model
    def cron_generate(self):
        """Cron diario: las plantillas que tocan hoy."""
        sgi_require_system(self.env)  # 57.91.0 (K-06)
        day = sgi_today(self.env)
        templates = self.search([]).filtered(lambda t: t._sgi_due_today(day))
        self._sgi_warn_without_equipment(templates)
        return len(templates._sgi_generate(day))

    @api.model
    def _sgi_warn_without_equipment(self, templates):
        """G-022: una plantilla activa sin equipos no genera hojas; el Jefe MAST
        recibe un aviso por plantilla (uno por episodio) y el aviso se cierra
        solo cuando la plantilla ya tiene equipos."""
        Cron = self.env['sgi.cron']._sgi_new_run()
        manager_id = Cron._sgi_manager_user_id()
        empty = templates.filtered(lambda t: not t.equipment_ids)
        for template in empty:
            _logger.warning("SGI checklist: la plantilla %s no tiene equipos; no genera hojas.",
                            template.display_name)
            Cron._sgi_step(
                "aviso de plantilla sin equipos %s" % template.id,
                lambda template=template: Cron._sgi_schedule(
                    template, "Checklist sin equipos: %s" % template.name,
                    "La plantilla «%s» no tiene equipos o unidades: el cron no genera "
                    "hojas. Agregue los equipos en la plantilla." % template.name,
                    manager_id, date_deadline=sgi_today(self.env),
                    key='checklist_sin_equipos'))
        # Solo las plantillas que tocaban hoy se revisan: las demás conservan
        # su aviso hasta su día.
        stale = self.env['mail.activity'].sudo().with_context(active_test=False).search([
            ('res_model', '=', self._name), ('res_id', 'in', (templates - empty).ids),
            ('sgi_cron_key', '=', 'checklist_sin_equipos'), ('sgi_episode_closed', '=', False)])
        Cron._sgi_close_activities(stale, "la plantilla ya tiene equipos")
        return empty


class SgiChecklistTemplateItem(models.Model):
    """Punto a revisar de una plantilla de checklist."""
    _name = 'sgi.checklist.template.item'
    _description = "Punto a revisar de una plantilla de checklist"
    _order = 'template_id, sequence, id'

    template_id = fields.Many2one('sgi.checklist.template', required=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    name = fields.Char(string="Qué se revisa", required=True)
    hint = fields.Char(string="Cómo / criterio", help="Ej. «Presión entre 6 y 8 bar», «Llantas sin cortes».")


class SgiChecklistLine(models.Model):
    """Punto revisado en una hoja de checklist de mantenimiento (``maintenance.request``), con su
    respuesta y, si falla, la solicitud correctiva."""
    _name = 'sgi.checklist.line'
    _description = "Punto revisado en una hoja de mantenimiento"
    _order = 'request_id, sequence, id'

    request_id = fields.Many2one('maintenance.request', required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    name = fields.Char(string="Qué se revisa", required=True)
    hint = fields.Char(string="Criterio")
    answer = fields.Selection(_ANSWERS, string="Resultado")
    note = fields.Char(string="Observación")
    corrective_request_id = fields.Many2one('maintenance.request', string="Correctivo", readonly=True,
                                            copy=False)

    # 57.94.0 (U-01, I-03): una hoja firmada es evidencia; sus respuestas no se
    # cambian (la vista ya lo decía, el servidor no). «Crear correctivos» sí
    # escribe corrective_request_id después de firmar.
    # ``request_id``: un renglón no sale de una hoja firmada ni entra a una.
    _SGI_SIGNED_LOCKED = frozenset({'answer', 'note', 'name', 'hint', 'sequence', 'request_id'})

    def _sgi_check_signed_sheet(self, requests):
        # sudo al leer la hoja: la regla nativa de Mantenimiento («Users are
        # allowed to access their own maintenance requests») daría AccessError
        # a un usuario interno que no sigue la solicitud, en vez del aviso.
        if not self.env.su and any(req.sgi_checklist_employee_id for req in requests.sudo()):
            raise UserError("La hoja ya está firmada: sus respuestas son evidencia y no se cambian.")

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        # Después del alta: también cuenta un ``default_request_id`` del contexto.
        self._sgi_check_signed_sheet(lines.sudo().request_id)
        return lines

    def write(self, vals):
        if self._SGI_SIGNED_LOCKED & set(vals):
            self._sgi_check_signed_sheet(self.sudo().request_id)
            if vals.get('request_id'):
                self._sgi_check_signed_sheet(self.env['maintenance.request'].browse(vals['request_id']))
        return super().write(vals)

    def action_sgi_answer(self):
        """I-03: un toque por punto desde la tarjeta (Bien, Falla o No aplica).
        La respuesta viene en el contexto del botón y se valida aquí."""
        answer = self.env.context.get('sgi_answer')
        if answer not in dict(_ANSWERS):
            raise UserError("Respuesta no válida para el checklist.")
        self.write({'answer': answer})
        return True


class MaintenanceRequestChecklist(models.Model):
    # 57.94.0 (U-01): la hoja firmada con PIN guarda la tableta y la hora.
    # 57.94.1: con varias clases en _inherit, _name es obligatorio; sin él la
    # clase no extendía maintenance.request y el build de main no cargaba.
    _name = 'maintenance.request'
    _inherit = ['maintenance.request', 'sgi.pin.signature.mixin']

    # 57.94.0: copy=False: duplicar una hoja (o la recurrencia de un
    # preventivo) da una solicitud normal, no otra hoja de checklist.
    sgi_checklist_template_id = fields.Many2one('sgi.checklist.template', string="Checklist SGI",
                                                readonly=True, index=True, copy=False,
                                                help="Plantilla de la que salió esta hoja de checklist.")
    sgi_checklist_date = fields.Date(string="Día del checklist", readonly=True, index=True, copy=False,
                                     help="Día al que corresponde la hoja de checklist.")
    sgi_checklist_line_ids = fields.One2many('sgi.checklist.line', 'request_id', string="Hoja de checklist")
    sgi_checklist_employee_id = fields.Many2one(
        'hr.employee', string="Lo llenó", readonly=True, index=True, tracking=True, copy=False,
        help="Empleado que llenó la hoja (se registra al terminarla, con su PIN si está encendido).")
    sgi_checklist_done_at = fields.Datetime(string="Terminado el", readonly=True, copy=False,
                                            help="Fecha y hora en que se terminó la hoja.")
    sgi_checklist_state = fields.Selection([
        ('pendiente', "Pendiente"),
        ('completo', "Completo"),
        ('con_fallas', "Con fallas"),
    ], string="Checklist", compute='_compute_sgi_checklist_state', store=True,
        help="Pendiente, completo o con fallas según las respuestas de la hoja. Se calcula solo.")

    @api.depends('sgi_checklist_line_ids.answer')
    def _compute_sgi_checklist_state(self):
        for req in self:
            lines = req.sgi_checklist_line_ids
            if not lines:
                req.sgi_checklist_state = False
            elif any(l.answer == 'falla' for l in lines):
                req.sgi_checklist_state = 'con_fallas'
            elif all(lines.mapped('answer')):
                req.sgi_checklist_state = 'completo'
            else:
                req.sgi_checklist_state = 'pendiente'

    def action_sgi_checklist_finish(self):
        """Abre la firma de quien llenó la hoja (empleado + PIN)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "¿Quién llenó el checklist?",
            'res_model': 'sgi.checklist.finish', 'view_mode': 'form', 'target': 'new',
            'context': {'default_request_id': self.id},
        }

    _sgi_pin_employee_field = 'sgi_checklist_employee_id'

    def _sgi_pin_employee(self):
        return self.sgi_checklist_employee_id

    # 57.94.0 (U-01): quién llenó la hoja es la firma. El campo es readonly
    # solo en la vista; base.group_user escribe maintenance.request (ACL de
    # Mantenimiento), así que la cuenta de una tableta (o cualquiera) podía
    # poner a otro empleado como «Lo llenó» con un write por RPC. Solo el
    # sistema: el asistente «Terminar checklist» y SGI en planta escriben con
    # sudo después de validar el PIN.
    # La plantilla y el día también: solo los pone ``_sgi_generate`` (con
    # sudo); con ellos cualquiera fabricaba una «hoja» que sale en la tableta.
    _SGI_SIGNER_FIELDS = frozenset({'sgi_checklist_employee_id', 'sgi_checklist_done_at',
                                    'sgi_checklist_template_id', 'sgi_checklist_date'})

    def _sgi_check_signer_fields(self, keys):
        if self._SGI_SIGNER_FIELDS & set(keys) and not self.env.su:
            raise UserError("Quién llenó la hoja lo registra el sistema al terminarla (con su PIN, "
                            "en «Terminar checklist» o en SGI en planta); la hoja la genera el sistema "
                            "desde su plantilla.")

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            self._sgi_check_signer_fields(vals)
        requests = super().create(vals_list)
        # Después del alta: un ``default_*`` del contexto no pasa por vals_list.
        if not self.env.su and any(
                req.sgi_checklist_employee_id or req.sgi_checklist_done_at
                or req.sgi_checklist_template_id or req.sgi_checklist_date for req in requests.sudo()):
            self._sgi_check_signer_fields(self._SGI_SIGNER_FIELDS)
        return requests

    def write(self, vals):
        self._sgi_check_signer_fields(vals)
        return super().write(vals)

    def _sgi_checklist_precheck(self):
        self.ensure_one()
        # Solo hojas de checklist: sin esto el asistente servía para probar PIN
        # sobre cualquier solicitud de mantenimiento (sin puntos = «completa»).
        if not self.sgi_checklist_template_id or not self.sgi_checklist_line_ids:
            raise UserError("Esta solicitud no es una hoja de checklist.")
        if self.sgi_checklist_employee_id:
            raise UserError("Esta hoja ya la firmó %s." % self.sgi_checklist_employee_id.name)
        missing = self.sgi_checklist_line_ids.filtered(lambda l: not l.answer)
        if missing:
            raise UserError("Faltan %d punto(s) por marcar: %s." % (
                len(missing), ", ".join(missing.mapped('name')[:5])))

    def _sgi_checklist_sign(self, employee, signed_with_pin, tablet=False):
        """57.94.0 (U-01): firma común del asistente «Terminar checklist» y de
        SGI en planta. Quien llama ya validó al empleado y su PIN."""
        self.ensure_one()
        self._sgi_checklist_precheck()
        now = fields.Datetime.now()
        vals = {'sgi_checklist_employee_id': employee.id, 'sgi_checklist_done_at': now}
        if signed_with_pin:
            vals.update({'sgi_pin_signed_at': now, 'sgi_pin_tablet_id': tablet.id if tablet else False})
        self.sudo().write(vals)
        if not signed_with_pin:
            how = " (sin PIN registrado)"
        elif tablet:
            how = " con su PIN en la tableta %s" % tablet.name
        else:
            how = " con su PIN"
        self.sudo().message_post(body="Checklist llenado por %s%s." % (employee.name, how))
        return True

    def _sgi_checklist_fill_ok(self):
        """I-03: los puntos sin respuesta pasan a «Bien»; lo contestado no se toca."""
        for req in self:
            if req.sgi_checklist_employee_id:
                raise UserError("Esta hoja ya está firmada; sus respuestas no se cambian.")
            req.sgi_checklist_line_ids.filtered(lambda l: not l.answer).write({'answer': 'ok'})
        return True

    def action_sgi_checklist_all_ok(self):
        """I-03: botón «Marcar el resto como Bien» de la hoja."""
        return self._sgi_checklist_fill_ok()

    def action_sgi_create_correctives(self):
        """Una solicitud correctiva por punto con falla (una sola vez)."""
        for req in self:
            failing = req.sgi_checklist_line_ids.filtered(
                lambda l: l.answer == 'falla' and not l.corrective_request_id)
            if not failing:
                raise UserError("No hay fallas sin correctivo en esta hoja.")
            for line in failing:
                line.corrective_request_id = self.create({
                    'name': "Correctivo: %s — %s" % (line.name, req.equipment_id.name or req.name),
                    'equipment_id': req.equipment_id.id,
                    'maintenance_type': 'corrective',
                    'maintenance_team_id': req.maintenance_team_id.id,
                    'description': "Falla en el checklist %s: %s%s" % (
                        req.name, line.name, " — %s" % line.note if line.note else ''),
                })
        return True


class SgiChecklistFinish(models.TransientModel):
    """Asistente para terminar una hoja de checklist: quién la llenó y, si está encendido el
    parámetro, su PIN de empleado."""
    _name = 'sgi.checklist.finish'
    _description = "Terminar checklist: quién lo llenó"

    request_id = fields.Many2one('maintenance.request', required=True, ondelete='cascade',
                                 help="Hoja de checklist que se termina.")
    allowed_employee_ids = fields.Many2many('hr.employee', compute='_compute_allowed_employee_ids',
                                            help="Personas que pueden firmar esta hoja: las de «Quién lo "
                                                 "llena» en la plantilla.")
    employee_id = fields.Many2one('hr.employee', string="¿Quién lo llenó?", required=True,
                                  domain="allowed_employee_ids and [('id', 'in', allowed_employee_ids)] or []",
                                  help="Elija a la persona que llenó la hoja.")
    pin = fields.Char(string="PIN del empleado")

    @api.depends('request_id')
    def _compute_allowed_employee_ids(self):
        for wiz in self:
            wiz.allowed_employee_ids = wiz.request_id.sgi_checklist_template_id.employee_ids

    @api.model
    def _sgi_pin_required(self):
        """57.18.0 (I-005, D-08): ``quimibond_sgi.checklist_pin_required``.
        Apagado por default: RH captura los PIN antes de encenderlo."""
        value = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.checklist_pin_required', '') or ''
        return value.strip().lower() in ('1', 'true', 'yes', 'si', 'sí')

    def action_confirm(self):
        self.ensure_one()
        req = self.request_id
        req._sgi_checklist_precheck()
        # 57.94.0 (U-01): el PIN se valida en sgi.pin, igual que en la tableta.
        allowed = req.sgi_checklist_template_id.employee_ids
        with_pin = self.env['sgi.pin']._sgi_check_pin(
            self.employee_id, self.pin, required=self._sgi_pin_required(),
            allowed=allowed if allowed else None, purpose="firmar este checklist")
        # Si quien firma está en la cuenta de una tableta de planta, la hoja
        # dice en cuál.
        tablet = self.env['sgi.floor.tablet'].sudo().search(
            [('user_id', '=', self.env.uid), ('active', '=', True)], limit=1)
        req._sgi_checklist_sign(self.employee_id, with_pin, tablet=tablet if with_pin else False)
        return {'type': 'ir.actions.act_window_close'}
