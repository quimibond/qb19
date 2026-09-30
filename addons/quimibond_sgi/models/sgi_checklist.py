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
"""
import logging
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_is_business_day, sgi_local_datetime_utc, sgi_today

_logger = logging.getLogger(__name__)

_ANSWERS = [('ok', "Bien"), ('falla', "Falla"), ('na', "No aplica")]


class SgiChecklistTemplate(models.Model):
    _name = 'sgi.checklist.template'
    # 57.15.0 (G-022): lleva actividades para el aviso «Checklist sin equipos».
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _description = "Plantilla de checklist de mantenimiento (planta o unidades)"
    _order = 'code, name'

    name = fields.Char(string="Checklist", required=True)
    code = fields.Char(string="Clave", help="Actividad o formato que sustituye: S5.15, S5.16, S5.20…")
    frequency = fields.Selection([
        ('diaria', "Diaria (lunes a viernes)"),
        ('semanal', "Semanal (lunes)"),
    ], string="Frecuencia", required=True, default='diaria')
    equipment_ids = fields.Many2many('maintenance.equipment', string="Equipos o unidades", required=True)
    maintenance_team_id = fields.Many2one('maintenance.team', string="Equipo de mantenimiento")
    user_id = fields.Many2one('res.users', string="Responsable de llenarlo",
                              help="Usuario que ve las hojas: el de la tableta compartida o el jefe del área.")
    employee_ids = fields.Many2many(
        'hr.employee', 'sgi_checklist_template_employee_rel', 'template_id', 'employee_id',
        string="Quién lo llena", help="Electromecánicos o choferes que pueden firmar la hoja. Vacío: cualquiera.")
    item_ids = fields.One2many('sgi.checklist.template.item', 'template_id', string="Puntos a revisar")
    active = fields.Boolean(default=True)
    last_run = fields.Date(string="Última generación", readonly=True)
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
                    "hojas. Agrega los equipos en la plantilla." % template.name,
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
    _name = 'sgi.checklist.template.item'
    _description = "Punto a revisar de una plantilla de checklist"
    _order = 'template_id, sequence, id'

    template_id = fields.Many2one('sgi.checklist.template', required=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    name = fields.Char(string="Qué se revisa", required=True)
    hint = fields.Char(string="Cómo / criterio", help="Ej. «Presión entre 6 y 8 bar», «Llantas sin cortes».")


class SgiChecklistLine(models.Model):
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


class MaintenanceRequestChecklist(models.Model):
    _inherit = 'maintenance.request'

    sgi_checklist_template_id = fields.Many2one('sgi.checklist.template', string="Checklist SGI",
                                                readonly=True, index=True)
    sgi_checklist_date = fields.Date(string="Día del checklist", readonly=True, index=True)
    sgi_checklist_line_ids = fields.One2many('sgi.checklist.line', 'request_id', string="Hoja de checklist")
    sgi_checklist_employee_id = fields.Many2one(
        'hr.employee', string="Lo llenó", readonly=True, index=True, tracking=True, copy=False)
    sgi_checklist_done_at = fields.Datetime(string="Terminado el", readonly=True, copy=False)
    sgi_checklist_state = fields.Selection([
        ('pendiente', "Pendiente"),
        ('completo', "Completo"),
        ('con_fallas', "Con fallas"),
    ], string="Checklist", compute='_compute_sgi_checklist_state', store=True)

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
    _name = 'sgi.checklist.finish'
    _description = "Terminar checklist: quién lo llenó"

    request_id = fields.Many2one('maintenance.request', required=True, ondelete='cascade')
    allowed_employee_ids = fields.Many2many('hr.employee', compute='_compute_allowed_employee_ids')
    employee_id = fields.Many2one('hr.employee', string="¿Quién lo llenó?", required=True,
                                  domain="allowed_employee_ids and [('id', 'in', allowed_employee_ids)] or []")
    pin = fields.Char(string="PIN del empleado")

    @api.depends('request_id')
    def _compute_allowed_employee_ids(self):
        for wiz in self:
            wiz.allowed_employee_ids = wiz.request_id.sgi_checklist_template_id.employee_ids

    def action_confirm(self):
        self.ensure_one()
        req = self.request_id
        if req.sgi_checklist_employee_id:
            raise UserError("Esta hoja ya la firmó %s." % req.sgi_checklist_employee_id.name)
        missing = req.sgi_checklist_line_ids.filtered(lambda l: not l.answer)
        if missing:
            raise UserError("Faltan %d punto(s) por marcar: %s." % (
                len(missing), ", ".join(missing.mapped('name')[:5])))
        allowed = req.sgi_checklist_template_id.employee_ids
        if allowed and self.employee_id not in allowed:
            raise UserError("%s no está en la lista de quién llena este checklist." % self.employee_id.name)
        # sudo: el PIN es un campo de RH; el usuario de la tableta no lo lee.
        real_pin = self.employee_id.sudo().pin
        if real_pin and (self.pin or '') != real_pin:
            raise UserError("PIN incorrecto para %s." % self.employee_id.name)
        req.sudo().write({'sgi_checklist_employee_id': self.employee_id.id,
                          'sgi_checklist_done_at': fields.Datetime.now()})
        req.message_post(body="Checklist llenado por %s%s." % (
            self.employee_id.name, "" if real_pin else " (sin PIN registrado)"))
        return {'type': 'ir.actions.act_window_close'}
