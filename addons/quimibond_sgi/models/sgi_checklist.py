# -*- coding: utf-8 -*-
"""Checklists de planta y de unidades como hojas de mantenimiento (56.21.0).

S5.15 / S5.16 (checklist diario de planta) y S5.20 (checklist semanal de
unidades) dejan el papel: MAST o Mantenimiento define la plantilla (qué se
revisa y en qué equipos o unidades) y el cron diario crea una solicitud de
mantenimiento preventivo por equipo, con los puntos a revisar como hoja
dentro de la solicitud. Cada punto se marca «Bien», «Falla» o «No aplica»;
de las fallas sale una solicitud correctiva con un botón.
"""
from datetime import datetime, time

from odoo import api, fields, models
from odoo.exceptions import UserError

_ANSWERS = [('ok', "Bien"), ('falla', "Falla"), ('na', "No aplica")]


class SgiChecklistTemplate(models.Model):
    _name = 'sgi.checklist.template'
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
    user_id = fields.Many2one('res.users', string="Responsable de llenarlo")
    item_ids = fields.One2many('sgi.checklist.template.item', 'template_id', string="Puntos a revisar")
    active = fields.Boolean(default=True)
    last_run = fields.Date(string="Última generación", readonly=True)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)

    def _sgi_due_today(self, day):
        self.ensure_one()
        if day.weekday() >= 5:
            return False
        return self.frequency == 'diaria' or day.weekday() == 0

    def _sgi_generate(self, day=None):
        """Una solicitud preventiva por equipo, con la hoja de puntos. No
        duplica: si ya existe la del día para ese equipo, no crea otra."""
        day = day or fields.Date.context_today(self)
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
                created |= Request.create({
                    'name': "%s%s — %s — %s" % (
                        "%s " % template.code if template.code else '', template.name, equipment.name, day),
                    'equipment_id': equipment.id,
                    'maintenance_type': 'preventive',
                    'schedule_date': datetime.combine(day, time(8, 0)),
                    'maintenance_team_id': (template.maintenance_team_id or equipment.maintenance_team_id).id,
                    'user_id': (template.user_id or equipment.technician_user_id).id,
                    'sgi_checklist_template_id': template.id,
                    'sgi_checklist_date': day,
                    'sgi_checklist_line_ids': [(0, 0, {'sequence': item.sequence, 'name': item.name,
                                                       'hint': item.hint})
                                               for item in template.item_ids],
                })
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
        day = fields.Date.context_today(self)
        templates = self.search([]).filtered(lambda t: t._sgi_due_today(day))
        return len(templates._sgi_generate(day))


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
