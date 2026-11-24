# -*- coding: utf-8 -*-
"""Cuadrillas de turno y calendario de festivos (1.24.0).

**Cuadrillas.** RH pidió agrupar los turnos por área (correo de Miguel Medina
del 23-nov-2026): en cada área la gente que trabaja el mismo turno rota junta
cada 15 días. Una cuadrilla guarda el área, el turno vigente, el orden en que
rota («3,4» en Acabado; «3,5,6» en Tejido) y a sus integrantes. Cambiar el
turno de la cuadrilla, o el botón «Rotar», escribe el turno en la ficha de cada
integrante (``hr.employee.qb_turno``), que es lo que lee la lista de
incidencias. Quien tiene turno fijo queda fuera de la rotación.

**Festivos.** Los días de descanso obligatorio del calendario de RH (ley y
contrato colectivo). La lista de incidencias no los marca como falta y anota a
quien los trabaja, para pagarlos como dice el art. 75 de la LFT."""
from odoo import _, api, fields, models
from odoo.exceptions import UserError

from .incidencias_calculo import TURNOS_NOMBRE


class QbNominaCuadrilla(models.Model):
    _name = 'qb.nomina.cuadrilla'
    _description = 'Cuadrilla de turno'
    _inherit = ['mail.thread']
    _order = 'department_id, name'

    name = fields.Char(string="Cuadrilla", required=True, tracking=True)
    department_id = fields.Many2one('hr.department', string="Área", tracking=True)
    turno = fields.Selection(TURNOS_NOMBRE, string="Turno vigente", required=True, tracking=True)
    rotacion = fields.Char(
        string="Rotación", tracking=True,
        help="Turnos por los que rota la cuadrilla, en orden y separados por coma (por ejemplo 3,4 o 3,5,6). "
             "«Rotar» pasa al siguiente. Vacío: no rota.")
    vigente_desde = fields.Date(string="Turno vigente desde", tracking=True)
    he_fijas = fields.Boolean(
        string="Paga las 6 HE del turno 4", tracking=True,
        help="Cuando la cuadrilla trabaja el turno 4 y completa la noche del jueves, la lista de incidencias "
             "propone 6 horas extra (48 horas de noche contra las 42 de la jornada nocturna legal). RH, 24-nov-2026: "
             "solo en Inspección, Acabado, Tintorería y Mantenimiento.")
    employee_ids = fields.One2many('hr.employee', 'qb_cuadrilla_id', string="Integrantes")
    n_personas = fields.Integer(string="Personas", compute='_compute_n_personas')
    company_id = fields.Many2one('res.company', string="Empresa", required=True,
                                 default=lambda self: self.env.company)
    active = fields.Boolean(default=True)
    notas = fields.Text(string="Notas")

    @api.depends('employee_ids')
    def _compute_n_personas(self):
        for rec in self:
            rec.n_personas = len(rec.employee_ids)

    def _ciclo(self):
        self.ensure_one()
        validos = dict(TURNOS_NOMBRE)
        return [t.strip() for t in (self.rotacion or '').split(',') if t.strip() in validos]

    @api.constrains('rotacion')
    def _check_rotacion(self):
        validos = dict(TURNOS_NOMBRE)
        for rec in self:
            malos = [t.strip() for t in (rec.rotacion or '').split(',') if t.strip() and t.strip() not in validos]
            if malos:
                raise UserError(_("La rotación lleva turnos que no existen: %s", ', '.join(malos)))

    def _aplicar_turno(self):
        """Escribe el turno de la cuadrilla en la ficha de sus integrantes que no tienen turno fijo."""
        for rec in self:
            gente = rec.employee_ids.sudo().filtered(lambda e: not e.qb_turno_fijo and e.qb_turno != rec.turno)
            if gente:
                gente.write({'qb_turno': rec.turno})

    @api.model_create_multi
    def create(self, vals_list):
        recs = super().create(vals_list)
        recs._aplicar_turno()
        return recs

    def write(self, vals):
        res = super().write(vals)
        if 'turno' in vals or 'employee_ids' in vals:
            self._aplicar_turno()
        return res

    def action_rotar(self):
        """Pasa cada cuadrilla al siguiente turno de su rotación."""
        for rec in self:
            ciclo = rec._ciclo()
            if len(ciclo) < 2:
                raise UserError(_("La cuadrilla «%s» no tiene rotación: capture los turnos, por ejemplo 3,4.",
                                  rec.name))
            siguiente = ciclo[(ciclo.index(rec.turno) + 1) % len(ciclo)] if rec.turno in ciclo else ciclo[0]
            rec.write({'turno': siguiente, 'vigente_desde': fields.Date.context_today(rec)})
        return True


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    qb_cuadrilla_id = fields.Many2one(
        'qb.nomina.cuadrilla', string="Cuadrilla", groups='hr.group_hr_user', index=True,
        help="Cuadrilla de turno del área. El turno de la cuadrilla se copia al «Turno (RH)» de la persona, "
             "salvo que tenga turno fijo.")

    def _qb_turno_de_cuadrilla(self, vals):
        if vals.get('qb_cuadrilla_id') and 'qb_turno' not in vals and not vals.get('qb_turno_fijo'):
            cuadrilla = self.env['qb.nomina.cuadrilla'].sudo().browse(vals['qb_cuadrilla_id'])
            vals = dict(vals, qb_turno=cuadrilla.turno)
        return vals

    @api.model_create_multi
    def create(self, vals_list):
        return super().create([self._qb_turno_de_cuadrilla(v) for v in vals_list])

    def write(self, vals):
        res = super().write(vals)
        if vals.get('qb_cuadrilla_id') and 'qb_turno' not in vals:
            cuadrilla = self.env['qb.nomina.cuadrilla'].sudo().browse(vals['qb_cuadrilla_id'])
            cuadrilla._aplicar_turno()
        return res


class QbNominaFestivo(models.Model):
    _name = 'qb.nomina.festivo'
    _description = 'Día festivo de la nómina'
    _order = 'fecha'

    fecha = fields.Date(string="Fecha", required=True)
    name = fields.Char(string="Motivo", required=True)
    tipo = fields.Selection([
        ('ley', "Ley (art. 74 LFT)"),
        ('contrato', "Contrato colectivo"),
        ('empresa', "Empresa"),
    ], string="Tipo", required=True, default='ley')
    company_id = fields.Many2one('res.company', string="Empresa", required=True,
                                 default=lambda self: self.env.company)
    active = fields.Boolean(default=True)

    _fecha_uniq = models.Constraint('unique(company_id, fecha)', 'Ese día ya está en el calendario de festivos.')

    @api.model
    def qb_festivos(self, desde, hasta, company):
        """Fechas festivas entre ``desde`` y ``hasta`` (incluidas) de la empresa."""
        return set(self.sudo().search([
            ('company_id', '=', company.id), ('fecha', '>=', desde), ('fecha', '<=', hasta),
        ]).mapped('fecha'))
