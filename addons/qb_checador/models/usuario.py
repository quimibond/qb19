# -*- coding: utf-8 -*-
from odoo import api, fields, models


class ChecadorUsuario(models.Model):
    """Número de usuario en un reloj → empleado, para un reloj cuyos números no son los de nómina.

    El de Ciruelos (Hikvision) numeró a su gente 1, 3, 5, 7… y la nómina los tiene como C-26, C-33…
    Capturar el número en el empleado (``qb_checador_pin``) chocaría con los relojes de Toluca, donde el
    usuario 3 es otra persona; esta tabla vale solo para su reloj y gana sobre las demás ligas.
    """
    _name = 'qb.checador.usuario'
    _description = "Usuario de un reloj checador"
    _order = 'equipo_id, id'

    equipo_id = fields.Many2one('qb.checador.equipo', string="Reloj", required=True, ondelete='cascade', index=True)
    pin = fields.Char(string="Número en el reloj", required=True,
                      help="El número de usuario (employeeNo / PIN) tal como lo manda el reloj.")
    nombre_reloj = fields.Char(string="Nombre en el reloj", help="Solo de referencia.")
    employee_id = fields.Many2one('hr.employee', string="Empleado", required=True, ondelete='cascade')

    _pin_uniq = models.Constraint('unique(equipo_id, pin)', 'Ese número ya está ligado en este reloj.')

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('pin'):
                vals['pin'] = vals['pin'].strip()
        usuarios = super().create(vals_list)
        usuarios._religar_checadas()
        return usuarios

    def write(self, vals):
        if vals.get('pin'):
            vals['pin'] = vals['pin'].strip()
        res = super().write(vals)
        if 'pin' in vals or 'employee_id' in vals:
            self._religar_checadas()
        return res

    def _religar_checadas(self):
        """Las checadas de ese número que quedaron sin empleado se ligan ya y entran al siguiente emparejado."""
        Checada = self.env['qb.checada'].sudo()
        for u in self:
            sueltas = Checada.search([('equipo_id', '=', u.equipo_id.id), ('pin', '=', u.pin),
                                      ('state', '=', 'sin_empleado'), ('attendance_id', '=', False)])
            sueltas.write({'employee_id': u.employee_id.id, 'state': 'nueva', 'nota': False})
