# -*- coding: utf-8 -*-
"""57.94.0 «SGI en planta» (auditoría 2026-10: U-01, U-08).

La planta firma en la tableta compartida a su nombre:

- ``sgi.floor.tablet``: qué cuenta compartida es tableta (supervisor@,
  manufactura@…), de qué departamentos y qué checklists se llenan ahí. La da
  de alta el Jefe MAST; la cuenta recibe el grupo «Tableta de planta (SGI)».
- ``sgi.floor.kiosk``: lo que llama la pantalla (acción cliente
  ``sgi_floor_kiosk``). CADA método vuelve a validar la tableta, que la
  persona sea de sus departamentos y su PIN (``sgi.pin``), y escribe con sudo
  poniendo al empleado y la tableta. La cuenta de la tableta no tiene ACL de
  acuses, incidentes, EPP ni checklist: nada se firma a su nombre.
- RH (U-08): «Le falta» en el empleado, la lista «Empleados sin puesto, sin
  PIN o sin correo» y un aviso semanal por departamento en Mis pendientes.
"""
from odoo import api, fields, models
from odoo.exceptions import ValidationError


class SgiFloorTablet(models.Model):
    """Tableta de planta: una cuenta compartida, sus departamentos y sus checklists."""
    _name = 'sgi.floor.tablet'
    _description = "Tableta de planta (SGI en planta)"
    _order = 'name'

    name = fields.Char(string="Tableta", required=True,
                       help="Nombre que ve la gente y que queda en lo firmado: «Tableta Tejido», "
                            "«Tableta Mantenimiento».")
    user_id = fields.Many2one(
        'res.users', string="Cuenta de la tableta", required=True, ondelete='restrict',
        domain="[('share', '=', False)]",
        help="Usuario compartido con el que se abre la tableta (por ejemplo supervisor@). No debe "
             "estar ligado a un empleado: lo firmado queda a nombre de quien teclea su PIN.")
    department_ids = fields.Many2many(
        'hr.department', 'sgi_floor_tablet_department_rel', 'tablet_id', 'department_id',
        string="Departamentos", required=True,
        help="Las personas de estos departamentos (y de sus subdepartamentos) salen en la pantalla "
             "y pueden firmar en esta tableta.")
    checklist_template_ids = fields.Many2many(
        'sgi.checklist.template', 'sgi_floor_tablet_checklist_rel', 'tablet_id', 'template_id',
        string="Checklists que se llenan aquí",
        help="Las hojas de estas plantillas salen en «Checklist de mi equipo» de la tableta.")
    company_id = fields.Many2one('res.company', string="Empresa", required=True,
                                 default=lambda self: self.env['sgi.config']._sgi_company(),
                                 help="Empresa del SGI: solo su gente sale en la tableta.")
    active = fields.Boolean(default=True,
                            help="Archive la tableta para que deje de abrir SGI en planta sin borrarla "
                                 "(lo firmado en ella la sigue citando).")
    note = fields.Text(string="Dónde está", help="Lugar de la tableta y quién la cuida.")

    _user_uniq = models.Constraint('unique(user_id)', "Esa cuenta ya es de otra tableta.")

    @api.constrains('user_id')
    def _check_shared_user(self):
        for tablet in self:
            # El dominio share=False es solo de la vista: un usuario de portal
            # no puede recibir el grupo (implica Usuario interno).
            if tablet.user_id.share:
                raise ValidationError("La cuenta %s es de portal: una tableta usa un usuario interno."
                                      % tablet.user_id.login)
            if tablet.user_id.sudo().employee_ids:
                raise ValidationError(
                    "La cuenta %s está ligada a un empleado. Una tableta usa una cuenta compartida, "
                    "sin empleado: lo firmado queda a nombre de quien teclea su PIN." % tablet.user_id.login)

    @api.model_create_multi
    def create(self, vals_list):
        tablets = super().create(vals_list)
        tablets._sgi_grant_group()
        return tablets

    def write(self, vals):
        res = super().write(vals)
        if 'user_id' in vals or vals.get('active'):
            self._sgi_grant_group()
        return res

    def _sgi_grant_group(self):
        """La cuenta de una tableta activa recibe «Tableta de planta (SGI)».
        No quita grupos: eso lo decide Jose y lo hace Sistemas (Q8)."""
        group = self.env.ref('quimibond_sgi.group_sgi_floor_tablet').sudo()
        users = self.filtered('active').user_id - group.user_ids
        if users:
            group.write({'user_ids': [(4, user.id) for user in users]})
