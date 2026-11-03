# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError


class RolloDesperdicioRamaWizard(models.TransientModel):
    """Pesaje del subproducto Desperdicio Tejido (orilla) en Rama (diseño §16).

    Un solo pesaje por orden, con el peso real de la báscula IoT o capturado a mano. Sin tara: se
    registra el peso capturado.
    """
    _name = 'rollo.desperdicio.rama.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Pesaje de desperdicio en Rama'

    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Rama', required=True, readonly=True)
    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado_nombre')
    kg_desperdicio = fields.Float('Peso del desperdicio (kg)', digits=(12, 3))
    ya_pesado = fields.Boolean(compute='_compute_ya_pesado')

    @api.depends('empleado_numero')
    def _compute_empleado_nombre(self):
        Empleado = self.env['hr.employee']
        campo = 'x_studio_nmero_de_trabajador'
        for wiz in self:
            nombre = False
            if wiz.empleado_numero and campo in Empleado._fields:
                emp = Empleado.search([(campo, '=', wiz.empleado_numero)], limit=1)
                nombre = emp.name if emp else _('EMPLEADO NO ENCONTRADO')
            wiz.empleado_nombre = nombre

    @api.depends('production_id.desperdicio_kg')
    def _compute_ya_pesado(self):
        for wiz in self:
            wiz.ya_pesado = bool(wiz.production_id.desperdicio_kg)

    def action_confirmar(self):
        self.ensure_one()
        if self.kg_desperdicio <= 0:
            raise UserError(_('Captura el peso del desperdicio.'))
        self.production_id._rama_registrar_desperdicio(
            self.workorder_id, self.kg_desperdicio,
            ' - '.join(filter(None, [self.empleado_numero, self.empleado_nombre])),
            self.weighing_mode == 'manual')
        return {'type': 'ir.actions.act_window_close'}
