# -*- coding: utf-8 -*-
"""Desperdicio de la orden en metros, pesado en Empaque (3.13.0, A3.3b; diseño §16: SALDO TEJIDO D, almacén 17).

Se pesa neto (sin tara) mientras Empaque clasifica los rollos y la orden en metros sigue abierta. Al cerrar la orden, el
peso se suma al saldo Desperdicio de los rollos que Inspección clasificó como tal (mismo producto, mismo movimiento), con
un lote del mes como el desperdicio de Rama. Se puede volver a pesar mientras la orden siga abierta: el último peso
sustituye al anterior y queda nota.
"""
from odoo import api, fields, models, _
from odoo.exceptions import UserError

from .pesaje_manual import modo_por_defecto, motivo_por_defecto


class RolloEmpaqueDesperdicioWizard(models.TransientModel):
    _name = 'rollo.empaque.desperdicio.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Pesaje de desperdicio en Empaque'

    production_id = fields.Many2one('mrp.production', 'Orden (m)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Clasificación final', readonly=True)
    workcenter_id = fields.Many2one(
        'mrp.workcenter', 'Centro de Clasificación final', default=lambda self: self._default_workcenter())
    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado_nombre')
    kg_desperdicio = fields.Float('Peso del desperdicio (kg)', digits=(12, 3))
    motivo_manual = fields.Char('Motivo de la captura manual', default=lambda self: motivo_por_defecto(self))
    weighing_mode = fields.Selection(default=lambda self: modo_por_defecto(self))
    kg_anterior = fields.Float('Peso anterior (kg)', related='production_id.desperdicio_empaque_kg', digits=(12, 3))

    @api.model
    def _default_workcenter(self):
        return self.env['mrp.workorder'].browse(self.env.context.get('default_workorder_id')).workcenter_id.id or False

    @api.depends('empleado_numero')
    def _compute_empleado_nombre(self):
        for wiz in self:
            emp = self.env['hr.employee']._qb_operador(wiz.empleado_numero) if wiz.empleado_numero else False
            wiz.empleado_nombre = (emp.name if emp else _('EMPLEADO NO ENCONTRADO')) if wiz.empleado_numero else False

    def action_confirmar(self):
        self.ensure_one()
        orden = self.production_id
        if not orden._insp_orden_m_en_empaque():
            raise UserError(_('La orden %s no está en Clasificación final: el desperdicio se pesa mientras la orden sigue '
                              'abierta y con rollos por registrar.', orden.name))
        if self.kg_desperdicio <= 0:
            raise UserError(_('Captura el peso del desperdicio.'))
        self.env['hr.employee']._qb_operador_requerido(self.empleado_numero)
        manual = self.weighing_mode == 'manual'
        if manual and not (self.motivo_manual or '').strip():
            raise UserError(_('Captura el motivo de la captura manual del peso.'))
        operador = ' - '.join(filter(None, [self.empleado_numero, self.empleado_nombre]))
        anterior = orden.desperdicio_empaque_kg
        orden.write({
            'desperdicio_empaque_kg': self.kg_desperdicio,
            'desperdicio_empaque_operador': operador,
            'desperdicio_empaque_fecha': fields.Datetime.now(),
            'desperdicio_empaque_manual': manual,
            'desperdicio_empaque_motivo': (self.motivo_manual or '').strip() if manual else False,
        })
        orden._historial_pesaje(self.workcenter_id, 'DESPERDICIO EMPAQUE', self.kg_desperdicio, 0.0,
                                self.kg_desperdicio, operador)
        orden.message_post(body=_(
            'Desperdicio de Empaque: %(kg).3f kg%(a)s, pesado por %(o)s. Se registra como Saldo Desperdicio al cerrar la '
            'orden.', kg=self.kg_desperdicio, o=operador,
            a=_(' (antes %.3f kg)', anterior) if anterior else ''))
        return {'type': 'ir.actions.act_window_close'}
