# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError

from ..models.mrp_production import DESPERDICIO_RAMA
from .pesaje_manual import modo_por_defecto, motivo_por_defecto


class RolloDesperdicioRamaWizard(models.TransientModel):
    """Pesaje de los subproductos de desperdicio de Rama (diseño §16).

    3.23.0 (Dirección, 2026-11-22): son dos, DESPERDICIO TEJIDO (los lienzos o tela que se elimina durante el proceso) y
    DESPERDICIO ORILLAS (la orilla de la tela que se retira en la Rama). Cada uno se pesa una vez por orden, con el peso
    real de la báscula IoT o capturado a mano. Sin tara: se registra el peso capturado.
    """
    _name = 'rollo.desperdicio.rama.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Pesaje de desperdicio en Rama'

    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Rama', required=True, readonly=True)
    tipo = fields.Selection(
        [('tejido', 'Desperdicio Tejido (lienzos o tela que se elimina en el proceso)'),
         ('orillas', 'Desperdicio Orillas (orilla de la tela que se retira en la Rama)')],
        'Desperdicio a pesar', required=True, default='tejido')
    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado_nombre')
    kg_desperdicio = fields.Float('Peso del desperdicio (kg)', digits=(12, 3))
    motivo_manual = fields.Char('Motivo de la captura manual',
                                help='3.3.0: obligatorio cuando el peso se captura a mano (v5.4 §13).',
                                default=lambda self: motivo_por_defecto(self))
    weighing_mode = fields.Selection(default=lambda self: modo_por_defecto(self))  # 3.3.9
    ya_pesado = fields.Boolean(compute='_compute_ya_pesado')

    @api.depends('empleado_numero')
    def _compute_empleado_nombre(self):
        Empleado = self.env['hr.employee']
        for wiz in self:
            nombre = False
            if wiz.empleado_numero:
                emp = Empleado._qb_operador(wiz.empleado_numero)  # 3.5.1: número = referencia S-<número>
                nombre = emp.name if emp else _('EMPLEADO NO ENCONTRADO')
            wiz.empleado_nombre = nombre

    @api.depends('production_id.desperdicio_kg', 'production_id.desperdicio_orillas_kg', 'tipo')
    def _compute_ya_pesado(self):
        for wiz in self:
            wiz.ya_pesado = bool(wiz.production_id[DESPERDICIO_RAMA[wiz.tipo or 'tejido']['kg']])

    def action_confirmar(self):
        self.ensure_one()
        if self.kg_desperdicio <= 0:
            raise UserError(_('Captura el peso del desperdicio.'))
        self.env['hr.employee']._qb_operador_requerido(self.empleado_numero)  # 3.5.3
        manual = self.weighing_mode == 'manual'
        if manual and not (self.motivo_manual or '').strip():
            raise UserError(_('Captura el motivo de la captura manual del peso.'))
        operador = ' - '.join(filter(None, [self.empleado_numero, self.empleado_nombre]))
        campos = DESPERDICIO_RAMA[self.tipo]
        self.production_id._rama_registrar_desperdicio(
            self.workorder_id, self.kg_desperdicio, operador, manual, tipo=self.tipo)
        # 3.3.8: el desperdicio se pesa neto (sin tara).
        self.production_id._historial_pesaje(self.workorder_id.workcenter_id, campos['codigo'],
                                             self.kg_desperdicio, 0.0, self.kg_desperdicio, operador)
        if manual:
            self.production_id[campos['motivo']] = self.motivo_manual.strip()
        # 3.17.0: la etiqueta del desperdicio (con código de barras del lote) sale al pesar, como la de Empaque.
        if self.tipo == 'orillas':
            return self.production_id.action_imprimir_etiqueta_desperdicio_orillas()
        return self.production_id.action_imprimir_etiqueta_desperdicio_rama()
