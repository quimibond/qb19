# -*- coding: utf-8 -*-
"""Un asistente para las dos decisiones que piden motivo de lista: regresar
a recotizar (quien aprueba) y dar por perdida (Ventas)."""
from odoo import api, fields, models
from odoo.exceptions import UserError


class QbCotizadorDecisionWizard(models.TransientModel):
    _name = 'qb.cotizador.decision.wizard'
    _description = 'Decisión sobre una cotización (regreso o pérdida)'

    cotizacion_id = fields.Many2one('qb.cotizador.cotizacion', required=True,
                                    ondelete='cascade')
    tipo = fields.Selection([('regreso', 'Regresar a recotizar'),
                             ('perdida', 'Dar por perdida')], required=True)
    motivo_id = fields.Many2one('qb.cotizador.motivo', string='Motivo', required=True,
                                domain="[('tipo', '=', tipo)]")
    nota = fields.Text(string='Detalle (opcional)')

    @api.model
    def default_get(self, fields_list):
        vals = super().default_get(fields_list)
        ctx = self.env.context
        if ctx.get('active_model') == 'qb.cotizador.cotizacion' and ctx.get('active_id'):
            vals.setdefault('cotizacion_id', ctx['active_id'])
        if ctx.get('default_tipo'):
            vals['tipo'] = ctx['default_tipo']
        return vals

    def action_confirmar(self):
        self.ensure_one()
        if self.motivo_id.tipo != self.tipo:
            raise UserError('El motivo elegido no es de este tipo de decisión.')
        if self.tipo == 'regreso':
            self.cotizacion_id._regresar(self.motivo_id, self.nota)
        else:
            self.cotizacion_id._marcar_perdida(self.motivo_id, self.nota)
        return {'type': 'ir.actions.act_window_close'}
