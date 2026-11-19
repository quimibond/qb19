# -*- coding: utf-8 -*-
"""Un asistente para las dos decisiones que piden motivo de lista: regresar
a recotizar (quien aprueba) y dar por perdida (Ventas)."""
from odoo import api, fields, models
from odoo.exceptions import UserError


class QbCotizadorDecisionWizard(models.TransientModel):
    _name = 'qb.cotizador.decision.wizard'
    _description = 'Decisión sobre una cotización (regreso o pérdida)'

    cotizacion_id = fields.Many2one('qb.cotizador.cotizacion', ondelete='cascade')
    # 1.8.0: la negociación de precio sigue el mismo ciclo (regreso y pérdida con motivo).
    negociacion_id = fields.Many2one('qb.cotizador.negociacion', string='Negociación', ondelete='cascade')
    tipo = fields.Selection([('regreso', 'Regresar a recotizar'),
                             ('perdida', 'Dar por perdida')], required=True)
    motivo_id = fields.Many2one('qb.cotizador.motivo', string='Motivo', required=True,
                                domain="[('tipo', '=', tipo)]")
    nota = fields.Text(string='Detalle (opcional)')

    @api.model
    def default_get(self, fields_list):
        vals = super().default_get(fields_list)
        ctx = self.env.context
        campos = {'qb.cotizador.cotizacion': 'cotizacion_id', 'qb.cotizador.negociacion': 'negociacion_id'}
        if ctx.get('qb_decision_model') in campos and ctx.get('qb_decision_id'):
            vals.setdefault(campos[ctx['qb_decision_model']], ctx['qb_decision_id'])
        elif ctx.get('active_model') in campos and ctx.get('active_id'):
            vals.setdefault(campos[ctx['active_model']], ctx['active_id'])
        if ctx.get('default_tipo'):
            vals['tipo'] = ctx['default_tipo']
        return vals

    def action_confirmar(self):
        self.ensure_one()
        if self.motivo_id.tipo != self.tipo:
            raise UserError('El motivo elegido no es de este tipo de decisión.')
        doc = self.cotizacion_id or self.negociacion_id
        if not doc:
            raise UserError('Falta la cotización o la negociación sobre la que se decide.')
        if self.tipo == 'regreso':
            doc._regresar(self.motivo_id, self.nota)
        else:
            doc._marcar_perdida(self.motivo_id, self.nota)
        return {'type': 'ir.actions.act_window_close'}
