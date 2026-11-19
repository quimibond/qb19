# -*- coding: utf-8 -*-
from odoo import fields, models


class AccountMove(models.Model):
    _inherit = 'account.move'

    # 1.8.0: la nota de crédito que creó la liquidación de un acuerdo de volumen. No cuenta como venta del
    # producto (volumen de la negociación ni del acuerdo) y no se duplica al recalcular el periodo.
    qb_liquidacion_id = fields.Many2one('qb.cotizador.acuerdo.liquidacion', string='Liquidación de acuerdo de volumen',
                                        readonly=True, copy=False, index='btree_not_null', ondelete='set null')
