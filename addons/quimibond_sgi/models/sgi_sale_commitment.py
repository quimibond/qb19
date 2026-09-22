# -*- coding: utf-8 -*-
"""Cuándo y quién registró la fecha compromiso del pedido.

`commitment_date` no tenía seguimiento: no había forma de saber cuándo se
comprometió la fecha ni quién lo hizo, y el entregable «Fecha compromiso
registrada» (C2) se fechaba con la fecha del pedido. Aquí se guarda la
PRIMERA vez que la fecha compromiso pasa de vacía a tener valor; los cambios
posteriores quedan en el chatter (tracking). Sin backfill: los pedidos que ya
existían no tienen el dato y la medición empieza desde el despliegue.
"""
from odoo import api, fields, models


class SaleOrderCommitment(models.Model):
    _inherit = 'sale.order'

    commitment_date = fields.Datetime(tracking=True)
    sgi_commitment_set_at = fields.Datetime(
        string="Fecha compromiso registrada el", readonly=True, copy=False, index=True,
        help="Cuándo se registró por primera vez la fecha compromiso.")
    sgi_commitment_set_uid = fields.Many2one(
        'res.users', string="Fecha compromiso registrada por", readonly=True, copy=False,
        help="Quién registró por primera vez la fecha compromiso.")

    def _sgi_commitment_stamp(self):
        return {'sgi_commitment_set_at': fields.Datetime.now(),
                'sgi_commitment_set_uid': self.env.uid}

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('commitment_date') and not vals.get('sgi_commitment_set_at'):
                vals.update(self._sgi_commitment_stamp())
        return super().create(vals_list)

    def write(self, vals):
        first = self.browse()
        if vals.get('commitment_date'):
            first = self.filtered(
                lambda o: not o.commitment_date and not o.sgi_commitment_set_at)
        res = super().write(vals)
        if first:
            super(SaleOrderCommitment, first).write(self._sgi_commitment_stamp())
        return res
