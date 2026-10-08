# -*- coding: utf-8 -*-
from datetime import date, timedelta

from odoo import fields, models


class QbTejidoRitmoRecalcular(models.TransientModel):
    _name = 'qb.tejido.ritmo.recalcular'
    _description = 'Recalcular ritmo de tejido por rango de fechas'

    desde = fields.Date(required=True, default=lambda self: date.today() - timedelta(days=15))
    hasta = fields.Date(required=True, default=lambda self: date.today() + timedelta(days=1),
                        help='Exclusivo: se calcula hasta el día anterior.')

    def action_recalcular(self):
        self.ensure_one()
        n_int, n_rit = self.env['qb.tejido.ritmo'].recalcular(self.desde, self.hasta)
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'title': 'Ritmo de tejido',
                       'message': '%d intervalos y %d ritmos recalculados.' % (n_int, n_rit),
                       'type': 'success', 'sticky': False,
                       'next': {'type': 'ir.actions.act_window_close'}},
        }
