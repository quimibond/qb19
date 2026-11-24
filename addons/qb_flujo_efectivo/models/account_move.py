# -*- coding: utf-8 -*-
from odoo import fields, models


class AccountMove(models.Model):
    _inherit = 'account.move'

    qb_fecha_flujo = fields.Date(
        string='Fecha esperada en el flujo', copy=False, tracking=True,
        help='Cuándo se espera cobrar (factura de cliente) o se programa pagar (factura de proveedor). '
             'El flujo de efectivo a 13 semanas la usa en lugar del vencimiento y del atraso histórico; en '
             'una factura de cliente también gana sobre la regla de incobrable.')
