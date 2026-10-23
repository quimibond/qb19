# -*- coding: utf-8 -*-
from odoo import fields, models


class ResPartner(models.Model):
    _inherit = 'res.partner'

    qb_pedimento_virtual = fields.Boolean(
        'Venta con pedimento virtual',
        help='Cliente nacional con programa IMMEX al que se vende con pedimento virtual (V1). Sus facturas '
             'deben quedar amparadas por un pedimento; las que no, salen en «Facturas sin pedimento» '
             '(auditoría de la Secretaría de Finanzas del Edomex, 2026).')
