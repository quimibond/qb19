# -*- coding: utf-8 -*-
from odoo import fields, models


class ResPartner(models.Model):
    _inherit = 'res.partner'

    # 1.6.0: unidades en que se imprime el precio de sus cotizaciones por default (ninguna = la de la cotización).
    qb_cot_pdf_m = fields.Boolean(string='Cotizaciones: precio por m')
    qb_cot_pdf_yd = fields.Boolean(string='Cotizaciones: precio por yd')
    qb_cot_pdf_kg = fields.Boolean(string='Cotizaciones: precio por kg')
