# -*- coding: utf-8 -*-
from odoo import fields, models


class ResPartner(models.Model):
    _inherit = 'res.partner'

    collection_user_id = fields.Many2one(
        'res.users', string='Dueño de cobranza', company_dependent=True,
        help='Quién cobra a este cliente. Vacío: el default de la compañía.')
