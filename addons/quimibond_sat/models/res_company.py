# -*- coding: utf-8 -*-
from odoo import fields, models


class ResCompany(models.Model):
    _inherit = 'res.company'

    sat_sync_enabled = fields.Boolean(
        string='Sincronizar CFDI con Syntage',
        default=False,
        help='Si está activo, los crons de quimibond_sat descargan los CFDI de esta '
             'compañía (por su RFC) y piden la extracción diaria a Syntage.',
    )
    sat_syntage_entity_id = fields.Char(
        string='Entidad en Syntage',
        help='Identificador de la entidad en Syntage. Se resuelve solo la primera '
             'vez a partir del RFC; bórralo para forzar que se vuelva a buscar.',
    )
