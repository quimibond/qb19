# -*- coding: utf-8 -*-
from odoo import fields, models


class ResPartner(models.Model):
    _inherit = 'res.partner'

    sat_cfdi_policy = fields.Selection([
        ('factura', 'Factura con CFDI (se concilia)'),
        ('poliza', 'Se registra por póliza (banco, impuestos): el CFDI se ignora'),
        ('sin_cfdi', 'No emite CFDI (extranjero, nómina): sus facturas no se comparan'),
    ], string='CFDI del SAT', default='factura', required=True,
        help='Cómo trata la comparación SAT vs Odoo a este contacto. Los contactos con '
             'país distinto de México cuentan como "no emite CFDI" aunque estén en "factura".')
