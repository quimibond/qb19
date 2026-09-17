# -*- coding: utf-8 -*-
from datetime import timedelta

from odoo import fields, models


class SatPullWizard(models.TransientModel):
    _name = 'sat.pull.wizard'
    _description = 'Traer CFDI del SAT (Syntage)'

    company_id = fields.Many2one('res.company', string='Compañía', required=True,
                                 default=lambda self: self.env.company)
    mode = fields.Selection([
        ('pull', 'Descargar lo que Syntage ya tiene (API)'),
        ('extraction', 'Pedir a Syntage que extraiga del SAT'),
    ], default='pull', required=True,
        help='Descargar: trae a Odoo los CFDI que Syntage ya extrajo; es gratis e idempotente. '
             'Extraer: Syntage va al SAT por el periodo (tiene costo) y los datos llegan después '
             'por webhook; luego conviene descargar.')
    date_from = fields.Date(string='Desde', required=True,
                            default=lambda self: fields.Date.context_today(self) - timedelta(days=7))
    date_to = fields.Date(string='Hasta', required=True, default=lambda self: fields.Date.context_today(self))
    include_retentions = fields.Boolean(string='Incluir retenciones (solo extracción)')

    def action_run(self):
        self.ensure_one()
        client = self.env['sat.syntage.client']
        if self.mode == 'pull':
            log = client.pull_invoices(self.company_id, self.date_from, self.date_to, commit=False)
        else:
            log = client.request_extraction(self.company_id, self.date_from, self.date_to,
                                            self.include_retentions)
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'sat.sync.log',
            'res_id': log.id,
            'view_mode': 'form',
            'target': 'current',
        }
