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
    sat_auto_bill_journal_id = fields.Many2one(
        'account.journal', string='Facturas de proveedor desde el SAT en',
        domain="[('type', '=', 'purchase'), ('company_id', '=', id)]",
        help='Con un diario, los CFDI recibidos que no están en Odoo (ni hay una factura del mismo '
             'proveedor y monto) se vuelven facturas en borrador en este diario, con el XML del SAT. '
             'Vacío: apagado.')
    sat_data_since = fields.Date(
        string='Datos del SAT desde',
        help='Primer día con CFDI del SAT completos (lo que Syntage extrajo). Las facturas de Odoo '
             'anteriores no se comparan: quedan en "Antes de los datos del SAT", fuera de la '
             'conciliación. Vacío: se compara toda la historia de Odoo.')
    sat_data_until_issued = fields.Date(
        string='Datos del SAT hasta (emitidos)', compute='_compute_sat_data_until',
        help='Timbrado más reciente de un CFDI emitido que Syntage ya entregó. Las facturas de '
             'Odoo de esa fecha en adelante se muestran como "Aún no extraído del SAT", no como '
             '"solo Odoo".')
    sat_data_until_received = fields.Date(
        string='Datos del SAT hasta (recibidos)', compute='_compute_sat_data_until')

    def _compute_sat_data_until(self):
        Cfdi = self.env['sat.cfdi'].sudo()
        for company in self:
            until = {}
            for direction in ('issued', 'received'):
                groups = Cfdi._read_group(
                    [('company_id', '=', company.id), ('direction', '=', direction), ('tipo', 'in', ('I', 'E'))],
                    aggregates=['fecha_timbrado:max', 'fecha_emision:max'])
                stamped, issued = groups[0] if groups else (False, False)
                latest = stamped or issued
                until[direction] = fields.Date.to_date(latest) if latest else False
            company.sat_data_until_issued = until['issued']
            company.sat_data_until_received = until['received']

    sat_syntage_entity_id = fields.Char(
        string='Entidad en Syntage',
        help='Identificador de la entidad en Syntage. Se resuelve solo la primera '
             'vez a partir del RFC; bórralo para forzar que se vuelva a buscar.',
    )
