# -*- coding: utf-8 -*-
"""S4-03 (SGI): marcas en el lote de nómina para medir cuántos lotes se
recalcularon o necesitaron un complemento pagado. Viven aquí porque
``quimibond_sgi`` no depende de nómina."""
from odoo import fields, models


class HrPayslipRun(models.Model):
    _inherit = 'hr.payslip.run'

    sgi_recalculated = fields.Boolean(
        string="Se recalculó", tracking=True, copy=False,
        help="El lote se tuvo que recalcular después de generarse (S4-03).")
    sgi_complement_paid = fields.Boolean(
        string="Complemento pagado", tracking=True, copy=False,
        help="Hubo que pagar un complemento fuera del lote (S4-03).")
    sgi_incident_note = fields.Char(string="Motivo del recálculo o complemento")
