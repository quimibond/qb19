# -*- coding: utf-8 -*-
"""Registro patronal IMSS por contrato.

Odoo guarda un solo registro patronal, en ``res.company.l10n_mx_imss_id``
(verificado: el campo existe únicamente en ``res.company`` y en
``res.config.settings``). Quimibond tiene dos, confirmados contra los CFDI que
timbra el despacho: ``C-675994510-1`` (Toluca) y ``Y6087828106`` (México). El
campo vive en ``hr.version`` (el contrato en Odoo 19) y se refleja en el
empleado como hacen los demás campos de la localización."""
from odoo import fields, models


class HrVersion(models.Model):
    _inherit = 'hr.version'

    l10n_mx_employer_registration = fields.Char(
        string='Registro patronal IMSS',
        size=20,
        groups='hr.group_hr_user',
        help="Registro patronal con el que este contrato cotiza ante el IMSS. Sale en "
             "nomina12:Emisor/@RegistroPatronal del CFDI de nómina. Vacío = el de la compañía.",
    )


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    l10n_mx_employer_registration = fields.Char(
        related='version_id.l10n_mx_employer_registration',
        readonly=False,
        groups='hr.group_hr_user',
    )
