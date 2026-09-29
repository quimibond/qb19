# -*- coding: utf-8 -*-
"""D-03 (auditoría 2026-09, C-012 / H-021 / F-014): el SGI es de UNA sola
empresa, y se declara en el código.

La empresa del SGI es ``sgi.config._sgi_company()`` (parámetro
``quimibond_sgi.sgi_company_id``; por default la empresa principal, PNTQ id 1).
Los procesos solo se crean o se mueven a esa empresa: de ellos cuelgan
actividades, etapas, roles, flujos y responsabilidades por ``related``, así
que cerrar la puerta aquí basta para que la estructura no se reparta entre
razones sociales. Los ~50 modelos sin ``company_id`` (indicadores, riesgos,
NC, auditorías, normas, áreas…) son compartidos a propósito; si algún día el
SGI tuviera otra empresa, el camino está en C-012 (``company_id`` en los 12
modelos raíz, ``related`` en los hijos, ``_check_company_auto`` y claves
únicas por empresa)."""
from odoo import api, models
from odoo.exceptions import ValidationError


class SgiProcessSingleCompany(models.Model):
    _inherit = 'sgi.process'

    @api.constrains('company_id')
    def _check_sgi_single_company(self):
        company = self.env['sgi.config']._sgi_company()
        wrong = self.filtered(lambda p: p.company_id and p.company_id != company)
        if wrong:
            raise ValidationError(
                "El SGI es de una sola empresa (%s): los procesos no se crean "
                "ni se mueven a otra.\n\nProcesos: %s"
                % (company.display_name, ", ".join(wrong.mapped('display_name'))))
