# -*- coding: utf-8 -*-
"""57.112.0: medición manual a propósito.

«Se hace en Odoo, se mide a mano» pide definir un entregable. En una
revisión, un reporte o una junta (revisar la balanza, reportar la cartera,
la junta del S&OP) lo que se revisa vive en Odoo pero la evidencia es la
revisión hecha, no un registro: se mide con el registro de cumplimiento
(57.103.0). Con «Por qué se mide a mano» escrito, el aviso no sale. En
producción eran unas 45 de las 84 actividades con ese aviso (2026-10-06).
"""
from odoo import fields, models

from .sgi_measure_history import SgiActivityHistory


class SgiActivityManualReason(models.Model):
    _inherit = 'sgi.process.activity'

    _SGI_MEASURE_FIELDS = SgiActivityHistory._SGI_MEASURE_FIELDS | {'manual_reason'}

    manual_reason = fields.Text(
        string="Por qué se mide a mano",
        help="Cuando la actividad se hace en Odoo pero su evidencia no es un registro "
             "(una revisión, un reporte, una junta): diga qué se revisa y dónde queda "
             "la decisión. Se mide con el registro de cumplimiento y ya no sale el aviso "
             "«Se hace en Odoo, se mide a mano».")

    def _sgi_spec_problems(self):
        problems = super()._sgi_spec_problems()
        if (self.manual_reason or '').strip() and self.measure_method in ('manual', 'correo'):
            problems = [(code, msg) for code, msg in problems if code != 'odoo_measured_manual']
        return problems
