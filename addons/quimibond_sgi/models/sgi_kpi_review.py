# -*- coding: utf-8 -*-
"""Campos que faltaban para que los indicadores capturados a mano pasen a
fórmula configurable (55.0.0, 2026-09-25); guardados en la base para poder
filtrarlos desde un término (``sgi.indicator.term``). A-022 (auditoría
2026-09): antes vivían todos en ``sgi_kpi_fields.py``; ahora un archivo por
tema (``sgi_kpi_sales``, ``sgi_kpi_quality``, ``sgi_kpi_account``,
``sgi_kpi_review``, ``sgi_kpi_hr``). Mover código entre archivos no toca la
base.

Revisión por la dirección:

- E1-02 ``sgi.management.review.agreement.done_date``: cumplimiento del
  acuerdo (la de su acción, o a mano).
"""
from odoo import api, fields, models


# ---- E1-02 ----------------------------------------------------------------
class SgiManagementReviewAgreementDone(models.Model):
    _inherit = 'sgi.management.review.agreement'

    done_date = fields.Date(
        string="Cumplido el", compute='_compute_done_date', store=True, readonly=False,
        help="Fecha de cumplimiento del acuerdo: la de su acción al terminarse, o "
             "capturada a mano si el acuerdo no tiene acción (E1-02: cerrado antes de su límite).")

    @api.depends('action_line_id.date_done')
    def _compute_done_date(self):
        for agreement in self:
            if agreement.action_line_id.date_done:
                agreement.done_date = agreement.action_line_id.date_done
            elif not agreement.done_date:
                agreement.done_date = False
