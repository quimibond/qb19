# -*- coding: utf-8 -*-
"""Campos que faltaban para que los indicadores capturados a mano pasen a
fórmula configurable (55.0.0, 2026-09-25); guardados en la base para poder
filtrarlos desde un término (``sgi.indicator.term``). A-022 (auditoría
2026-09): antes vivían todos en ``sgi_kpi_fields.py``; ahora un archivo por
tema (``sgi_kpi_sales``, ``sgi_kpi_quality``, ``sgi_kpi_account``,
``sgi_kpi_review``, ``sgi_kpi_hr``). Mover código entre archivos no toca la
base.

Calidad:

- C5-01 ``quality.alert.sgi_claimed_meters``: metros reclamados; el denominador
  (metros embarcados) sale de los movimientos de la entrega.
- MT-01 no necesita campo: ``mrp.workcenter.productivity`` con
  ``loss_id.name = 'Mantenimiento'`` y ``duration`` (minutos, factor 1/60).
"""
from odoo import fields, models


# ---- C5-01 ----------------------------------------------------------------
class QualityAlertClaimedMeters(models.Model):
    _inherit = 'quality.alert'

    sgi_claimed_meters = fields.Float(
        string="Metros reclamados", digits=(16, 2),
        help="Metros que el cliente reclama en esta NC (C5-01).")
