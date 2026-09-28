# -*- coding: utf-8 -*-
"""Actividades específicas (19.0.30.0.0): calcula los faltantes de
especificación de las actividades que ya existían. No toca sus datos: solo
llena sgi.activity.spec.gap y spec_complete. Un tropiezo aquí no tumba el
update (se recalculan solos en el siguiente cambio de cada actividad)."""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    try:
        with cr.savepoint():
            env = api.Environment(cr, SUPERUSER_ID, {})
            acts = env['sgi.process.activity'].with_context(active_test=False).search([])
            acts._sgi_refresh_spec_gaps()
            _logger.info("SGI 30.0: faltantes de especificación de %d actividad(es).", len(acts))
    except Exception:  # noqa: BLE001
        _logger.exception("SGI 30.0: no se pudieron calcular los faltantes; se "
                          "calcularán al editar cada actividad.")
