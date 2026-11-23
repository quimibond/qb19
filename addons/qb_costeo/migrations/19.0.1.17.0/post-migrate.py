# -*- coding: utf-8 -*-
"""1.17.0: la primera para planear tiene su propia captura. Lo capturado
hasta hoy mandaba en costeo y planeación; se copia a la captura para planear
para que nada cambie el día uno (en main: WC090Q11JNT168 0.80,
NN040Q66JNT163 0.88). Luego se recalcula con los lotes accidentados fuera."""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    cr.execute("""
        UPDATE qb_producto_rendimiento
           SET plan_manual = rendimiento_manual,
               plan_manual_motivo = manual_motivo,
               plan_manual_hasta = manual_vigente_hasta
         WHERE COALESCE(rendimiento_manual, 0) > 0
           AND COALESCE(plan_manual, 0) = 0
    """)
    _logger.info('qb_costeo 1.17.0: %s capturas copiadas a planear', cr.rowcount)
    env = api.Environment(cr, SUPERUSER_ID, {})
    for company in env['res.company'].search([]):
        try:
            env['qb.producto.rendimiento'].with_company(company).recalcular()
        except Exception:  # noqa: BLE001 - la migración no debe tumbar el build
            _logger.exception('qb_costeo 1.17.0: falló el recálculo en %s',
                              company.name)
