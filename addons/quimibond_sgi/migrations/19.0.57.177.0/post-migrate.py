# -*- coding: utf-8 -*-
"""19.0.57.177.0 — El flujo atrasado (entradas con plazo vencido sin salida)
ya no cuenta como actividad atrasada de la persona en Mi procedimiento: es
«Atrasada en la planta». Las cifras guardadas en cada puesto (Mi equipo) se
marcan por recalcular; mientras corre el cron se calculan al vuelo."""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute("UPDATE hr_job SET sgi_mp_stats_at = NULL WHERE sgi_mp_stats_at IS NOT NULL")
    _logger.info("SGI 57.177.0: %d puesto(s) con cifras de Mi procedimiento por recalcular.", cr.rowcount)
