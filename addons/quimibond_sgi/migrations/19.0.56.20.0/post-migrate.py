# -*- coding: utf-8 -*-
"""56.20.0 (2026-09-28): las NC que nacieron de un incidente SST quedan con
origen «Incidente/accidente SST» (antes se guardaban como «Proceso»). Solo
toca el origen de las NC ligadas a un incidente que siguen en «Proceso»."""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    cr.execute("""
        UPDATE quality_alert
           SET sgi_origin_type = 'incidente_sst'
         WHERE sgi_incident_id IS NOT NULL
           AND sgi_origin_type = 'proceso'
    """)
    _logger.info("SGI 56.20.0: %d NC de incidentes con origen «Incidente/accidente SST».", cr.rowcount)
