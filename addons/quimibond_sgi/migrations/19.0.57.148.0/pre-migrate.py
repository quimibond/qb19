# -*- coding: utf-8 -*-
"""19.0.57.148.0 — Un documento generado por empresa y tipo.

``sgi.miid`` pasa de UNIQUE(company_id) a UNIQUE(company_id, kind) para que
el P-G01 conviva con el MIID. Odoo recrea la restricción al cargar el
modelo, pero si la vieja siguiera con otro nombre impediría crear el P-G01:
se quita aquí, antes de cargar el módulo."""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute("ALTER TABLE sgi_miid DROP CONSTRAINT IF EXISTS sgi_miid_company_uniq")
    _logger.info("SGI 57.148.0: restricción única vieja de sgi_miid retirada (si existía).")
