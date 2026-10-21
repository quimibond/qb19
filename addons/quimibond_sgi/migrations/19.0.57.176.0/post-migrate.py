# -*- coding: utf-8 -*-
"""19.0.57.176.0 — Las mediciones se validan solas cuando no hay nada que
mirar (models/sgi_indicator_autovalidate.py), así que SG-05 «Mediciones
validadas a tiempo» se retira: se archiva con su historia (no se borra) y una
nota en su chatter. El XML es noupdate, por eso lo hace este script. Las
mediciones ya capturadas de indicadores oficiales siguen la regla nueva
(hoy, 2026-10-21, ninguno es oficial: no cambia nada)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    sg05 = env.ref('quimibond_sgi.sgi_ind_salud_validacion', raise_if_not_found=False)
    if sg05 and sg05.active:
        sg05.write({'active': False})
        sg05.message_post(body="57.176.0: se retira. Las mediciones sin nada que mirar se validan "
                               "solas; los rojos con respuesta los mide SG-06.")
        _logger.info("SGI 57.176.0: SG-05 archivado (id %s).", sg05.id)
    measures = env['sgi.indicator.measure'].search(
        [('state', '=', 'capturado'), ('indicator_id.status', '=', 'oficial')])
    validated = measures._sgi_auto_validate()
    _logger.info("SGI 57.176.0: %d medición(es) capturadas de indicadores oficiales validadas solas.",
                 len(validated))
