# -*- coding: utf-8 -*-
"""19.0.57.159.0 — Escalamientos del SGI en pausa.

Jose 2026-10-19: mientras la gente todavía no usa el SGI, lo vencido le llega
solo a quien lo tiene que hacer. El parámetro nuevo
``quimibond_sgi.escalations_enabled`` nace vacío (apagado), así que este paso
cierra con nota lo que ya estaba escalado (acciones vencidas al jefe y a
Dirección, plazos de NC al dueño del proceso y a MAST, proveedor sin
respuesta, indicadores en rojo a Dirección, pasos del desarrollo al dueño y a
Dirección) y quita los renglones «Escalamiento» de Mis pendientes. No borra
avisos, configuración ni roles «Escala». Si alguien ya encendió el parámetro,
no hace nada. Idempotente."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

TAG = "57.159.0"


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    if env['sgi.cron']._sgi_pause_escalations_now():
        _logger.info("%s: escalamientos en pausa; lo ya escalado quedó cerrado con nota.", TAG)
    else:
        _logger.info("%s: escalamientos encendidos (parámetro puesto); nada que cerrar.", TAG)
