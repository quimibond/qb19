# -*- coding: utf-8 -*-
"""57.65.0 (bloque 2 de formularios 6/6, decisión 2026-09-30 «Inventario de
formularios»): modelo de Odoo en los entregables propios cuyo nombre dice sin
duda dónde viven (``sgi.deliverable._sgi_fill_evident_models``, tabla
``SGI_DELIVERABLE_MODELS`` en ``models/sgi_deliverable_models.py``).

Entregable por código en la empresa del SGI. Solo escribe si «Modelo de
Odoo» está vacío; se salta el que ya mide alguna actividad. Lo escrito queda
en el log con «vacío → modelo». Idempotente. Nada se borra.

Esperado en producción (MCP, solo lectura, 2026-09-30): 20 entregables, los
20 sin modelo, sin actividades que se midan con ellos y con fecha
``create_date`` sin campo de usuario: C2-ACUSE (171), C2-CITA (159),
C2-CRUCE (169), C2-PEDIMENTO (167), C2-TRANSPORTE (160), C3-VIEJAS (315),
C4-PARAMETROS (331), C4-TARJETA (321), C5-CONCESION (358), C5-LIBERADO (151),
C5-PRUEBAS (352), C6-DESCARGA (258), C6-QUIMICOS (265), S1-FECHA (294),
S2-CONTRARRECIBO (207), S2-PORTAL (202), S3-BLOQUEO (408), S3-CONCIL-BANCO
(403), S5-PREV-HECHO (387) y S5-REPARACION (383). El modelo viaja a sus
flujos entre procesos.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    written = env['sgi.deliverable']._sgi_fill_evident_models()
    _logger.info("SGI 57.65.0: entregables con modelo nuevo: %s (esperado: 20).",
                 ", ".join(sorted(written)) or "ninguno nuevo")
