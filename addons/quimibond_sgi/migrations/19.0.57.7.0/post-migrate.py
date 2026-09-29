# -*- coding: utf-8 -*-
"""57.7.0 (entrega 2 de la auditoría, decisiones D-14 y D-15).

1. **D-14.** El cron ``quimibond_sgi.sgi_cron_weekly_digest`` (id 201 en
   producción, apagado a mano desde el 24-ago-2026, B-017) deja de mandar el
   resumen a MAST y Dirección y pasa a mandar a cada persona lo atrasado de su
   «Mis pendientes» (``model.cron_weekly_overdue_mail()``). Su XML es
   ``noupdate``, por eso se reescribe aquí. **Queda apagado**: Jose lo
   enciende a mano. El registro se reutiliza (no se borra ni se duplica).
2. **D-15.** Archiva (nunca borra) «Solicitud de compra SGI» (13), «Cambio de
   proceso / infraestructura (MOC SGI)» (14) y la familia de puestos OP-PTAR
   (57), cada una con un CSV de respaldo adjunto. Una categoría con alguna
   solicitud o una familia con algún rol NO se archiva: queda en el log.

Producción el 2026-09-29 (MCP, solo lectura, empresa 1):
- ``approval.request`` por categoría: 0 en la 13 y 0 en la 14 (de ningún
  estado); ``sgi.activity.role`` con esas categorías: 0.
- ``sgi.job.family`` 57 OP-PTAR: activa, 2 puestos (275, 200), 3 empleados,
  0 roles de actividad.

Idempotente. Reversa: reactivar los tres registros (``active = True``); el
cron: ``model.cron_weekly_digest()`` ya no existe en el código.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def _repurpose_cron(env):
    cron = env.ref('quimibond_sgi.sgi_cron_weekly_digest', raise_if_not_found=False)
    if not cron:
        _logger.warning("SGI 57.7.0: no existe el cron sgi_cron_weekly_digest; nada que cambiar.")
        return
    was_active = cron.active
    cron.write({
        'name': "SGI: Mis pendientes atrasados (correo semanal por persona)",
        'code': "model.cron_weekly_overdue_mail()",
        'active': False,
    })
    _logger.info("SGI 57.7.0: cron %d reescrito al correo semanal por persona; queda APAGADO "
                 "(antes activo=%s). Lo enciende Jose.", cron.id, was_active)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    _repurpose_cron(env)
    report = env['sgi.config']._sgi_archive_unused_catalogs()
    _logger.info("SGI 57.7.0 (D-15): archivados %s; con uso (no se tocan): %s "
                 "(esperado en producción: 13, 14 y OP-PTAR archivados, ninguno con uso).",
                 report['archived'], report['kept'])
