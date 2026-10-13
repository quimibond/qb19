# -*- coding: utf-8 -*-
"""57.145.0 — Mis pendientes deja de ser transitorio.

``sgi.my.pending`` pasa de ``TransientModel`` a ``Model`` (una fila por persona
y registro pendiente, actualizada en su lugar). La tabla es la misma
(``sgi_my_pending``); Odoo agrega las columnas nuevas al actualizar. Lo que
había en la tabla eran renglones transitorios de listas abiertas (sin clave
estable y sin ``detail``): se vacía para que la primera apertura y el
respaldo nocturno la llenen con la forma nueva. No es dato de negocio."""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'sgi_my_pending'")
    if not cr.fetchone():
        return
    cr.execute("DELETE FROM sgi_my_pending")
    _logger.info("SGI 57.145.0: %d renglón(es) transitorios de Mis pendientes vaciados.", cr.rowcount)
