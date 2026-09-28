# -*- coding: utf-8 -*-
"""19.0.56.7.0: los documentos controlados en piloto o vigente quedan de solo
lectura para los usuarios internos (access_internal = 'view').

Antes el SGI subía «sin acceso» a «lectura» pero nunca bajaba un «editor»: al
28-sep-2026 había 486 documentos vigentes que cualquier usuario interno podía
editar o reemplazar. Los cambios van por Cambios documentales y el Jefe MAST
edita como gerente de Documentos. Solo toca la columna de acceso interno."""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    cr.execute("""
        SELECT 1 FROM information_schema.columns
         WHERE table_name = 'documents_document' AND column_name = 'access_internal'
    """)
    if not cr.fetchone():
        return
    cr.execute("""
        UPDATE documents_document
           SET access_internal = 'view'
         WHERE sgi_is_controlled
           AND sgi_state IN ('piloto', 'vigente')
           AND access_internal IS DISTINCT FROM 'view'
    """)
    _logger.info("quimibond_sgi 56.7.0: %s documentos controlados pasan a solo lectura.", cr.rowcount)
