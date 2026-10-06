# -*- coding: utf-8 -*-
"""1.2.0: el SGI en Conocimiento.

Corre después de cargar los datos del módulo (el ``<function>`` de
``data/sgi_knowledge_data.xml`` ya sembró). Idempotente:

1. vuelve a sembrar (no hace nada si ya corrió; cubre un artículo que falló
   en el ``<function>`` por un dato raro);
2. sincroniza los miembros (Jefe MAST y dueños de proceso);
3. una actividad para el Jefe MAST con los siguientes pasos (una sola vez,
   clave ``kb_conocimiento_120``).

No importa documentos (lo decide el Jefe MAST con el asistente) y no toca
``documents.document`` ni ``sgi.process.activity``.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

NOTE = (
    "Se creó el espacio «SGI» en Conocimiento con los manuales por rol. Para pasar los instructivos, "
    "controles operacionales, protocolos y reglamentos vigentes: SGI → Administración → Transición → "
    "Importar documentos a Conocimiento (empiece con «CO y C4»). El espacio de 2025 se renombró a "
    "«SGI (estructura 2025, sin uso)»: decida si lo archiva."
)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Article = env['knowledge.article']
    result = Article._sgi_kb_seed()
    added = Article._sgi_kb_sync_members()
    root = Article._sgi_kb_find('raiz')
    mast_id = env['sgi.cron']._sgi_manager_user_id()
    if root and mast_id:
        env['sgi.cron']._sgi_schedule(
            root, "Documentación del SGI en Conocimiento", NOTE, mast_id, key='kb_conocimiento_120')
    renamed = Article.sudo().search_count([('name', '=', 'SGI (estructura 2025, sin uso)')])
    _logger.info(
        "quimibond_sgi_knowledge 1.2.0: siembra %s; espacio de 2025 renombrado: %s; %d miembro(s) agregados; "
        "aviso al Jefe MAST: %s.", result, 'sí' if renamed else 'no', added, 'sí' if (root and mast_id) else 'no')
