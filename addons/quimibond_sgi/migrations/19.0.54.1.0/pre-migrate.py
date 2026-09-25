# -*- coding: utf-8 -*-
"""54.1.0 (2026-09-25): la ficha del proceso vuelve a cambiar de forma (sin
pestaña Conexiones) y las herencias del propio módulo guardadas en la base
(procedimiento, estructura) se revalidan contra el padre nuevo antes de
recargarse: el build reventó igual que en 54.0.0. Desde esta versión la ficha
completa vive en sgi_process_views.xml y el módulo ya no hereda su propia
vista, así que estas filas solo hay que borrarlas una vez. Idempotente."""
import logging

_logger = logging.getLogger(__name__)

STALE_VIEWS = ('sgi_process_view_form_structure', 'sgi_process_view_form_hierarchy',
               'sgi_process_view_form_procedure')


def migrate(cr, version):
    cr.execute("""
        SELECT d.id, d.res_id, d.name FROM ir_model_data d
         WHERE d.module = 'quimibond_sgi' AND d.model = 'ir.ui.view' AND d.name IN %s
    """, (STALE_VIEWS,))
    rows = cr.fetchall()
    if not rows:
        _logger.info("SGI 54.1: sin herencias propias de la ficha del proceso que borrar")
        return
    view_ids = tuple(r[1] for r in rows)
    cr.execute("DELETE FROM ir_ui_view WHERE inherit_id IN %s AND id NOT IN %s", (view_ids, view_ids))
    cr.execute("DELETE FROM ir_ui_view WHERE id IN %s", (view_ids,))
    cr.execute("DELETE FROM ir_model_data WHERE id IN %s", (tuple(r[0] for r in rows),))
    _logger.info("SGI 54.1: herencias propias de la ficha del proceso borradas: %s",
                 ", ".join(r[2] for r in rows))
