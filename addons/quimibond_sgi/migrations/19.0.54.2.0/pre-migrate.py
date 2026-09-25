# -*- coding: utf-8 -*-
"""54.2.0 (2026-09-25): «Mi procedimiento» sin pestañas; la herencia propia
sgi_my_procedure_view_form_pr6 (lista de documentos por revisar) guardada en
la base apuntaba a un campo que ya no está en la vista. Se borra antes de
cargar los XML; el módulo ya no hereda sus propias vistas. Idempotente."""
import logging

_logger = logging.getLogger(__name__)

STALE_VIEWS = ('sgi_my_procedure_view_form_pr6', 'sgi_process_view_form_structure',
               'sgi_process_view_form_hierarchy', 'sgi_process_view_form_procedure')


def migrate(cr, version):
    cr.execute("""
        SELECT d.id, d.res_id, d.name FROM ir_model_data d
         WHERE d.module = 'quimibond_sgi' AND d.model = 'ir.ui.view' AND d.name IN %s
    """, (STALE_VIEWS,))
    rows = cr.fetchall()
    if not rows:
        _logger.info("SGI 54.2: sin herencias propias que borrar")
        return
    view_ids = tuple(r[1] for r in rows)
    cr.execute("DELETE FROM ir_ui_view WHERE inherit_id IN %s AND id NOT IN %s", (view_ids, view_ids))
    cr.execute("DELETE FROM ir_ui_view WHERE id IN %s", (view_ids,))
    cr.execute("DELETE FROM ir_model_data WHERE id IN %s", (tuple(r[0] for r in rows),))
    _logger.info("SGI 54.2: herencias propias borradas: %s", ", ".join(r[2] for r in rows))
