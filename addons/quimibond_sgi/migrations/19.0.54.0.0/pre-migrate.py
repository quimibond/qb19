# -*- coding: utf-8 -*-
"""54.0.0 (2026-09-25): la ficha del proceso cambió de forma (pestañas →
botones). Odoo revalida las vistas heredadas que YA están en la base en
cuanto carga la vista padre nueva, antes de llegar al archivo que las
corrige; la de estructura tenía xpaths sobre `indicator_ids`/`risk_ids` que
ya no existen y el build de main reventó («no puede ser localizado en la
vista padre»). Se borran aquí las vistas heredadas del módulo sobre esa
ficha: las que siguen en el código se vuelven a crear al cargar los XML;
la de hierarchy (botones de diagrama) ya no existe. Idempotente."""
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
        _logger.info("SGI 54.0: sin vistas heredadas de la ficha del proceso que borrar")
        return
    view_ids = tuple(r[1] for r in rows)
    # Primero las que heredan de estas (estructura hereda de procedimiento).
    cr.execute("DELETE FROM ir_ui_view WHERE inherit_id IN %s AND id NOT IN %s", (view_ids, view_ids))
    cr.execute("DELETE FROM ir_ui_view WHERE id IN %s", (view_ids,))
    cr.execute("DELETE FROM ir_model_data WHERE id IN %s", (tuple(r[0] for r in rows),))
    _logger.info("SGI 54.0: vistas heredadas de la ficha del proceso borradas para recrearlas: %s",
                 ", ".join(r[2] for r in rows))
