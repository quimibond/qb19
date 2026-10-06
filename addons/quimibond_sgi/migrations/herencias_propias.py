# -*- coding: utf-8 -*-
"""Borra de la base las herencias de vistas del propio SGI que se integraron
a su vista padre (auditoría 2026-09, A-008; entrega 5, `e5-herencias-propias`).

No es un script de migración: está en la raíz de ``migrations/`` y Odoo lo
ignora. Lo cargan por ruta los ``pre-migrate.py`` de 57.22.0 en adelante,
igual que ``mudanza.py``.

Por qué en el pre-migrate (regla de CLAUDE.md, builds 54.0.0 y 54.1.0): al
cargar la vista padre nueva, Odoo revalida las herencias que YA están en la
base antes de llegar al archivo que las quitó. La herencia vieja, aplicada
sobre un padre que ya trae su contenido, duplica botones o no encuentra su
ancla y el build revienta. Se borran antes de cargar los XML.

Qué borra: solo vistas cuyo XML ID es ``quimibond_sgi.<nombre>`` de la lista
y que heredan una vista (``inherit_id`` no nulo); su fila de
``ir_model_data``. Una vista de OTRO módulo que heredara una de ellas no se
borra: se re-apunta al padre de la borrada (su contenido ya vive ahí) y se
avisa en el log. Idempotente: si ya no están, no hace nada.
"""
import logging

_logger = logging.getLogger(__name__)


def borrar_herencias(cr, version_label, names):
    cr.execute("""
        SELECT d.id, d.res_id, d.name, v.inherit_id
          FROM ir_model_data d
          JOIN ir_ui_view v ON v.id = d.res_id
         WHERE d.module = 'quimibond_sgi' AND d.model = 'ir.ui.view'
           AND d.name IN %s AND v.inherit_id IS NOT NULL
    """, (tuple(names),))
    rows = cr.fetchall()
    if not rows:
        _logger.info("SGI %s: sin herencias propias que borrar (ya no están).", version_label)
        return []
    view_ids = tuple(r[1] for r in rows)
    parent_of = {r[1]: r[3] for r in rows}
    # Hijas de otra vista que no está en la lista: se re-apuntan al padre.
    cr.execute("""
        SELECT v.id, v.inherit_id, COALESCE(d.module || '.' || d.name, v.name)
          FROM ir_ui_view v
          LEFT JOIN ir_model_data d ON d.model = 'ir.ui.view' AND d.res_id = v.id
         WHERE v.inherit_id IN %s AND v.id NOT IN %s
    """, (view_ids, view_ids))
    for child_id, old_parent, label in cr.fetchall():
        new_parent = parent_of[old_parent]
        while new_parent in parent_of:
            new_parent = parent_of[new_parent]
        cr.execute("UPDATE ir_ui_view SET inherit_id = %s WHERE id = %s", (new_parent, child_id))
        _logger.warning("SGI %s: la vista %s (id %s) heredaba una herencia propia que se "
                        "borra; ahora hereda la vista %s.", version_label, label, child_id, new_parent)
    # Hijas antes que madres (cadenas dentro de la lista): el FK es restrict.
    remaining = set(view_ids)
    while remaining:
        cr.execute("SELECT id FROM ir_ui_view WHERE id IN %s AND id NOT IN "
                   "(SELECT inherit_id FROM ir_ui_view WHERE inherit_id IN %s)",
                   (tuple(remaining), tuple(remaining)))
        leaves = [r[0] for r in cr.fetchall()]
        if not leaves:
            raise RuntimeError("SGI %s: ciclo de herencias en %s" % (version_label, remaining))
        cr.execute("DELETE FROM ir_ui_view WHERE id IN %s", (tuple(leaves),))
        remaining -= set(leaves)
    cr.execute("DELETE FROM ir_model_data WHERE id IN %s", (tuple(r[0] for r in rows),))
    borradas = sorted("%s (id %s)" % (r[2], r[1]) for r in rows)
    _logger.info("SGI %s: %d herencias propias borradas (integradas a su padre): %s.",
                 version_label, len(rows), ", ".join(borradas))
    return borradas
