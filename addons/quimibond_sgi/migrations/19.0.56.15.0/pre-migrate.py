# -*- coding: utf-8 -*-
"""56.15.0 (2026-09-28): se retira ``sgi.employer.obligation`` (S4-04), sin
registros en producción; las obligaciones viven en ``qb_obligation``.

Antes de cargar los XML se borran sus vistas, su acción y su menú (por xmlid y,
por si Studio agregó algo, por modelo), junto con sus filas de
``ir_model_data``. Así la actualización no valida vistas de un modelo que ya no
está en el registro. No se borra la tabla ni se archiva nada más: el
``ir.model``, sus campos y sus accesos los limpia Odoo al terminar la
actualización. Idempotente."""
import logging

_logger = logging.getLogger(__name__)

MODEL = 'sgi.employer.obligation'
VIEW_XMLIDS = ('sgi_employer_obligation_view_list', 'sgi_employer_obligation_view_form',
               'sgi_employer_obligation_view_search')
ACTION_XMLIDS = ('sgi_employer_obligation_action',)
MENU_XMLIDS = ('menu_sgi_employer_obligations',)


def _xmlid_rows(cr, model, names):
    cr.execute("""
        SELECT id, res_id FROM ir_model_data
         WHERE module = 'quimibond_sgi' AND model = %s AND name IN %s
    """, (model, names))
    return cr.fetchall()


def migrate(cr, version):
    view_rows = _xmlid_rows(cr, 'ir.ui.view', VIEW_XMLIDS)
    action_rows = _xmlid_rows(cr, 'ir.actions.act_window', ACTION_XMLIDS)
    menu_rows = _xmlid_rows(cr, 'ir.ui.menu', MENU_XMLIDS)

    # Acciones: las del xmlid y cualquier otra ventana sobre el modelo.
    cr.execute("SELECT id FROM ir_act_window WHERE res_model = %s", (MODEL,))
    action_ids = {r[0] for r in cr.fetchall()} | {r[1] for r in action_rows}

    # Menús: los del xmlid y los que abren alguna de esas acciones.
    menu_ids = {r[1] for r in menu_rows}
    if action_ids:
        refs = tuple('ir.actions.act_window,%d' % a for a in action_ids)
        cr.execute("SELECT id FROM ir_ui_menu WHERE action IN %s", (refs,))
        menu_ids |= {r[0] for r in cr.fetchall()}
    if menu_ids:
        cr.execute("DELETE FROM ir_ui_menu WHERE id IN %s", (tuple(menu_ids),))
    if action_ids:
        cr.execute("DELETE FROM ir_act_window WHERE id IN %s", (tuple(action_ids),))

    # Vistas: las del xmlid y cualquier otra del modelo (Studio), con sus
    # herencias primero.
    cr.execute("SELECT id FROM ir_ui_view WHERE model = %s", (MODEL,))
    view_ids = {r[0] for r in cr.fetchall()} | {r[1] for r in view_rows}
    if view_ids:
        view_ids = tuple(view_ids)
        cr.execute("DELETE FROM ir_ui_view WHERE inherit_id IN %s AND id NOT IN %s", (view_ids, view_ids))
        cr.execute("DELETE FROM ir_ui_view WHERE id IN %s", (view_ids,))

    data_ids = tuple(r[0] for r in view_rows + action_rows + menu_rows)
    if data_ids:
        cr.execute("DELETE FROM ir_model_data WHERE id IN %s", (data_ids,))
    _logger.info("SGI 56.15: %s retirado (vistas %d, acciones %d, menús %d, xmlids %d)",
                 MODEL, len(view_ids), len(action_ids), len(menu_ids), len(data_ids))
