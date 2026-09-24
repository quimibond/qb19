# -*- coding: utf-8 -*-
"""Limpieza antes de producción (CEO, 2026-09-24). Deja la base igual que
`main`: lo que ya no está en el código se borra aquí de forma explícita
(menús, acciones y vistas por xmlid, y los dos menús de Studio con su
acción), lo que tiene datos se religa (documentos, riesgos e indicador de
los procesos P-*/MP-* viejos) y nada se archiva. Corre una sola vez; todo
es idempotente por si el update se repite.

Lo que se escribe en el log (grep "SGI 45"):
- cada xmlid borrado y cada menú/acción de Studio borrado,
- las vistas de Studio sobre modelos sgi.* (solo se listan: no se borran
  sin ver qué hacen),
- los dos campos Studio de approval.request (se borran solo si están vacíos),
- el resumen del religado y, uno por uno, los documentos de MP-ADM para
  revisarlos a mano.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

_STUDIO_FIELDS = (
    ('approval.request', 'x_studio_maquina'),
    ('approval.request', 'x_studio_maquina_o_equipo'),
)


def _remove_xmlids(env):
    from odoo.addons.quimibond_sgi.models.sgi_cleanup import SGI_REMOVED_XMLIDS
    Data = env['ir.model.data'].sudo()
    for xmlid in SGI_REMOVED_XMLIDS:
        module, name = xmlid.split('.', 1)
        data = Data.search([('module', '=', module), ('name', '=', name)], limit=1)
        if not data:
            continue
        model = data.model
        record = env[model].sudo().browse(data.res_id).exists()
        if record:
            # unlink() borra también su ir.model.data: leer todo antes.
            record.unlink()
        if data.exists():
            data.unlink()
        _logger.info("SGI 45: borrado %s (%s).", xmlid, model)


def _remove_studio_menus(env):
    """Los dos «Tipo de Documento» de Studio bajo el raíz del SGI y su acción.
    Se reconocen porque NO tienen xmlid del módulo."""
    root = env.ref('quimibond_sgi.menu_sgi_root', raise_if_not_found=False)
    if not root:
        return
    Menu = env['ir.ui.menu'].sudo().with_context(active_test=False)
    Data = env['ir.model.data'].sudo()
    menus = Menu.search([('parent_id', 'child_of', root.id),
                         ('name', 'in', ('Tipo de Documento', 'Configuración/Tipo de Documento'))])
    for menu in menus:
        if Data.search_count([('model', '=', 'ir.ui.menu'), ('res_id', '=', menu.id),
                              ('module', '=', 'quimibond_sgi')]):
            continue
        action = menu.action
        _logger.info("SGI 45: borrado menú de Studio %s (id %s, creado por %s).",
                     menu.complete_name, menu.id, menu.create_uid.name)
        menu.unlink()
        if action and action._name == 'ir.actions.act_window' and not Data.search_count([
                ('model', '=', 'ir.actions.act_window'), ('res_id', '=', action.id),
                ('module', '=', 'quimibond_sgi')]):
            _logger.info("SGI 45: borrada acción de Studio %s (id %s).", action.name, action.id)
            action.sudo().unlink()


def _log_studio_views(env):
    views = env['ir.ui.view'].sudo().with_context(active_test=False).search(
        [('model', 'like', 'sgi.%')])
    data = env['ir.model.data'].sudo().search([
        ('model', '=', 'ir.ui.view'), ('res_id', 'in', views.ids),
        ('module', '=', 'studio_customization')])
    studio = views.browse(data.mapped('res_id'))
    if not studio:
        _logger.info("SGI 45: sin vistas de Studio sobre modelos sgi.*.")
    for view in studio:
        _logger.info("SGI 45: VISTA STUDIO %s · %s · hereda de %s · activa=%s (id %s)",
                     view.name, view.model, view.inherit_id.name or '-', view.active, view.id)


def _remove_empty_studio_fields(env):
    Field = env['ir.model.fields'].sudo()
    for model_name, field_name in _STUDIO_FIELDS:
        field = Field.search([('model', '=', model_name), ('name', '=', field_name),
                              ('state', '=', 'manual')], limit=1)
        if not field or model_name not in env:
            continue
        Model = env[model_name].sudo().with_context(active_test=False)
        if field_name not in Model._fields:
            continue
        used = Model.search_count([(field_name, '!=', False)])
        if used:
            _logger.warning("SGI 45: %s.%s tiene %d registro(s) con dato; NO se borra.",
                            model_name, field_name, used)
            continue
        try:
            with env.cr.savepoint():
                field.unlink()
        except Exception as exc:  # una vista de Studio que lo use lo impide
            _logger.warning("SGI 45: %s.%s no se pudo borrar (%s); se queda.",
                            model_name, field_name, exc)
            continue
        _logger.info("SGI 45: borrado campo Studio vacío %s.%s.", model_name, field_name)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    _remove_xmlids(env)
    _remove_studio_menus(env)
    _log_studio_views(env)
    _remove_empty_studio_fields(env)
    summary = env['sgi.process']._sgi_relink_from_archived()
    _logger.info("SGI 45: religado terminado: %s.", summary)
    # Punto 5, por si algún proceso ya estaba vigente antes de esta versión.
    vigentes = env['sgi.process'].search([('state', '=', 'vigente')])
    vigentes._sgi_obsolete_replaced_documents()
    offenders = env['ir.ui.menu']._sgi_menu_tree_offenders()
    if offenders:
        _logger.warning("SGI 45: quedan %d menú(s) fuera de las cinco entradas: %s",
                        len(offenders), ", ".join(offenders.mapped('complete_name')))
    else:
        _logger.info("SGI 45: el árbol del SGI queda con sus cinco entradas.")
