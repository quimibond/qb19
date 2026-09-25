# -*- coding: utf-8 -*-
"""Tipo de vista «sgi_diagram» (19.0.54.0.0): el diagrama entra al selector
de vistas de cualquier acción (lista · kanban · diagrama · formulario), como
`hierarchy` en web_hierarchy. Así un menú tiene una sola entrada y el
diagrama es un clic dentro de ella, sin duplicar menús.

Arquitectura mínima:

    <sgi_diagram kind="risk_matrix"/>
    <sgi_diagram kind="process_map" kinds="process_map,interaction_matrix,roles_map"/>

`kind` es el diagrama que abre; `kinds` (opcional) los que ofrece como
pestañas. Los datos siguen saliendo de sgi.diagram.data() (JS en
static/src/diagram/diagram_view.js).
"""
from odoo import _, fields, models

DIAGRAM_VALID_ATTRIBUTES = {'__validate__', 'kind', 'kinds', 'string', 'class', 'js_class'}


class IrUiView(models.Model):
    _inherit = 'ir.ui.view'

    type = fields.Selection(selection_add=[('sgi_diagram', "Diagrama SGI")])

    def _get_view_info(self):
        return {'sgi_diagram': {'icon': 'fa fa-sitemap'}} | super()._get_view_info()

    def _validate_tag_sgi_diagram(self, node, name_manager, node_info):
        if not node_info['validate']:
            return
        kind = node.get('kind')
        known = self.env['sgi.diagram']._kinds()
        if not kind or kind not in known:
            self._raise_view_error(_("La vista sgi_diagram necesita un atributo kind válido (%s)",
                                     ", ".join(sorted(known))), node)
        for extra in (node.get('kinds') or '').split(','):
            if extra.strip() and extra.strip() not in known:
                self._raise_view_error(_("Diagrama desconocido en kinds: %s", extra.strip()), node)
        for child in node:
            if isinstance(child.tag, str):
                self._raise_view_error(_("La vista sgi_diagram no lleva hijos, encontré <%s>", child.tag), child)
        remaining = set(node.attrib) - DIAGRAM_VALID_ATTRIBUTES
        if remaining:
            self._raise_view_error(_("Atributos no válidos en sgi_diagram: %s", ", ".join(sorted(remaining))), node)


class IrActionsActWindowView(models.Model):
    _inherit = 'ir.actions.act_window.view'

    view_mode = fields.Selection(selection_add=[('sgi_diagram', "Diagrama SGI")],
                                 ondelete={'sgi_diagram': 'cascade'})
