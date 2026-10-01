# -*- coding: utf-8 -*-
"""57.90.0: cada versión que se instala en producción deja su solicitud de
cambio (S6.06 a S6.08, indicador S6-04).

Las versiones 57.14 a 57.89 de quimibond_sgi se instalaron sin solicitud
«Cambio en Odoo (S6)», y S6-04 («Cambios instalados con prueba y
aprobación») no las veía. Ahora, cada día en el cron de NC,
``_sgi_register_deploys`` compara
la versión instalada de los módulos del repositorio con la última que
registró (parámetro ``quimibond_sgi.deploy_versions``). Por cada módulo que
cambió crea una solicitud en borrador en la categoría «Cambio en Odoo (S6)»
con el módulo, la versión, la entrada del CHANGELOG y el commit desplegado.
La solicitud cuenta en el denominador de S6-04 y solo en el numerador cuando
se aprueba.

Nada se envía ni se aprueba solo. La categoría pide la evidencia de la prueba
en un adjunto, y quien la envía es el dueño de la solicitud.

- Solo en producción: una base neutralizada (copias de Odoo.sh) no crea
  nada.
- La primera corrida solo guarda las versiones de base.
- Parámetros:
  - ``quimibond_sgi.change_approval_category_id``: por omisión, la categoría
    llamada «Cambio en Odoo (S6)».
  - ``quimibond_sgi.change_request_owner_id``: por omisión, el primer
    aprobador de la categoría.
"""
import json
import logging
import os
import re

from markupsafe import Markup, escape

from odoo import api, fields, models
from odoo.modules.module import get_module_path

_logger = logging.getLogger(__name__)

DEPLOY_VERSIONS_PARAM = 'quimibond_sgi.deploy_versions'
CHANGE_CATEGORY_PARAM = 'quimibond_sgi.change_approval_category_id'
CHANGE_OWNER_PARAM = 'quimibond_sgi.change_request_owner_id'
CHANGE_CATEGORY_NAME = 'Cambio en Odoo (S6)'
REPO_URL = 'https://github.com/quimibond/qb19'


def _git_root(path):
    """Carpeta del repositorio git que contiene ``path`` (None si no hay)."""
    current = os.path.abspath(path)
    while True:
        if os.path.isdir(os.path.join(current, '.git')):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def _git_head(root):
    """Commit desplegado (sha completo) leyendo .git sin ejecutar git."""
    if not root:
        return None
    git_dir = os.path.join(root, '.git')
    try:
        with open(os.path.join(git_dir, 'HEAD'), encoding='utf-8') as handle:
            head = handle.read().strip()
        if not head.startswith('ref: '):
            return head or None
        ref = head[5:]
        ref_file = os.path.join(git_dir, ref)
        if os.path.isfile(ref_file):
            with open(ref_file, encoding='utf-8') as handle:
                return handle.read().strip() or None
        packed = os.path.join(git_dir, 'packed-refs')
        if os.path.isfile(packed):
            with open(packed, encoding='utf-8') as handle:
                for line in handle:
                    parts = line.strip().split(' ')
                    if len(parts) == 2 and parts[1] == ref:
                        return parts[0]
    except OSError:
        return None
    return None


def _changelog_entry(module_path, version):
    """Texto de la sección «## <versión>» del CHANGELOG del módulo ('' si no
    hay)."""
    path = os.path.join(module_path or '', 'CHANGELOG.md')
    if not module_path or not os.path.isfile(path):
        return ''
    try:
        with open(path, encoding='utf-8') as handle:
            text = handle.read()
    except OSError:
        return ''
    match = re.search(r'^## %s\b.*?$(.*?)(?=^## |\Z)' % re.escape(version), text,
                      re.MULTILINE | re.DOTALL)
    return match.group(1).strip() if match else ''


class SgiCronDeployChange(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def _sgi_repo_modules(self):
        """{módulo instalado del repositorio: (versión, ruta)}. Del
        repositorio = su carpeta está dentro del mismo git que quimibond_sgi;
        sin git, los de autor Quimibond."""
        root = _git_root(get_module_path('quimibond_sgi') or '')
        result = {}
        modules = self.env['ir.module.module'].sudo().search([('state', '=', 'installed')])
        for module in modules:
            path = get_module_path(module.name)
            if not path:
                continue
            inside = (root and os.path.abspath(path).startswith(root + os.sep)) or (
                not root and 'quimibond' in (module.author or '').lower())
            if inside and module.latest_version:
                result[module.name] = (module.latest_version, path)
        return result

    @api.model
    def _sgi_change_category(self):
        Category = self.env['approval.category'].sudo()
        param = self.env['ir.config_parameter'].sudo().get_param(CHANGE_CATEGORY_PARAM)
        if param and str(param).isdigit():
            category = Category.browse(int(param)).exists()
            if category:
                return category
        return Category.search([('name', '=', CHANGE_CATEGORY_NAME)], limit=1)

    @api.model
    def _sgi_change_owner(self, category):
        param = self.env['ir.config_parameter'].sudo().get_param(CHANGE_OWNER_PARAM)
        if param and str(param).isdigit():
            user = self.env['res.users'].sudo().browse(int(param)).exists()
            if user:
                return user
        return category.approver_ids.user_id[:1] or self.env.user

    @api.model
    def _sgi_register_deploys(self, force=False):
        """Crea una solicitud de cambio en borrador por cada módulo del
        repositorio cuya versión instalada cambió desde la última corrida.
        ``force`` corre también en una base neutralizada (pruebas).
        Devuelve las solicitudes creadas."""
        Request = self.env['approval.request'].sudo()
        Param = self.env['ir.config_parameter'].sudo()
        if not force and Param.get_param('database.is_neutralized'):
            return Request
        category = self._sgi_change_category()
        if not category:
            return Request
        current = self._sgi_repo_modules()
        try:
            known = json.loads(Param.get_param(DEPLOY_VERSIONS_PARAM) or '{}')
        except ValueError:
            known = {}
        Param.set_param(DEPLOY_VERSIONS_PARAM, json.dumps(
            {name: version for name, (version, _path) in sorted(current.items())}))
        if not known:
            _logger.info("SGI despliegues: versiones de base guardadas (%s módulos).", len(current))
            return Request
        owner = self._sgi_change_owner(category)
        commit = _git_head(_git_root(get_module_path('quimibond_sgi') or ''))
        created = Request
        for name, (version, path) in sorted(current.items()):
            previous = known.get(name)
            if previous == version:
                continue
            created |= Request.create(self._sgi_deploy_request_vals(
                category, owner, name, previous, version, path, commit))
        if created:
            _logger.info("SGI despliegues: %s solicitudes de cambio creadas: %s",
                         len(created), ', '.join(created.mapped('name')))
        return created

    @api.model
    def _sgi_deploy_request_vals(self, category, owner, name, previous, version, path, commit):
        entry = _changelog_entry(path, version)
        today = fields.Date.context_today(self)
        reference = "%s %s" % (name, version)
        if commit:
            reference += " — commit %s" % commit[:10]
        parts = [Markup("<p><b>Instalado en producción</b> el %s: módulo <b>%s</b> %s → %s.</p>") % (
            today, name, previous or "nuevo", version)]
        if entry:
            parts.append(Markup("<p><b>Qué cambia</b> (CHANGELOG):</p>"))
            parts.extend(Markup("<p>%s</p>") % Markup("<br/>").join(escape(line) for line in block.splitlines())
                         for block in entry.split("\n\n") if block.strip())
        if commit:
            url = "%s/commit/%s" % (REPO_URL, commit)
            parts.append(Markup('<p><b>Código:</b> <a href="%s">%s</a> (GitHub muestra el PR '
                                'que lo trajo).</p>') % (url, url))
        parts.append(Markup(
            "<p>Solicitud creada sola al detectar la versión nueva. Adjunte la "
            "evidencia de la prueba en la copia de pruebas, agregue como aprobador "
            "al dueño del proceso afectado y envíela.</p>"))
        return {
            'name': "Despliegue %s %s" % (name, version),
            'category_id': category.id,
            'request_owner_id': owner.id,
            'reference': reference,
            'reason': Markup('').join(parts),
        }
