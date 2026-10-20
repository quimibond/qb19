# -*- coding: utf-8 -*-
"""Rutas de menú que se le dicen al usuario (I-009, entrega 4).

Los textos del Diagnóstico, de los avisos y de los errores decían rutas del
árbol viejo («SGI → Medición → Indicadores», «SGI → Panel → …») que ya no
existen. Todas salen de aquí, con los nombres del árbol decidido
(views/sgi_menus.xml y tools/sgi_menu_tree.txt; carpetas por capítulo desde
57.113.0).

Cada entrada es (xmlid del menú, nombres de la ruta). La prueba
tests/test_menu_tree.py sube por los padres del menú y compara los nombres de
los menús del módulo con esta ruta: si alguien renombra o mueve un menú sin
cambiar aquí, la prueba falla.

Módulo sin modelos: se puede importar a nivel de módulo desde cualquier
archivo de models/.
"""
from markupsafe import Markup, escape

from odoo import SUPERUSER_ID
from odoo.exceptions import RedirectWarning, UserError

SGI_MENU_PATHS = {
    'mapa_procesos': ('quimibond_sgi.menu_sgi_process_map',
                      ('SGI', 'Sistema', 'Mapa de procesos')),
    'mi_procedimiento': ('quimibond_sgi.menu_sgi_my_procedure',
                         ('SGI', 'Inicio', 'Mi procedimiento')),
    'documentos_vigentes': ('quimibond_sgi.menu_sgi_current_documents',
                            ('SGI', 'Inicio', 'Documentos vigentes')),
    'indicadores': ('quimibond_sgi.menu_sgi_indicators',
                    ('SGI', 'Desempeño', 'Indicadores', 'Todos los indicadores')),
    'mediciones': ('quimibond_sgi.menu_sgi_measures',
                   ('SGI', 'Desempeño', 'Indicadores', 'Mediciones')),
    'solicitudes_cambio': ('quimibond_sgi.menu_sgi_doc_changes',
                           ('SGI', 'Sistema', 'Documentos', 'Solicitudes de cambio')),
    'politica': ('quimibond_sgi.menu_sgi_policy',
                 ('SGI', 'Planeación', 'Política integral')),
    'miid': ('quimibond_sgi.menu_sgi_miid',
             ('SGI', 'Sistema', 'Manual del SGI', 'Manual (MIID)')),
    # 57.148.0: el P-G01 generado, en la misma carpeta.
    'pg01': ('quimibond_sgi.menu_sgi_pg01',
             ('SGI', 'Sistema', 'Manual del SGI', 'Control de información documentada (P-G01)')),
    'riesgos': ('quimibond_sgi.menu_sgi_risks',
                ('SGI', 'Planeación', 'Riesgos y oportunidades')),
    'requisitos_legales': ('quimibond_sgi.menu_sgi_legal',
                           ('SGI', 'Planeación', 'Requisitos legales')),
    'partes_interesadas': ('quimibond_sgi.menu_sgi_interested_parties',
                           ('SGI', 'Planeación', 'Partes interesadas')),
    'planes_emergencia': ('quimibond_sgi.menu_sgi_emergency_plans',
                          ('SGI', 'Seguridad y ambiente', 'Planes de emergencia')),
    'aspectos_ambientales': ('quimibond_sgi.menu_sgi_env_aspects',
                             ('SGI', 'Planeación', 'Aspectos ambientales')),
    'no_conformidades': ('quimibond_sgi.menu_sgi_nc',
                         ('SGI', 'Mejora', 'No conformidades')),
    'formatos_odoo': ('quimibond_sgi.menu_sgi_config_format_map',
                      ('SGI', 'Administración', 'Configuración',
                       'Formatos en documentos de Odoo')),
    'fuentes_nc': ('quimibond_sgi.menu_sgi_config_alert_sources',
                   ('SGI', 'Administración', 'Configuración', 'Fuentes de NC automáticas')),
    'ajustes': ('quimibond_sgi.menu_sgi_config_settings',
                ('SGI', 'Administración', 'Configuración', 'Ajustes')),
    'publicar_mi_procedimiento': ('quimibond_sgi.menu_sgi_my_procedure_publish',
                                  ('SGI', 'Administración', 'Publicar Mi procedimiento')),
    'tabletas_planta': ('quimibond_sgi.menu_sgi_floor_tablets',
                        ('SGI', 'Administración', 'Configuración', 'Tabletas de planta')),
    # 57.113.0: los textos del asistente de aprobaciones la decían a mano.
    'aprobaciones': ('quimibond_sgi.menu_sgi_approvals_native',
                     ('SGI', 'Administración', 'Aprobaciones del SGI')),
    # Fuera del SGI: la raíz es de otra app («Empleados»); la prueba solo
    # compara los menús del módulo.
    'rh_faltantes': ('quimibond_sgi.menu_hr_sgi_employee_gaps',
                     ('Empleados', 'Empleados', 'Empleados sin puesto o sin correo')),
    'brechas_competencia': ('quimibond_sgi.menu_sgi_competences',
                            ('Empleados', 'Competencias SGI', 'Brechas de competencia (DNC)')),
}


def sgi_menu_path(key, suffix=None):
    """«SGI → Planeación → Política integral» (con «, suffix» si se da)."""
    text = " → ".join(SGI_MENU_PATHS[key][1])
    return "%s, %s" % (text, suffix) if suffix else text


def sgi_menu_from_text(env, text, _cache=None):
    """57.165.0: el menú cuya ruta empieza el texto («SGI → Desempeño →
    Indicadores, columna Responsable» → el menú «Indicadores»), o un menú
    vacío. Se queda con la ruta más larga que coincide. ``_cache`` es un dict
    para resolver muchos textos con una sola lectura de menús."""
    Menu = env['ir.ui.menu'].sudo().with_context(lang='es_MX')
    if not text:
        return Menu
    if _cache is None:
        _cache = {}
    if 'paths' not in _cache:
        paths = {}
        for menu in Menu.search([]):
            parts = [p.strip() for p in (menu.complete_name or '').split('/') if p.strip()]
            if parts:
                paths.setdefault(" → ".join(parts), menu.id)
        _cache['paths'] = sorted(paths.items(), key=lambda kv: -len(kv[0]))
    clean = " ".join(str(text).split())
    for path, menu_id in _cache['paths']:
        if clean.startswith(path) and (len(clean) == len(path) or clean[len(path)] in ' ,.;:('):
            return Menu.browse(menu_id)
    return Menu.browse()


class SgiNoAccess(UserError):
    """57.166.0: el usuario no ve la pantalla. Quien cae a otra pantalla
    cuando la ruta no abre (Mis pendientes) no se traga este aviso."""


def sgi_action_dict(env, action):
    """57.166.0: el diccionario de cualquier acción (ventana, reporte,
    acción de cliente o de servidor) para devolverlo desde un botón. Antes
    solo se abrían ventanas y los menús de reportes («Balance de
    comprobación», «Conciliar», «Programa maestro») daban error. Una ventana
    de un modelo que el usuario no puede leer lo dice en vez de abrir una
    pantalla que Odoo le negaría."""
    action = action.sudo().exists() if action else action
    if not action:
        raise UserError("La acción ligada ya no existe.")
    if action._name == 'ir.actions.act_window' and action.res_model in env \
            and not env(su=False)[action.res_model].has_access('read'):
        raise SgiNoAccess("Su usuario no tiene acceso a «%s». Pida el acceso al Jefe MAST." % action.name)
    return action._get_action_dict()


def sgi_open_menu_action(env, menu):
    """57.165.0: la acción que abre el menú. Si el usuario no ve el menú, lo
    dice en vez de abrir una pantalla que Odoo le negaría. Lo usan el MIID,
    el Diagnóstico, Mis pendientes y «Ir a hacerlo» de las actividades.
    57.166.0: cualquier tipo de acción (antes solo ventanas) y la
    visibilidad se revisa con el usuario real aunque se llame en sudo."""
    if not menu:
        raise UserError("No hay pantalla de Odoo ligada.")
    menu = menu.sudo()
    # El superusuario (crons, pruebas) ve todo.
    if env.uid != SUPERUSER_ID and menu.id not in env(su=False)['ir.ui.menu']._visible_menu_ids():
        raise SgiNoAccess("Su usuario no tiene acceso a «%s». Pida el acceso al Jefe MAST." % " → ".join(
            p.strip() for p in (menu.with_context(lang='es_MX').complete_name or '').split('/')))
    if menu.action:
        return sgi_action_dict(env, menu.action)
    return {'type': 'ir.actions.client', 'tag': 'reload', 'params': {'menu_id': menu.id}}

def _sgi_menu_window(env, key):
    """57.166.0: (menú, acción de ventana) de la ruta ``key``, en sudo; vacíos
    si el menú o su acción no existen."""
    menu = env.ref(SGI_MENU_PATHS[key][0], raise_if_not_found=False)
    action = menu.sudo().action if menu else False
    if not action or action._name != 'ir.actions.act_window':
        return menu, False
    return menu, action


def sgi_menu_links(env, text):
    """57.166.0: el HTML de un aviso con cada ruta de ``SGI_MENU_PATHS``
    convertida en liga que abre la pantalla (``/odoo/action-<id>``). El texto
    se trata como lo trata el campo Html de la actividad (un ``str`` ya era
    HTML: no se escapa). Sin rutas, queda igual."""
    if not text or '→' not in str(text):
        return text
    out = str(text)
    links = {}
    for key in sorted(SGI_MENU_PATHS, key=lambda k: -len(sgi_menu_path(k))):
        path = sgi_menu_path(key)
        if path not in out:
            continue
        _menu, action = _sgi_menu_window(env, key)
        if not action:
            continue
        # Marcador para que una ruta más corta no vuelva a ligar el texto de
        # una más larga ya ligada.
        mark = '\x00%d\x00' % len(links)
        links[mark] = '<a href="/odoo/action-%d">%s</a>' % (action.id, escape(path))
        out = out.replace(path, mark)
    if not links:
        return text
    for mark, link in links.items():
        out = out.replace(mark, link)
    return Markup(out)


def sgi_route_error(env, message, key):
    """57.166.0: el error que dice dónde se arregla trae el botón que abre esa
    pantalla (``RedirectWarning``). Si el usuario no ve el menú, es el
    ``UserError`` de siempre. Se usa ``raise sgi_route_error(...)``."""
    menu, action = _sgi_menu_window(env, key)
    if not action or menu.id not in env(su=False)['ir.ui.menu']._visible_menu_ids():
        return UserError(message)
    return SgiRedirectWarning(message, action.id, "Abrir «%s»" % menu.with_context(lang='es_MX').name)


class SgiRedirectWarning(RedirectWarning, UserError):
    """57.166.0: ``RedirectWarning`` que además es ``UserError``: los ``except
    UserError`` y los ``assertRaises(UserError)`` lo siguen atrapando. El
    cliente web elige el diálogo con botón por el nombre
    «odoo.exceptions.RedirectWarning» (``serialize_exception``), por eso se
    renombra abajo."""

    def __init__(self, message, action, button_text, additional_context=None):
        Exception.__init__(self, message, action, button_text, additional_context)


SgiRedirectWarning.__module__ = 'odoo.exceptions'
SgiRedirectWarning.__name__ = 'RedirectWarning'
