# -*- coding: utf-8 -*-
"""19.0.57.113.0 — Menús por capítulos del MIID (plan
docs/superpowers/plans/2026-10-06-sgi-57-113-0-menus.md).

Los menús los mueve el XML (views/sgi_menus.xml); aquí no se toca ningún
ir.ui.menu ni se borra nada. Este paso lleva a producción lo que es
``noupdate`` o ya está guardado con la ruta vieja, solo donde nadie lo editó:

1. MIID: las 13 secciones que dicen una ruta del menú, por
   ``sgi.miid.section._sgi_seed_update`` (solo las que nunca se editaron).
2. Indicador de salud SG-01: su fuente, si sigue como se sembró.
3. «Dónde se ejecuta» que sembró 57.54.0 (sgi_sst_links): solo donde sigue
   idéntico al texto sembrado.
4. Re-sello de «Mi procedimiento» (Q8): la ruta del menú entra en la huella;
   un documento recibe la huella nueva solo si, con las rutas viejas, la
   huella de hoy es EXACTAMENTE la guardada (nadie vuelve a firmar por un
   cambio de menú). Con nota en el chatter de cada documento.
5. Un aviso al Jefe MAST para difundir el menú nuevo.

Cada paso es idempotente y deja su línea en el log."""
import logging
from datetime import timedelta

from markupsafe import Markup

from odoo import SUPERUSER_ID, api, fields
from odoo.addons.quimibond_sgi.models.sgi_sst_links import SGI_SST_ACTIVITY_LINKS

_logger = logging.getLogger(__name__)

TAG = "57.113.0"

MIID_BODY_XMLIDS = (
    'sgi_miid_section_08_referencias',
    'sgi_miid_section_09_terminos',
    'sgi_miid_section_12_partes',
    'sgi_miid_section_14_procesos',
    'sgi_miid_section_16_liderazgo',
    'sgi_miid_section_17_politica',
    'sgi_miid_section_20_riesgos',
    'sgi_miid_section_21_objetivos',
    'sgi_miid_section_28_informacion',
    'sgi_miid_section_34_seguimiento',
    'sgi_miid_section_35_auditoria',
    'sgi_miid_section_36_revision',
    'sgi_miid_section_42_correspondencia',
)
MIID_REASON = "las rutas nuevas del menú (menús por capítulos)"

HEALTH_SOURCE_OLD = "SGI → Procesos (estado del proceso)"
HEALTH_SOURCE_NEW = "SGI → Sistema → Mapa de procesos (estado del proceso)"

# «Dónde se ejecuta en Odoo» como lo sembró 57.54.0 (models/sgi_sst_links.py
# de 57.110.0). El texto nuevo sale de SGI_SST_ACTIVITY_LINKS.
OLD_SST_WHERE = {
    'E2.23': "SGI → Seguridad y ambiente → Aspectos ambientales",
    'E2.30': "SGI → Dirección → Objetivos integrales (objetivos de SST con su plan de "
             "acciones = programa anual)",
    'E2.34': "SGI → Seguridad y ambiente → Aspectos ambientales (control operacional de "
             "los significativos) · SGI → Dirección → Riesgos y oportunidades (IPER)",
    'E2.37': "SGI → Mejora → Auditorías → Auditorías",
}

OLD_MENU_PATHS = {
    # Menús con acción cuya ruta cambia (de tools/sgi_menu_tree.txt de 57.110.0).
    'menu_sgi_process_map': "SGI/Procesos/Mapa de procesos",
    'menu_sgi_activities': "SGI/Procesos/Actividades",
    'menu_sgi_deliverable_list': "SGI/Procesos/Entregables",
    'menu_sgi_process_flows': "SGI/Procesos/Flujos entre procesos",
    'menu_sgi_analysis_who': "SGI/Procesos/Matriz de responsabilidades",
    'menu_sgi_jobs_roles': "SGI/Procesos/Puestos y procesos",
    'menu_sgi_machine_sheets': "SGI/Procesos/Fichas de proceso por máquina",
    'menu_sgi_dropbox_search': "SGI/Procesos/Del Dropbox a Odoo/Buscador por clave anterior",
    'menu_sgi_dropbox_procedures': "SGI/Procesos/Del Dropbox a Odoo/Procedimientos anteriores",
    'menu_sgi_migration': "SGI/Procesos/Del Dropbox a Odoo/Formatos y documentos anteriores",
    'menu_sgi_dropbox_routines': "SGI/Procesos/Del Dropbox a Odoo/Rutina por rutina",
    'menu_sgi_dropbox_progress': "SGI/Procesos/Del Dropbox a Odoo/Avance de la transición",
    'menu_sgi_audit_programs': "SGI/Mejora/Auditorías/Programa",
    'menu_sgi_audit_list': "SGI/Mejora/Auditorías/Auditorías",
    'menu_sgi_audit_findings': "SGI/Mejora/Auditorías/Hallazgos",
    'menu_sgi_env_aspects': "SGI/Seguridad y ambiente/Aspectos ambientales",
    'menu_sgi_dashboard_health': "SGI/Dirección/Tablero",
    'menu_sgi_mgmt_review': "SGI/Dirección/Revisión por la dirección",
    'menu_sgi_miid': "SGI/Dirección/Manual del SGI (MIID)",
    'menu_sgi_policy': "SGI/Dirección/Política integral",
    'menu_sgi_objectives': "SGI/Dirección/Objetivos integrales",
    'menu_sgi_risks': "SGI/Dirección/Riesgos y oportunidades",
    'menu_sgi_legal': "SGI/Dirección/Requisitos legales",
    'menu_sgi_legal_evaluations': "SGI/Dirección/Evaluaciones de cumplimiento legal",
    'menu_sgi_interested_parties': "SGI/Dirección/Partes interesadas",
    'menu_sgi_satisfaction': "SGI/Dirección/Satisfacción del cliente",
    'menu_sgi_documents': "SGI/Administración SGI/Documentos/Documentos",
    'menu_sgi_master_list': "SGI/Administración SGI/Documentos/Lista maestra",
    'menu_sgi_external_docs': "SGI/Administración SGI/Documentos/Documentos externos",
    'menu_sgi_doc_changes': "SGI/Administración SGI/Documentos/Solicitudes de cambio",
    'menu_sgi_config_doc_types': "SGI/Administración SGI/Documentos/Tipos de documento",
    'menu_sgi_indicators': "SGI/Administración SGI/Indicadores/Indicadores",
    'menu_sgi_indicator_wizard': "SGI/Administración SGI/Indicadores/Nuevo indicador",
    'menu_sgi_measures': "SGI/Administración SGI/Indicadores/Mediciones",
    'menu_sgi_measures_split': "SGI/Administración SGI/Indicadores/Mediciones por equipo o mercado",
    'menu_sgi_approvals_native': "SGI/Administración SGI/Aprobaciones del SGI",
    'menu_sgi_diagnostic': "SGI/Administración SGI/Diagnóstico/Diagnóstico del SGI",
    'menu_sgi_compliance': "SGI/Administración SGI/Diagnóstico/Cumplimiento de procedimientos",
    'menu_sgi_spec_gaps': "SGI/Administración SGI/Diagnóstico/Faltantes de especificación",
    'menu_sgi_activity_executions': "SGI/Administración SGI/Diagnóstico/Registro de cumplimiento",
    'menu_sgi_measure_reviews': "SGI/Administración SGI/Diagnóstico/Revisiones de medición",
    'menu_sgi_my_procedure_publish': "SGI/Administración SGI/Firmas de lectura/Publicar Mi procedimiento",
    'menu_sgi_config_acks': "SGI/Administración SGI/Firmas de lectura/Acuses de lectura",
    'menu_sgi_config_load': "SGI/Administración SGI/Configuración/Cargar catálogo",
    'menu_sgi_job_families': "SGI/Administración SGI/Configuración/Familias de puesto",
    'menu_sgi_config_settings': "SGI/Administración SGI/Configuración/Ajustes",
    'menu_sgi_config_areas': "SGI/Administración SGI/Configuración/Áreas",
    'menu_sgi_config_norms': "SGI/Administración SGI/Configuración/Normas",
    'menu_sgi_config_clauses': "SGI/Administración SGI/Configuración/Cláusulas",
    'menu_sgi_config_risk_cat': "SGI/Administración SGI/Configuración/Categorías de riesgo",
    'menu_sgi_checklist_templates': "SGI/Administración SGI/Configuración/Checklists de planta y unidades",
    'menu_sgi_floor_tablets': "SGI/Administración SGI/Configuración/Tabletas de planta",
    'menu_sgi_config_format_map': "SGI/Administración SGI/Configuración/Formatos en documentos de Odoo",
    'menu_sgi_config_alert_sources': "SGI/Administración SGI/Configuración/Fuentes de NC automáticas",
    'menu_sgi_config_ppap_elements': "SGI/Administración SGI/Configuración/Elementos PPAP",
    'menu_sgi_config_company_fix': "SGI/Administración SGI/Configuración/Empresa en documentos controlados",
    'menu_sgi_config_env_aspect_transfer': "SGI/Administración SGI/Configuración/Traspaso de riesgos ambientales",
    'menu_sgi_config_work_permit_skills': "SGI/Administración SGI/Configuración/Competencias por tipo de permiso",
    'menu_sgi_config_indicator_sources': "SGI/Administración SGI/Configuración/Fuentes de indicadores",
    # Archivados en su lugar: su ruta también cambia (Administración SGI →
    # Administración) y una actividad podría apuntarles.
    'menu_sgi_activity_methods': "SGI/Administración SGI/Diagnóstico/Cobertura de medición",
    'menu_sgi_week_stats': "SGI/Administración SGI/Diagnóstico/Cumplimiento semanal",
    'menu_sgi_menu_mismatch': "SGI/Administración SGI/Diagnóstico/Medición por revisar",
}

NOTICE_KEY = 'menus_capitulos_57113'
NOTICE_SUMMARY = "Difundir el menú nuevo del SGI"
NOTICE_NOTE = Markup(
    "<p>El menú del SGI sigue ahora los capítulos de la norma y del MIID (57.113.0). "
    "Avise al personal y a los dueños de proceso:</p>"
    "<ul>"
    "<li><b>Procesos</b> se llama <b>Sistema</b> (mapa, actividades, Documentos, el MIID y "
    "«Del Dropbox a Odoo»).</li>"
    "<li><b>Dirección</b> se llama <b>Planeación</b> (política, objetivos, partes interesadas, "
    "riesgos, aspectos ambientales y requisitos legales).</li>"
    "<li><b>Desempeño</b> es nueva: Tablero, Indicadores, Satisfacción del cliente, Auditorías y "
    "Revisión por la dirección.</li>"
    "<li><b>Mejora</b> queda con NC, acciones correctivas, reclamaciones, mejora continua, "
    "lecciones y quejas del personal.</li>"
    "<li><b>Administración SGI</b> se llama <b>Administración</b>: Diagnóstico, Aprobaciones, "
    "Publicar Mi procedimiento, Transición y Configuración (con Tipos de documento).</li>"
    "<li>«Cobertura de medición», «Cumplimiento semanal» y «Medición por revisar» ya no son menú: "
    "son la agrupación por método de medición en Cumplimiento de procedimientos, el botón "
    "«Por semana» de Registro de cumplimiento y el filtro «Medición por revisar» de Faltantes de "
    "especificación.</li>"
    "</ul>"
    "<p>Nadie perdió ninguna pantalla. Los manuales por rol ya traen las rutas nuevas "
    "(docs/sgi/usuarios/ y docs/sgi/administracion/manual-jefe-mast.md).</p>")


def _miid(env):
    result = env['sgi.miid.section']._sgi_seed_update(MIID_BODY_XMLIDS, {}, TAG, reason=MIID_REASON)
    _logger.info("SGI %s: secciones del MIID con rutas del menú: %s", TAG, result)


def _health_source(env):
    indicator = env.ref('quimibond_sgi.sgi_ind_salud_procesos', raise_if_not_found=False)
    if not indicator:
        _logger.info("SGI %s: no existe SG-01; se salta su fuente.", TAG)
        return
    if (indicator.source or '').strip() == HEALTH_SOURCE_OLD:
        indicator.sudo().write({'source': HEALTH_SOURCE_NEW})
        _logger.info("SGI %s: fuente de SG-01 con la ruta nueva.", TAG)
    else:
        _logger.info("SGI %s: la fuente de SG-01 ya no es la sembrada («%s»); se respeta.",
                     TAG, indicator.source or '')


def _sst_where(env):
    company = env['sgi.config']._sgi_company()
    new_where = {number: where for number, _menu, where, _codes in SGI_SST_ACTIVITY_LINKS}
    Activity = env['sgi.process.activity'].sudo().with_context(active_test=False)
    changed = kept = 0
    for number, old in OLD_SST_WHERE.items():
        new = new_where.get(number)
        if not new or new == old:
            continue
        for activity in Activity.search([('number', '=', number), ('company_id', '=', company.id)]):
            if (activity.odoo_ref or '').strip() != old:
                kept += 1
                _logger.info("SGI %s: %s (%d) dice dónde se ejecuta otra cosa («%s»); se respeta.",
                             TAG, number, activity.id, activity.odoo_ref or '')
                continue
            activity.write({'odoo_ref': new})
            activity.message_post(body=Markup(
                "%s: «Dónde se ejecuta en Odoo» con la ruta del menú nuevo (menús por capítulos): "
                "<b>%s</b>.") % (TAG, new))
            changed += 1
    _logger.info("SGI %s: «Dónde se ejecuta» sembrado: %d actualizado(s), %d respetado(s).",
                 TAG, changed, kept)


def _reseal(env):
    old_paths = {}
    for xmlid, path in OLD_MENU_PATHS.items():
        menu = env.ref('quimibond_sgi.' + xmlid, raise_if_not_found=False)
        if menu:
            old_paths[menu] = path
        else:
            _logger.info("SGI %s: no existe el menú %s; se salta en el re-sello.", TAG, xmlid)
    result = env['hr.job']._sgi_mp_reseal_menu_moves(old_paths, tag=TAG)
    _logger.info("SGI %s: re-sello de Mi procedimiento: %s", TAG, result)


def _notice(env):
    company = env['sgi.config']._sgi_company()
    miid = env['sgi.miid'].sudo().search([('company_id', '=', company.id)], limit=1)
    if not miid:
        _logger.info("SGI %s: no hay Manual del SGI en %s; no se agenda el aviso del menú nuevo.",
                     TAG, company.display_name)
        return
    Cron = env['sgi.cron']
    manager_id = Cron._sgi_manager_user_id()
    if not manager_id:
        _logger.info("SGI %s: no hay Jefe MAST; no se agenda el aviso del menú nuevo.", TAG)
        return
    Cron._sgi_schedule(miid, NOTICE_SUMMARY, NOTICE_NOTE, manager_id,
                       date_deadline=fields.Date.context_today(miid) + timedelta(days=7),
                       key=NOTICE_KEY, anywhere=True)
    _logger.info("SGI %s: aviso «%s» al usuario %s.", TAG, NOTICE_SUMMARY, manager_id)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    _miid(env)
    _health_source(env)
    _sst_where(env)
    _reseal(env)
    _notice(env)
