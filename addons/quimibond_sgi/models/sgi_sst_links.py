# -*- coding: utf-8 -*-
"""57.54.0 (bloque 1 de formularios 5/5): liga las actividades críticas de
seguridad, salud y ambiente del inventario de formularios (2026-09-30, §3.3)
a la pantalla de Odoo y al formato donde se ejecutan.

Cada renglón localiza la actividad por su numeral (en la empresa del SGI), el
menú por XML ID y los formatos por su clave vigente (los documentos del
Dropbox no tienen XML ID). Solo escribe lo que está vacío: si MAST ya capturó
un menú, un texto o formatos, se respeta y queda en el log. Idempotente: la
segunda corrida no cambia nada.
"""
import logging

from odoo import api, models

_logger = logging.getLogger(__name__)

# numeral → (XML ID del menú, «Dónde se ejecuta en Odoo», claves de formato)
SGI_SST_ACTIVITY_LINKS = (
    ('E2.18', 'quimibond_sgi.menu_sgi_work_permits',
     "SGI → Seguridad y ambiente → Permisos de trabajo de alto riesgo", ()),
    ('E2.21', 'quimibond_sgi.menu_sgi_emergency_plans',
     "SGI → Seguridad y ambiente → Planes de emergencia (el programa interno "
     "de Protección Civil se revalida en su portal)", ()),
    ('E2.23', 'quimibond_sgi.menu_sgi_env_aspects',
     "SGI → Seguridad y ambiente → Aspectos ambientales", ()),
    ('E2.28', 'quimibond_sgi.menu_sgi_incidents',
     "SGI → Seguridad y ambiente → Incidentes y accidentes (agrupar por mes y tipo "
     "para la estadística)", ('F-P-S02-01', 'F-P-S02-02')),
    ('E2.30', 'quimibond_sgi.menu_sgi_objectives',
     "SGI → Dirección → Objetivos integrales (objetivos de SST con su plan de "
     "acciones = programa anual)", ()),
    ('E2.33', 'maintenance.menu_m_request_form',
     "Mantenimiento → Solicitudes (preventivo mensual de control de plagas con el "
     "reporte del proveedor)", ()),
    ('E2.34', 'quimibond_sgi.menu_sgi_env_aspects',
     "SGI → Seguridad y ambiente → Aspectos ambientales (control operacional de "
     "los significativos) · SGI → Dirección → Riesgos y oportunidades (IPER)",
     ('F-P-E03-01',)),
    ('E2.35', 'survey.menu_survey_form',
     "Encuestas → encuesta anual de consulta y participación de los trabajadores",
     ('F-P-A10-05',)),
    ('E2.37', 'quimibond_sgi.menu_sgi_audit_list',
     "SGI → Mejora → Auditorías → Auditorías", ()),
    ('S4.34', 'quimibond_sgi.menu_sgi_incidents',
     "SGI → Seguridad y ambiente → Incidentes y accidentes (aviso ST-7 al IMSS "
     "adjunto al accidente)", ('F-P-S02-01',)),
    ('S5.14', 'quimibond_sgi.menu_sgi_loto',
     "SGI → Seguridad y ambiente → Bloqueo y etiquetado", ()),
    ('C5.23', 'quality_control.menu_quality_control_points',
     "Calidad → Control de calidad → Puntos de control (especificación y pruebas "
     "de la materia prima o el proveedor nuevo)", ('F-P-C04-06',)),
    ('C4.24', 'mrp.menu_mrp_workorder_todo',
     "Manufactura → Operaciones → Órdenes de trabajo (revisión del crudo)",
     ('F-IT-P-P01-08-03',)),
    ('C2.39', 'stock.out_picking',
     "Inventario → Entregas (certificado de origen T-MEC por cliente)", ('F-P-A16-04',)),
)


class SgiProcessActivitySstLinks(models.Model):
    _inherit = 'sgi.process.activity'

    @api.model
    def _sgi_link_activity_screens(self, table=SGI_SST_ACTIVITY_LINKS, company=None):
        """Escribe menú, «Dónde se ejecuta en Odoo» y formatos en las
        actividades de ``table`` que los tienen vacíos. Devuelve
        ``{numeral: {campo: (antes, después)}}`` con lo escrito."""
        company = company or self.env['sgi.config']._sgi_company()
        Activity = self.sudo()
        Doc = self.env['documents.document'].sudo()
        written = {}
        for number, menu_xmlid, where, codes in table:
            activities = Activity.search([('number', '=', number),
                                          ('company_id', '=', company.id)])
            if not activities:
                _logger.warning("SGI ligas SST: no existe la actividad %s en %s; se salta.",
                                number, company.display_name)
                continue
            menu = self.env.ref(menu_xmlid, raise_if_not_found=False) if menu_xmlid else None
            if menu_xmlid and not menu:
                _logger.warning("SGI ligas SST: %s: no existe el menú %s.", number, menu_xmlid)
            docs = Doc.browse()
            for code in codes:
                doc = Doc.search([('sgi_code', '=', code), ('sgi_state', '=', 'vigente'),
                                  ('sgi_is_controlled', '=', True)], limit=1)
                if doc:
                    docs |= doc
                else:
                    _logger.warning("SGI ligas SST: %s: no hay formato vigente con clave %s.",
                                    number, code)
            for activity in activities:
                vals, changes = {}, {}
                if menu:
                    if activity.odoo_menu_id:
                        _logger.info("SGI ligas SST: %s (%d) ya tiene menú %s; se respeta.",
                                     number, activity.id, activity.odoo_menu_id.complete_name)
                    else:
                        vals['odoo_menu_id'] = menu.id
                        changes['odoo_menu_id'] = (False, menu.complete_name)
                if where:
                    if activity.odoo_ref:
                        _logger.info("SGI ligas SST: %s (%d) ya dice dónde se ejecuta («%s»); "
                                     "se respeta.", number, activity.id, activity.odoo_ref)
                    else:
                        vals['odoo_ref'] = where
                        changes['odoo_ref'] = (False, where)
                if docs:
                    if activity.format_document_ids:
                        _logger.info("SGI ligas SST: %s (%d) ya tiene formatos %s; se respetan.",
                                     number, activity.id,
                                     ", ".join(activity.format_document_ids.mapped('sgi_code')))
                    else:
                        vals['format_document_ids'] = [(6, 0, docs.ids)]
                        changes['format_document_ids'] = (False, ", ".join(docs.mapped('sgi_code')))
                if vals:
                    activity.write(vals)
                    written[number] = changes
                    _logger.info("SGI ligas SST: %s (%d): %s", number, activity.id, "; ".join(
                        "%s: %s → %s" % (field, old or "vacío", new)
                        for field, (old, new) in changes.items()))
        _logger.info("SGI ligas SST: %d actividad(es) ligadas de %d en la tabla.",
                     len(written), len(table))
        return written
