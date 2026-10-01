# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI",
    'summary': "Sistema de Gestión Integral (ISO 9001/14001/45001) sobre apps nativas de Odoo 19",
    'description': """
Sistema de Gestión Integral de Productora de No Tejidos Quimibond (PNTQ):
ISO 9001 y 14001 certificadas, 45001 en certificación.

Procesos y actividades con responsable por puesto, vencimiento, entregable
medible y escalamiento; Mis pendientes, Mi procedimiento con firma de
lectura, control documental, no conformidades, indicadores, auditorías,
riesgos, seguridad y ambiente, y revisión por la dirección, sobre las apps
nativas de Odoo.

Se instala vacío: el mapa de procesos va en quimibond_sgi_mapa y se carga a
mano. Documentación: docs/sgi/ del repositorio; cambios: CHANGELOG.md.
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Services/SGI',
    'version': '19.0.57.89.0',
    'license': 'OPL-1',
    'application': True,
    # 57.9.0 (A-011, A-012, A-015, A-014, A-010): solo las dependencias
    # directas, cada una con lo que la usa. Las que ya traen otras (base,
    # mail, hr, stock, purchase, approvals, quality_control) no se repiten.
    # Salieron sale_management y hr_timesheet (sin uso), knowledge (DOC-5,
    # ahora quimibond_sgi_knowledge) y web_studio (la regla de aprobación del
    # botón, ahora quimibond_sgi_studio). En producción no se desinstala nada.
    # 57.11.0 (A-016, A-013): web_grid y account_budget se van con el
    # presupuesto de ventas a quimibond_ventas_presupuesto.
    'depends': [
        'documents',  # control documental (documents.document)
        'approvals_purchase',  # requisiciones (sgi_links); trae approvals y purchase
        'quality_mrp',  # NC, alertas y puntos de control; trae quality_control
        'mrp',  # centro de trabajo de la actividad (sgi_activity_spec)
        'helpdesk',  # reclamaciones y mesa interna
        'project',  # mejoras y diseño y desarrollo (P-D01)
        'sale_stock',  # salidas del pedido (COA por entrega, OTIF)
        'maintenance',  # calibraciones y equipos
        'stock_account',  # stock.move.value / stock.quant.value (AL-01)
        'survey',  # evaluaciones, DNC y encuestas como entregable
        'hr_skills',  # competencias por puesto; trae hr
        'web_hierarchy',  # organigrama de procesos y puestos
        'sign',  # firmas de documentos, registros y aprobaciones
        'portal',  # respuesta del proveedor a su NC (controllers/portal_nc.py)
        'website_slides',  # cursos ligados a competencias
        'spreadsheet_dashboard',  # Tablero SGI (se arma con los pivotes de Análisis)
    ],
    'data': [
        # security
        'security/sgi_security.xml',
        'security/ir.model.access.csv',
        # data
        'data/sgi_sequences.xml',
        'data/sgi_document_types.xml',
        'data/sgi_sequences_audit_risk.xml',
        'data/sgi_areas.xml',
        'data/sgi_norms.xml',
        # 57.4.0 (A-002, decisión 6): el SGI se instala sin procesos. El mapa
        # viejo (sgi_process_data.xml, sgi_process_flows_extra.xml) está en
        # docs/historico/quimibond_sgi_data/; sus 58 XML IDs pasan a __export__.
        'data/sgi_stages.xml',
        'data/sgi_objectives.xml',
        'data/sgi_indicators_data.xml',
        'data/sgi_expansion_data.xml',
        # 56.35.0 (A-004/D-01): sgi_indicator_formula_data.xml salió del núcleo
        # (IDs de producción); los términos viajan en quimibond_sgi_mapa.
        'data/sgi_risk_data.xml',
        # 57.6.0 (B-009, D-16): sgi_audit_data.xml (encuesta 151 «Checklist
        # Auditoría Interna ISO 9001», legado) salió a docs/historico/; sus 36
        # XML IDs pasan a __export__ y la encuesta se queda archivada.
        'data/sgi_helpdesk_interno.xml',
        'data/sgi_mgmt_review_data.xml',
        'data/sgi_cron.xml',
        'data/sgi_cron_indicators_audit.xml',
        'data/sgi_sequences_quality_sst.xml',
        'data/sgi_sequences_policy_budget.xml',
        'data/sgi_ppap_elements.xml',
        'data/sgi_cron_calibration_budget.xml',
        'data/sgi_dnc_survey.xml',
        'data/sgi_control_plans.xml',
        'data/sgi_format_map_data.xml',
        'data/sgi_parameters.xml',
        'data/sgi_alert_source_data.xml',
        'data/sgi_emergency_satisfaction_data.xml',
        'data/sgi_operational_signals_data.xml',
        'data/sgi_measure_cron.xml',
        'data/sgi_approval_cron.xml',
        'data/sgi_cumplimiento_data.xml',
        'data/sgi_mail_templates.xml',
        'data/sgi_moc_data.xml',
        'data/sgi_dyd_data.xml',
        'data/sgi_sign_elearning_data.xml',
        'data/sgi_doc_change_sign_data.xml',
        'data/sgi_checklist_cron.xml',
        'data/sgi_coa_data.xml',
        'data/sgi_my_procedure_data.xml',
        'data/sgi_mp_change_category_data.xml',
        'data/sgi_epp_data.xml',
        'data/sgi_supplier_nc_data.xml',
        'data/sgi_offboarding_plan_data.xml',
        'data/sgi_sst_sequences.xml',
        # views
        'views/sgi_area_views.xml',
        'report/report_compliance_matrix.xml',
        'views/sgi_norm_views.xml',
        'views/sgi_process_views.xml',
        'views/sgi_process_procedure_views.xml',
        'views/sgi_document_views.xml',
        'views/sgi_doc_change_views.xml',
        'views/sgi_nonconformity_views.xml',
        'views/sgi_complaint_views.xml',
        'views/sgi_improvement_views.xml',
        'views/sgi_integration_views.xml',
        'views/sgi_policy_views.xml',
        'views/sgi_objective_views.xml',
        'views/sgi_indicator_views.xml',
        'views/sgi_indicator_formula_views.xml',
        'views/sgi_management_review_views.xml',
        'views/sgi_audit_views.xml',
        'views/sgi_risk_views.xml',
        'views/sgi_legal_views.xml',
        'views/sgi_context_views.xml',
        'views/sgi_supplier_eval_views.xml',
        'views/sgi_res_partner_views.xml',
        'views/sgi_control_plan_views.xml',
        'views/sgi_calibration_views.xml',
        'views/sgi_emergency_views.xml',
        'views/sgi_msa_views.xml',
        'views/sgi_fmea_views.xml',
        'views/sgi_ppap_views.xml',
        'views/sgi_incident_views.xml',
        'views/sgi_competence_views.xml',
        'views/sgi_dashboard_views.xml',
        'views/sgi_diagnostic_views.xml',
        'views/sgi_epp_views.xml',
        'views/sgi_my_procedure_tab_views.xml',
        'views/sgi_direction_board_views.xml',
        'views/sgi_portal_templates.xml',
        'views/sgi_map_hooks_views.xml',
        'views/sgi_format_map_views.xml',
        'views/sgi_alert_source_views.xml',
        'views/sgi_action_line_views.xml',
        'views/sgi_settings_views.xml',
        'views/sgi_sign_elearning_views.xml',
        'views/sgi_catalog_views.xml',
        'views/sgi_mp_change_views.xml',
        'views/sgi_my_procedure_views.xml',
        'views/sgi_my_pending_views.xml',
        'views/sgi_approval_native_views.xml',
        'views/sgi_coa_views.xml',
        # Firmas (Sign) sobre vistas de otros módulos. Desde 57.28.0 ya no
        # hereda vistas propias (A-008): sus herencias viven en su vista base.
        'views/sgi_supplier_audit_sign_views.xml',
        'views/sgi_hierarchy_views.xml',
        'views/sgi_links_views.xml',
        'views/sgi_diagram_views.xml',
        'views/sgi_kpi_fields_views.xml',
        'views/sgi_dev_request_views.xml',
        'views/sgi_machine_sheet_views.xml',
        'views/sgi_staff_efficiency_views.xml',
        # reports
        'report/report_nc.xml',
        'report/report_8d.xml',
        'report/report_news.xml',
        'report/report_audit.xml',
        'report/report_mgmt_review.xml',
        'report/report_coa.xml',
        'report/report_fmea.xml',
        'report/report_incident.xml',
        'report/report_procedure.xml',
        'report/report_doc_change.xml',
        'report/report_sign_sheet.xml',
        'report/report_my_procedure.xml',
        'report/report_master_list.xml',
        'report/report_direction.xml',
        'report/report_retention.xml',
        'report/sgi_format_footer.xml',
        'report/report_dev_request.xml',
        'report/report_machine_sheet.xml',
        'report/report_calibration_label.xml',
        'report/report_staff_efficiency.xml',
        'report/report_epp_delivery.xml',
        'report/report_master_list_all.xml',
        'report/report_env_aspect.xml',
        'report/report_work_permit.xml',
        'report/report_loto.xml',
        # 57.63.0: etiquetas de material liberado, rechazado y detenido.
        'report/report_lot_label.xml',
        'views/sgi_business_line_views.xml',
        'views/sgi_external_doc_views.xml',
        'views/sgi_hse_views.xml',
        'views/sgi_activity_spec_views.xml',
        'views/sgi_current_documents_views.xml',
        # 57.0.0 (entrega 6): «Del Dropbox a Odoo» (rutinas, buscador, avance).
        'views/sgi_dropbox_views.xml',
        # 57.14.0 (RH-01): «Plantilla autorizada» en las vistas nativas del puesto.
        'views/sgi_hr_job_headcount_views.xml',
        # 57.50.0 y siguientes (bloque 1 de formularios): seguridad, salud y
        # ambiente. Cada ficha completa en su archivo, sin herencias.
        'views/sgi_env_aspect_views.xml',
        'views/sgi_work_permit_views.xml',
        'views/sgi_loto_views.xml',
        'views/sgi_audit_finding_legal_eval_views.xml',
        # menus: TODOS en un archivo y al final (A-025, entrega 4): las
        # acciones ya están cargadas y el padre va antes que el hijo.
        'views/sgi_menus.xml',
    ],
    'demo': [
        'demo/sgi_demo_quality.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'assets': {
        'web.assets_backend': [
            'quimibond_sgi/static/src/diagram/**/*',
            'quimibond_sgi/static/src/my_procedure/**/*',
        ],
    },
    'installable': True,
}
