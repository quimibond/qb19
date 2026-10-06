# -*- coding: utf-8 -*-
from . import sgi_base
from . import sgi_pin
from . import sgi_area
from . import sgi_norm
from . import sgi_alert_source
from . import sgi_legal
from . import sgi_context
from . import sgi_process
from . import sgi_process_procedure
from . import sgi_document
from . import sgi_doc_change
from . import sgi_nonconformity
from . import sgi_complaint
from . import sgi_improvement
from . import sgi_integration
from . import sgi_sale_commitment
from . import sgi_policy
from . import sgi_objective
from . import sgi_indicator
from . import sgi_audit
from . import sgi_risk
from . import sgi_supplier_eval
from . import sgi_management_review
from . import sgi_control_plan
from . import sgi_calibration
from . import sgi_fmea
from . import sgi_ppap
from . import sgi_incident
from . import sgi_competence
from . import sgi_format_map
from . import sgi_format_map_seed
from . import sgi_catalog
from . import sgi_deliverable
from . import sgi_deliverable_models
from . import sgi_load
from . import sgi_exec_stat
from . import sgi_load_wizard
from . import sgi_export
from . import sgi_diagnostic
from . import sgi_emergency
from . import sgi_msa
from . import sgi_settings
from . import sgi_sign_elearning
from . import sgi_cron
from . import sgi_activity_spec
from . import sgi_coa
from . import sgi_release
from . import sgi_indicator_detail
from . import sgi_indicator_i3
from . import sgi_indicator_p21
from . import sgi_indicator_formula
from . import sgi_indicator_plan
from . import sgi_indicator_trajectory
from . import sgi_my_procedure
from . import sgi_epp
from . import sgi_my_procedure_screen
from . import sgi_structure
from . import sgi_cleanup
from . import sgi_direction_board
from . import sgi_supplier_nc
from . import sgi_sign_record
from . import sgi_hierarchy
from . import sgi_diagram
from . import sgi_links
from . import sgi_diagram_iso
from . import sgi_diagram_view
from . import sgi_kpi_sales
from . import sgi_kpi_quality
from . import sgi_kpi_account
from . import sgi_kpi_review
from . import sgi_kpi_hr
from . import sgi_dev_characteristic
from . import sgi_dev_request
from . import sgi_dev_project
from . import sgi_dev_product
from . import sgi_dev_analysis
from . import sgi_machine_sheet
from . import sgi_staff_efficiency
from . import sgi_epp_sign
from . import sgi_mp_change
from . import sgi_my_pending
from . import sgi_approval_native
from . import sgi_relative_roles
from . import sgi_business_line
from . import sgi_doc_change_sign
from . import sgi_sign_builder
from . import sgi_my_procedure_sign
from . import sgi_external_doc
from . import sgi_hse_records
from . import sgi_checklist
from . import sgi_customer_reply
from . import sgi_norm_compliance
from . import sgi_archived_filters
from . import sgi_document_owner
from . import sgi_multicompany
from . import sgi_current_documents
from . import sgi_legacy_routine
from . import sgi_dropbox_views
from . import sgi_legacy_routine_import
from . import sgi_weekly_overdue
from . import sgi_indicator_ind2
from . import sgi_business_calendar
from . import sgi_env_aspect
from . import sgi_work_permit
from . import sgi_loto
from . import sgi_sst_links
from . import sgi_formatos_bloque3
from . import sgi_deploy_change
from . import sgi_floor_kiosk
from . import sgi_company_fix
from . import sgi_env_aspect_transfer
from . import sgi_incident_leave
# 57.99.0: constantes sin modelos y, al final (hereda indicador, medición,
# proceso, Tablero y sgi.cron), la salud del SGI.
from . import sgi_health_const
from . import sgi_indicator_health
# 57.100.0: al final (heredan medición, desglose, indicador, empleado,
# currículum, encuestas, cursos y NC, todos definidos antes).
from . import sgi_indicator_integrity
from . import sgi_competence_grant
from . import sgi_ai
# 57.101.0: reportes y diagramas (heredan indicador, medición, proceso,
# sgi.diagram, programa de auditorías y riesgo, todos definidos antes).
from . import sgi_indicator_sheet
from . import sgi_report_print
# 57.103.0: registro de cumplimiento por actividad, responsable y periodo
# (hereda la actividad y lo lee Mis pendientes, ambos definidos antes).
from . import sgi_activity_execution
# 57.105.0: MIID desde Odoo (hereda approval.request —su extensión de Sign ya
# cargó— y sgi.cron; usa sgi_report_print).
from . import sgi_miid
# 57.107.0: revisión mensual de la medición por el dueño del proceso (hereda
# la actividad y la lee Mis pendientes, ambos definidos antes).
from . import sgi_measure_review
# 57.108.0: proponer una actividad en lenguaje normal (hereda la propuesta y
# approval.request, definidos antes en sgi_mp_change).
from . import sgi_mp_change_simple
# 57.109.0: asistente de aprobaciones (hereda el rol con su aprobación nativa
# y la actividad, definidos antes).
from . import sgi_approval_wizard
# 57.109.0: asistente «Nuevo indicador» y medición de la actividad sin nombres
# técnicos (heredan indicador, término y actividad, definidos antes).
from . import sgi_indicator_wizard
# 57.109.0: reportar un riesgo u oportunidad en lenguaje normal (crea sgi.risk).
from . import sgi_risk_report
# 57.111.0: quién hizo la actividad según el historial de su estado (hereda
# entregable y actividad, y usa los ganchos de sgi_process_procedure).
from . import sgi_measure_history
# 57.112.0: medición manual a propósito (hereda la actividad y extiende los
# campos de medición de sgi_measure_history).
from . import sgi_measure_manual_reason
# 57.116.0: asuntos de las categorías de Aprobaciones compartidas (hereda
# approval.request y approval.category, y usa el rol «Aprueba»).
from . import sgi_approval_subject
