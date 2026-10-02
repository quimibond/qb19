<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# Vistas, acciones y menús del SGI

## Árbol de menús

Sacado de los `<menuitem>` del núcleo y los satélites, ordenado por secuencia dentro de cada padre. El árbol que valida `test_menu_tree` está en `addons/quimibond_sgi/tools/sgi_menu_tree.txt`.

- **Checklists de hoy** — `maintenance.request`; bajo `maintenance.menu_maintenance_title`
- **Empleados sin puesto o sin correo** — `hr.employee`; grupos: hr.group_hr_user; bajo `hr.menu_hr_employee_payroll`
- **Calidad preventiva** — grupos: quality.group_quality_user, quimibond_sgi.group_sgi_user; bajo `quality_control.menu_quality_root`
  - **Planes de control** — `sgi.control.plan`
  - **AMEF** — `sgi.fmea`
  - **PPAP** — `sgi.ppap`
  - **Metrología**
    - **Equipos de medición** — `maintenance.equipment`
    - **Equipos de laboratorio** — `maintenance.equipment`
    - **Calibraciones** — `sgi.calibration`
    - **Verificaciones de laboratorio** — `sgi.calibration`
    - **Estudios MSA** — `sgi.msa.study`
  - **CoA recibidos** — `sgi.coa.inbox`; grupos: quality.group_quality_user, quimibond_sgi.group_sgi_manager
- **Paretos de calidad** — grupos: quality.group_quality_user, quimibond_sgi.group_sgi_manager; bajo `quality_control.menu_quality_root`
  - **Pareto de alertas de calidad** — `quality.alert`
  - **Pareto de defectos de revisado** — `mrp.revision.log`
- **Evaluación de proveedores** — `sgi.supplier.eval`; grupos: purchase.group_purchase_user, quimibond_sgi.group_sgi_user; bajo `purchase.menu_purchase_root`
- **Competencias SGI** — grupos: hr.group_hr_user, quimibond_sgi.group_sgi_user; bajo `hr.menu_hr_root`
  - **Brechas de competencia (DNC)** — `sgi.competence.gap`
  - **Encuesta DNC** — `survey.survey`
  - **Cursos y competencias (eLearning)** — `slide.channel`; grupos: quimibond_sgi.group_sgi_manager
- **Eficiencias de personal** — grupos: hr.group_hr_user; bajo `hr.menu_hr_root`
  - **Hojas mensuales** — `sgi.staff.efficiency`
  - **Análisis por empleado** — `sgi.staff.efficiency.line`
- **SGI** — grupos: quimibond_sgi.group_sgi_user, quimibond_sgi.group_sgi_auditor
  - **Inicio**
    - **Mis pendientes** — `sgi.my.pending`
    - **Mi procedimiento** — `sgi.my.procedure`
    - **Documentos vigentes** — `documents.document`
    - **Mis indicadores** — `sgi.indicator`
    - **Mi equipo** — `hr.employee.public`
    - **Eficiencias de mi área** — `sgi.staff.efficiency`; grupos: quimibond_sgi.group_sgi_efficiency_capture
  - **Procesos**
    - **Mapa de procesos** — `sgi.process`
    - **Actividades** — `sgi.process.activity`
    - **Entregables** — `sgi.deliverable`
    - **Flujos entre procesos** — `sgi.process.flow`
    - **Matriz de responsabilidades** — `sgi.activity.exec.stat`
    - **Puestos y procesos** — `hr.job`
    - **Fichas de proceso por máquina** — `sgi.machine.sheet`
    - **Del Dropbox a Odoo**
      - **Buscador por clave anterior** — `sgi.dropbox.key`
      - **Procedimientos anteriores** — `documents.document`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director, quimibond_sgi.group_sgi_process_owner
      - **Formatos y documentos anteriores** — `documents.document`; grupos: -quimibond_sgi.group_sgi_auditor, -quimibond_sgi.group_sgi_manager, -quimibond_sgi.group_sgi_director
      - **Rutina por rutina** — `sgi.legacy.routine`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director, quimibond_sgi.group_sgi_process_owner
      - **Avance de la transición** — `sgi.dropbox.progress`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director, quimibond_sgi.group_sgi_process_owner
  - **Mejora**
    - **No conformidades** — `quality.alert`
    - **Reclamaciones de clientes** — `helpdesk.ticket`
    - **Acciones correctivas** — `sgi.action.line`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
    - **Mejora continua** — `project.task`
    - **Lecciones aprendidas** — `quality.alert`
    - **Quejas y sugerencias del personal** — `helpdesk.ticket`
    - **Auditorías**
      - **Programa** — `sgi.audit.program`
      - **Auditorías** — `sgi.audit`
      - **Hallazgos** — `sgi.audit.finding`
  - **Seguridad y ambiente**
    - **Incidentes y accidentes** — `sgi.incident`
    - **Planes de emergencia** — `sgi.emergency.plan`
    - **Simulacros** — `sgi.emergency.drill`
    - **Recorridos CSH** — `sgi.csh.inspection`
    - **Estudios de higiene y exámenes médicos** — `sgi.health.record`; grupos: quimibond_sgi.group_sgi_health, quimibond_sgi.group_sgi_manager, -hr.group_hr_user
    - **Responsivas de EPP** — `sgi.epp.delivery`
    - **Hojas de checklist** — `maintenance.request`
    - **Aspectos ambientales** — `sgi.env.aspect`
    - **Permisos de trabajo de alto riesgo** — `sgi.work.permit`
    - **Bloqueo y etiquetado** — `sgi.loto`
  - **Dirección** — grupos: -quimibond_sgi.group_sgi_director, -quimibond_sgi.group_sgi_manager, -quimibond_sgi.group_sgi_auditor
    - **Tablero** — `sgi.direction.board`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
    - **Revisión por la dirección** — `sgi.management.review`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
    - **Política integral** — `sgi.policy`
    - **Objetivos integrales** — `sgi.objective`
    - **Riesgos y oportunidades** — `sgi.risk`
    - **Requisitos legales** — `sgi.legal.requirement`
    - **Evaluaciones de cumplimiento legal** — `sgi.legal.evaluation`
    - **Partes interesadas** — `sgi.interested.party`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
    - **Satisfacción del cliente** — `survey.user.input`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
  - **Administración SGI** — grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
    - **Documentos**
      - **Documentos** — `documents.document`
      - **Lista maestra** — `documents.document`
      - **Documentos externos** — `documents.document`
      - **Solicitudes de cambio** — `approval.request`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
      - **Tipos de documento** — `sgi.document.type`; grupos: quimibond_sgi.group_sgi_manager
    - **Indicadores**
      - **Indicadores** — `sgi.indicator`
      - **Mediciones** — `sgi.indicator.measure`
      - **Mediciones por equipo o mercado** — `sgi.indicator.measure.split`
    - **Aprobaciones del SGI** — `sgi.activity.role`; grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
    - **Diagnóstico** — grupos: quimibond_sgi.group_sgi_auditor, quimibond_sgi.group_sgi_manager, quimibond_sgi.group_sgi_director
      - **Diagnóstico del SGI** — `sgi.diagnostic`; grupos: -quimibond_sgi.group_sgi_manager
      - **Cobertura de medición** — `sgi.process.activity`
      - **Cumplimiento de procedimientos** — `sgi.process.activity`
      - **Faltantes de especificación** — `sgi.activity.spec.gap`
      - **Cumplimiento semanal** — `sgi.activity.week.stat`
    - **Firmas de lectura**
      - **Publicar Mi procedimiento** — `sgi.my.procedure.check`; grupos: quimibond_sgi.group_sgi_manager
      - **Acuses de lectura** — `sgi.document.ack`
    - **Configuración** — grupos: quimibond_sgi.group_sgi_manager
      - **Cargar catálogo** — `sgi.catalog.load.wizard`; grupos: quimibond_sgi.group_sgi_admin
      - **Familias de puesto** — `sgi.job.family`
      - **Cargar mapa de procesos** — `sgi.mapa.load.wizard`; grupos: quimibond_sgi.group_sgi_admin
      - **Ajustes** — `res.config.settings`; grupos: base.group_system
      - **Áreas** — `sgi.area`
      - **Normas** — `sgi.norm`
      - **Cláusulas** — `sgi.norm.clause`
      - **Categorías de riesgo** — `sgi.risk.category`
      - **Checklists de planta y unidades** — `sgi.checklist.template`
      - **Tabletas de planta** — `sgi.floor.tablet`
      - **Formatos en documentos de Odoo** — `sgi.format.map`
      - **Fuentes de NC automáticas** — `sgi.alert.source`
      - **Elementos PPAP** — `sgi.ppap.element.template`
- **Bitácora de bloqueo contable** — `sgi.lock.date.log`; bajo `account.menu_finance_reports`
- **SGI en planta** — `sgi_floor_kiosk_action`; grupos: quimibond_sgi.group_sgi_floor_tablet, quimibond_sgi.group_sgi_manager
- **Valor del inventario por mes** — `sgi.inventory.value`; bajo `account.menu_finance_reports`

## Acciones (103)

| Acción | Tipo | Título | Modelo | Vistas | Ayuda de pantalla vacía | Archivo |
|---|---|---|---|---|---|---|
| `quimibond_sgi.sgi_action_line_action_all` | act_window | Acciones correctivas | `sgi.action.line` | list,kanban,form | sí | `addons/quimibond_sgi/views/sgi_action_line_views.xml` |
| `quimibond_sgi.sgi_activity_change_action_new` | server | Nueva actividad (propuesta) | `sgi.activity.change` |  |  | `addons/quimibond_sgi/views/sgi_mp_change_views.xml` |
| `quimibond_sgi.sgi_activity_compliance_action` | act_window | Cumplimiento de procedimientos | `sgi.process.activity` | pivot,graph,list | sí | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `quimibond_sgi.sgi_activity_exec_stat_action_who` | act_window | Matriz de responsabilidades | `sgi.activity.exec.stat` | pivot,sgi_diagram,list,graph | sí | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `quimibond_sgi.sgi_activity_method_action` | act_window | Cobertura de medición | `sgi.process.activity` | pivot,list | sí | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `quimibond_sgi.sgi_activity_role_action_approval` | act_window | Aprobaciones del SGI | `sgi.activity.role` | list | sí | `addons/quimibond_sgi/views/sgi_approval_native_views.xml` |
| `quimibond_sgi.sgi_activity_spec_gap_action` | act_window | Faltantes de especificación | `sgi.activity.spec.gap` | pivot,list | sí | `addons/quimibond_sgi/views/sgi_activity_spec_views.xml` |
| `quimibond_sgi.sgi_activity_week_stat_action` | act_window | Cumplimiento semanal | `sgi.activity.week.stat` | pivot,list | sí | `addons/quimibond_sgi/views/sgi_activity_spec_views.xml` |
| `quimibond_sgi.sgi_alert_source_action` | act_window | Fuentes de NC automáticas | `sgi.alert.source` | list,form | sí | `addons/quimibond_sgi/views/sgi_alert_source_views.xml` |
| `quimibond_sgi.sgi_area_action` | act_window | Áreas | `sgi.area` | list,form | sí | `addons/quimibond_sgi/views/sgi_area_views.xml` |
| `quimibond_sgi.sgi_audit_action` | act_window | Auditorías | `sgi.audit` | list,calendar,form,activity | sí | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `quimibond_sgi.sgi_audit_finding_list_action` | act_window | Hallazgos | `sgi.audit.finding` | list,form | sí | `addons/quimibond_sgi/views/sgi_audit_finding_legal_eval_views.xml` |
| `quimibond_sgi.sgi_audit_program_action` | act_window | Programa | `sgi.audit.program` | list,sgi_diagram,form | sí | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `quimibond_sgi.sgi_calibration_action` | act_window | Calibraciones | `sgi.calibration` | list,calendar,form,activity | sí | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `quimibond_sgi.sgi_calibration_action_lab` | act_window | Verificaciones de laboratorio | `sgi.calibration` | list,form | sí | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `quimibond_sgi.sgi_catalog_load_wizard_action` | act_window | Cargar catálogo | `sgi.catalog.load.wizard` | form |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `quimibond_sgi.sgi_checklist_request_action` | act_window | Hojas de checklist | `maintenance.request` | list,form,pivot | sí | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `quimibond_sgi.sgi_checklist_template_action` | act_window | Checklists de planta y unidades | `sgi.checklist.template` | list,form | sí | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `quimibond_sgi.sgi_checklist_today_action` | act_window | Checklists de hoy | `maintenance.request` | kanban,list,form | sí | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `quimibond_sgi.sgi_coa_inbox_action` | act_window | CoA recibidos | `sgi.coa.inbox` | list,form | sí | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `quimibond_sgi.sgi_competence_gap_action` | act_window | Brechas de competencia (DNC) | `sgi.competence.gap` | pivot,sgi_diagram,list | sí | `addons/quimibond_sgi/views/sgi_competence_views.xml` |
| `quimibond_sgi.sgi_complaint_action` | server | Reclamaciones de clientes | `helpdesk.ticket` |  |  | `addons/quimibond_sgi/views/sgi_complaint_views.xml` |
| `quimibond_sgi.sgi_config_settings_action` | act_window | Ajustes | `res.config.settings` | form |  | `addons/quimibond_sgi/views/sgi_settings_views.xml` |
| `quimibond_sgi.sgi_control_plan_action` | act_window | Planes de control | `sgi.control.plan` | list,form | sí | `addons/quimibond_sgi/views/sgi_control_plan_views.xml` |
| `quimibond_sgi.sgi_csh_inspection_action` | act_window | Recorridos CSH | `sgi.csh.inspection` | list,form,activity | sí | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `quimibond_sgi.sgi_current_document_action` | act_window | Documentos vigentes | `documents.document` | list,kanban | sí | `addons/quimibond_sgi/views/sgi_current_documents_views.xml` |
| `quimibond_sgi.sgi_deliverable_list_action` | act_window | Entregables | `sgi.deliverable` | list,form | sí | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `quimibond_sgi.sgi_diagnostic_line_action` | act_window | Diagnóstico del SGI | `sgi.diagnostic.line` | list,form | sí | `addons/quimibond_sgi/views/sgi_diagnostic_views.xml` |
| `quimibond_sgi.sgi_diagnostic_run_action` | server | Diagnóstico del SGI | `sgi.diagnostic` |  |  | `addons/quimibond_sgi/views/sgi_diagnostic_views.xml` |
| `quimibond_sgi.sgi_direction_board_action_open` | server | Tablero | `sgi.direction.board` |  |  | `addons/quimibond_sgi/views/sgi_direction_board_views.xml` |
| `quimibond_sgi.sgi_dnc_survey_action` | server | Encuesta DNC | `survey.survey` |  |  | `addons/quimibond_sgi/views/sgi_competence_views.xml` |
| `quimibond_sgi.sgi_doc_change_board_action` | server | Solicitudes de cambio | `approval.request` |  |  | `addons/quimibond_sgi/views/sgi_doc_change_views.xml` |
| `quimibond_sgi.sgi_document_ack_action` | act_window | Acuses de lectura | `sgi.document.ack` | list,form | sí | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `quimibond_sgi.sgi_document_action` | act_window | Documentos | `documents.document` | list,sgi_diagram,form | sí | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `quimibond_sgi.sgi_document_assign_new_code_action` | server | Asignar clave nueva (SGI) | `documents.document` |  |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `quimibond_sgi.sgi_document_master_list_action` | act_window | Lista maestra | `documents.document` | list,form | sí | `addons/quimibond_sgi/report/report_master_list_all.xml` |
| `quimibond_sgi.sgi_document_resolve_menu_action` | server | Resolver menú de Odoo (SGI) | `documents.document` |  |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `quimibond_sgi.sgi_document_type_action` | act_window | Tipos de documento | `sgi.document.type` | list | sí | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `quimibond_sgi.sgi_dropbox_key_action` | act_window | Buscador por clave anterior | `sgi.dropbox.key` | list,form | sí | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `quimibond_sgi.sgi_dropbox_procedure_action` | act_window | Procedimientos anteriores | `documents.document` | list,form | sí | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `quimibond_sgi.sgi_dropbox_progress_action` | act_window | Avance de la transición | `sgi.dropbox.progress` | list,graph,pivot | sí | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `quimibond_sgi.sgi_dropbox_routine_action` | act_window | Rutina por rutina | `sgi.legacy.routine` | list,form,pivot | sí | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `quimibond_sgi.sgi_emergency_drill_action` | act_window | Simulacros | `sgi.emergency.drill` | list,calendar,form | sí | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `quimibond_sgi.sgi_emergency_plan_action` | act_window | Planes de emergencia | `sgi.emergency.plan` | list,sgi_diagram,form,activity | sí | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `quimibond_sgi.sgi_env_aspect_action` | act_window | Aspectos ambientales | `sgi.env.aspect` | list,form | sí | `addons/quimibond_sgi/views/sgi_env_aspect_views.xml` |
| `quimibond_sgi.sgi_epp_delivery_action` | act_window | Responsivas de EPP | `sgi.epp.delivery` | list,kanban,form | sí | `addons/quimibond_sgi/views/sgi_epp_views.xml` |
| `quimibond_sgi.sgi_equipment_action_lab` | act_window | Equipos de laboratorio | `maintenance.equipment` | list,form | sí | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `quimibond_sgi.sgi_equipment_action_measuring` | act_window | Equipos de medición | `maintenance.equipment` | list,sgi_diagram,form | sí | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `quimibond_sgi.sgi_external_doc_action` | act_window | Documentos externos | `documents.document` | list,form | sí | `addons/quimibond_sgi/views/sgi_external_doc_views.xml` |
| `quimibond_sgi.sgi_floor_kiosk_action` | client | SGI en planta |  |  |  | `addons/quimibond_sgi/views/sgi_floor_views.xml` |
| `quimibond_sgi.sgi_floor_tablet_action` | act_window | Tabletas de planta | `sgi.floor.tablet` | list,form | sí | `addons/quimibond_sgi/views/sgi_floor_views.xml` |
| `quimibond_sgi.sgi_fmea_action` | act_window | AMEF | `sgi.fmea` | list,form,activity | sí | `addons/quimibond_sgi/views/sgi_fmea_views.xml` |
| `quimibond_sgi.sgi_format_map_action` | act_window | Formatos en documentos de Odoo | `sgi.format.map` | list,form | sí | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `quimibond_sgi.sgi_health_record_action` | act_window | Estudios de higiene y exámenes médicos | `sgi.health.record` | list,form,activity | sí | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `quimibond_sgi.sgi_hr_employee_gaps_action` | act_window | Empleados sin puesto o sin correo | `hr.employee` | list,form | sí | `addons/quimibond_sgi/views/sgi_floor_views.xml` |
| `quimibond_sgi.sgi_hr_job_action_roles` | act_window | Puestos y procesos | `hr.job` | sgi_diagram,list,form | sí | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `quimibond_sgi.sgi_improvement_action` | server | Mejora continua | `project.task` |  |  | `addons/quimibond_sgi/views/sgi_improvement_views.xml` |
| `quimibond_sgi.sgi_incident_action` | act_window | Incidentes y accidentes | `sgi.incident` | list,kanban,form,graph,pivot,activity | sí | `addons/quimibond_sgi/views/sgi_incident_views.xml` |
| `quimibond_sgi.sgi_indicator_action` | act_window | Indicadores | `sgi.indicator` | list,sgi_diagram,form,activity | sí | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `quimibond_sgi.sgi_indicator_action_mine` | act_window | Mis indicadores | `sgi.indicator` | list,kanban,form | sí | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `quimibond_sgi.sgi_interested_party_action` | act_window | Partes interesadas | `sgi.interested.party` | list,sgi_diagram,form | sí | `addons/quimibond_sgi/views/sgi_context_views.xml` |
| `quimibond_sgi.sgi_internal_complaint_action` | server | Quejas y sugerencias del personal | `helpdesk.ticket` |  |  | `addons/quimibond_sgi/views/sgi_complaint_views.xml` |
| `quimibond_sgi.sgi_inventory_value_action` | act_window | Valor del inventario por mes | `sgi.inventory.value` | list | sí | `addons/quimibond_sgi/views/sgi_kpi_fields_views.xml` |
| `quimibond_sgi.sgi_job_family_action` | act_window | Familias de puesto | `sgi.job.family` | list,form | sí | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `quimibond_sgi.sgi_legacy_routine_import_action` | act_window | Importar rutinas | `sgi.legacy.routine.import` | form |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `quimibond_sgi.sgi_legal_evaluation_action` | act_window | Evaluaciones de cumplimiento legal | `sgi.legal.evaluation` | list,form | sí | `addons/quimibond_sgi/views/sgi_audit_finding_legal_eval_views.xml` |
| `quimibond_sgi.sgi_legal_requirement_action` | act_window | Requisitos legales | `sgi.legal.requirement` | list,sgi_diagram,form,activity | sí | `addons/quimibond_sgi/views/sgi_legal_views.xml` |
| `quimibond_sgi.sgi_lock_date_log_action` | act_window | Bitácora de bloqueo contable | `sgi.lock.date.log` | list | sí | `addons/quimibond_sgi/views/sgi_kpi_fields_views.xml` |
| `quimibond_sgi.sgi_loto_action` | act_window | Bloqueo y etiquetado | `sgi.loto` | list,form | sí | `addons/quimibond_sgi/views/sgi_loto_views.xml` |
| `quimibond_sgi.sgi_machine_sheet_action` | act_window | Fichas de proceso por máquina | `sgi.machine.sheet` | list,form,activity | sí | `addons/quimibond_sgi/views/sgi_machine_sheet_views.xml` |
| `quimibond_sgi.sgi_management_review_action` | act_window | Revisión por la dirección | `sgi.management.review` | list,sgi_diagram,form | sí | `addons/quimibond_sgi/views/sgi_management_review_views.xml` |
| `quimibond_sgi.sgi_measure_action` | act_window | Mediciones | `sgi.indicator.measure` | list,graph,pivot,form | sí | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `quimibond_sgi.sgi_measure_split_action` | act_window | Mediciones por equipo o mercado | `sgi.indicator.measure.split` | list,pivot,graph | sí | `addons/quimibond_sgi/views/sgi_business_line_views.xml` |
| `quimibond_sgi.sgi_migration_action` | act_window | Formatos y documentos anteriores | `documents.document` | kanban,list,form | sí | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `quimibond_sgi.sgi_msa_study_action` | act_window | Estudios MSA | `sgi.msa.study` | list,form | sí | `addons/quimibond_sgi/views/sgi_msa_views.xml` |
| `quimibond_sgi.sgi_my_pending_action_mine` | server | Mis pendientes | `sgi.my.pending` |  |  | `addons/quimibond_sgi/data/sgi_my_procedure_data.xml` |
| `quimibond_sgi.sgi_my_procedure_action_mine` | server | Mi procedimiento | `sgi.my.procedure` |  |  | `addons/quimibond_sgi/data/sgi_my_procedure_data.xml` |
| `quimibond_sgi.sgi_my_procedure_action_publish` | server | Publicar Mi procedimiento | `sgi.my.procedure.check` |  |  | `addons/quimibond_sgi/data/sgi_my_procedure_data.xml` |
| `quimibond_sgi.sgi_my_team_action_mine` | server | Mi equipo | `hr.employee.public` |  |  | `addons/quimibond_sgi/data/sgi_my_procedure_data.xml` |
| `quimibond_sgi.sgi_nc_board_action` | server | No conformidades | `quality.alert` |  |  | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `quimibond_sgi.sgi_nc_lessons_action` | act_window | Lecciones aprendidas | `quality.alert` | list,form | sí | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `quimibond_sgi.sgi_norm_action` | act_window | Normas | `sgi.norm` | list,form | sí | `addons/quimibond_sgi/views/sgi_norm_views.xml` |
| `quimibond_sgi.sgi_norm_clause_action` | act_window | Cláusulas | `sgi.norm.clause` | list,form | sí | `addons/quimibond_sgi/views/sgi_norm_views.xml` |
| `quimibond_sgi.sgi_objective_action` | act_window | Objetivos integrales | `sgi.objective` | list,sgi_diagram,form,activity | sí | `addons/quimibond_sgi/views/sgi_objective_views.xml` |
| `quimibond_sgi.sgi_policy_action` | act_window | Política integral | `sgi.policy` | list,sgi_diagram,form | sí | `addons/quimibond_sgi/views/sgi_policy_views.xml` |
| `quimibond_sgi.sgi_ppap_action` | act_window | PPAP | `sgi.ppap` | list,form | sí | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `quimibond_sgi.sgi_ppap_element_template_action` | act_window | Elementos PPAP | `sgi.ppap.element.template` | list | sí | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `quimibond_sgi.sgi_process_action` | act_window | Mapa de procesos | `sgi.process` | sgi_diagram,kanban,list,hierarchy,form | sí | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `quimibond_sgi.sgi_process_activity_action` | act_window | Actividades | `sgi.process.activity` | list,kanban,sgi_diagram,hierarchy,form | sí | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `quimibond_sgi.sgi_process_flow_list_action` | act_window | Flujos entre procesos | `sgi.process.flow` | list,form | sí | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `quimibond_sgi.sgi_quality_alert_action_pareto` | act_window | Pareto de alertas de calidad | `quality.alert` | pivot,graph,list | sí | `addons/quimibond_sgi/views/sgi_dashboard_views.xml` |
| `quimibond_sgi.sgi_risk_action` | act_window | Riesgos y oportunidades | `sgi.risk` | list,kanban,sgi_diagram,pivot,form,activity | sí | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `quimibond_sgi.sgi_risk_category_action` | act_window | Categorías de riesgo | `sgi.risk.category` | list | sí | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `quimibond_sgi.sgi_satisfaction_action` | server | Satisfacción del cliente | `survey.user.input` |  |  | `addons/quimibond_sgi/views/sgi_complaint_views.xml` |
| `quimibond_sgi.sgi_slide_channel_action` | act_window | Cursos y competencias (eLearning) | `slide.channel` | list | sí | `addons/quimibond_sgi/views/sgi_sign_elearning_views.xml` |
| `quimibond_sgi.sgi_spreadsheet_dashboard_action` | client | Tablero SGI |  |  |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `quimibond_sgi.sgi_staff_efficiency_action` | act_window | Eficiencias de personal | `sgi.staff.efficiency` | list,form | sí | `addons/quimibond_sgi/views/sgi_staff_efficiency_views.xml` |
| `quimibond_sgi.sgi_staff_efficiency_line_action` | act_window | Análisis por empleado | `sgi.staff.efficiency.line` | pivot,list | sí | `addons/quimibond_sgi/views/sgi_staff_efficiency_views.xml` |
| `quimibond_sgi.sgi_supplier_eval_action` | act_window | Evaluación de proveedores | `sgi.supplier.eval` | list,form | sí | `addons/quimibond_sgi/views/sgi_supplier_eval_views.xml` |
| `quimibond_sgi.sgi_supplier_eval_recompute_action` | server | Recalcular métricas | `sgi.supplier.eval` |  |  | `addons/quimibond_sgi/views/sgi_supplier_eval_views.xml` |
| `quimibond_sgi.sgi_work_permit_action` | act_window | Permisos de trabajo de alto riesgo | `sgi.work.permit` | list,kanban,form | sí | `addons/quimibond_sgi/views/sgi_work_permit_views.xml` |
| `quimibond_sgi_mapa.sgi_mapa_load_wizard_action` | act_window | Cargar mapa de procesos | `sgi.mapa.load.wizard` | form |  | `addons/quimibond_sgi_mapa/views/sgi_mapa_views.xml` |
| `quimibond_sgi_revisado.mrp_revision_log_action_pareto` | act_window | Pareto de defectos de revisado | `mrp.revision.log` | pivot,graph,list | sí | `addons/quimibond_sgi_revisado/views/mrp_revision_log_views.xml` |

## Vistas por modelo (358; 63 heredan de otra vista, 0 de su propio módulo)

| Modelo | Vista | Tipo | Hereda de | Archivo |
|---|---|---|---|---|
| `account.move` | `quimibond_sgi.sgi_account_move_view_form_kpi` | herencia | `account.view_move_form` | `addons/quimibond_sgi/views/sgi_kpi_fields_views.xml` |
| `account.move` | `quimibond_sgi.sgi_account_move_view_form_links` | herencia | `account.view_move_form` | `addons/quimibond_sgi/views/sgi_links_views.xml` |
| `approval.category` | `quimibond_sgi.sgi_approval_category_view_form` | herencia | `approvals.approval_category_view_form` | `addons/quimibond_sgi/views/sgi_doc_change_views.xml` |
| `approval.request` | `quimibond_sgi.sgi_approval_request_view_form` | herencia | `approvals.approval_request_view_form` | `addons/quimibond_sgi/views/sgi_doc_change_views.xml` |
| `approval.request` | `quimibond_sgi.sgi_approval_request_view_form_mp_change` | herencia | `approvals.approval_request_view_form` | `addons/quimibond_sgi/views/sgi_mp_change_views.xml` |
| `approval.request` | `quimibond_sgi.sgi_doc_change_view_list` | list |  | `addons/quimibond_sgi/views/sgi_doc_change_views.xml` |
| `approval.request` | `quimibond_sgi.sgi_doc_change_view_search` | search |  | `addons/quimibond_sgi/views/sgi_doc_change_views.xml` |
| `crm.team` | `quimibond_sgi.sgi_crm_team_view_form_complaint_days` | herencia | `sales_team.crm_team_view_form` | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `documents.document` | `quimibond_sgi.documents_document_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_current_document_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_current_documents_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_current_document_view_list` | list |  | `addons/quimibond_sgi/views/sgi_current_documents_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_current_document_view_search` | search |  | `addons/quimibond_sgi/views/sgi_current_documents_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_document_master_list_view_list` | list |  | `addons/quimibond_sgi/report/report_master_list_all.xml` |
| `documents.document` | `quimibond_sgi.sgi_document_view_form` | form |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_document_view_list` | list |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_document_view_search` | search |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_dropbox_document_view_form` | form |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_dropbox_procedure_view_form` | form |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_dropbox_procedure_view_list` | list |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_dropbox_procedure_view_search` | search |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_external_doc_view_list` | list |  | `addons/quimibond_sgi/views/sgi_external_doc_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_external_doc_view_search` | search |  | `addons/quimibond_sgi/views/sgi_external_doc_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_migration_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_migration_view_list` | list |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `documents.document` | `quimibond_sgi.sgi_migration_view_search` | search |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `helpdesk.team` | `quimibond_sgi.sgi_helpdesk_team_view_form` | herencia | `helpdesk.helpdesk_team_view_form` | `addons/quimibond_sgi/views/sgi_complaint_views.xml` |
| `helpdesk.ticket` | `quimibond_sgi.sgi_helpdesk_ticket_view_form` | herencia | `helpdesk.helpdesk_ticket_view_form` | `addons/quimibond_sgi/views/sgi_complaint_views.xml` |
| `hr.employee` | `quimibond_sgi.sgi_employee_view_form_competence` | herencia | `hr.view_employee_form` | `addons/quimibond_sgi/views/sgi_competence_views.xml` |
| `hr.employee` | `quimibond_sgi.sgi_hr_employee_gaps_view_list` | list |  | `addons/quimibond_sgi/views/sgi_floor_views.xml` |
| `hr.employee` | `quimibond_sgi.sgi_hr_employee_gaps_view_search` | search |  | `addons/quimibond_sgi/views/sgi_floor_views.xml` |
| `hr.employee` | `quimibond_sgi.sgi_hr_employee_view_form` | herencia | `hr.view_employee_form` | `addons/quimibond_sgi/views/sgi_integration_views.xml` |
| `hr.employee` | `quimibond_sgi.sgi_hr_employee_view_form_epp` | herencia | `hr.view_employee_form` | `addons/quimibond_sgi/views/sgi_epp_views.xml` |
| `hr.employee` | `quimibond_sgi.sgi_hr_employee_view_form_health` | herencia | `hr.view_employee_form` | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `hr.employee` | `quimibond_sgi.sgi_hr_employee_view_form_mp_tab` | herencia | `hr.view_employee_form` | `addons/quimibond_sgi/views/sgi_my_procedure_tab_views.xml` |
| `hr.employee.public` | `quimibond_sgi.sgi_hr_employee_public_view_form_mp_tab` | herencia | `hr.hr_employee_public_view_form` | `addons/quimibond_sgi/views/sgi_my_procedure_tab_views.xml` |
| `hr.employee.public` | `quimibond_sgi.sgi_my_team_view_hierarchy` | hierarchy |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `hr.employee.public` | `quimibond_sgi.sgi_my_team_view_list` | list |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `hr.employee.public` | `quimibond_sgi.sgi_my_team_view_search` | search |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `hr.job` | `quimibond_sgi.hr_job_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `hr.job` | `quimibond_sgi.sgi_hr_job_view_form` | herencia | `hr.view_hr_job_form` | `addons/quimibond_sgi/views/sgi_integration_views.xml` |
| `hr.job` | `quimibond_sgi.sgi_hr_job_view_form_headcount` | herencia | `hr.view_hr_job_form` | `addons/quimibond_sgi/views/sgi_hr_job_headcount_views.xml` |
| `hr.job` | `quimibond_sgi.sgi_hr_job_view_form_mp_tab` | herencia | `hr.view_hr_job_form` | `addons/quimibond_sgi/views/sgi_my_procedure_tab_views.xml` |
| `hr.job` | `quimibond_sgi.sgi_hr_job_view_list_headcount` | herencia | `hr.view_hr_job_tree` | `addons/quimibond_sgi/views/sgi_hr_job_headcount_views.xml` |
| `maintenance.equipment` | `quimibond_sgi.maintenance_equipment_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `maintenance.equipment` | `quimibond_sgi.sgi_equipment_view_form` | herencia | `maintenance.hr_equipment_view_form` | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `maintenance.equipment` | `quimibond_sgi.sgi_equipment_view_form_msa` | herencia | `maintenance.hr_equipment_view_form` | `addons/quimibond_sgi/views/sgi_msa_views.xml` |
| `maintenance.equipment` | `quimibond_sgi.sgi_equipment_view_list_lab` | list |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `maintenance.equipment` | `quimibond_sgi.sgi_equipment_view_list_measuring` | list |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `maintenance.equipment` | `quimibond_sgi.sgi_equipment_view_list_sgi` | herencia | `maintenance.hr_equipment_view_tree` | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `maintenance.equipment` | `quimibond_sgi.sgi_equipment_view_search_lab` | search |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `maintenance.equipment` | `quimibond_sgi.sgi_equipment_view_search_measuring` | search |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `maintenance.request` | `quimibond_sgi.sgi_checklist_request_view_form` | form |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `maintenance.request` | `quimibond_sgi.sgi_checklist_request_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `maintenance.request` | `quimibond_sgi.sgi_checklist_request_view_list` | list |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `maintenance.request` | `quimibond_sgi.sgi_checklist_request_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `maintenance.request` | `quimibond_sgi.sgi_format_banner_maintenance_request` | herencia | `maintenance.hr_equipment_request_view_form` | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `maintenance.request` | `quimibond_sgi.sgi_maintenance_request_form` | herencia | `maintenance.hr_equipment_request_view_form` | `addons/quimibond_sgi/views/sgi_map_hooks_views.xml` |
| `maintenance.request` | `quimibond_sgi.sgi_maintenance_request_view_form_checklist` | herencia | `maintenance.hr_equipment_request_view_form` | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `mrp.bom` | `quimibond_sgi.sgi_mrp_bom_view_form_links` | herencia | `mrp.mrp_bom_form_view` | `addons/quimibond_sgi/views/sgi_links_views.xml` |
| `mrp.eco` | `quimibond_sgi_plm.sgi_mrp_eco_view_form` | herencia | `mrp_plm.mrp_eco_view_form` | `addons/quimibond_sgi_plm/views/mrp_eco_views.xml` |
| `mrp.production` | `quimibond_sgi.sgi_format_banner_mrp_production` | herencia | `mrp.mrp_production_form_view` | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `mrp.production` | `quimibond_sgi.sgi_mrp_production_view_form` | herencia | `mrp.mrp_production_form_view` | `addons/quimibond_sgi/views/sgi_integration_views.xml` |
| `mrp.production` | `quimibond_sgi.sgi_mrp_production_view_form_links` | herencia | `mrp.mrp_production_form_view` | `addons/quimibond_sgi/views/sgi_links_views.xml` |
| `mrp.revision.log` | `quimibond_sgi_revisado.mrp_revision_log_view_graph` | graph |  | `addons/quimibond_sgi_revisado/views/mrp_revision_log_views.xml` |
| `mrp.revision.log` | `quimibond_sgi_revisado.mrp_revision_log_view_pivot` | pivot |  | `addons/quimibond_sgi_revisado/views/mrp_revision_log_views.xml` |
| `mrp.revision.log` | `quimibond_sgi_revisado.mrp_revision_log_view_search` | search |  | `addons/quimibond_sgi_revisado/views/mrp_revision_log_views.xml` |
| `mrp.workcenter` | `quimibond_sgi.sgi_workcenter_view_form_machine_sheet` | herencia | `mrp.mrp_workcenter_view` | `addons/quimibond_sgi/views/sgi_machine_sheet_views.xml` |
| `product.template` | `quimibond_sgi.sgi_ppap_product_template_form` | herencia | `product.product_template_form_view` | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `product.template` | `quimibond_sgi.sgi_product_template_view_form_kpi` | herencia | `product.product_template_form_view` | `addons/quimibond_sgi/views/sgi_kpi_fields_views.xml` |
| `product.template` | `quimibond_sgi.sgi_product_template_view_form_spec` | herencia | `product.product_template_form_view` | `addons/quimibond_sgi/views/sgi_integration_views.xml` |
| `project.project` | `quimibond_sgi.sgi_project_view_form_dev_request` | herencia | `project.edit_project` | `addons/quimibond_sgi/views/sgi_dev_request_views.xml` |
| `project.task` | `quimibond_sgi.sgi_project_task_view_form` | herencia | `project.view_task_form2` | `addons/quimibond_sgi/views/sgi_improvement_views.xml` |
| `project.task` | `quimibond_sgi.sgi_project_task_view_form_links` | herencia | `project.view_task_form2` | `addons/quimibond_sgi/views/sgi_links_views.xml` |
| `purchase.order` | `quimibond_sgi.sgi_format_banner_purchase_order` | herencia | `purchase.purchase_order_form` | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `purchase.order` | `quimibond_sgi.sgi_purchase_order_form` | herencia | `purchase.purchase_order_form` | `addons/quimibond_sgi/views/sgi_map_hooks_views.xml` |
| `purchase.order` | `quimibond_sgi.sgi_purchase_order_view_form_links` | herencia | `purchase.purchase_order_form` | `addons/quimibond_sgi/views/sgi_links_views.xml` |
| `purchase.order` | `quimibond_sgi.sgi_purchase_order_view_form_sign` | herencia | `purchase.purchase_order_form` | `addons/quimibond_sgi/views/sgi_supplier_audit_sign_views.xml` |
| `quality.alert` | `quimibond_sgi.quality_alert_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `quality.alert` | `quimibond_sgi.sgi_nc_lessons_view_list` | list |  | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `quality.alert` | `quimibond_sgi.sgi_nc_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `quality.alert` | `quimibond_sgi.sgi_nc_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `quality.alert` | `quimibond_sgi.sgi_nc_view_list` | list |  | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `quality.alert` | `quimibond_sgi.sgi_nc_view_search` | search |  | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `quality.alert` | `quimibond_sgi.sgi_quality_alert_view_form` | herencia | `quality_control.quality_alert_view_form` | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `quality.alert` | `quimibond_sgi.sgi_quality_alert_view_graph` | graph |  | `addons/quimibond_sgi/views/sgi_dashboard_views.xml` |
| `quality.alert` | `quimibond_sgi.sgi_quality_alert_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_dashboard_views.xml` |
| `quality.point` | `quimibond_sgi.sgi_quality_point_view_form` | herencia | `quality.quality_point_view_form` | `addons/quimibond_sgi/views/sgi_control_plan_views.xml` |
| `res.config.settings` | `quimibond_sgi.res_config_settings_view_form_sgi` | herencia | `base.res_config_settings_view_form` | `addons/quimibond_sgi/views/sgi_settings_views.xml` |
| `res.config.settings` | `quimibond_sgi_pesaje.res_config_settings_view_form_sgi_pesaje` | herencia | `quimibond_sgi.res_config_settings_view_form_sgi` | `addons/quimibond_sgi_pesaje/views/res_config_settings_views.xml` |
| `res.partner` | `quimibond_sgi.sgi_coa_res_partner_view_form` | herencia | `base.view_partner_form` | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `res.partner` | `quimibond_sgi.sgi_ppap_res_partner_form` | herencia | `base.view_partner_form` | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `res.partner` | `quimibond_sgi.sgi_res_partner_supplier_view_form` | herencia | `base.view_partner_form` | `addons/quimibond_sgi/views/sgi_res_partner_views.xml` |
| `res.users` | `quimibond_sgi.sgi_res_users_view_form_preferences` | herencia | `base.view_users_form_simple_modif` | `addons/quimibond_sgi/views/sgi_my_pending_views.xml` |
| `sale.order` | `quimibond_sgi.sgi_coa_sale_order_view_form` | herencia | `sale.view_order_form` | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `sale.order` | `quimibond_sgi.sgi_coa_sale_order_view_list` | herencia | `sale.view_order_tree` | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `sale.order` | `quimibond_sgi.sgi_coa_sale_order_view_search` | herencia | `sale.view_sales_order_filter` | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `sale.order` | `quimibond_sgi.sgi_format_banner_sale_order` | herencia | `sale.view_order_form` | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `sgi.action.line` | `quimibond_sgi.sgi_action_line_view_form` | form |  | `addons/quimibond_sgi/views/sgi_action_line_views.xml` |
| `sgi.action.line` | `quimibond_sgi.sgi_action_line_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_action_line_views.xml` |
| `sgi.action.line` | `quimibond_sgi.sgi_action_line_view_list` | list |  | `addons/quimibond_sgi/views/sgi_action_line_views.xml` |
| `sgi.action.line` | `quimibond_sgi.sgi_action_line_view_search` | search |  | `addons/quimibond_sgi/views/sgi_action_line_views.xml` |
| `sgi.activity.change` | `quimibond_sgi.sgi_activity_change_view_form` | form |  | `addons/quimibond_sgi/views/sgi_mp_change_views.xml` |
| `sgi.activity.exec.stat` | `quimibond_sgi.sgi_activity_exec_stat_view_list` | list |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.activity.exec.stat` | `quimibond_sgi.sgi_activity_exec_stat_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.activity.exec.stat` | `quimibond_sgi.sgi_activity_exec_stat_view_search` | search |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.activity.exec.stat` | `quimibond_sgi.sgi_activity_exec_stat_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.activity.link` | `quimibond_sgi.sgi_activity_link_view_list` | list |  | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `sgi.activity.link` | `quimibond_sgi.sgi_activity_link_view_search` | search |  | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_kanban_mp` | kanban |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_list` | list |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_list_approval` | list |  | `addons/quimibond_sgi/views/sgi_approval_native_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_list_mp_embedded` | list |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_list_mp_received` | list |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_list_mp_short` | list |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_list_my_procedure` | list |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_search` | search |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.activity.role` | `quimibond_sgi.sgi_activity_role_view_search_approval` | search |  | `addons/quimibond_sgi/views/sgi_approval_native_views.xml` |
| `sgi.activity.role` | `quimibond_sgi_studio.sgi_activity_role_view_list_approval_studio` | herencia | `quimibond_sgi.sgi_activity_role_view_list_approval` | `addons/quimibond_sgi_studio/views/sgi_approval_studio_views.xml` |
| `sgi.activity.role` | `quimibond_sgi_studio.sgi_activity_role_view_search_approval_studio` | herencia | `quimibond_sgi.sgi_activity_role_view_search_approval` | `addons/quimibond_sgi_studio/views/sgi_approval_studio_views.xml` |
| `sgi.activity.spec.gap` | `quimibond_sgi.sgi_activity_spec_gap_view_list` | list |  | `addons/quimibond_sgi/views/sgi_activity_spec_views.xml` |
| `sgi.activity.spec.gap` | `quimibond_sgi.sgi_activity_spec_gap_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_activity_spec_views.xml` |
| `sgi.activity.spec.gap` | `quimibond_sgi.sgi_activity_spec_gap_view_search` | search |  | `addons/quimibond_sgi/views/sgi_activity_spec_views.xml` |
| `sgi.activity.week.stat` | `quimibond_sgi.sgi_activity_week_stat_view_list` | list |  | `addons/quimibond_sgi/views/sgi_activity_spec_views.xml` |
| `sgi.activity.week.stat` | `quimibond_sgi.sgi_activity_week_stat_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_activity_spec_views.xml` |
| `sgi.activity.week.stat` | `quimibond_sgi.sgi_activity_week_stat_view_search` | search |  | `addons/quimibond_sgi/views/sgi_activity_spec_views.xml` |
| `sgi.acuse.attach.wizard` | `quimibond_sgi.sgi_acuse_attach_wizard_view_form` | form |  | `addons/quimibond_sgi/views/sgi_links_views.xml` |
| `sgi.alert.source` | `quimibond_sgi.sgi_alert_source_view_form` | form |  | `addons/quimibond_sgi/views/sgi_alert_source_views.xml` |
| `sgi.alert.source` | `quimibond_sgi.sgi_alert_source_view_list` | list |  | `addons/quimibond_sgi/views/sgi_alert_source_views.xml` |
| `sgi.alert.source` | `quimibond_sgi.sgi_alert_source_view_search` | search |  | `addons/quimibond_sgi/views/sgi_alert_source_views.xml` |
| `sgi.area` | `quimibond_sgi.sgi_area_view_form` | form |  | `addons/quimibond_sgi/views/sgi_area_views.xml` |
| `sgi.area` | `quimibond_sgi.sgi_area_view_list` | list |  | `addons/quimibond_sgi/views/sgi_area_views.xml` |
| `sgi.area` | `quimibond_sgi.sgi_area_view_search` | search |  | `addons/quimibond_sgi/views/sgi_area_views.xml` |
| `sgi.audit` | `quimibond_sgi.sgi_audit_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit` | `quimibond_sgi.sgi_audit_view_calendar` | calendar |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit` | `quimibond_sgi.sgi_audit_view_form` | form |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit` | `quimibond_sgi.sgi_audit_view_list` | list |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit` | `quimibond_sgi.sgi_audit_view_search` | search |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit.finding` | `quimibond_sgi.sgi_audit_finding_view_form` | form |  | `addons/quimibond_sgi/views/sgi_audit_finding_legal_eval_views.xml` |
| `sgi.audit.finding` | `quimibond_sgi.sgi_audit_finding_view_list` | list |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit.finding` | `quimibond_sgi.sgi_audit_finding_view_search` | search |  | `addons/quimibond_sgi/views/sgi_audit_finding_legal_eval_views.xml` |
| `sgi.audit.program` | `quimibond_sgi.sgi_audit_program_view_form` | form |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit.program` | `quimibond_sgi.sgi_audit_program_view_list` | list |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit.program` | `quimibond_sgi.sgi_audit_program_view_search` | search |  | `addons/quimibond_sgi/views/sgi_audit_views.xml` |
| `sgi.audit.program` | `quimibond_sgi.sgi_audit_program_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.calibration` | `quimibond_sgi.sgi_calibration_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `sgi.calibration` | `quimibond_sgi.sgi_calibration_view_calendar` | calendar |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `sgi.calibration` | `quimibond_sgi.sgi_calibration_view_form` | form |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `sgi.calibration` | `quimibond_sgi.sgi_calibration_view_list` | list |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `sgi.calibration` | `quimibond_sgi.sgi_calibration_view_search` | search |  | `addons/quimibond_sgi/views/sgi_calibration_views.xml` |
| `sgi.catalog.load.wizard` | `quimibond_sgi.sgi_catalog_load_wizard_view_form` | form |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.checklist.finish` | `quimibond_sgi.sgi_checklist_finish_view_form` | form |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.checklist.template` | `quimibond_sgi.sgi_checklist_template_view_form` | form |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.checklist.template` | `quimibond_sgi.sgi_checklist_template_view_list` | list |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.checklist.template` | `quimibond_sgi.sgi_checklist_template_view_search` | search |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.coa.attach.wizard` | `quimibond_sgi.sgi_coa_attach_wizard_view_form` | form |  | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `sgi.coa.exception.wizard` | `quimibond_sgi.sgi_coa_exception_wizard_view_form` | form |  | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `sgi.coa.inbox` | `quimibond_sgi.sgi_coa_inbox_view_form` | form |  | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `sgi.coa.inbox` | `quimibond_sgi.sgi_coa_inbox_view_list` | list |  | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `sgi.coa.inbox` | `quimibond_sgi.sgi_coa_inbox_view_search` | search |  | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `sgi.competence.gap` | `quimibond_sgi.sgi_competence_gap_view_list` | list |  | `addons/quimibond_sgi/views/sgi_competence_views.xml` |
| `sgi.competence.gap` | `quimibond_sgi.sgi_competence_gap_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_competence_views.xml` |
| `sgi.competence.gap` | `quimibond_sgi.sgi_competence_gap_view_search` | search |  | `addons/quimibond_sgi/views/sgi_competence_views.xml` |
| `sgi.competence.gap` | `quimibond_sgi.sgi_competence_gap_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.control.plan` | `quimibond_sgi.sgi_control_plan_view_form` | form |  | `addons/quimibond_sgi/views/sgi_control_plan_views.xml` |
| `sgi.control.plan` | `quimibond_sgi.sgi_control_plan_view_list` | list |  | `addons/quimibond_sgi/views/sgi_control_plan_views.xml` |
| `sgi.control.plan` | `quimibond_sgi.sgi_control_plan_view_search` | search |  | `addons/quimibond_sgi/views/sgi_control_plan_views.xml` |
| `sgi.csh.inspection` | `quimibond_sgi.sgi_csh_inspection_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.csh.inspection` | `quimibond_sgi.sgi_csh_inspection_view_form` | form |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.csh.inspection` | `quimibond_sgi.sgi_csh_inspection_view_list` | list |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.csh.inspection` | `quimibond_sgi.sgi_csh_inspection_view_search` | search |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.deliverable` | `quimibond_sgi.sgi_deliverable_view_form` | form |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.deliverable` | `quimibond_sgi.sgi_deliverable_view_list` | list |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.deliverable` | `quimibond_sgi.sgi_deliverable_view_search` | search |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.diagnostic.line` | `quimibond_sgi.sgi_diagnostic_line_view_form` | form |  | `addons/quimibond_sgi/views/sgi_diagnostic_views.xml` |
| `sgi.diagnostic.line` | `quimibond_sgi.sgi_diagnostic_line_view_list` | list |  | `addons/quimibond_sgi/views/sgi_diagnostic_views.xml` |
| `sgi.diagnostic.line` | `quimibond_sgi.sgi_diagnostic_line_view_search` | search |  | `addons/quimibond_sgi/views/sgi_diagnostic_views.xml` |
| `sgi.direction.board` | `quimibond_sgi.sgi_direction_board_view_form` | form |  | `addons/quimibond_sgi/views/sgi_direction_board_views.xml` |
| `sgi.document.ack` | `quimibond_sgi.sgi_document_ack_view_form` | form |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `sgi.document.ack` | `quimibond_sgi.sgi_document_ack_view_list` | list |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `sgi.document.ack` | `quimibond_sgi.sgi_document_ack_view_search` | search |  | `addons/quimibond_sgi/views/sgi_document_views.xml` |
| `sgi.document.type` | `quimibond_sgi.sgi_document_type_view_list` | list |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.document.type` | `quimibond_sgi.sgi_document_type_view_search` | search |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.dropbox.key` | `quimibond_sgi.sgi_dropbox_key_view_form` | form |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.dropbox.key` | `quimibond_sgi.sgi_dropbox_key_view_list` | list |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.dropbox.key` | `quimibond_sgi.sgi_dropbox_key_view_search` | search |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.dropbox.progress` | `quimibond_sgi.sgi_dropbox_progress_view_graph` | graph |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.dropbox.progress` | `quimibond_sgi.sgi_dropbox_progress_view_list` | list |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.dropbox.progress` | `quimibond_sgi.sgi_dropbox_progress_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.emergency.drill` | `quimibond_sgi.sgi_emergency_drill_view_calendar` | calendar |  | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `sgi.emergency.drill` | `quimibond_sgi.sgi_emergency_drill_view_form` | form |  | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `sgi.emergency.drill` | `quimibond_sgi.sgi_emergency_drill_view_list` | list |  | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `sgi.emergency.drill` | `quimibond_sgi.sgi_emergency_drill_view_search` | search |  | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `sgi.emergency.plan` | `quimibond_sgi.sgi_emergency_plan_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `sgi.emergency.plan` | `quimibond_sgi.sgi_emergency_plan_view_form` | form |  | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `sgi.emergency.plan` | `quimibond_sgi.sgi_emergency_plan_view_list` | list |  | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `sgi.emergency.plan` | `quimibond_sgi.sgi_emergency_plan_view_search` | search |  | `addons/quimibond_sgi/views/sgi_emergency_views.xml` |
| `sgi.emergency.plan` | `quimibond_sgi.sgi_emergency_plan_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.env.aspect` | `quimibond_sgi.sgi_env_aspect_view_form` | form |  | `addons/quimibond_sgi/views/sgi_env_aspect_views.xml` |
| `sgi.env.aspect` | `quimibond_sgi.sgi_env_aspect_view_list` | list |  | `addons/quimibond_sgi/views/sgi_env_aspect_views.xml` |
| `sgi.env.aspect` | `quimibond_sgi.sgi_env_aspect_view_search` | search |  | `addons/quimibond_sgi/views/sgi_env_aspect_views.xml` |
| `sgi.epp.delivery` | `quimibond_sgi.sgi_epp_delivery_view_form` | form |  | `addons/quimibond_sgi/views/sgi_epp_views.xml` |
| `sgi.epp.delivery` | `quimibond_sgi.sgi_epp_delivery_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_epp_views.xml` |
| `sgi.epp.delivery` | `quimibond_sgi.sgi_epp_delivery_view_list` | list |  | `addons/quimibond_sgi/views/sgi_epp_views.xml` |
| `sgi.epp.delivery` | `quimibond_sgi.sgi_epp_delivery_view_search` | search |  | `addons/quimibond_sgi/views/sgi_epp_views.xml` |
| `sgi.floor.tablet` | `quimibond_sgi.sgi_floor_tablet_view_form` | form |  | `addons/quimibond_sgi/views/sgi_floor_views.xml` |
| `sgi.floor.tablet` | `quimibond_sgi.sgi_floor_tablet_view_list` | list |  | `addons/quimibond_sgi/views/sgi_floor_views.xml` |
| `sgi.floor.tablet` | `quimibond_sgi.sgi_floor_tablet_view_search` | search |  | `addons/quimibond_sgi/views/sgi_floor_views.xml` |
| `sgi.fmea` | `quimibond_sgi.sgi_fmea_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_fmea_views.xml` |
| `sgi.fmea` | `quimibond_sgi.sgi_fmea_view_form` | form |  | `addons/quimibond_sgi/views/sgi_fmea_views.xml` |
| `sgi.fmea` | `quimibond_sgi.sgi_fmea_view_list` | list |  | `addons/quimibond_sgi/views/sgi_fmea_views.xml` |
| `sgi.fmea` | `quimibond_sgi.sgi_fmea_view_search` | search |  | `addons/quimibond_sgi/views/sgi_fmea_views.xml` |
| `sgi.format.map` | `quimibond_sgi.sgi_format_map_view_form` | form |  | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `sgi.format.map` | `quimibond_sgi.sgi_format_map_view_list` | list |  | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `sgi.format.map` | `quimibond_sgi.sgi_format_map_view_search` | search |  | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `sgi.health.record` | `quimibond_sgi.sgi_health_record_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.health.record` | `quimibond_sgi.sgi_health_record_view_form` | form |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.health.record` | `quimibond_sgi.sgi_health_record_view_list` | list |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.health.record` | `quimibond_sgi.sgi_health_record_view_search` | search |  | `addons/quimibond_sgi/views/sgi_hse_views.xml` |
| `sgi.incident` | `quimibond_sgi.sgi_incident_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_incident_views.xml` |
| `sgi.incident` | `quimibond_sgi.sgi_incident_view_form` | form |  | `addons/quimibond_sgi/views/sgi_incident_views.xml` |
| `sgi.incident` | `quimibond_sgi.sgi_incident_view_graph` | graph |  | `addons/quimibond_sgi/views/sgi_incident_views.xml` |
| `sgi.incident` | `quimibond_sgi.sgi_incident_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_incident_views.xml` |
| `sgi.incident` | `quimibond_sgi.sgi_incident_view_list` | list |  | `addons/quimibond_sgi/views/sgi_incident_views.xml` |
| `sgi.incident` | `quimibond_sgi.sgi_incident_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_incident_views.xml` |
| `sgi.incident` | `quimibond_sgi.sgi_incident_view_search` | search |  | `addons/quimibond_sgi/views/sgi_incident_views.xml` |
| `sgi.indicator` | `quimibond_sgi.sgi_indicator_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator` | `quimibond_sgi.sgi_indicator_view_form` | form |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator` | `quimibond_sgi.sgi_indicator_view_kanban_mine` | kanban |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.indicator` | `quimibond_sgi.sgi_indicator_view_list` | list |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator` | `quimibond_sgi.sgi_indicator_view_list_mine` | list |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.indicator` | `quimibond_sgi.sgi_indicator_view_search` | search |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator` | `quimibond_sgi.sgi_indicator_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.indicator.measure` | `quimibond_sgi.sgi_measure_view_form` | form |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator.measure` | `quimibond_sgi.sgi_measure_view_graph` | graph |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator.measure` | `quimibond_sgi.sgi_measure_view_list` | list |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator.measure` | `quimibond_sgi.sgi_measure_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator.measure` | `quimibond_sgi.sgi_measure_view_search` | search |  | `addons/quimibond_sgi/views/sgi_indicator_views.xml` |
| `sgi.indicator.measure.split` | `quimibond_sgi.sgi_measure_split_view_graph` | graph |  | `addons/quimibond_sgi/views/sgi_business_line_views.xml` |
| `sgi.indicator.measure.split` | `quimibond_sgi.sgi_measure_split_view_list` | list |  | `addons/quimibond_sgi/views/sgi_business_line_views.xml` |
| `sgi.indicator.measure.split` | `quimibond_sgi.sgi_measure_split_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_business_line_views.xml` |
| `sgi.indicator.measure.split` | `quimibond_sgi.sgi_measure_split_view_search` | search |  | `addons/quimibond_sgi/views/sgi_business_line_views.xml` |
| `sgi.indicator.term` | `quimibond_sgi.sgi_indicator_term_view_list` | list |  | `addons/quimibond_sgi/views/sgi_indicator_formula_views.xml` |
| `sgi.instruction.publish` | `quimibond_sgi_knowledge.sgi_instruction_publish_view_form` | form |  | `addons/quimibond_sgi_knowledge/views/sgi_instruction_knowledge_views.xml` |
| `sgi.interested.party` | `quimibond_sgi.sgi_interested_party_view_form` | form |  | `addons/quimibond_sgi/views/sgi_context_views.xml` |
| `sgi.interested.party` | `quimibond_sgi.sgi_interested_party_view_list` | list |  | `addons/quimibond_sgi/views/sgi_context_views.xml` |
| `sgi.interested.party` | `quimibond_sgi.sgi_interested_party_view_search` | search |  | `addons/quimibond_sgi/views/sgi_context_views.xml` |
| `sgi.interested.party` | `quimibond_sgi.sgi_interested_party_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.inventory.value` | `quimibond_sgi.sgi_inventory_value_view_list` | list |  | `addons/quimibond_sgi/views/sgi_kpi_fields_views.xml` |
| `sgi.job.family` | `quimibond_sgi.sgi_job_family_view_form` | form |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.job.family` | `quimibond_sgi.sgi_job_family_view_list` | list |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.job.family` | `quimibond_sgi.sgi_job_family_view_search` | search |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.legacy.routine` | `quimibond_sgi.sgi_legacy_routine_view_form` | form |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.legacy.routine` | `quimibond_sgi.sgi_legacy_routine_view_list` | list |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.legacy.routine` | `quimibond_sgi.sgi_legacy_routine_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.legacy.routine` | `quimibond_sgi.sgi_legacy_routine_view_search` | search |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.legacy.routine.import` | `quimibond_sgi.sgi_legacy_routine_import_view_form` | form |  | `addons/quimibond_sgi/views/sgi_dropbox_views.xml` |
| `sgi.legal.evaluate` | `quimibond_sgi.sgi_legal_evaluate_view_form` | form |  | `addons/quimibond_sgi/views/sgi_legal_views.xml` |
| `sgi.legal.evaluation` | `quimibond_sgi.sgi_legal_evaluation_view_form` | form |  | `addons/quimibond_sgi/views/sgi_audit_finding_legal_eval_views.xml` |
| `sgi.legal.evaluation` | `quimibond_sgi.sgi_legal_evaluation_view_list` | list |  | `addons/quimibond_sgi/views/sgi_legal_views.xml` |
| `sgi.legal.evaluation` | `quimibond_sgi.sgi_legal_evaluation_view_search` | search |  | `addons/quimibond_sgi/views/sgi_audit_finding_legal_eval_views.xml` |
| `sgi.legal.requirement` | `quimibond_sgi.sgi_legal_requirement_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_legal_views.xml` |
| `sgi.legal.requirement` | `quimibond_sgi.sgi_legal_requirement_view_form` | form |  | `addons/quimibond_sgi/views/sgi_legal_views.xml` |
| `sgi.legal.requirement` | `quimibond_sgi.sgi_legal_requirement_view_list` | list |  | `addons/quimibond_sgi/views/sgi_legal_views.xml` |
| `sgi.legal.requirement` | `quimibond_sgi.sgi_legal_requirement_view_search` | search |  | `addons/quimibond_sgi/views/sgi_legal_views.xml` |
| `sgi.legal.requirement` | `quimibond_sgi.sgi_legal_requirement_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.lock.date.log` | `quimibond_sgi.sgi_lock_date_log_view_list` | list |  | `addons/quimibond_sgi/views/sgi_kpi_fields_views.xml` |
| `sgi.loto` | `quimibond_sgi.sgi_loto_view_form` | form |  | `addons/quimibond_sgi/views/sgi_loto_views.xml` |
| `sgi.loto` | `quimibond_sgi.sgi_loto_view_list` | list |  | `addons/quimibond_sgi/views/sgi_loto_views.xml` |
| `sgi.loto` | `quimibond_sgi.sgi_loto_view_search` | search |  | `addons/quimibond_sgi/views/sgi_loto_views.xml` |
| `sgi.machine.sheet` | `quimibond_sgi.sgi_machine_sheet_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_machine_sheet_views.xml` |
| `sgi.machine.sheet` | `quimibond_sgi.sgi_machine_sheet_view_form` | form |  | `addons/quimibond_sgi/views/sgi_machine_sheet_views.xml` |
| `sgi.machine.sheet` | `quimibond_sgi.sgi_machine_sheet_view_list` | list |  | `addons/quimibond_sgi/views/sgi_machine_sheet_views.xml` |
| `sgi.machine.sheet` | `quimibond_sgi.sgi_machine_sheet_view_search` | search |  | `addons/quimibond_sgi/views/sgi_machine_sheet_views.xml` |
| `sgi.management.review` | `quimibond_sgi.sgi_management_review_view_form` | form |  | `addons/quimibond_sgi/views/sgi_management_review_views.xml` |
| `sgi.management.review` | `quimibond_sgi.sgi_management_review_view_list` | list |  | `addons/quimibond_sgi/views/sgi_management_review_views.xml` |
| `sgi.management.review` | `quimibond_sgi.sgi_management_review_view_search` | search |  | `addons/quimibond_sgi/views/sgi_management_review_views.xml` |
| `sgi.management.review` | `quimibond_sgi.sgi_management_review_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.mapa.load.wizard` | `quimibond_sgi_mapa.sgi_mapa_load_wizard_view_form` | form |  | `addons/quimibond_sgi_mapa/views/sgi_mapa_views.xml` |
| `sgi.msa.study` | `quimibond_sgi.sgi_msa_study_view_form` | form |  | `addons/quimibond_sgi/views/sgi_msa_views.xml` |
| `sgi.msa.study` | `quimibond_sgi.sgi_msa_study_view_list` | list |  | `addons/quimibond_sgi/views/sgi_msa_views.xml` |
| `sgi.msa.study` | `quimibond_sgi.sgi_msa_study_view_search` | search |  | `addons/quimibond_sgi/views/sgi_msa_views.xml` |
| `sgi.my.pending` | `quimibond_sgi.sgi_my_pending_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_my_pending_views.xml` |
| `sgi.my.pending` | `quimibond_sgi.sgi_my_pending_view_list` | list |  | `addons/quimibond_sgi/views/sgi_my_pending_views.xml` |
| `sgi.my.pending` | `quimibond_sgi.sgi_my_pending_view_search` | search |  | `addons/quimibond_sgi/views/sgi_my_pending_views.xml` |
| `sgi.my.procedure` | `quimibond_sgi.sgi_my_procedure_view_form` | form |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.my.procedure.check` | `quimibond_sgi.sgi_my_procedure_check_view_form` | form |  | `addons/quimibond_sgi/views/sgi_my_procedure_views.xml` |
| `sgi.nc.cancel` | `quimibond_sgi.sgi_nc_cancel_view_form` | form |  | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `sgi.nc.force.close` | `quimibond_sgi.sgi_nc_force_close_view_form` | form |  | `addons/quimibond_sgi/views/sgi_nonconformity_views.xml` |
| `sgi.norm` | `quimibond_sgi.sgi_norm_view_form` | form |  | `addons/quimibond_sgi/views/sgi_norm_views.xml` |
| `sgi.norm` | `quimibond_sgi.sgi_norm_view_list` | list |  | `addons/quimibond_sgi/views/sgi_norm_views.xml` |
| `sgi.norm` | `quimibond_sgi.sgi_norm_view_search` | search |  | `addons/quimibond_sgi/views/sgi_norm_views.xml` |
| `sgi.norm.clause` | `quimibond_sgi.sgi_norm_clause_view_form` | form |  | `addons/quimibond_sgi/views/sgi_norm_views.xml` |
| `sgi.norm.clause` | `quimibond_sgi.sgi_norm_clause_view_list` | list |  | `addons/quimibond_sgi/views/sgi_norm_views.xml` |
| `sgi.norm.clause` | `quimibond_sgi.sgi_norm_clause_view_search` | search |  | `addons/quimibond_sgi/views/sgi_norm_views.xml` |
| `sgi.objective` | `quimibond_sgi.sgi_objective_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_objective_views.xml` |
| `sgi.objective` | `quimibond_sgi.sgi_objective_view_form` | form |  | `addons/quimibond_sgi/views/sgi_objective_views.xml` |
| `sgi.objective` | `quimibond_sgi.sgi_objective_view_list` | list |  | `addons/quimibond_sgi/views/sgi_objective_views.xml` |
| `sgi.objective` | `quimibond_sgi.sgi_objective_view_search` | search |  | `addons/quimibond_sgi/views/sgi_objective_views.xml` |
| `sgi.objective` | `quimibond_sgi.sgi_objective_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.policy` | `quimibond_sgi.sgi_policy_view_form` | form |  | `addons/quimibond_sgi/views/sgi_policy_views.xml` |
| `sgi.policy` | `quimibond_sgi.sgi_policy_view_list` | list |  | `addons/quimibond_sgi/views/sgi_policy_views.xml` |
| `sgi.policy` | `quimibond_sgi.sgi_policy_view_search` | search |  | `addons/quimibond_sgi/views/sgi_policy_views.xml` |
| `sgi.policy` | `quimibond_sgi.sgi_policy_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.ppap` | `quimibond_sgi.sgi_ppap_view_form` | form |  | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `sgi.ppap` | `quimibond_sgi.sgi_ppap_view_list` | list |  | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `sgi.ppap` | `quimibond_sgi.sgi_ppap_view_search` | search |  | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `sgi.ppap.element.template` | `quimibond_sgi.sgi_ppap_element_template_view_list` | list |  | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `sgi.ppap.element.template` | `quimibond_sgi.sgi_ppap_element_template_view_search` | search |  | `addons/quimibond_sgi/views/sgi_ppap_views.xml` |
| `sgi.process` | `quimibond_sgi.sgi_process_view_form` | form |  | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `sgi.process` | `quimibond_sgi.sgi_process_view_hierarchy` | hierarchy |  | `addons/quimibond_sgi/views/sgi_hierarchy_views.xml` |
| `sgi.process` | `quimibond_sgi.sgi_process_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `sgi.process` | `quimibond_sgi.sgi_process_view_list` | list |  | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `sgi.process` | `quimibond_sgi.sgi_process_view_search` | search |  | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `sgi.process` | `quimibond_sgi.sgi_process_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_activity_compliance_view_graph` | graph |  | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_activity_compliance_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_activity_method_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_catalog_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_process_activity_view_form` | form |  | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_process_activity_view_hierarchy` | hierarchy |  | `addons/quimibond_sgi/views/sgi_hierarchy_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_process_activity_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_process_activity_view_list` | list |  | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_process_activity_view_search` | search |  | `addons/quimibond_sgi/views/sgi_process_procedure_views.xml` |
| `sgi.process.activity` | `quimibond_sgi.sgi_process_activity_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.process.activity` | `quimibond_sgi_knowledge.sgi_process_activity_view_form_knowledge` | herencia | `quimibond_sgi.sgi_process_activity_view_form` | `addons/quimibond_sgi_knowledge/views/sgi_instruction_knowledge_views.xml` |
| `sgi.process.flow` | `quimibond_sgi.sgi_process_flow_view_form` | form |  | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `sgi.process.flow` | `quimibond_sgi.sgi_process_flow_view_list` | list |  | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `sgi.process.flow` | `quimibond_sgi.sgi_process_flow_view_search` | search |  | `addons/quimibond_sgi/views/sgi_process_views.xml` |
| `sgi.risk` | `quimibond_sgi.sgi_risk_view_activity` | activity |  | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `sgi.risk` | `quimibond_sgi.sgi_risk_view_form` | form |  | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `sgi.risk` | `quimibond_sgi.sgi_risk_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `sgi.risk` | `quimibond_sgi.sgi_risk_view_list` | list |  | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `sgi.risk` | `quimibond_sgi.sgi_risk_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `sgi.risk` | `quimibond_sgi.sgi_risk_view_search` | search |  | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `sgi.risk` | `quimibond_sgi.sgi_risk_view_sgi_diagram` | sgi_diagram |  | `addons/quimibond_sgi/views/sgi_diagram_views.xml` |
| `sgi.risk.category` | `quimibond_sgi.sgi_risk_category_view_list` | list |  | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `sgi.risk.category` | `quimibond_sgi.sgi_risk_category_view_search` | search |  | `addons/quimibond_sgi/views/sgi_risk_views.xml` |
| `sgi.sign.request.wizard` | `quimibond_sgi.sgi_sign_request_wizard_view_form` | form |  | `addons/quimibond_sgi/views/sgi_supplier_audit_sign_views.xml` |
| `sgi.staff.efficiency` | `quimibond_sgi.sgi_staff_efficiency_view_form` | form |  | `addons/quimibond_sgi/views/sgi_staff_efficiency_views.xml` |
| `sgi.staff.efficiency` | `quimibond_sgi.sgi_staff_efficiency_view_list` | list |  | `addons/quimibond_sgi/views/sgi_staff_efficiency_views.xml` |
| `sgi.staff.efficiency` | `quimibond_sgi.sgi_staff_efficiency_view_search` | search |  | `addons/quimibond_sgi/views/sgi_staff_efficiency_views.xml` |
| `sgi.staff.efficiency.line` | `quimibond_sgi.sgi_staff_efficiency_line_view_list` | list |  | `addons/quimibond_sgi/views/sgi_staff_efficiency_views.xml` |
| `sgi.staff.efficiency.line` | `quimibond_sgi.sgi_staff_efficiency_line_view_pivot` | pivot |  | `addons/quimibond_sgi/views/sgi_staff_efficiency_views.xml` |
| `sgi.supplier.eval` | `quimibond_sgi.sgi_supplier_eval_view_form` | form |  | `addons/quimibond_sgi/views/sgi_supplier_eval_views.xml` |
| `sgi.supplier.eval` | `quimibond_sgi.sgi_supplier_eval_view_list` | list |  | `addons/quimibond_sgi/views/sgi_supplier_eval_views.xml` |
| `sgi.supplier.eval` | `quimibond_sgi.sgi_supplier_eval_view_search` | search |  | `addons/quimibond_sgi/views/sgi_supplier_eval_views.xml` |
| `sgi.work.permit` | `quimibond_sgi.sgi_work_permit_view_form` | form |  | `addons/quimibond_sgi/views/sgi_work_permit_views.xml` |
| `sgi.work.permit` | `quimibond_sgi.sgi_work_permit_view_kanban` | kanban |  | `addons/quimibond_sgi/views/sgi_work_permit_views.xml` |
| `sgi.work.permit` | `quimibond_sgi.sgi_work_permit_view_list` | list |  | `addons/quimibond_sgi/views/sgi_work_permit_views.xml` |
| `sgi.work.permit` | `quimibond_sgi.sgi_work_permit_view_search` | search |  | `addons/quimibond_sgi/views/sgi_work_permit_views.xml` |
| `slide.channel` | `quimibond_sgi.sgi_slide_channel_view_list` | list |  | `addons/quimibond_sgi/views/sgi_sign_elearning_views.xml` |
| `stock.lot` | `quimibond_sgi.sgi_format_banner_stock_lot` | herencia | `stock.view_production_lot_form` | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `stock.lot` | `quimibond_sgi.sgi_stock_lot_view_form` | herencia | `stock.view_production_lot_form` | `addons/quimibond_sgi/views/sgi_control_plan_views.xml` |
| `stock.lot` | `quimibond_sgi.sgi_stock_lot_view_form_sign` | herencia | `stock.view_production_lot_form` | `addons/quimibond_sgi/views/sgi_supplier_audit_sign_views.xml` |
| `stock.picking` | `quimibond_sgi.sgi_coa_stock_picking_view_form` | herencia | `stock.view_picking_form` | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `stock.picking` | `quimibond_sgi.sgi_coa_stock_picking_view_search` | herencia | `stock.view_picking_internal_search` | `addons/quimibond_sgi/views/sgi_coa_views.xml` |
| `stock.picking` | `quimibond_sgi.sgi_format_banner_stock_picking` | herencia | `stock.view_picking_form` | `addons/quimibond_sgi/views/sgi_format_map_views.xml` |
| `stock.picking` | `quimibond_sgi.sgi_stock_picking_view_form` | herencia | `stock.view_picking_form` | `addons/quimibond_sgi/views/sgi_integration_views.xml` |
| `stock.picking` | `quimibond_sgi.sgi_stock_picking_view_form_kpi` | herencia | `stock.view_picking_form` | `addons/quimibond_sgi/views/sgi_kpi_fields_views.xml` |
