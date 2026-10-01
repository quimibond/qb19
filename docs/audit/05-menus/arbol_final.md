# Árbol final del menú SGI (propuesta del agente E)

**Fecha:** 2026-09-29 · **Base:** árbol real de producción (`ir.ui.menu` `child_of` 2377, 74 entradas, todas activas; 0 archivadas) + los 31 menús del módulo fuera del SGI + decisión 2 y 7 del brief + `decisiones.md`. Detalle renglón por renglón: `menus_final.csv` (110 renglones: 105 existentes + 5 nuevos) y `acciones_revision.csv` (121 acciones).

**Convenciones**
- `id` = `ir.ui.menu` en producción. **Ningún menú existente se borra ni se vuelve a crear**: se renombra o se reparenta conservando su `res_id`, porque 57 actividades y 23 documentos apuntan a menús del módulo por `odoo_menu_id` (E-002).
- Grupos: 316 Usuario SGI · 317 Auditor SGI · 318 Jefe MAST y SGI · 319 Dirección de Operaciones · 325 Administrador SGI · 327 Captura de eficiencias · **SO** = grupo nuevo «Salud ocupacional» (decisión de Jose) · 3 `base.group_system` · 66 `quality.group_quality_user` · 68 `hr.group_hr_user` · 38 `purchase.group_purchase_user`.
- «hereda» = sin `groups` propio: lo ve quien ve la carpeta.
- Supuesto: se aplica F-013 (319 deja de implicar 318; «Dirección consulta y aprueba sin permisos de Jefe MAST»). Por eso 319 aparece explícito donde hoy lo cubría 318.
- **[existe]** = la acción ya existe; **[cambia]** = existe y hay que ajustarla; **[crear]** = acción nueva.
- El nombre de la acción (lo que sale en la miga de pan) se iguala al del menú; ver `acciones_revision.csv`, columna `nombre_propuesto`.

## Árbol dentro de «SGI» (raíz `menu_sgi_root`, id 2377, sec. 90, grupos 316, 317)

| Sec. | Menú (nombre final) | xml_id | id | Acción | Grupos finales | Cambio |
|---:|---|---|---:|---|---|---|
| 10 | **Inicio** | `menu_sgi_panel` | 2378 | — (carpeta) | hereda | — |
| 10 | · Mis pendientes | `menu_sgi_my_pending` | 2594 | `sgi_my_pending_action_mine` (server 4079) [cambia: E-003] | hereda | — |
| 20 | · Mi procedimiento | `menu_sgi_my_procedure` | 2568 | `sgi_my_procedure_action_mine` (server 4040) [existe] | hereda | — |
| 30 | · Mis indicadores | `menu_sgi_my_indicators` | 2595 | `sgi_indicator_action_mine` (4082) [existe] | hereda | — |
| 40 | · Mi equipo | `menu_sgi_my_team` | 2569 | `sgi_my_team_action_mine` (server 4041) [existe] | hereda | — |
| 50 | · Eficiencias de mi área | `menu_sgi_my_staff_efficiency` | 2604 | `sgi_staff_efficiency_action` (4067) [existe] | 327 | — |
| 20 | **Procesos** | `menu_sgi_processes` | 2381 | — | hereda | — |
| 10 | · Mapa de procesos | `menu_sgi_process_map` | 2513 | `sgi_process_action` (3832) [cambia: nombre «Mapa de procesos»] | hereda | — |
| 20 | · Actividades | `menu_sgi_activities` | 2584 | `sgi_process_activity_action` (3880) [cambia: nombre] | hereda | — |
| 30 | · Matriz de responsabilidades | `menu_sgi_analysis_who` | 2552 | `sgi_activity_exec_stat_action_who` (4026) [cambia: nombre, hoy «Quién hace qué»] | hereda | — |
| 40 | · Puestos y procesos | `menu_sgi_jobs_roles` | 2585 | `sgi_hr_job_action_roles` (4060) [existe] | hereda | — |
| 50 | · Fichas de proceso por máquina | `menu_sgi_machine_sheets` | 2592 | `sgi_machine_sheet_action` (4066) [existe] | hereda | — |
| 60 | · **Del Dropbox a Odoo** | `menu_sgi_dropbox` **(nuevo)** | — | — (carpeta) | hereda (316, 317); **edita solo 318** por ACL/vista | **Se crea** (decisión 7) |
| 10 | ·· Buscador por clave anterior | `menu_sgi_dropbox_search` **(nuevo)** | — | `sgi_dropbox_key_action` **[crear]** | hereda | ver abajo |
| 20 | ·· Procedimientos anteriores | `menu_sgi_dropbox_procedures` **(nuevo)** | — | `sgi_dropbox_procedure_action` **[crear]** | hereda | ver abajo |
| 30 | ·· Formatos y documentos anteriores | `menu_sgi_migration` (se **mueve**) | 2419 | `sgi_migration_action` (3870) **[cambia]** | hereda (hoy 318) | Se mueve desde Administración SGI → Documentos y se renombra |
| 40 | ·· Rutina por rutina | `menu_sgi_dropbox_routines` **(nuevo)** | — | `sgi_dropbox_routine_action` **[crear]** (modelo del agente L) | hereda | ver abajo |
| 50 | ·· Avance de la transición | `menu_sgi_dropbox_progress` **(nuevo)** | — | `sgi_dropbox_progress_action` **[crear]** | hereda | ver abajo |
| 30 | **Mejora** | `menu_sgi_improvement_group` | 2424 | — | hereda | — |
| 10 | · No conformidades | `menu_sgi_nc` | 2385 | `sgi_nc_board_action` (server 3973) [cambia: nombre y dominio de respaldo, E-012] | hereda | — |
| 20 | · Reclamaciones de clientes | `menu_sgi_complaints` | 2388 | `sgi_complaint_action` (server 3840) [cambia: E-012] | hereda | — |
| 30 | · Acciones correctivas | `menu_sgi_nc_all_actions` | 2422 | `sgi_action_line_action_all` (3873) [cambia: nombre] | **317, 318, 319** (hoy 318) | Grupos |
| 40 | · Mejora continua | `menu_sgi_improvements` | 2389 | `sgi_improvement_action` (server 3841) [cambia: nombre] | hereda | — |
| 50 | · Lecciones aprendidas | `menu_sgi_lessons` | 2502 | `sgi_nc_lessons_action` (3962) [existe] | hereda | — |
| 60 | · Quejas y sugerencias del personal | `menu_sgi_internal_complaints` | 2501 | `sgi_internal_complaint_action` (server 3964) [cambia: nombre, E-012] | hereda | — |
| 70 | · Auditorías | `menu_sgi_audits` | 2570 | — | hereda | — |
| 10 | ·· Programa | `menu_sgi_audit_programs` | 2396 | `sgi_audit_program_action` (3845) [existe] | hereda | Se renombra (hoy «Programa de auditorías») |
| 20 | ·· Auditorías realizadas | `menu_sgi_audit_list` | 2397 | `sgi_audit_action` (3846) [cambia: nombre] | hereda | — |
| 35 | **Seguridad y ambiente** | `menu_sgi_safety` | 2571 | — | hereda | — |
| 10 | · Incidentes y accidentes | `menu_sgi_incidents` | 2409 | `sgi_incident_action` (3858) [cambia: nombre, hoy «Incidentes SST»] | hereda | (ACL: F-009) |
| 20 | · Planes de emergencia | `menu_sgi_emergency_plans` | 2506 | `sgi_emergency_plan_action` (3965) [existe] | hereda | — |
| 30 | · Simulacros | `menu_sgi_emergency_drills` | 2507 | `sgi_emergency_drill_action` (3966) [existe] | hereda | — |
| 40 | · Recorridos CSH | `menu_sgi_csh_inspections` | 2601 | `sgi_csh_inspection_action` (4091) [cambia: nombre] | hereda | Se renombra |
| 50 | · Estudios de higiene y exámenes médicos | `menu_sgi_health_records` | 2600 | `sgi_health_record_action` (4090) [existe] | **SO, 318** (hoy 68, 318) | Grupos (F-004) |
| 60 | · Responsivas de EPP | `menu_sgi_epp_deliveries` | 2576 | `sgi_epp_delivery_action` (4046) [existe] | hereda | `<menuitem>` a `sgi_menus.xml` (A-001) |
| 70 | · Hojas de checklist | `menu_sgi_checklist_requests` | 2602 | `sgi_checklist_request_action` (4093) [cambia: agregar `help`] | hereda | Se renombra |
| 40 | **Dirección** | `menu_sgi_direction` | 2572 | — | **hereda (316, 317)** (hoy 317, 318, 319) | Grupos (F-011) |
| 10 | · Tablero | `menu_sgi_dashboard_health` | 2548 | `sgi_direction_board_action_open` (server 4047) [existe] | **317, 318, 319** | Se renombra + grupos |
| 20 | · Revisión por la dirección | `menu_sgi_mgmt_review` | 2400 | `sgi_management_review_action` (3851) [cambia: nombre] | **317, 318, 319** (hoy 318) | Grupos |
| 30 | · Política integral | `menu_sgi_policy` | 2429 | `sgi_policy_action` (3878) [cambia: nombre] | hereda (todos) | — |
| 40 | · Objetivos integrales | `menu_sgi_objectives` | 2394 | `sgi_objective_action` (3842) [cambia: nombre] | hereda (todos) | — |
| 50 | · Riesgos y oportunidades | `menu_sgi_risks` | 2398 | `sgi_risk_action` (3849) [existe] | hereda (todos) | — |
| 60 | · Requisitos legales | `menu_sgi_legal` | 2516 | `sgi_legal_requirement_action` (3982) [existe] | hereda (todos) | — |
| 70 | · Partes interesadas | `menu_sgi_interested_parties` | 2517 | `sgi_interested_party_action` (3983) [existe] | **317, 318, 319** | Grupos |
| 80 | · Satisfacción del cliente | `menu_sgi_satisfaction` | 2503 | `sgi_satisfaction_action` (server 3963) [existe] | **317, 318, 319** | Grupos |
| 90 | **Administración SGI** | `menu_sgi_admin` | 2573 | — | **317, 318, 319** (hoy 317, 318) | Grupos |
| 10 | · Documentos | `menu_sgi_documental` | 2425 | — | hereda | — |
| 10 | ·· Documentos | `menu_sgi_documents` | 2383 | `sgi_document_action` (3834) [cambia: nombre] | hereda | — |
| 20 | ·· Lista maestra | `menu_sgi_master_list` | 2593 | `sgi_document_master_list_action` (4075) [cambia: quitar «(DOC-3)»] | hereda | Se renombra |
| 30 | ·· Documentos externos | `menu_sgi_external_docs` | 2599 | `sgi_external_doc_action` (4089) [existe] | hereda | — |
| 40 | ·· Solicitudes de cambio | `menu_sgi_doc_changes` | 2390 | `sgi_doc_change_board_action` (server 3972) [cambia: nombre; `help` sin «F-P-G01-06»] | **317, 318, 319** (hoy 318) | Se renombra + grupos |
| 50 | ·· Tipos de documento | `menu_sgi_config_doc_types` | 2557 | `sgi_document_type_action` (4031) [existe] | 318 | Secuencia (60 → 50) |
| 20 | · Indicadores | `menu_sgi_admin_indicators` | 2574 | — | hereda | — |
| 10 | ·· Indicadores | `menu_sgi_indicators` | 2392 | `sgi_indicator_action` (3843) [existe] | hereda | — |
| 20 | ·· Mediciones | `menu_sgi_measures` | 2393 | `sgi_measure_action` (3844) [existe] | hereda | — |
| 30 | ·· Mediciones por equipo o mercado | `menu_sgi_measures_split` | 2598 | `sgi_measure_split_action` (4088) [existe] | hereda | — |
| 25 | · Aprobaciones del SGI | `menu_sgi_approvals_native` | 2596 | `sgi_activity_role_action_approval` (4083) [existe] | **317, 318, 319** (hoy 318) | Grupos (lectura para 317/319 según 06) |
| 30 | · Diagnóstico | `menu_sgi_analysis` | 2551 | — | **317, 318, 319** (hoy 318) | Grupos (pregunta 2) |
| 10 | ·· Diagnóstico del SGI | `menu_sgi_diagnostic` | 2509 | `sgi_diagnostic_run_action` (server 4045) [cambia: `group_ids`, F-016] | hereda | — |
| 20 | ·· Cobertura de medición | `menu_sgi_activity_methods` | 2554 | `sgi_activity_method_action` (4028) [existe] | hereda | — |
| 30 | ·· Cumplimiento de procedimientos | `menu_sgi_compliance` | 2515 | `sgi_activity_compliance_action` (3977) [existe] | hereda | — |
| 40 | ·· Faltantes de especificación | `menu_sgi_spec_gaps` | 2558 | `sgi_activity_spec_gap_action` (4032) [existe] | hereda | Secuencia 80 → 40; `<menuitem>` a `sgi_menus.xml` (A-025) |
| 50 | ·· Cumplimiento semanal | `menu_sgi_week_stats` | 2559 | `sgi_activity_week_stat_action` (4033) [existe] | hereda | Secuencia 90 → 50 (A-025) |
| 40 | · Firmas de lectura | `menu_sgi_admin_signatures` | 2575 | — | hereda | — |
| 10 | ·· Publicar Mi procedimiento | `menu_sgi_my_procedure_publish` | 2597 | `sgi_my_procedure_action_publish` (server 4080) [cambia: `group_ids` 318, F-016] | 318 | — |
| 20 | ·· Acuses de lectura | `menu_sgi_config_acks` | 2416 | `sgi_document_ack_action` (3836) [existe] | hereda | — |
| 50 | · Configuración | `menu_sgi_config` | 2411 | — | 318 | — |
| 2 | ·· Cargar catálogo | `menu_sgi_config_load` | 2555 | `sgi_catalog_load_wizard_action` (4029) [existe] | 325 | — |
| 3 | ·· Familias de puesto | `menu_sgi_job_families` | 2556 | `sgi_job_family_action` (4025) [cambia: nombre «Familias de puesto»] | hereda | — |
| 5 | ·· Ajustes | `menu_sgi_config_settings` | 2426 | `sgi_config_settings_action` (3874) [existe] | `base.group_system` | — |
| 10 | ·· Áreas | `menu_sgi_config_areas` | 2413 | `sgi_area_action` (3828) [cambia: nombre «Áreas»] | hereda | — |
| 20 | ·· Normas | `menu_sgi_config_norms` | 2414 | `sgi_norm_action` (3829) [cambia: nombre «Normas», no «Normas ISO»] | hereda | — |
| 30 | ·· Cláusulas | `menu_sgi_config_clauses` | 2415 | `sgi_norm_clause_action` (3830) [existe] | hereda | — |
| 40 | ·· Categorías de riesgo | `menu_sgi_config_risk_cat` | 2417 | `sgi_risk_category_action` (3848) [existe] | hereda | Secuencia 50 → 40 |
| 50 | ·· Checklists de planta y unidades | `menu_sgi_checklist_templates` | 2603 | `sgi_checklist_template_action` (4092) [existe] | hereda | Secuencia 60 → 50 (empataba con PPAP) |
| 60 | ·· Formatos en documentos de Odoo | `menu_sgi_config_format_map` | 2420 | `sgi_format_map_action` (3871) [existe] | hereda | Secuencia 70 → 60; es configuración viva del pie de formato (C-006), no se mueve; se enlaza desde «Del Dropbox a Odoo» |
| 70 | ·· Fuentes de NC automáticas | `menu_sgi_config_alert_sources` | 2491 | `sgi_alert_source_action` (3942) [existe] | hereda | Secuencia 80 → 70 |
| — | ~~Elementos PPAP~~ | `menu_sgi_config_ppap_elements` | 2412 | `sgi_ppap_element_template_action` (3857) | — | **Sale al satélite automotriz** (decisión 5): Calidad → Calidad preventiva → Configuración, conservando el `res_id` |

**Resultado:** 6 entradas de primer nivel (Inicio, Procesos, Mejora, Seguridad y ambiente, Dirección, Administración SGI) con 78 entradas bajo «SGI» (74 de hoy − Elementos PPAP + 5 nuevas), igual que la decisión 2 más la sección de la decisión 7.

## «Del Dropbox a Odoo»: el antes y el después (decisión 7)

Carpeta `menu_sgi_dropbox` en Procesos, secuencia 60. **Todos consultan** (316 y 317 por la raíz); **solo 318 edita**. Como los submenús apuntan a `documents.document` (que el Usuario SGI puede escribir por la app Documentos), el «solo lectura» no sale de los grupos del menú: las vistas de la sección llevan `create="0" edit="0"` para quien no sea 318 (vistas propias de la sección, o `readonly="not user_has_groups(...)"` en los campos de migración). Coordinar con 04-vistas y 06-seguridad (E-005).

| Sec. | Submenú | Qué muestra (antes → después) | Acción | Estado | Qué falta |
|---:|---|---|---|---|---|
| 10 | **Buscador por clave anterior** | Escribes `P-A14`, `F-P-C09-02`, `IT-…` o un número de rutina viejo y te dice **qué es hoy**: documento vigente u obsoleto, proceso que lo sustituye, menú o worksheet de Odoo donde se captura, actividad que lo cubre | `sgi_dropbox_key_action` | **Crear** | Lista (`documents.document`) con `domain [('sgi_previous_code','!=',False)]`, contexto `{'active_test': False}` y un campo de búsqueda **sin el límite de 12 meses** que hoy tiene el filtro de Documentos (`sgi_document_views.xml:258`). Para buscar también rutinas (`sgi.process.activity.legacy_number`, 144, todas archivadas) hace falta un modelo de consulta (vista SQL «clave anterior → destino») que diseña L. **Depende de C-006 → C-004/C-005**: hoy `sgi_previous_code` tiene 0 datos |
| 20 | **Procedimientos anteriores** | Los 52 procedimientos P del Dropbox con su proceso sustituto (`sgi_replaced_by_process_id`), estado de migración (En curso / Baja tramitada / No aplica), y los que se quedan como control operacional (P-A17…A20, P-S03) | `sgi_dropbox_procedure_action` | **Crear** | `documents.document`, `domain [('sgi_is_controlled','=',True),('sgi_doc_type','=','procedimiento')]` **sin** filtrar obsoletos, `search_default_group_by` estado de migración, vistas lista/ficha propias de solo lectura. Los 25 `sgi.process` archivados (P-*/MP-*) se ven desde la ficha (liga «Copia cargada en Odoo (archivada)») y no como menú aparte, para no traer procesos archivados al árbol (decisión 3) |
| 30 | **Formatos y documentos anteriores** | Formatos F y F-IT **con su destino** en Odoo (menú, worksheet o texto) y el resto de lo que vino del Dropbox: IT, DAT, anexos, protocolos, reglamentos, manual, diagrama | `sgi_migration_action` (id 3870, hoy «Migración de formatos a Odoo») | **Existe, cambia** | (1) Dominio: `sgi_doc_type in (formato, formato_it, formulario_odoo, instructivo, dat, anexo, protocolo, reglamento, miid, diagrama)` y **sin** `('sgi_state','!=','obsoleto')`: los formatos migrados pasan a obsoletos (decisiones, pregunta 7) y hoy desaparecerían. Filtro por defecto «No obsoletos» quitable. (2) Nombre «Formatos y documentos anteriores». (3) Columna destino = `sgi_destination_label` (C-011). (4) La acción masiva «Resolver menú de Odoo» se queda aquí, con `group_ids` 318 (F-016). Los 64 «Formulario de Odoo (vista)» **son formatos del Dropbox ya migrados** (clave F-…, clase A, todos «migrado»): deben seguir en esta lista |
| 40 | **Rutina por rutina** | Cada rutina de los procedimientos anteriores (~99 omitidas, ~30 reemplazadas, el resto cubiertas) con su destino: actividad nueva, cubierta por otra, omitida con motivo | `sgi_dropbox_routine_action` | **Crear** | **No hay modelo** (00-inventario). El modelo lo diseña L en la tanda 3; el menú y la acción quedan reservados aquí (lista agrupada por procedimiento anterior, filtros por destino) |
| 50 | **Avance de la transición** | Cuántos documentos del Dropbox hay por tipo y estado de migración; procedimientos sustituidos contra pendientes; rutinas cubiertas | `sgi_dropbox_progress_action` | **Crear** | Primera versión sin modelo nuevo: `act_window` pivote/gráfica sobre `documents.document` (mismo dominio que el submenú 30 + procedimientos), filas `sgi_doc_type_id`, columnas `sgi_migration_state`. Cuando exista el modelo de rutinas, L decide si pasa a tablero |

Lo que **no** entra a esta sección y por qué: «Formatos en documentos de Odoo» (`sgi.format.map`, 9 registros) es la configuración viva del pie de formato de cotizaciones, órdenes, etc. (C-006); se queda en Configuración y la sección la enlaza desde la ficha del formato.

## Menús del módulo fuera del SGI

| App → ruta | xml_id | id | Acción | Grupos finales | Veredicto |
|---|---|---:|---|---|---|
| Calidad → Calidad preventiva (sec. 23) | `menu_sgi_automotive` | 2401 | — | 66, 316 | Se queda. `parent="quality_control.menu_quality_root"` directo, sin la función de runtime (E-008) |
| · Planes de control | `menu_sgi_control_plans` | 2402 | 3852 | hereda | Se queda (núcleo, 10 registros) |
| · AMEF | `menu_sgi_fmea` | 2403 | 3855 | hereda | **Sale al satélite automotriz** (decisión 5), mismo `res_id` |
| · PPAP | `menu_sgi_ppap` | 2404 | 3856 | hereda | **Sale al satélite automotriz** (CLI-01), mismo `res_id` |
| · Metrología | `menu_sgi_metrology` | 2405 | — | hereda | Se queda |
| ·· Equipos de medición | `menu_sgi_measuring_equipment` | 2406 | 3854 | hereda | Se queda (quitar `search_default_group_category: 0`, E-016) |
| ·· Calibraciones | `menu_sgi_calibrations` | 2407 | 3853 | hereda | Se queda (46 registros) |
| ·· Estudios MSA | `menu_sgi_msa` | 2508 | 3967 | hereda | **Sale al satélite automotriz** |
| · COA recibidos | `menu_sgi_coa_inbox` | 2567 | 4038 | 66, 318 | Se queda (A-018: 0 registros; no está en la decisión 5) |
| · Configuración → Elementos PPAP | (hoy `menu_sgi_config_ppap_elements`) | 2412 | 3857 | 67, 318 | Llega del SGI con el satélite |
| Calidad → Tableros (SGI) (sec. 24) | `menu_sgi_dashboards` | 2379 | — | 66, 318 | Se queda; parent real (E-008). Nombre: pregunta 5 |
| · Pareto de alertas de calidad | `menu_sgi_dashboard_alerts` | 2380 | 3860 | hereda | Se queda |
| · Pareto de defectos (revisado) | `quimibond_sgi_revisado.menu_sgi_dashboard_defectos` | 2418 | 3869 | hereda | Satélite; depende de que `menu_sgi_dashboards` conserve su xml_id |
| Compras → Evaluación de proveedores | `menu_sgi_suppliers` | 2399 | 3850 | 38, 316 | Se queda |
| Empleados → Competencias SGI | `menu_hr_sgi_competences` | 2512 | — | 68, 316 | Se queda; nombre sin paréntesis |
| · Brechas de competencia (DNC) | `menu_sgi_competences` | 2410 | 3859 | hereda | Se queda |
| · Encuesta DNC | `menu_sgi_dnc_survey` | 2504 | server 3968 | hereda | Se queda; título interno sin «F-P-A01-17» (E-006) |
| · Cursos y competencias (eLearning) | `menu_sgi_elearning_skills` | 2518 | 3988 | 318 | Se queda |
| Empleados → Eficiencias de personal | `menu_hr_sgi_staff_efficiency` | 2589 | — | 68 → depurado (F-004) | Se queda; nombre sin paréntesis |
| · Hojas mensuales | `menu_sgi_staff_efficiency` | 2590 | 4067 | hereda | Se queda |
| · Análisis por empleado | `menu_sgi_staff_efficiency_analysis` | 2591 | 4068 | hereda | Se queda; acción con `help` |
| Mantenimiento → Checklists de hoy | `menu_sgi_checklist_today` | 2605 | 4095 | ninguno (F-021) | Se queda; secuencia 1 empata con «Mantenimiento» (E-013) |
| Ventas → Presupuesto y pronóstico (+ Presupuestos, Pronósticos, Análisis → Por mercado/cliente/producto/Global) | `menu_sale_sgi_*` (8) | 2438, 2434, 2435, 2449, 2444–2447 | 3886, 3887, 3891–3894 | 316 → grupo de ventas | **Sale al módulo de presupuesto** (decisión 5), conservando `res_id` (Presupuestos: 3 documentos y 2 actividades apuntan a 2434). Acciones sin «(F-P-A28-…)» |
| Contabilidad → Reportes → Bitácora de bloqueo contable | `menu_sgi_lock_date_log` | 2587 | 4063 | contabilidad | Sale al módulo contable (A-017, decisión 5) |
| Contabilidad → Reportes → Valor del inventario por mes | `menu_sgi_inventory_value` | 2588 | 4064 | — | **Archivar** (A-017: nada lo alimenta; 0 registros; decisión 5 «si nada los alimenta, proponer quitarlos») |
| Tableros → SGI → «Salud del SGI» | `sgi_spreadsheet_dashboard_health` (spreadsheet.dashboard 65) | — | client 4030 | 316 | Pendiente de verificación visual (E-017) |
