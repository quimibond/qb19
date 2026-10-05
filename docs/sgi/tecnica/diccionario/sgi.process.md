<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.process`

**Proceso SGI** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Proceso del SGI: dueño, etapas, actividades, entradas y salidas, documentos, indicadores, riesgos y semáforo. Es dato: se captura o se carga, no viene en el módulo.

Orden: `process_type, code`.

Archivos: `addons/quimibond_sgi/models/sgi_process.py`, `addons/quimibond_sgi/models/sgi_activity_spec.py`, `addons/quimibond_sgi/models/sgi_archived_filters.py`, `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_cleanup.py`, `addons/quimibond_sgi/models/sgi_deliverable.py`, `addons/quimibond_sgi/models/sgi_export.py`, `addons/quimibond_sgi/models/sgi_hierarchy.py`, `addons/quimibond_sgi/models/sgi_indicator_health.py`, `addons/quimibond_sgi/models/sgi_indicator_sheet.py`, `addons/quimibond_sgi/models/sgi_load.py`, `addons/quimibond_sgi/models/sgi_multicompany.py`, `addons/quimibond_sgi/models/sgi_process_procedure.py`, `addons/quimibond_sgi/models/sgi_structure.py`.

## Campos (77)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:49` |
| `activity_count` | Integer | # Actividades |  |  |  | compute `_compute_activity_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:63` |
| `activity_green_count` | Integer | En verde |  |  |  | compute `_compute_activity_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:84` |
| `activity_grey_count` | Integer | Sin evidencia aún | Pendientes de conector o registro, no aplica o sin medir. |  |  | compute `_compute_activity_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:88` |
| `activity_no_method_count` | Integer | Sin método |  |  |  | compute `_compute_activity_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:91` |
| `activity_red_count` | Integer | En rojo |  |  |  | compute `_compute_activity_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:86` |
| `activity_team_ids` | Many2many | Equipos de sus actividades | Equipos de venta que aparecen en las actividades del proceso. |  | `crm.team` | compute `_compute_activity_team_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:127` |
| `chain_link_count` | Integer | Ligas de la cadena | Entregas entre actividades que tocan este proceso (entran o salen). |  |  | compute `_compute_chain_link_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:133` |
| `chain_link_ids` | Many2many | Ligas entre actividades |  |  | `sgi.activity.link` | compute `_compute_chain_link_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:105` |
| `chain_stuck_count` | Integer | Eslabones atorados | Ligas de este proceso donde el paso origen entregó pero el destino no tiene evidencia en su periodo. |  |  | compute `_compute_chain_link_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:182` |
| `channel_manual_pct` | Integer | % en papel, correo o teléfono | La lista de trabajo de automatización. |  |  | compute `_compute_channel_pct`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:846` |
| `channel_odoo_pct` | Integer | % en Odoo | Actividades con canal Odoo, sobre las que tienen canal. |  |  | compute `_compute_channel_pct`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:843` |
| `child_count` | Integer | Núm. de subprocesos |  |  |  | compute `_compute_child_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_hierarchy.py:55` |
| `child_ids` | One2many | Subprocesos |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process.py:42` |
| `code` | Char | Clave |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:27` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_process.py:29` |
| `department_id` | Many2one | Departamento | Departamento responsable del proceso. |  | `hr.department` |  |  | `addons/quimibond_sgi/models/sgi_process.py:46` |
| `doc_approver_id` | Many2one | Aprueba | Quien aprueba el procedimiento del proceso. Por omisión, el dueño del proceso. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:114` |
| `doc_owner_id` | Many2one | Responsable del documento | Elabora / es dueño del procedimiento (bloque de firmas). |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:111` |
| `doc_vobo_id` | Many2one | Vo.Bo. | Quien da el visto bueno al procedimiento del proceso. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:117` |
| `document_count` | Integer | # Documentos |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:120` |
| `end_trigger` | Text | Termina cuando | Qué marca el fin del proceso (ej. la factura queda cobrada). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:59` |
| `env_aspects` | Text | Descripción de aspectos ambientales | Aspectos ambientales del proceso (sección 5 del procedimiento). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:46` |
| `flow_count` | Integer | # Conexiones | Entregables que recibe de otros procesos más los que entrega. |  |  | compute `_compute_flow_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:123` |
| `health` | Selection | Salud del proceso | Verde sin nada abierto; amarillo con algo abierto; rojo con un riesgo de atención máxima o con NC abierta e indicador en rojo a la vez. Se calcula al mostrarlo. |  |  | compute `_compute_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:113` |
| `in_flow_ids` | One2many | Flujos de entrada |  |  | `sgi.process.flow` |  |  | `addons/quimibond_sgi/models/sgi_process.py:91` |
| `inbound_reference_count` | Integer | Me referencian |  |  |  | compute `_compute_inbound_references`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:143` |
| `inbound_reference_ids` | Many2many | Actividades que me referencian |  |  | `sgi.process.activity` | compute `_compute_inbound_references`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:140` |
| `indicator_count` | Integer | # Indicadores |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:121` |
| `indicator_ids` | One2many | Indicadores |  |  | `sgi.indicator` |  |  | `addons/quimibond_sgi/models/sgi_process.py:101` |
| `input_deliverable_ids` | Many2many | Inicia con | Inicio del proceso: lo que sus actividades reciben y ninguna actividad del proceso produce. |  | `sgi.deliverable` | compute `_compute_io_deliverables`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:405` |
| `inputs` | Text | Entradas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:62` |
| `job_ids` | Many2many | Puestos | Puestos que participan en el proceso. |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_process.py:48` |
| `job_responsibility_ids` | One2many | Responsabilidades de las áreas |  |  | `sgi.process.responsibility` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:53` |
| `linked_document_ids` | One2many | Documentos del proceso |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_process.py:95` |
| `measurable_activity_count` | Integer | Actividades medibles |  |  |  | compute `_compute_measure_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:67` |
| `measure_adherence_avg` | Float | Adherencia promedio (%) | Promedio de adherencia de las actividades con campo de usuario. |  |  | compute `_compute_activity_board`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:93` |
| `measure_method_summary` | Char | Actividades por método |  |  |  | compute `_compute_measure_methods`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:82` |
| `measure_real_pct` | Integer | % medido de verdad | Actividades con método «Registro en Odoo» o «Por consecuencia» entre el total de actividades activas del proceso. |  |  | compute `_compute_measure_methods`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:78` |
| `measure_red_count` | Integer | Sin evidencia en su periodo |  |  |  | compute `_compute_measure_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:69` |
| `measure_status` | Selection | Estado de medición | Resumen de la medición de sus actividades: todo con evidencia, alguna sin evidencia aún o alguna en rojo. Se calcula solo. |  |  | compute `_compute_measure_status`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:98` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:28` |
| `nc_count` | Integer | NC abiertas |  |  |  | compute `_compute_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:108` |
| `nc_open_ids` | Many2many | No conformidades abiertas |  |  | `quality.alert` | compute `_compute_nc_open_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_structure.py:21` |
| `norm_ids` | Many2many | Marco normativo | Normas ISO que rigen el proceso (sección 7). |  | `sgi.norm` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:49` |
| `odoo_measured_pct` | Integer | % de lo que se hace en Odoo que se mide solo | Actividades con canal Odoo que se miden por su entregable. Si se hace en Odoo y no se mide solo, es un hueco del SGI, no del proceso. |  |  | compute `_compute_channel_pct`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:849` |
| `odoo_model_ids` | Many2many | Módulos de Odoo conectados | Modelos donde viven los registros reales de las entradas/salidas. |  | `ir.model` | compute `_compute_odoo_models`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:103` |
| `open_high_risk_count` | Integer | Riesgos altos abiertos |  |  |  | compute `_compute_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:111` |
| `out_flow_ids` | One2many | Flujos de salida |  |  | `sgi.process.flow` |  |  | `addons/quimibond_sgi/models/sgi_process.py:92` |
| `output_deliverable_ids` | Many2many | Termina con | Fin del proceso: lo que sus actividades entregan y ninguna actividad del proceso recibe (sale hacia otros procesos o al cliente). |  | `sgi.deliverable` | compute `_compute_io_deliverables`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:409` |
| `outputs` | Text | Salidas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:63` |
| `overdue_action_count` | Integer | Acciones vencidas |  |  |  | compute `_compute_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:109` |
| `owner_id` | Many2one | Dueño del proceso | Empleado dueño del proceso: recibe los escalamientos y aprueba los cambios a sus actividades. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_process.py:43` |
| `owner_valid` | Boolean | Dueño válido | El dueño es un empleado activo con usuario de Odoo. Sin eso nadie recibe los avisos del proceso y la salud se pinta en rojo. |  |  | compute `_compute_owner_valid`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:86` |
| `parent_id` | Many2one | Macroproceso | Macroproceso al que pertenece este proceso. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process.py:39` |
| `parent_path` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:41` |
| `procedure_activity_ids` | One2many | Actividades del procedimiento |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:60` |
| `procedure_compliance` | Integer | % Cumplimiento del procedimiento | Porcentaje de actividades medibles con evidencia dentro de su cadencia esperada (lo calcula el cron de medición). |  |  | compute `_compute_measure_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:71` |
| `procedure_ids` | One2many | Procedimientos e instructivos |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_process.py:97` |
| `process_type` | Selection | Tipo | Cadena de valor (COP), estratégico o de soporte. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:32` |
| `purpose` | Text | Objetivo del proceso | Para qué existe el proceso (de la caracterización/SIPOC). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:51` |
| `red_kpi_count` | Integer | KPIs en rojo |  |  |  | compute `_compute_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:110` |
| `replaced_by_id` | Many2one | Sustituido por | Proceso que tomó el lugar de este al archivarlo. La carga lo llena con «replaces»; en un proceso archivado sin sucesor se captura a mano y la siguiente carga mueve al sucesor lo que quede colgado (indicadores, riesgos abiertos, documentos vigentes). |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process.py:78` |
| `replaced_document_ids` | One2many | Procedimientos que sustituye | Procedimientos del Dropbox que este proceso sustituye. Se captura en la ficha de cada procedimiento («Lo sustituye el proceso»). Siguen vigentes mientras el proceso esté en borrador o piloto; al entrar en vigor el proceso pasan a obsoletos y a «Baja tramitada». |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_process.py:68` |
| `risk_count` | Integer | # Riesgos |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:122` |
| `risk_ids` | One2many | Riesgos y oportunidades |  |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_process.py:102` |
| `scope` | Text | Alcance | A qué áreas/actividades aplica el procedimiento (sección 2). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:43` |
| `sgi_health_idle_days` | Integer | Días sin movimiento | Días desde la última vez que el dueño creó, modificó o comentó algo del SGI. 91 significa más de 90. Vacío si el dueño no tiene usuario. |  |  | compute `_compute_sgi_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_health.py:537` |
| `sgi_health_late_validation_count` | Integer | Validaciones atrasadas | Mediciones que el dueño debía validar y cuyo plazo ya pasó. |  |  | compute `_compute_sgi_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_health.py:534` |
| `sgi_health_overdue_count` | Integer | Avisos vencidos | Avisos del SGI vencidos que tiene el dueño del proceso. |  |  | compute `_compute_sgi_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_health.py:531` |
| `sgi_health_owner_user_id` | Many2one | Usuario del dueño | Usuario activo del dueño del proceso. Vacío si el dueño no tiene usuario. |  | `res.users` | compute `_compute_sgi_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_health.py:528` |
| `spec_error_count` | Integer | Faltantes que bloquean |  |  |  | compute `_compute_spec_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:839` |
| `spec_warning_count` | Integer | Advertencias |  |  |  | compute `_compute_spec_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:841` |
| `stage_ids` | One2many | Etapas |  |  | `sgi.process.stage` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:404` |
| `start_trigger` | Text | Disparador de inicio | Qué hace que el proceso arranque (ej. llega un pedido). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:56` |
| `state` | Selection | Estado | Borrador y piloto se pueden cargar incompletos (los faltantes se ven). Vigente exige la especificación completa de actividades e indicadores. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:832` |
| `structure_status` | Char | Estado del proceso | Una línea: dueño, estado, semáforo, actividades atrasadas, KPIs en rojo y NC abiertas. |  |  | compute `_compute_structure_status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_structure.py:24` |

## Métodos públicos (23)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_documents` | — |
| `action_open_flows` | Botón «Conexiones»: los flujos que entran y salen del proceso. |
| `action_open_high_risks` | — |
| `action_open_indicators` | — |
| `action_open_ncs` | — |
| `action_open_overdue_actions` | — |
| `action_open_red_kpis` | — |
| `action_open_risks` | — |
| `action_sgi_publish` | Publicar (vigente) exige la especificación completa. |
| `action_sgi_register_finding` | «Registrar hallazgo» (auditor, nivel 2): una NC nueva ya ligada al proceso. Los permisos sobre quality.alert son los de la app Calidad. |
| `action_sgi_request_change` | «Proponer cambio»: abre una solicitud de cambio documental (F-P-G01-06) ya apuntando al procedimiento vigente del proceso. La aprueba MAST y edita la actividad; ficha, Mi procedimiento y PDF se regen… |
| `action_sgi_set_draft` | — |
| `action_sgi_set_pilot` | — |
| `action_sgi_view_diagram` | «Ver en diagrama»: el flujo de actividades del proceso por etapa, con los eslabones como flechas (componente sgi_diagram). |
| `action_view_activities` | — |
| `action_view_chain` | La cadena del proceso: qué entregables entran y salen, paso a paso. |
| `action_view_red_activities` | — |
| `action_view_spec_gaps` | — |
| `create` | — |
| `export_payload` | El mapa de la empresa en el formato de ``load_payload`` (su inverso exacto). ``process_codes`` limita a esos procesos (y a los entregables, familias e indicadores que usan). Solo Administrador SGI. |
| `export_payload_json` | Lo mismo, como texto JSON estable (para guardar en un archivo). |
| `load_payload` | Carga el catálogo del SGI. Ver el docstring del módulo para el formato; responde {ok, summary, changes, errors, warnings}. |
| `write` | — |
