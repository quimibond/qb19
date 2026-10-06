<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.process.activity`

**Actividad del procedimiento** (Model).

Actividad (numeral) del Desarrollo del procedimiento (sección 4).

Orden: `process_id, sequence, step, id`.

Archivos: `addons/quimibond_sgi/models/sgi_process_procedure.py`, `addons/quimibond_sgi/models/sgi_activity_execution.py`, `addons/quimibond_sgi/models/sgi_activity_spec.py`, `addons/quimibond_sgi/models/sgi_approval_native.py`, `addons/quimibond_sgi/models/sgi_approval_wizard.py`, `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_deliverable.py`, `addons/quimibond_sgi/models/sgi_hierarchy.py`, `addons/quimibond_sgi/models/sgi_indicator_wizard.py`, `addons/quimibond_sgi/models/sgi_legacy_routine.py`, `addons/quimibond_sgi/models/sgi_measure_history.py`, `addons/quimibond_sgi/models/sgi_measure_manual_reason.py`, `addons/quimibond_sgi/models/sgi_measure_review.py`, `addons/quimibond_sgi/models/sgi_mp_change.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`, `addons/quimibond_sgi/models/sgi_sst_links.py`, `addons/quimibond_sgi/models/sgi_structure.py`, `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py`.

## Campos (94)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | La carga por API archiva las actividades que ya no vienen en el catálogo del proceso; nunca las borra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:474` |
| `approver_role_ids` | Many2many | Aprueba | Roles que aprueban la actividad. |  | `sgi.activity.role` | compute `_compute_role_views`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:617` |
| `automation_level_current` | Selection | Automatización actual | Qué tan automatizada está hoy la actividad. Una actividad automática no lleva quien la ejecute. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:537` |
| `automation_level_target` | Selection | Automatización meta | Nivel de automatización al que se quiere llevar la actividad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:542` |
| `automation_method` | Selection | Método de automatización | Cómo se automatiza o se automatizaría la actividad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:545` |
| `block` | Selection | Bloque (anterior) | Agrupación fija de la versión anterior; hoy mandan las etapas. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:499` |
| `check_against` | Char | Contra qué se compara | Lista de precios vigente, release anterior, IT-C2-06… Obligatorio cuando el verbo es de comparación (verificar, revisar, validar…). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:176` |
| `company_id` | Many2one | Empresa |  |  |  | related `process_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:471` |
| `department_ids` | Many2many | Departamentos | Departamentos de los puestos que la ejecutan (se calcula). |  | `hr.department` | compute `_compute_department_ids`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:78` |
| `description` | Text | Descripción | Texto completo del numeral del procedimiento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:509` |
| `done_criteria` | Text | Criterio de terminado | Cómo se sabe que quedó bien hecha, en una frase que se contesta con sí o no: «El pedido coincide con el release en cantidad, fecha y planta, y trae número de OC». |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:181` |
| `due_business_day` | Integer | Vence el día hábil (mensual) | Cadencia mensual: día hábil del mes en que vence (1 a 23). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:194` |
| `due_day` | Integer | Vence el día | Día del mes en que vence (1 a 31; si el mes es más corto, el último día). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:204` |
| `due_month` | Selection | Vence en el mes | Trimestral, semestral o anual: mes en que vence dentro del periodo. Trimestral con «Febrero» = febrero, mayo, agosto y noviembre. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:200` |
| `due_weekday` | Selection | Vence el (semanal) | Cadencia semanal: día de la semana en que vence. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:191` |
| `exec_channel` | Selection | Dónde se hace | Dónde se hace el trabajo (no cómo se mide). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:208` |
| `execution_ids` | One2many | Registro de cumplimiento |  |  | `sgi.activity.execution` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:466` |
| `executor_role_ids` | Many2many | Ejecuta | Roles que ejecutan la actividad. |  | `sgi.activity.role` | compute `_compute_role_views`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:614` |
| `external_system` | Char | Sistema externo | Portal proveedores GM, VUCEM, sistema del agente aduanal… |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:215` |
| `fiscal_position_ids` | Many2many | Mercado (posición fiscal) | Mercado al que aplica (Nacional, Cliente extranjero…). Vacío = a todos. |  | `account.fiscal.position` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:74` |
| `flow_child_count` | Integer | Siguientes |  |  |  | compute `_compute_flow_child_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_hierarchy.py:27` |
| `flow_child_ids` | One2many | Pasos siguientes (diagrama) |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_hierarchy.py:25` |
| `flow_executor` | Char | Quién ejecuta |  |  |  | compute `_compute_flow_executor`, sin guardar |  | `addons/quimibond_sgi/models/sgi_hierarchy.py:28` |
| `flow_parent_id` | Many2one | Paso anterior | El paso del mismo proceso que entrega a esta actividad (el primero en la secuencia). Es lo que dibuja el diagrama; se calcula de los eslabones «Recibe de». |  | `sgi.process.activity` | compute `_compute_flow_parent_id`, guardado |  | `addons/quimibond_sgi/models/sgi_hierarchy.py:19` |
| `format_document_ids` | Many2many | Formatos referenciados | Claves de formato en rojo que la actividad genera o usa. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:558` |
| `how_steps` | Text | Cómo (pasos) | De 1 a 5 pasos cortos cuando no amerita un instructivo: «Abrir el pedido → actualizar cantidad y fecha → capturar la OC → confirmar». |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:227` |
| `in_link_ids` | One2many | Recibe de |  |  | `sgi.activity.link` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:580` |
| `informed_role_ids` | Many2many | Se entera | Roles que se enteran de la actividad. |  | `sgi.activity.role` | compute `_compute_role_views`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:620` |
| `input_deliverable_ids` | Many2many | Entregables que recibe |  |  | `sgi.deliverable` | compute `_compute_input_deliverables`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:585` |
| `input_ids` | One2many | Recibe | Qué entregables recibe y en cuántos días hábiles deben llegarle. |  | `sgi.activity.input` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:582` |
| `instruction_article_id` | Many2one | Instructivo en Knowledge | Artículo donde se escribe el instructivo. «Publicar como instructivo» lo congela como revisión del IT. |  | `knowledge.article` |  |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:48` |
| `instruction_article_stale` | Boolean | Artículo cambió desde la última revisión | El artículo de Conocimiento del instructivo cambió después de publicarse como revisión. |  |  | compute `_compute_instruction_article_stale`, sin guardar |  | `addons/quimibond_sgi_knowledge/models/sgi_instruction_knowledge.py:52` |
| `instruction_id` | Many2one | Instructivo | Instructivo (IT) que explica cómo se hace el paso. El «Procedimiento relacionado» es otra cosa: el procedimiento que rige la actividad. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:522` |
| `legacy_number` | Char | Numeral anterior | Numeral en texto de la versión anterior del procedimiento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:490` |
| `location_id` | Many2one | Ubicación | Ubicación física cuando la actividad mueve o toca material. |  | `stock.location` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:218` |
| `manual_reason` | Text | Por qué se mide a mano | Cuando la actividad se hace en Odoo pero su evidencia no es un registro (una revisión, un reporte, una junta): diga qué se revisa y dónde queda la decisión. Se mide con el registro de cumplimiento y ya no sale el aviso «Se hace en Odoo, se mide a mano». |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_manual_reason.py:21` |
| `market_filter_id` | Many2one | Mercado (con las generales) | Filtro por mercado que incluye las actividades generales. |  | `account.fiscal.position` | compute `_compute_line_filters`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:86` |
| `measure_adherence_pct` | Float | Adherencia (%) | Ejecuciones de las últimas 4 semanas hechas por el puesto asignado, entre todas las que no son del sistema. 0 si no aplica (rol relativo o sin campo de usuario). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:944` |
| `measure_cadence` | Selection | Cadencia esperada | Cada cuánto DEBE haber evidencia. «Por evento» solo cuenta, sin juzgar cumplimiento (actividades que dependen de demanda). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:869` |
| `measure_count_30d` | Integer | Ejecuciones (30 días) | Ejecuciones registradas en los últimos 30 días. Lo escribe la medición diaria. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:884` |
| `measure_count_generic` | Integer | Por cuenta genérica (4 sem.) | Ejecuciones con una cuenta compartida (quimibond_sgi.generic_user_ids): no se pueden atribuir a nadie. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:953` |
| `measure_count_no_employee` | Integer | Sin empleado (4 sem.) | Ejecuciones de usuarios sin empleado activo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:957` |
| `measure_count_other_job` | Integer | Por otro puesto (4 sem.) | Ejecuciones de empleados de un puesto al que no le toca. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:964` |
| `measure_count_system` | Integer | Del sistema (4 sem.) | Ejecuciones de OdooBot o procesos automáticos: no cuentan en la adherencia. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:960` |
| `measure_date_field` | Char | Campo de fecha | Campo del modelo que fecha la ejecución (create_date, date_done, date_approve…). Si no existe, se usa create_date. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:865` |
| `measure_date_field_id` | Many2one | Fecha que cuenta | La fecha del registro que dice cuándo se hizo la actividad (Fecha efectiva, Fecha de factura…). |  | `ir.model.fields` | compute `_compute_measure_field_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:276` |
| `measure_deliverable_id` | Many2one | Se mide con el entregable | Con el método «Por su entregable», la actividad copia el modelo, el filtro, la fecha y el usuario de este entregable. |  | `sgi.deliverable` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:592` |
| `measure_domain` | Char | Filtro de evidencia | Dominio sobre el modelo para acotar qué registros cuentan, ej. [('state', '=', 'done')]. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:861` |
| `measure_history_field` | Char | Campo de estado del historial | El campo cuyo cambio dice quién hizo la actividad. Vacío: el modelo no guarda historial de su estado y la casilla no sirve. |  |  | compute `_compute_measure_history_field`, sin guardar |  | `addons/quimibond_sgi/models/sgi_measure_history.py:64` |
| `measure_justification` | Text | Por qué no se mide | Obligatoria con «No aplica»: dónde se mide su resultado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:925` |
| `measure_last_date` | Datetime | Última ejecución | Fecha de la última ejecución registrada. Lo escribe la medición diaria. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:881` |
| `measure_method` | Selection | Método de medición | De mejor a peor. Odoo: deja registro (modelo, dominio, fecha, usuario). Por su entregable: igual que Odoo, pero el modelo, el filtro y la fecha son los del entregable que produce (se capturan una vez, en el entregable). Consecuencia: no deja rastro pero la actividad que la prueba sí (se copia su co… |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:897` |
| `measure_model_id` | Many2one | Modelo que la materializa | Modelo de Odoo cuyos registros son la evidencia de que la actividad se ejecutó (sale.order para cotizar, mrp.production para cerrar una orden, quality.check para inspeccionar…). |  | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:851` |
| `measure_model_name` | Char | Modelo técnico | Nombre técnico (sale.order, mrp.production…). Escribirlo resuelve solo el modelo — útil para capturas masivas. |  |  | compute `_compute_measure_model_name`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:856` |
| `measure_preview_html` | Html | Lo que cuenta hoy |  |  |  | compute `_compute_measure_preview_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:289` |
| `measure_proxy_activity_id` | Many2one | Se prueba con | Actividad (de este proceso o de otro) cuya evidencia prueba que esta se hizo: si se validó la recepción, se descargó el camión. |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:915` |
| `measure_state` | Selection | Cumplimiento | En cumplimiento, sin evidencia en su periodo, pendiente de conector o no se mide. Lo escribe la medición diaria. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:887` |
| `measure_top_users` | Char | Quién la ejecuta | Usuarios con más ejecuciones en las últimas 4 semanas (✓ = puesto asignado). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:950` |
| `measure_user_field` | Char | Campo de usuario | Campo del modelo de evidencia que dice QUÉ USUARIO ejecutó la actividad (create_uid, user_id…). Con él se mide si la hizo el puesto que debía. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:932` |
| `measure_user_field_id` | Many2one | Quién la hizo | El usuario del registro que hizo la actividad (Responsable, Validado por…). Evite «Última actualización por»: es el último que editó, no quien la hizo. |  | `ir.model.fields` | compute `_compute_measure_field_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_wizard.py:282` |
| `measure_user_history` | Boolean | Quién lo hizo: quien lo pasó a su estado (historial) | HISTORY_HELP |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_history.py:62` |
| `measure_warning` | Text | Avisos de medición |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:967` |
| `name` | Char | Resumen | Resumen corto de la actividad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:508` |
| `next_activity_ids` | Many2many | Siguientes pasos | Actividades que reciben lo que esta entrega. |  | `sgi.process.activity` | compute `_compute_chain`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:582` |
| `norm_clause_ids` | Many2many | Cumple con | Puntos de la norma (ISO 9001, 14001, 45001…) que esta actividad cumple. Alimenta la Matriz de cumplimiento y el checklist de auditoría. |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:24` |
| `note` | Text | Nota | Notas resaltadas del procedimiento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:574` |
| `number` | Char | Numeral | Clave del proceso + paso, ej. C6.22. Se calcula. |  |  | compute `_compute_number`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:487` |
| `odoo_action_id` | Many2one | Acción de Odoo | Pantalla que abre «Ir a hacerlo»; sale del menú y se puede cambiar. |  | `ir.actions.act_window` | compute `_compute_odoo_action_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:211` |
| `odoo_menu_id` | Many2one | Menú de Odoo | Menú real donde se ejecuta la actividad; el texto impreso se toma de la ruta. |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:570` |
| `odoo_ref` | Char | Dónde se ejecuta en Odoo | Ej. 'Ventas > Pedidos', 'Helpdesk Servicio Técnico'. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:567` |
| `on_fail` | Text | Si no se puede cumplir | Qué hace quien ejecuta si no se puede o el resultado no pasa: «Si cambia cantidad o fecha, avisar a Planeación el mismo día». |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:186` |
| `out_link_ids` | One2many | Entrega a |  |  | `sgi.activity.link` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:578` |
| `output_deliverable_ids` | Many2many | Entrega | Entregables que produce la actividad. |  | `sgi.deliverable` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:588` |
| `place_note` | Char | Lugar | Andén, laboratorio, oficina de embarques… |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:224` |
| `prev_activity_ids` | Many2many | Pasos anteriores |  |  | `sgi.process.activity` | compute `_compute_chain`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:586` |
| `process_id` | Many2one | Proceso | Proceso al que pertenece la actividad. | sí | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:467` |
| `recent_exec_stat_ids` | Many2many | Últimas 4 semanas | Ejecuciones de las últimas 4 semanas por persona. |  | `sgi.activity.exec.stat` | compute `_compute_recent_exec_stat_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:940` |
| `related_procedure_id` | Many2one | Procedimiento relacionado | Otro procedimiento que rige esta actividad (ej. marca P-A22). |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:563` |
| `responsible_job_ids` | Many2many | Puestos que ejecutan |  |  | `hr.job` | compute `_compute_responsible_job_ids`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:518` |
| `responsible_role` | Char | Rol responsable | Nombre del rol en negritas del procedimiento (no siempre mapea a un puesto de hr.job). |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:554` |
| `role_ids` | One2many | Roles |  |  | `sgi.activity.role` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:516` |
| `sale_team_ids` | Many2many | Aplica a (equipo de ventas) | Líneas de negocio (equipos de venta de Odoo) a las que aplica la actividad. Vacío = a todas. |  | `crm.team` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:69` |
| `sample_cadence` | Selection | Cadencia de muestreo | Cada cuánto se verifica una muestra, cuando la actividad se mide por muestreo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:920` |
| `section` | Char | Sección | Nombre de la etapa (se calcula de «Etapa»). |  |  | compute `_compute_section`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:505` |
| `sequence` | Integer | Secuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:478` |
| `sgi_legacy_routine_ids` | Many2many | Viene de (rutinas anteriores) |  |  | `sgi.legacy.routine` |  | quimibond_sgi.group_sgi_auditor,quimibond_sgi.group_sgi_manager,quimibond_sgi.group_sgi_director | `addons/quimibond_sgi/models/sgi_legacy_routine.py:768` |
| `spec_complete` | Boolean | Especificación completa | Sin faltantes de tipo error. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:234` |
| `spec_gap_ids` | One2many | Faltantes |  |  | `sgi.activity.spec.gap` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:232` |
| `spec_gap_summary` | Text | Qué le falta |  |  |  | compute `_compute_spec_gap_summary`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:237` |
| `stage_id` | Many2one | Etapa | Etapa del proceso a la que pertenece la actividad. |  | `sgi.process.stage` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:495` |
| `step` | Integer | Paso | Número del paso dentro del proceso. Vacío = el siguiente libre. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:484` |
| `team_filter_id` | Many2one | Equipo (con las generales) | Filtro por equipo de ventas que incluye las actividades generales. |  | `crm.team` | compute `_compute_line_filters`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:82` |
| `value_class` | Selection | Clase de valor | Si la actividad agrega valor al cliente, es necesaria sin agregarlo o es desperdicio. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:528` |
| `workcenter_id` | Many2one | Centro de trabajo | Centro de trabajo donde se hace la actividad. |  | `mrp.workcenter` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:221` |

## Métodos públicos (12)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_instruction` | «Ver instructivo»: el archivo o enlace del IT de la actividad. |
| `action_open_next` | Navega al siguiente paso de la cadena (o a la lista si hay varios). |
| `action_open_odoo` | Abre el menú real de Odoo donde se ejecuta la actividad, respetando el dominio/contexto/vistas de su acción original. Sin menú ligado intenta resolverlo del texto y, si tampoco, abre la evidencia — e… |
| `action_publish_instruction` | — |
| `action_sgi_register_finding` | «Registrar hallazgo» desde la actividad: NC ligada al proceso con la actividad en el título. |
| `action_sgi_view_legacy_routines` | — |
| `action_view_measure_records` | Abre los registros reales que son la evidencia de la actividad. |
| `action_view_recent_records` | «Ver registros recientes»: la evidencia real, la más nueva primero. |
| `create` | — |
| `cron_measure_activities` | Cron diario: resuelve menús pendientes, mide las actividades con entregable medible y evalúa los eslabones de la cadena (la extensión de ``sgi_activity_spec`` suma las cifras semanales y las de Mi pr… |
| `unlink` | — |
| `write` | — |
