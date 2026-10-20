<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.my.pending`

**Mis pendientes (SGI)** (Model).

Mis pendientes: bandeja de cada persona con actividades atrasadas, escalamientos, firmas, acuses, capturas, validaciones, aprobaciones y avisos. Una fila por persona y registro pendiente; se actualiza al abrir la lista y cada noche (``_sgi…

Orden: `state_rank, date_due, id`.

Archivos: `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi/models/sgi_archived_filters.py`.

## Campos (29)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `can_done` | Boolean |  | «Hecho» aplica a este renglón para quien abre la lista: la actividad con registro de cumplimiento (o que lo lleva), el aviso propio y la acción propia. |  |  | compute `_compute_can_act`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:294` |
| `can_mark_exec` | Boolean |  | Quien abre la lista puede marcar el avance del registro de cumplimiento (la persona, su jefe o el Jefe MAST). |  |  | compute `_compute_can_act`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:291` |
| `can_snooze` | Boolean |  | Aviso propio: se puede posponer una semana. |  |  | compute `_compute_can_act`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:298` |
| `date_due` | Date | Vence | Fecha en que vence el pendiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:263` |
| `detail` | Html | Qué hay que hacer | Criterio de terminado, pasos, instructivo, nota del aviso, plazos de la NC… lo que hace falta saber para resolverlo sin salir de aquí. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:270` |
| `employee_id` | Many2one | Persona | Persona a la que le toca el pendiente. |  | `hr.employee.public` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:255` |
| `exec_date_estimated` | Date | Fecha estimada | Cuándo espera terminarla quien la marcó «En proceso». |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:287` |
| `exec_manual` | Boolean |  | Actividad de registro manual: «Ir» abre su registro. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:289` |
| `exec_note` | Char | Nota de avance |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:286` |
| `exec_state` | Selection | Avance | Actividades del periodo: pendiente o en proceso (con nota de avance). «En proceso» no quita el atraso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:280` |
| `is_mine` | Boolean |  | El renglón es de quien abre la lista. |  |  | compute `_compute_is_mine`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:277` |
| `kind` | Selection | Tipo | Qué hay que hacer: acción, NC, medición, validación, acuse, firma, aprobación, actividad, escalamiento, aviso… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:257` |
| `name` | Char | Qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:260` |
| `nc_containment_state` | Selection | Contención |  |  |  | related `nc_id.sgi_containment_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:307` |
| `nc_due_containment` | Date | Contención vence |  |  |  | related `nc_id.sgi_due_containment`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:306` |
| `nc_due_plan` | Date | Plan de acción vence |  |  |  | related `nc_id.sgi_due_plan`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:310` |
| `nc_due_root_cause` | Date | Causa raíz vence |  |  |  | related `nc_id.sgi_due_root_cause`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:308` |
| `nc_effectiveness_due` | Date | Eficacia vence |  |  |  | related `nc_id.sgi_effectiveness_due`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:312` |
| `nc_id` | Many2one | NC |  |  | `quality.alert` | compute `_compute_links`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:305` |
| `nc_plan_state` | Selection | Plan de acción |  |  |  | related `nc_id.sgi_plan_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:311` |
| `nc_root_cause_state` | Selection | Causa raíz |  |  |  | related `nc_id.sgi_root_cause_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:309` |
| `process_id` | Many2one | Proceso | Proceso del pendiente. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:261` |
| `res_id` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:268` |
| `res_model` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:267` |
| `source_name` | Char | Registro | El registro de Odoo sobre el que está el pendiente; «Ir» lo abre. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:273` |
| `state` | Selection | Estado | Atrasado, por vencer o al día. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:264` |
| `state_rank` | Integer |  | Orden para mostrar primero lo atrasado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:266` |
| `user_id` | Many2one | Usuario |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:254` |
| `where_menu_id` | Many2one | Dónde se hace | Pantalla de Odoo donde se hace la actividad («Ir a hacerlo»). |  | `ir.ui.menu` | compute `_compute_links`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:302` |

## Métodos públicos (25)

| Método | Qué hace (docstring) |
|---|---|
| `action_doc_review` | Documento por revisar: sigue vigente sin cambios o necesita cambio. |
| `action_done` | «Hecho»: resuelve el pendiente según su tipo. Lo que se resuelve en su propio registro (NC, requisito legal, documento, aprobación, firma, revisión) lo dice y no hace nada. 57.149.0: el escalamiento … |
| `action_done_action_line` | 57.145.0: «Hecho» termina la acción (de NC, riesgo, incidente…) del responsable: fecha de hoy y avance al 100 %, como el botón de la acción. Solo el responsable o el Jefe MAST. |
| `action_done_notice` | 57.92.0 (U-03): «Hecho» marca hecho el aviso (la actividad nativa), solo si es de quien abre la lista. |
| `action_exec_done` | 57.103.0: «Hecho» desde el renglón (evidencia si es de registro manual o si el sistema no vio el registro). |
| `action_exec_not_applicable` | 57.145.0: «No aplica este periodo» desde el renglón (con motivo). |
| `action_exec_progress` | 57.103.0: «En proceso» desde el renglón (nota de avance y fecha estimada). |
| `action_legal_evaluate` | Requisito legal: el asistente de evaluación de cumplimiento. |
| `action_nc_step` | NC: el asistente del paso que toca (contención, causa raíz o plan). |
| `action_open` | Abre el registro de origen (la acción, la NC, la medición…). |
| `action_open_home` | 57.98.0 (U-07, Q20): acción del menú raíz «SGI». Dirección abre en el Tablero; los demás, en Mis pendientes (como antes). No toca la acción de inicio de ningún usuario. |
| `action_open_mine` | Inicio → Mis pendientes: los del usuario actual. Sin empleado ligado, los renglones van solo con el usuario. |
| `action_open_where` | — |
| `action_request_approve` | — |
| `action_request_refuse` | — |
| `action_review_confirm` | Revisión mensual de la medición: «Sí, esto es lo que hago». |
| `action_review_not_matching` | Revisión mensual: «No corresponde» (pide la nota). |
| `action_rule_approve` | — |
| `action_rule_refuse` | — |
| `action_sgi_report` | 57.98.0 (U-07): SGI → Reportar abre la ficha NUEVA de una NC, un incidente o una queja, con su equipo del SGI. Sin el equipo (base sin datos) avisa en lugar de abrir Calidad o Soporte completos. |
| `action_sign_ack` | 57.92.0 (U-05): «Leído y entendido» desde el renglón. El candado de identidad de ``sgi.document.ack`` decide si quien abre la lista puede firmar: el propio empleado o el Jefe MAST. |
| `action_sign_ack_selected` | «Leído y entendido» a todos los acuses seleccionados de quien abre la lista (el candado de identidad de sgi.document.ack decide). |
| `action_snooze_notice` | 57.145.0: «Posponer una semana» un aviso propio: su fecha límite pasa a 5 días hábiles desde hoy y el renglón sale de lo urgente. |
| `action_validate_measure` | «Validar» desde el renglón (I-006). Valida quien abre la lista, con sus permisos: solo el dueño del indicador o el Jefe MAST pueden (``_sgi_check_validate_access``). |
| `action_validate_selected` | 57.92.0 (U-02): «Validar seleccionadas». Ignora los renglones que no son mediciones. Valida, con los permisos de quien abre la lista, solo las mediciones que puede validar (``sgi_can_validate``: dueñ… |
