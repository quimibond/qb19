<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.my.procedure`

**Mi procedimiento (pantalla)** (TransientModel).

Pantalla de Mi procedimiento de una persona o un puesto: actividades por rol, documentos, indicadores, EPP y acuse. Se arma al abrirla.

Archivos: `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`, `addons/quimibond_sgi/models/sgi_mp_change.py`, `addons/quimibond_sgi/models/sgi_my_pending.py`.

## Campos (43)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `ack_ids` | Many2many | Mis acuses de lectura | Acuses de lectura de la persona. |  | `sgi.document.ack` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:466` |
| `ack_label` | Char |  |  |  |  | compute `_compute_ack`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:412` |
| `ack_state` | Selection |  | Si el Mi procedimiento del puesto está publicado y si la persona ya lo firmó. |  |  | compute `_compute_ack`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:405` |
| `activity_count` | Integer | Actividades |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:433` |
| `allowed_employee_ids` | Many2many | Empleados visibles |  |  | `hr.employee.public` | compute `_compute_scope`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:400` |
| `allowed_job_ids` | Many2many | Puestos visibles |  |  | `hr.job` | compute `_compute_scope`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:402` |
| `boss_id` | Many2one | Mi jefe | Jefe directo de la persona. |  | `hr.employee.public` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:416` |
| `can_pick` | Boolean |  |  |  |  | compute `_compute_scope`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:398` |
| `can_publish` | Boolean |  |  |  |  | compute `_compute_scope`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:399` |
| `department_id` | Many2one | Área | Área de la persona. |  | `hr.department` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:418` |
| `doc_id` | Many2one |  |  |  | `documents.document` | compute `_compute_ack`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:413` |
| `document_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:662` |
| `document_ids` | Many2many | Documentos que aplican al puesto | Documentos controlados que aplican al puesto. |  | `documents.document` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:469` |
| `employee_avatar` | Binary | Foto |  |  |  | related `employee_id.avatar_128`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:394` |
| `employee_id` | Many2one | Ver como: empleado | Elija una persona para ver su procedimiento. Solo se ven las personas permitidas (usted, su equipo o todas si es MAST o Dirección). |  | `hr.employee.public` |  |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:390` |
| `epp_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:664` |
| `epp_delivery_ids` | Many2many | Responsivas de EPP | Responsivas de equipo de protección personal de la persona. |  | `sgi.epp.delivery` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:475` |
| `epp_label` | Char | EPP |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:479` |
| `epp_pending_sign` | Boolean | Responsiva de EPP por firmar |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:478` |
| `epp_required` | Text | EPP requerido por el puesto |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:474` |
| `family_id` | Many2one | Familia de puestos | Familia de puestos del puesto. |  | `sgi.job.family` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:420` |
| `has_obligations` | Boolean |  |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:463` |
| `has_user` | Boolean |  |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:450` |
| `indicator_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:663` |
| `indicator_ids` | Many2many | Mis indicadores | Indicadores a cargo de la persona. |  | `sgi.indicator` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:457` |
| `is_me` | Boolean |  |  |  |  | compute `_compute_ack`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:404` |
| `job_employee_ids` | Many2many | Personas en el puesto | Personas que ocupan el puesto. |  | `hr.employee.public` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:424` |
| `job_id` | Many2one | Ver como: puesto | Elija un puesto para ver su procedimiento. Se llena con el puesto de la persona elegida. |  | `hr.job` | compute `_compute_job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:395` |
| `late_count` | Integer | Atrasadas |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:429` |
| `no_employee` | Boolean | Usuario sin empleado | El usuario no está ligado a un empleado y no eligió a nadie: la pantalla muestra el aviso en vez de quedar vacía. |  |  | compute `_compute_no_employee`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change.py:163` |
| `official_indicator_ids` | Many2many | Indicadores oficiales a mi cargo |  |  | `sgi.indicator` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:461` |
| `ok_count` | Integer | Al día |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:430` |
| `pending_ack_count` | Integer | Firmas pendientes |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:432` |
| `pending_late` | Integer | Pendientes atrasados | Pendientes atrasados de la persona. |  |  | compute `_compute_pending_summary`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:639` |
| `pending_total` | Integer | Mis pendientes | Total de pendientes de la persona. |  |  | compute `_compute_pending_summary`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:637` |
| `process_ids` | Many2many | Procesos donde participa | Procesos en los que participa el puesto. |  | `sgi.process` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:422` |
| `received_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:660` |
| `received_late_count` | Integer | Escalamientos atrasados | Actividades que escalan a este puesto y hoy van atrasadas (I-012). |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:442` |
| `received_role_ids` | Many2many | Escalamientos que recibe | Actividades cuyo atraso le escala a la persona. |  | `sgi.activity.role` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:439` |
| `role_ids` | Many2many | Mis actividades | Actividades que la persona ejecuta o aprueba. |  | `sgi.activity.role` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:436` |
| `short_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:661` |
| `short_role_ids` | Many2many | Participa o se entera | Actividades en las que la persona participa o solo se entera. |  | `sgi.activity.role` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:445` |
| `unmeasured_count` | Integer | Sin medición automática |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:431` |

## Métodos públicos (14)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_doc` | — |
| `action_open_for` | «Ver su procedimiento» desde la ficha del empleado, la del puesto o una fila de Mi equipo, respetando el alcance del usuario. |
| `action_open_mine` | Inicio → Mi procedimiento: la pantalla del puesto del usuario. |
| `action_open_obligations` | Mis obligaciones (módulo qb_obligation, si está instalado): la lista nativa de ese módulo, acotada al usuario. |
| `action_print` | — |
| `action_propose_new_activity` | — |
| `action_publish` | — |
| `action_publish_all` | — |
| `action_show_acks` | — |
| `action_show_all` | — |
| `action_show_indicators` | — |
| `action_show_pending` | — |
| `action_show_received` | I-012 (56.36.0): arranca en los atrasados; sin atrasados, todos. |
| `action_sign` | Firma «leído y entendido» del propio empleado contra la revisión vigente. El candado de identidad vive en sgi.document.ack.write(). |
