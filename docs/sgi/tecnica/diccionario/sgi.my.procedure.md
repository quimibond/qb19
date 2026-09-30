<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.my.procedure`

**Mi procedimiento (pantalla)** (TransientModel).

Pantalla de Mi procedimiento de una persona o un puesto: actividades por rol, documentos, indicadores, EPP y acuse. Se arma al abrirla.

Archivos: `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`, `addons/quimibond_sgi/models/sgi_mp_change.py`, `addons/quimibond_sgi/models/sgi_my_pending.py`.

## Campos (43)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `ack_ids` | Many2many | Mis acuses de lectura |  |  | `sgi.document.ack` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:432` |
| `ack_label` | Char |  |  |  |  | compute `_compute_ack`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:387` |
| `ack_state` | Selection |  |  |  |  | compute `_compute_ack`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:381` |
| `activity_count` | Integer | Actividades |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:403` |
| `allowed_employee_ids` | Many2many | Empleados visibles |  |  | `hr.employee.public` | compute `_compute_scope`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:376` |
| `allowed_job_ids` | Many2many | Puestos visibles |  |  | `hr.job` | compute `_compute_scope`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:378` |
| `boss_id` | Many2one | Mi jefe |  |  | `hr.employee.public` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:391` |
| `can_pick` | Boolean |  |  |  |  | compute `_compute_scope`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:374` |
| `can_publish` | Boolean |  |  |  |  | compute `_compute_scope`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:375` |
| `department_id` | Many2one | Área |  |  | `hr.department` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:392` |
| `doc_id` | Many2one |  |  |  | `documents.document` | compute `_compute_ack`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:388` |
| `document_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:625` |
| `document_ids` | Many2many | Documentos que aplican al puesto |  |  | `documents.document` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:434` |
| `employee_avatar` | Binary | Foto |  |  |  | related `employee_id.avatar_128`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:371` |
| `employee_id` | Many2one | Ver como: empleado |  |  | `hr.employee.public` |  |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:370` |
| `epp_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:627` |
| `epp_delivery_ids` | Many2many | Responsivas de EPP |  |  | `sgi.epp.delivery` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:439` |
| `epp_label` | Char | EPP |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:442` |
| `epp_pending_sign` | Boolean | Responsiva de EPP por firmar |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:441` |
| `epp_required` | Text | EPP requerido por el puesto |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:438` |
| `family_id` | Many2one | Familia de puestos |  |  | `sgi.job.family` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:393` |
| `has_obligations` | Boolean |  |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:429` |
| `has_user` | Boolean |  |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:417` |
| `indicator_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:626` |
| `indicator_ids` | Many2many | Mis indicadores |  |  | `sgi.indicator` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:424` |
| `is_me` | Boolean |  |  |  |  | compute `_compute_ack`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:380` |
| `job_employee_ids` | Many2many | Personas en el puesto |  |  | `hr.employee.public` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:395` |
| `job_id` | Many2one | Ver como: puesto |  |  | `hr.job` | compute `_compute_job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:372` |
| `late_count` | Integer | Atrasadas |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:399` |
| `no_employee` | Boolean | Usuario sin empleado | El usuario no está ligado a un empleado y no eligió a nadie: la pantalla muestra el aviso en vez de quedar vacía. |  |  | compute `_compute_no_employee`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change.py:159` |
| `official_indicator_ids` | Many2many | Indicadores oficiales a mi cargo |  |  | `sgi.indicator` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:427` |
| `ok_count` | Integer | Al día |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:400` |
| `pending_ack_count` | Integer | Firmas pendientes |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:402` |
| `pending_late` | Integer | Pendientes atrasados |  |  |  | compute `_compute_pending_summary`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:633` |
| `pending_total` | Integer | Mis pendientes |  |  |  | compute `_compute_pending_summary`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:632` |
| `process_ids` | Many2many | Procesos donde participa |  |  | `sgi.process` | compute `_compute_who`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:394` |
| `received_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:623` |
| `received_late_count` | Integer | Escalamientos atrasados | Actividades que escalan a este puesto y hoy van atrasadas (I-012). |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:410` |
| `received_role_ids` | Many2many | Escalamientos que recibe |  |  | `sgi.activity.role` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:408` |
| `role_ids` | Many2many | Mis actividades |  |  | `sgi.activity.role` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:406` |
| `short_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:624` |
| `short_role_ids` | Many2many | Participa o se entera |  |  | `sgi.activity.role` | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:413` |
| `unmeasured_count` | Integer | Sin medición automática |  |  |  | compute `_compute_lists`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:401` |

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
