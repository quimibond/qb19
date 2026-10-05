<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.change`

**Propuesta de cambio a una actividad (Mi procedimiento)** (Model).

Propuesta de cambio a una actividad: los mismos campos de la actividad con los valores propuestos, más quién la hace. Nace con los valores de hoy; lo que la persona cambie es lo que se aprueba y se aplica.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_mp_change.py`, `addons/quimibond_sgi/models/sgi_mp_change_simple.py`.

## Campos (42)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad | Actividad a la que se refiere la propuesta. Vacío si se propone una actividad nueva. |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:199` |
| `allowed_process_ids` | Many2many | Procesos del puesto | Procesos en los que participa el puesto de quien propone. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:224` |
| `attachment` | Binary | Adjunto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:232` |
| `attachment_name` | Char | Nombre del adjunto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:233` |
| `change_type` | Selection | Qué propone | Agregar una actividad nueva, cambiar esta o quitarla. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:197` |
| `check_against` | Char | Contra qué se compara |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:267` |
| `description` | Text | Descripción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:237` |
| `diff_html` | Html | Qué cambia |  |  |  | compute `_compute_diff_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change.py:271` |
| `diff_snapshot` | Html | Cambio enviado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:274` |
| `display_title` | Char |  |  |  |  | compute `_compute_display_title`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change.py:234` |
| `done_criteria` | Text | Criterio de terminado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:268` |
| `due_business_day` | Integer | Vence el día hábil (mensual) | Para actividades mensuales: día hábil del mes en que vence (por ejemplo, 3 = tercer día hábil). |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:254` |
| `due_day` | Integer | Vence el día | Día del mes en que vence la actividad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:259` |
| `due_month` | Selection | Vence en el mes | Mes en que vence la actividad, si es anual. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:257` |
| `due_weekday` | Selection | Vence el (semanal) | Para actividades semanales: día de la semana en que vence. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:252` |
| `exec_channel` | Selection | Dónde se hace | Dónde se hace el trabajo (Odoo, correo, papel…), no cómo se mide. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:260` |
| `external_system` | Char | Sistema externo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:265` |
| `format_document_ids` | Many2many | Formatos referenciados | Formatos controlados que se usan en la actividad. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:242` |
| `hints_html` | Html | Avisos |  |  |  | compute `_compute_preview_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:102` |
| `how_steps` | Text | Cómo (pasos) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:238` |
| `instruction_id` | Many2one | Instructivo | Instructivo que explica cómo se hace la actividad. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:239` |
| `is_sgi_manager` | Boolean |  |  |  |  | compute `_compute_is_sgi_manager`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:107` |
| `job_id` | Many2one | Puesto de quien propone | Puesto de la persona que hace la propuesta. |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:228` |
| `measure_cadence` | Selection | Cadencia esperada | Cada cuánto se espera que la actividad se haga. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:250` |
| `missing_html` | Html | Lo que falta para publicarla | Lo que el Jefe MAST completa antes de aprobar (lo mismo que pide «Faltantes de especificación» a una actividad). |  |  | compute `_compute_missing_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:103` |
| `name` | Char | Resumen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:236` |
| `odoo_menu_id` | Many2one | Menú de Odoo | Menú de Odoo donde se hace la actividad; de ahí sale el botón «Ir a hacerlo». |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:262` |
| `on_fail` | Text | Si no se puede cumplir |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:269` |
| `place_note` | Char | Lugar |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:266` |
| `preview_html` | Html | Así quedará en su procedimiento |  |  |  | compute `_compute_preview_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:100` |
| `process_id` | Many2one | Proceso | Proceso al que pertenece la actividad propuesta. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:222` |
| `q_other_cadence` | Selection | ¿Cada cuánto? |  |  |  | compute `_compute_q_answers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:88` |
| `q_when` | Selection | ¿Cada cuándo? |  |  |  | compute `_compute_q_answers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:85` |
| `q_where` | Selection | ¿Dónde se hace? |  |  |  | compute `_compute_q_answers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:97` |
| `q_who` | Selection | ¿Quién la hace? |  |  |  | compute `_compute_q_answers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:91` |
| `q_who_job_id` | Many2one | ¿Qué puesto? |  |  | `hr.job` | compute `_compute_q_answers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:94` |
| `reason` | Text | Por qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:231` |
| `related_procedure_id` | Many2one | Procedimiento relacionado | Procedimiento que rige la actividad. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:246` |
| `request_id` | Many2one | Solicitud |  |  | `approval.request` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:230` |
| `role_line_ids` | One2many | Quién la hace |  |  | `sgi.activity.change.role` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:270` |
| `state` | Selection | Estado | Borrador mientras se escribe; enviada cuando está en aprobación; aplicada cuando el cambio ya quedó en la actividad. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:190` |
| `trigger_note` | Char | Qué la dispara | Para las que se hacen cada vez que pasa algo: qué pasa («llega un pedido nuevo», «se rechaza un lote»). Al aprobarse queda al inicio de la descripción. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change_simple.py:81` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_full_form` | «Ver todos los campos» (Jefe MAST): la ficha técnica completa. |
| `action_sgi_mast_complete` | «Guardar y actualizar la solicitud»: lo que el Jefe MAST completó pasa a la solicitud (antes → después y motivo) antes de aprobar. |
| `action_sgi_new_from_context` | «Nuevo» del kanban de Mis actividades: la propuesta de actividad nueva con el puesto y los procesos que trae el contexto. |
| `action_submit` | — |
| `unlink` | — |
| `write` | — |
