<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.change`

**Propuesta de cambio a una actividad (Mi procedimiento)** (Model).

Propuesta de cambio a una actividad: los mismos campos de la actividad con los valores propuestos, más quién la hace. Nace con los valores de hoy; lo que la persona cambie es lo que se aprueba y se aplica.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_mp_change.py`.

## Campos (32)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:192` |
| `allowed_process_ids` | Many2many | Procesos del puesto |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:214` |
| `attachment` | Binary | Adjunto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:220` |
| `attachment_name` | Char | Nombre del adjunto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:221` |
| `change_type` | Selection | Qué propones |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:191` |
| `check_against` | Char | Contra qué se compara |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:244` |
| `description` | Text | Descripción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:225` |
| `diff_html` | Html | Qué cambia |  |  |  | compute `_compute_diff_html`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change.py:248` |
| `diff_snapshot` | Html | Cambio enviado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:251` |
| `display_title` | Char |  |  |  |  | compute `_compute_display_title`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change.py:222` |
| `done_criteria` | Text | Criterio de terminado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:245` |
| `due_business_day` | Integer | Vence el día hábil (mensual) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:237` |
| `due_day` | Integer | Vence el día |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:239` |
| `due_month` | Selection | Vence en el mes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:238` |
| `due_weekday` | Selection | Vence el (semanal) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:236` |
| `exec_channel` | Selection | Dónde se hace |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:240` |
| `external_system` | Char | Sistema externo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:242` |
| `format_document_ids` | Many2many | Formatos referenciados |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:229` |
| `how_steps` | Text | Cómo (pasos) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:226` |
| `instruction_id` | Many2one | Instructivo |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:227` |
| `job_id` | Many2one | Puesto de quien propone |  |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:217` |
| `measure_cadence` | Selection | Cadencia esperada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:235` |
| `name` | Char | Resumen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:224` |
| `odoo_menu_id` | Many2one | Menú de Odoo |  |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:241` |
| `on_fail` | Text | Si no se puede cumplir |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:246` |
| `place_note` | Char | Lugar |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:243` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:213` |
| `reason` | Text | Por qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:219` |
| `related_procedure_id` | Many2one | Procedimiento relacionado |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:232` |
| `request_id` | Many2one | Solicitud |  |  | `approval.request` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:218` |
| `role_line_ids` | One2many | Quién la hace |  |  | `sgi.activity.change.role` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:247` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:186` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_new_from_context` | «Nuevo» del kanban de Mis actividades: la propuesta de actividad nueva con el puesto y los procesos que trae el contexto. |
| `action_submit` | — |
| `unlink` | — |
| `write` | — |
