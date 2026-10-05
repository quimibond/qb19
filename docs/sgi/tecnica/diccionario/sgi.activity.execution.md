<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.execution`

**Registro de cumplimiento de actividad** (Model). Hereda de: `mail.thread`.

Cumplimiento de una actividad en un periodo, por persona.

Orden: `date_due desc, activity_id, employee_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_activity_execution.py`.

## Campos (28)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:54` |
| `can_mark` | Boolean | Puede marcarla | La persona, su jefe o el Jefe MAST. |  |  | compute `_compute_can_mark`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:111` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:59` |
| `date_done` | Datetime | Hecha el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:85` |
| `date_due` | Date | Vence | Vencimiento de la actividad en este periodo. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:70` |
| `date_estimated` | Date | Fecha estimada | Cuándo espera terminarla (opcional). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:82` |
| `done_auto` | Boolean | Por el registro de Odoo | La marcó hecha el sistema al encontrar el registro de evidencia en el periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:87` |
| `done_by_id` | Many2one | Marcada por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:86` |
| `done_criteria` | Text | Criterio de terminado |  |  |  | related `activity_id.done_criteria`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:102` |
| `employee_id` | Many2one | Responsable | Persona que ejecuta la actividad (su puesto tiene el rol «Ejecuta»). | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:61` |
| `evidence_attachment_ids` | Many2many | Archivos de evidencia |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:96` |
| `evidence_note` | Text | Evidencia | Qué demuestra que se hizo: folio, documento, registro, foto… |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:93` |
| `has_related_screen` | Boolean | Tiene pantalla relacionada |  |  |  | compute `_compute_needs_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:109` |
| `how_steps` | Text | Cómo (pasos) |  |  |  | related `activity_id.how_steps`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:103` |
| `instruction_id` | Many2one | Instructivo |  |  |  | related `activity_id.instruction_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:104` |
| `measure_method` | Selection |  |  |  |  | related `activity_id.measure_method`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:101` |
| `na_reason` | Text | Por qué no aplica |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:99` |
| `name` | Char | Qué |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:73` |
| `needs_evidence` | Boolean | Con evidencia obligatoria | Registro manual: «Hecha» pide una nota o un archivo. |  |  | compute `_compute_needs_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:106` |
| `on_fail` | Text | Si no se puede cumplir |  |  |  | related `activity_id.on_fail`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:105` |
| `on_time` | Boolean | A tiempo | Hecha a más tardar en su vencimiento (hora de México). |  |  | compute `_compute_on_time`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:90` |
| `period_end` | Date | Fin del periodo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:69` |
| `period_label` | Char | Periodo |  |  |  | compute `_compute_period_label`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:72` |
| `period_start` | Date | Inicio del periodo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:67` |
| `process_id` | Many2one | Proceso |  |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:57` |
| `progress_note` | Text | Nota de avance | Qué lleva y qué le falta. «En proceso» no quita el atraso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:79` |
| `state` | Selection | Estado | Pendiente, en proceso (con nota de avance), hecha (con evidencia si es de registro manual) o no aplica este periodo (con motivo). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:74` |
| `user_id` | Many2one | Usuario |  |  |  | related `employee_id.user_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:65` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_done` | — |
| `action_mark_not_applicable` | — |
| `action_mark_progress` | — |
| `action_open_related_screen` | «Abrir pantalla relacionada»: el menú o la acción de Odoo de la actividad, solo si está capturado. |
| `action_reopen` | — |
| `action_view_instruction` | — |
