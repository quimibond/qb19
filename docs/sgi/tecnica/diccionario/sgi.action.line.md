<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.action.line`

**Acción / corrección de no conformidad** (Model). Hereda de: `mail.thread`.

Acción o corrección con responsable y fecha compromiso. Cuelga de una NC, riesgo, AMEF, incidente, simulacro, medición en rojo, objetivo o acuerdo de la revisión por la dirección; se cierra con «Marcar hecha».

Orden: `date_commit, id`.

Archivos: `addons/quimibond_sgi/models/sgi_nonconformity.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`, `addons/quimibond_sgi/models/sgi_management_review.py`.

## Campos (21)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_type` | Selection | Tipo | Contención y corrección atienden el efecto; la acción correctiva ataca la causa; la preventiva, una causa potencial; el acuerdo es una salida de la revisión por la dirección. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1093` |
| `activity_id` | Many2one | Actividad |  |  | `mail.activity` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1142` |
| `alert_id` | Many2one | No conformidad | No conformidad a la que pertenece la acción. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1079` |
| `control_hierarchy` | Selection | Jerarquía del control | CONTROL_HIERARCHY_HELP |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1106` |
| `date_commit` | Date | Compromiso | Fecha en que el responsable se compromete a terminar la acción. Pasada esta fecha, la acción se marca vencida y escala. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1114` |
| `date_done` | Date | Terminada el | Fecha en que se terminó la acción. Al capturarla, la acción queda terminada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1117` |
| `drill_id` | Many2one | Simulacro | Simulacro al que pertenece la acción. |  | `sgi.emergency.drill` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1088` |
| `effectiveness_round` | Integer | Ronda de eficacia | Veces que la eficacia de la NC había salido «No eficaz» cuando se registró la acción. Tras un «No eficaz» la NC pide una correctiva de la ronda nueva. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1145` |
| `evidence_attachment_ids` | Many2many | Archivos de evidencia | Fotos, registros o documentos que demuestran la acción. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1131` |
| `evidence_note` | Text | Evidencia | Qué demuestra que la acción se hizo: número de orden, documento, registro o foto. Una acción correctiva no se termina sin evidencia (esta nota o un archivo). |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1127` |
| `fmea_line_id` | Many2one | Modo de falla (AMEF) | Modo de falla del AMEF al que pertenece la acción. |  | `sgi.fmea.line` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1083` |
| `incident_id` | Many2one | Incidente SST | Incidente o accidente de seguridad al que pertenece la acción. |  | `sgi.incident` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1086` |
| `measure_id` | Many2one | Medición roja | Plan de acción de una medición en rojo (I-4). |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:75` |
| `name` | Char | Descripción |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1109` |
| `objective_id` | Many2one | Objetivo integral | Plan de acción del objetivo (ISO 6.2.2). |  | `sgi.objective` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1090` |
| `origin_display` | Char | Origen |  |  |  | compute `_compute_origin_display`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1149` |
| `progress` | Selection | Avance | Avance de la acción según el responsable. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1120` |
| `responsible_id` | Many2one | Responsable | Persona que ejecuta la acción. La ve en Mis pendientes y recibe los avisos de vencimiento. | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1110` |
| `review_id` | Many2one | Revisión por la Dirección |  |  | `sgi.management.review` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:700` |
| `risk_id` | Many2one | Riesgo / Oportunidad | Riesgo u oportunidad al que pertenece la acción. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1081` |
| `state` | Selection | Estado | Abierta, vencida (pasó el compromiso) o terminada (tiene fecha de término). Se calcula sola. |  |  | compute `_compute_state`, guardado |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:1135` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_done` | El click más usado del empleado: terminar su acción. Sella la fecha de hoy y el avance al 100%; el write cierra la actividad espejo y revalida el candado del riesgo si aplica. |
| `action_open_origin` | Abre el registro que originó la acción (NC, riesgo, incidente, simulacro o AMEF): el contexto completo de QUÉ hay que resolver. |
| `create` | — |
| `unlink` | — |
| `write` | — |
