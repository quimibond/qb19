<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.action.line`

**Acción / corrección de No Conformidad** (Model). Hereda de: `mail.thread`.

Acción o corrección con responsable y fecha compromiso. Cuelga de una NC, riesgo, AMEF, incidente, simulacro, medición en rojo, objetivo o acuerdo de la revisión por la dirección; se cierra con «Marcar hecha».

Orden: `date_commit, id`.

Archivos: `addons/quimibond_sgi/models/sgi_nonconformity.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`, `addons/quimibond_sgi/models/sgi_management_review.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_type` | Selection | Tipo | Contención y corrección atienden el efecto; la acción correctiva ataca la causa; la preventiva, una causa potencial. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:806` |
| `activity_id` | Many2one | Actividad |  |  | `mail.activity` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:838` |
| `alert_id` | Many2one | No Conformidad | No conformidad a la que pertenece la acción. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:792` |
| `date_commit` | Date | Compromiso | Fecha en que el responsable se compromete a terminar la acción. Pasada esta fecha, la acción se marca vencida y escala. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:819` |
| `date_done` | Date | Terminada el | Fecha en que se terminó la acción. Al capturarla, la acción queda terminada. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:822` |
| `drill_id` | Many2one | Simulacro | Simulacro al que pertenece la acción. |  | `sgi.emergency.drill` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:801` |
| `fmea_line_id` | Many2one | Modo de falla (AMEF) | Modo de falla del AMEF al que pertenece la acción. |  | `sgi.fmea.line` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:796` |
| `incident_id` | Many2one | Incidente SST | Incidente o accidente de seguridad al que pertenece la acción. |  | `sgi.incident` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:799` |
| `measure_id` | Many2one | Medición roja | Plan de acción de una medición en rojo (I-4). |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:75` |
| `name` | Char | Descripción |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:814` |
| `objective_id` | Many2one | Objetivo integral | Plan de acción del objetivo (ISO 6.2.2). |  | `sgi.objective` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:803` |
| `origin_display` | Char | Origen |  |  |  | compute `_compute_origin_display`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:840` |
| `progress` | Selection | Avance | Avance de la acción según el responsable. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:825` |
| `responsible_id` | Many2one | Responsable | Persona que ejecuta la acción. La ve en Mis pendientes y recibe los avisos de vencimiento. | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:815` |
| `review_id` | Many2one | Revisión por la Dirección |  |  | `sgi.management.review` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:460` |
| `risk_id` | Many2one | Riesgo / Oportunidad | Riesgo u oportunidad al que pertenece la acción. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:794` |
| `state` | Selection | Estado | Abierta, vencida (pasó el compromiso) o terminada (tiene fecha de término). Se calcula sola. |  |  | compute `_compute_state`, guardado |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:831` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_done` | El click más usado del empleado: terminar su acción. Sella la fecha de hoy y el avance al 100%; el write cierra la actividad espejo y revalida el candado del riesgo si aplica. |
| `action_open_origin` | Abre el registro que originó la acción (NC, riesgo, incidente, simulacro o AMEF): el contexto completo de QUÉ hay que resolver. |
| `create` | — |
| `unlink` | — |
| `write` | — |
