<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.action.line`

**Acción / corrección de No Conformidad** (Model). Hereda de: `mail.thread`.

Acción o corrección con responsable y fecha compromiso. Cuelga de una NC, riesgo, AMEF, incidente, simulacro, medición en rojo, objetivo o acuerdo de la revisión por la dirección; se cierra con «Marcar hecha».

Orden: `date_commit, id`.

Archivos: `addons/quimibond_sgi/models/sgi_nonconformity.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`, `addons/quimibond_sgi/models/sgi_management_review.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:700` |
| `activity_id` | Many2one | Actividad |  |  | `mail.activity` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:722` |
| `alert_id` | Many2one | No Conformidad |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:691` |
| `date_commit` | Date | Compromiso |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:709` |
| `date_done` | Date | Terminada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:710` |
| `drill_id` | Many2one | Simulacro |  |  | `sgi.emergency.drill` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:696` |
| `fmea_line_id` | Many2one | Modo de falla (AMEF) |  |  | `sgi.fmea.line` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:693` |
| `incident_id` | Many2one | Incidente SST |  |  | `sgi.incident` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:695` |
| `measure_id` | Many2one | Medición roja | Plan de acción de una medición en rojo (I-4). |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:75` |
| `name` | Char | Descripción |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:706` |
| `objective_id` | Many2one | Objetivo integral | Plan de acción del objetivo (ISO 6.2.2). |  | `sgi.objective` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:697` |
| `origin_display` | Char | Origen |  |  |  | compute `_compute_origin_display`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:724` |
| `progress` | Selection | Avance |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:711` |
| `responsible_id` | Many2one | Responsable |  | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:707` |
| `review_id` | Many2one | Revisión por la Dirección |  |  | `sgi.management.review` |  |  | `addons/quimibond_sgi/models/sgi_management_review.py:437` |
| `risk_id` | Many2one | Riesgo / Oportunidad |  |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:692` |
| `state` | Selection | Estado |  |  |  | compute `_compute_state`, guardado |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:716` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_done` | El click más usado del empleado: terminar su acción. Sella la fecha de hoy y el avance al 100%; el write cierra la actividad espejo y revalida el candado del riesgo si aplica. |
| `action_open_origin` | Abre el registro que originó la acción (NC, riesgo, incidente, simulacro o AMEF): el contexto completo de QUÉ hay que resolver. |
| `create` | — |
| `unlink` | — |
| `write` | — |
