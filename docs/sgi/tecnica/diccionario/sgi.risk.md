<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.risk`

**Riesgo / Oportunidad SGI** (Model). Hereda de: `sgi.base.mixin`.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_risk.py`.

## Campos (33)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:90` |
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:104` |
| `attention_level` | Selection | Nivel de atención |  |  |  | compute `_compute_score`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:110` |
| `category_id` | Many2one | Categoría |  |  | `sgi.risk.category` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:61` |
| `condition` | Selection | Condición (IPER) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:78` |
| `consequence` | Text | Consecuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:49` |
| `eval_impact` | Selection | Impacto / Severidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:108` |
| `eval_probability` | Selection | Probabilidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:107` |
| `existing_controls` | Text | Controles existentes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:75` |
| `foda_type` | Selection | Tipo FODA |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:83` |
| `has_finished_actions` | Boolean | Acciones terminadas |  |  |  | compute `_compute_has_finished_actions`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk.py:123` |
| `high_without_action` | Boolean | Alto sin acción abierta | Riesgo de atención alta o inmediata, no cerrado, sin ninguna acción de tratamiento pendiente. |  |  | compute `_compute_high_without_action`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:132` |
| `instrument` | Selection | Instrumento |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:50` |
| `job_id` | Many2one | Puesto |  |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:74` |
| `kind` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:57` |
| `last_eval_date` | Date | Última evaluación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:128` |
| `name` | Char | Aspecto / Peligro / Situación |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:48` |
| `next_review_date` | Date | Próxima revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:97` |
| `operational_control_id` | Many2one | Control operacional (ambiental) |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:76` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:66` |
| `residual_impact` | Selection | Impacto residual |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:115` |
| `residual_level` | Selection | Nivel residual |  |  |  | compute `_compute_residual`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:117` |
| `residual_note` | Text | Justificación del riesgo residual | Obligatoria para controlar/cerrar un riesgo de atención máxima si el riesgo residual no baja respecto al inicial. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:119` |
| `residual_probability` | Selection | Probabilidad residual |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:114` |
| `residual_score` | Integer | Riesgo residual |  |  |  | compute `_compute_residual`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:116` |
| `score` | Integer | Nivel de riesgo |  |  |  | compute `_compute_score`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:109` |
| `semaphore` | Selection | Semáforo |  |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:129` |
| `sgi_area_id` | Many2one | Área SGI |  |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:73` |
| `sgi_nc_count` | Integer | # NCs ligadas |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk.py:95` |
| `sgi_nc_ids` | Many2many | NCs ligadas |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:92` |
| `sgi_process_active` | Boolean | Proceso vigente | El proceso al que pertenece está activo. Sin proceso o con el proceso archivado, queda pendiente de proceso nuevo. |  |  | related `process_id.active`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:69` |
| `source` | Selection | Origen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:62` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:98` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_evaluate` | Sella la evaluación de hoy y programa la siguiente (enero / julio). |
| `action_set_cerrado` | — |
| `action_set_controlado` | — |
| `action_set_en_tratamiento` | — |
| `action_set_identificado` | — |
| `action_view_sgi_ncs` | — |
| `write` | — |
