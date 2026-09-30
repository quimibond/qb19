<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.risk`

**Riesgo / Oportunidad SGI** (Model). Hereda de: `sgi.base.mixin`.

Riesgo u oportunidad con su instrumento (R&O, IPER, aspectos ambientales, patrimonial, FODA), evaluación, nivel residual y acciones. Se reevalúa periódicamente.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_risk.py`.

## Campos (33)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:93` |
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:107` |
| `attention_level` | Selection | Nivel de atención |  |  |  | compute `_compute_score`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:113` |
| `category_id` | Many2one | Categoría |  |  | `sgi.risk.category` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:64` |
| `condition` | Selection | Condición (IPER) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:81` |
| `consequence` | Text | Consecuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:52` |
| `eval_impact` | Selection | Impacto / Severidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:111` |
| `eval_probability` | Selection | Probabilidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:110` |
| `existing_controls` | Text | Controles existentes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:78` |
| `foda_type` | Selection | Tipo FODA |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:86` |
| `has_finished_actions` | Boolean | Acciones terminadas |  |  |  | compute `_compute_has_finished_actions`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk.py:126` |
| `high_without_action` | Boolean | Alto sin acción abierta | Riesgo de atención alta o inmediata, no cerrado, sin ninguna acción de tratamiento pendiente. |  |  | compute `_compute_high_without_action`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:135` |
| `instrument` | Selection | Instrumento |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:53` |
| `job_id` | Many2one | Puesto |  |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:77` |
| `kind` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:60` |
| `last_eval_date` | Date | Última evaluación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:131` |
| `name` | Char | Aspecto / Peligro / Situación |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:51` |
| `next_review_date` | Date | Próxima revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:100` |
| `operational_control_id` | Many2one | Control operacional (ambiental) |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:79` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:69` |
| `residual_impact` | Selection | Impacto residual |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:118` |
| `residual_level` | Selection | Nivel residual |  |  |  | compute `_compute_residual`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:120` |
| `residual_note` | Text | Justificación del riesgo residual | Obligatoria para controlar/cerrar un riesgo de atención máxima si el riesgo residual no baja respecto al inicial. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:122` |
| `residual_probability` | Selection | Probabilidad residual |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:117` |
| `residual_score` | Integer | Riesgo residual |  |  |  | compute `_compute_residual`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:119` |
| `score` | Integer | Nivel de riesgo |  |  |  | compute `_compute_score`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:112` |
| `semaphore` | Selection | Semáforo |  |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:132` |
| `sgi_area_id` | Many2one | Área SGI |  |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:76` |
| `sgi_nc_count` | Integer | # NCs ligadas |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk.py:98` |
| `sgi_nc_ids` | Many2many | NCs ligadas |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:95` |
| `sgi_process_active` | Boolean | Proceso vigente | El proceso al que pertenece está activo. Sin proceso o con el proceso archivado, queda pendiente de proceso nuevo. |  |  | related `process_id.active`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:72` |
| `source` | Selection | Origen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:65` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:101` |

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
