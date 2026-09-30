<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.risk`

**Riesgo / Oportunidad SGI** (Model). Hereda de: `sgi.base.mixin`.

Riesgo u oportunidad con su instrumento (R&O, IPER, aspectos ambientales, patrimonial, FODA), evaluación, nivel residual y acciones. Se reevalúa periódicamente.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_risk.py`.

## Campos (33)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:103` |
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:120` |
| `attention_level` | Selection | Nivel de atención | Nivel de atención según el puntaje y el instrumento. Se calcula solo. |  |  | compute `_compute_score`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:129` |
| `category_id` | Many2one | Categoría | Categoría del riesgo u oportunidad. |  | `sgi.risk.category` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:67` |
| `condition` | Selection | Condición (IPER) | En la matriz IPER, si la actividad es rutinaria, no rutinaria o de emergencia. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:89` |
| `consequence` | Text | Consecuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:52` |
| `eval_impact` | Selection | Impacto / Severidad | Impacto o severidad, de 1 a 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:125` |
| `eval_probability` | Selection | Probabilidad | Probabilidad de que ocurra, de 1 a 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:123` |
| `existing_controls` | Text | Controles existentes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:85` |
| `foda_type` | Selection | Tipo FODA | En el análisis FODA: fortaleza, oportunidad, debilidad o amenaza. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:95` |
| `has_finished_actions` | Boolean | Acciones terminadas | Indica que todas sus acciones ya terminaron. |  |  | compute `_compute_has_finished_actions`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk.py:148` |
| `high_without_action` | Boolean | Alto sin acción abierta | Riesgo de atención alta o inmediata, no cerrado, sin ninguna acción de tratamiento pendiente. |  |  | compute `_compute_high_without_action`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:160` |
| `instrument` | Selection | Instrumento | Con qué instrumento se evalúa: riesgos y oportunidades, IPER, aspecto ambiental, patrimonial o FODA. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:53` |
| `job_id` | Many2one | Puesto | Puesto expuesto al riesgo (IPER). |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:84` |
| `kind` | Selection | Tipo | Riesgo u oportunidad. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:62` |
| `last_eval_date` | Date | Última evaluación | Fecha de la última evaluación. La registra «Registrar evaluación». |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:154` |
| `name` | Char | Aspecto / Peligro / Situación |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:51` |
| `next_review_date` | Date | Próxima revisión | Fecha de la próxima reevaluación. Al vencer, llega un aviso al dueño del proceso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:110` |
| `operational_control_id` | Many2one | Control operacional (ambiental) | Documento de control operacional del aspecto ambiental. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:86` |
| `process_id` | Many2one | Proceso | Proceso al que pertenece el riesgo. Su dueño recibe las revisiones. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:74` |
| `residual_impact` | Selection | Impacto residual | Impacto después de las acciones, de 1 a 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:137` |
| `residual_level` | Selection | Nivel residual | Nivel de atención después de las acciones. Se calcula solo. |  |  | compute `_compute_residual`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:141` |
| `residual_note` | Text | Justificación del riesgo residual | Obligatoria para controlar/cerrar un riesgo de atención máxima si el riesgo residual no baja respecto al inicial. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:144` |
| `residual_probability` | Selection | Probabilidad residual | Probabilidad después de las acciones, de 1 a 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:135` |
| `residual_score` | Integer | Riesgo residual | Probabilidad residual × impacto residual. Se calcula solo. |  |  | compute `_compute_residual`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:139` |
| `score` | Integer | Nivel de riesgo | Probabilidad × impacto. Se calcula solo. |  |  | compute `_compute_score`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:127` |
| `semaphore` | Selection | Semáforo | Semáforo según el nivel de atención. Se calcula solo. |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:156` |
| `sgi_area_id` | Many2one | Área SGI | Área del SGI del riesgo. |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:82` |
| `sgi_nc_count` | Integer | # NCs ligadas |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk.py:108` |
| `sgi_nc_ids` | Many2many | NCs ligadas |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:105` |
| `sgi_process_active` | Boolean | Proceso vigente | El proceso al que pertenece está activo. Sin proceso o con el proceso archivado, queda pendiente de proceso nuevo. |  |  | related `process_id.active`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:78` |
| `source` | Selection | Origen | Si el riesgo viene de dentro o de fuera de la empresa. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:69` |
| `state` | Selection | Estado | Identificado, en tratamiento, controlado o cerrado. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:113` |

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
