<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.risk`

**Riesgo / Oportunidad SGI** (Model). Hereda de: `sgi.base.mixin`.

Riesgo u oportunidad con su instrumento (R&O, IPER, aspectos ambientales, patrimonial, FODA), evaluación, nivel residual y acciones. Se reevalúa periódicamente.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_risk.py`, `addons/quimibond_sgi/models/sgi_report_print.py`.

## Campos (35)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:143` |
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:160` |
| `attention_level` | Selection | Nivel de atención | Nivel de atención según el puntaje y el instrumento. Se calcula solo. |  |  | compute `_compute_score`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:169` |
| `category_id` | Many2one | Categoría | Categoría del riesgo u oportunidad. |  | `sgi.risk.category` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:98` |
| `condition` | Selection | Condición (IPER) | En la matriz IPER, si la actividad es rutinaria, no rutinaria o de emergencia. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:129` |
| `consequence` | Text | Consecuencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:83` |
| `control_hierarchy` | Selection | Control existente de mayor nivel | CONTROL_HIERARCHY_HELP |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:118` |
| `eval_impact` | Selection | Impacto / Severidad | Impacto o severidad, de 1 a 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:165` |
| `eval_probability` | Selection | Probabilidad | Probabilidad de que ocurra, de 1 a 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:163` |
| `existing_controls` | Text | Controles existentes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:116` |
| `foda_type` | Selection | Tipo FODA | En el análisis FODA: fortaleza, oportunidad, debilidad o amenaza. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:135` |
| `has_finished_actions` | Boolean | Acciones terminadas | Indica que todas sus acciones ya terminaron. |  |  | compute `_compute_has_finished_actions`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk.py:188` |
| `high_without_action` | Boolean | Alto sin acción abierta | Riesgo de atención alta o inmediata, no cerrado, sin ninguna acción de tratamiento pendiente. |  |  | compute `_compute_high_without_action`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:200` |
| `instrument` | Selection | Instrumento | Con qué instrumento se evalúa: riesgos y oportunidades, IPER, aspecto ambiental, patrimonial o FODA. «Aspecto ambiental» solo lo pone la matriz de aspectos (Tratar como riesgo). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:84` |
| `job_id` | Many2one | Puesto | Puesto expuesto al riesgo (IPER). |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:115` |
| `kind` | Selection | Tipo | Riesgo u oportunidad. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:93` |
| `last_eval_date` | Date | Última evaluación | Fecha de la última evaluación. La registra «Registrar evaluación». |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:194` |
| `name` | Char | Aspecto / Peligro / Situación |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:82` |
| `next_review_date` | Date | Próxima revisión | Fecha de la próxima reevaluación. Al vencer, llega un aviso al dueño del proceso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:150` |
| `operational_control_id` | Many2one | Control operacional (ambiental) | Documento de control operacional del aspecto ambiental. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:126` |
| `process_id` | Many2one | Proceso | Proceso al que pertenece el riesgo. Su dueño recibe las revisiones. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:105` |
| `residual_impact` | Selection | Impacto residual | Impacto después de las acciones, de 1 a 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:177` |
| `residual_level` | Selection | Nivel residual | Nivel de atención después de las acciones. Se calcula solo. |  |  | compute `_compute_residual`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:181` |
| `residual_note` | Text | Justificación del riesgo residual | Obligatoria para controlar/cerrar un riesgo de atención máxima si el riesgo residual no baja respecto al inicial. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:184` |
| `residual_probability` | Selection | Probabilidad residual | Probabilidad después de las acciones, de 1 a 5. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:175` |
| `residual_score` | Integer | Riesgo residual | Probabilidad residual × impacto residual. Se calcula solo. |  |  | compute `_compute_residual`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:179` |
| `score` | Integer | Nivel de riesgo | Probabilidad × impacto. Se calcula solo. |  |  | compute `_compute_score`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:167` |
| `semaphore` | Selection | Semáforo | Semáforo según el nivel de atención. Se calcula solo. |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:196` |
| `sgi_area_id` | Many2one | Área SGI | Área del SGI del riesgo. |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:113` |
| `sgi_env_aspect_ids` | One2many | Aspecto ambiental de la matriz | Aspecto ambiental cuya evaluación vive en la matriz; este riesgo guarda sus acciones de tratamiento. |  | `sgi.env.aspect` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:122` |
| `sgi_nc_count` | Integer | # NC ligadas |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_risk.py:148` |
| `sgi_nc_ids` | Many2many | NC ligadas |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_risk.py:145` |
| `sgi_process_active` | Boolean | Proceso vigente | El proceso al que pertenece está activo. Sin proceso o con el proceso archivado, queda pendiente de proceso nuevo. |  |  | related `process_id.active`, guardado |  | `addons/quimibond_sgi/models/sgi_risk.py:109` |
| `source` | Selection | Origen | Si el riesgo viene de dentro o de fuera de la empresa. |  |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:100` |
| `state` | Selection | Estado | Identificado, en tratamiento, controlado o cerrado. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_risk.py:153` |

## Métodos públicos (8)

| Método | Qué hace (docstring) |
|---|---|
| `action_evaluate` | Sella la evaluación de hoy y programa la siguiente (enero / julio). |
| `action_set_cerrado` | — |
| `action_set_controlado` | — |
| `action_set_en_tratamiento` | — |
| `action_set_identificado` | — |
| `action_view_sgi_ncs` | — |
| `create` | — |
| `write` | — |
