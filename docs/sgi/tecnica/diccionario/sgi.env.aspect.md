<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.env.aspect`

**Aspecto e impacto ambiental (ISO 14001 6.1.2)** (Model). Hereda de: `sgi.base.mixin`, `sgi.format.mixin`.

Orden: `significant desc, score desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_env_aspect.py`.

## Campos (25)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:75` |
| `activity` | Char | Actividad u operación | Qué se hace: «Lavado de tambos», «Carga de caldera». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:79` |
| `aspect_type` | Selection | Tipo de aspecto |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:81` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:72` |
| `condition` | Selection | Condición | Normal: operación diaria. Anormal: arranques, paros, mantenimiento. Emergencia: derrame, fuga, incendio. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:88` |
| `control_description` | Text | Control operacional | Cómo se controla el aspecto: procedimiento, instructivo, equipo, almacén temporal, contratista autorizado… |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:119` |
| `frequency` | Selection | Frecuencia | 1 = rara vez … 5 = continuo o diario. |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:100` |
| `impact` | Text | Impacto ambiental | El cambio en el ambiente que causa: «Contaminación del suelo». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:86` |
| `last_review_date` | Date | Última revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:136` |
| `legal_requirement` | Boolean | Tiene requisito legal | Un requisito legal aplicable vuelve significativo el aspecto. Se marca solo al ligar un requisito legal; también se puede marcar a mano. |  |  | compute `_compute_legal_requirement`, guardado |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:105` |
| `legal_requirement_ids` | Many2many | Requisitos legales aplicables |  |  | `sgi.legal.requirement` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:102` |
| `level` | Selection | Nivel |  |  |  | compute `_compute_significance`, guardado |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:112` |
| `life_cycle_stage` | Selection | Etapa del ciclo de vida | En qué etapa del ciclo de vida del producto ocurre el aspecto (ISO 14001 6.1.2). Se pide para registrar la evaluación. |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:92` |
| `name` | Char | Aspecto ambiental | El elemento de la actividad que interactúa con el ambiente: «Generación de estopas impregnadas de aceite». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:83` |
| `next_review_date` | Date | Próxima revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:138` |
| `operational_control_id` | Many2one | Documento del control |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:123` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:76` |
| `responsible_id` | Many2one | Responsable del área |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:131` |
| `review_frequency_months` | Integer | Frecuencia de revisión (meses) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:134` |
| `risk_id` | Many2one | Riesgo ambiental (tratamiento) | Riesgo del instrumento «Aspecto ambiental» donde viven las acciones de tratamiento, si hacen falta. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:126` |
| `score` | Integer | Puntaje | Severidad × frecuencia. |  |  | compute `_compute_significance`, guardado |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:110` |
| `severity` | Selection | Severidad | 1 = sin efecto apreciable … 5 = daño grave o irreversible. |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:98` |
| `sgi_area_id` | Many2one | Área SGI |  |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:78` |
| `significant` | Boolean | Significativo | Nivel moderado o mayor, o con requisito legal aplicable. |  |  | compute `_compute_significance`, guardado |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:114` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:139` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_create_risk` | Crea (o abre) el riesgo ambiental donde viven las acciones de tratamiento. Probabilidad = frecuencia, impacto = severidad. |
| `action_evaluate` | Sella la revisión de hoy, programa la siguiente y deja el aspecto evaluado. Sirve igual para la evaluación inicial y para la revisión anual o por cambio (P-E01). |
| `action_set_borrador` | — |
| `action_set_obsoleto` | — |
