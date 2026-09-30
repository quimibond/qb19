<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.env.aspect`

**Aspecto e impacto ambiental (ISO 14001 6.1.2)** (Model). Hereda de: `sgi.base.mixin`, `sgi.format.mixin`.

Orden: `significant desc, score desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_env_aspect.py`.

## Campos (24)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:47` |
| `activity` | Char | Actividad u operación | Qué se hace: «Lavado de tambos», «Carga de caldera». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:51` |
| `aspect_type` | Selection | Tipo de aspecto |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:53` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:44` |
| `condition` | Selection | Condición | Normal: operación diaria. Anormal: arranques, paros, mantenimiento. Emergencia: derrame, fuga, incendio. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:71` |
| `control_description` | Text | Control operacional | Cómo se controla el aspecto: procedimiento, instructivo, equipo, almacén temporal, contratista autorizado… |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:101` |
| `frequency` | Selection | Frecuencia | 1 = rara vez … 5 = continuo o diario. |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:82` |
| `impact` | Text | Impacto ambiental | El cambio en el ambiente que causa: «Contaminación del suelo». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:69` |
| `last_review_date` | Date | Última revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:118` |
| `legal_requirement` | Boolean | Tiene requisito legal | Un requisito legal aplicable vuelve significativo el aspecto. Se marca solo al ligar un requisito legal; también se puede marcar a mano. |  |  | compute `_compute_legal_requirement`, guardado |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:87` |
| `legal_requirement_ids` | Many2many | Requisitos legales aplicables |  |  | `sgi.legal.requirement` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:84` |
| `level` | Selection | Nivel |  |  |  | compute `_compute_significance`, guardado |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:94` |
| `name` | Char | Aspecto ambiental | El elemento de la actividad que interactúa con el ambiente: «Generación de estopas impregnadas de aceite». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:66` |
| `next_review_date` | Date | Próxima revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:120` |
| `operational_control_id` | Many2one | Documento del control |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:105` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:48` |
| `responsible_id` | Many2one | Responsable del área |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:113` |
| `review_frequency_months` | Integer | Frecuencia de revisión (meses) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:116` |
| `risk_id` | Many2one | Riesgo ambiental (tratamiento) | Riesgo del instrumento «Aspecto ambiental» donde viven las acciones de tratamiento, si hacen falta. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:108` |
| `score` | Integer | Puntaje | Severidad × frecuencia. |  |  | compute `_compute_significance`, guardado |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:92` |
| `severity` | Selection | Severidad | 1 = sin efecto apreciable … 5 = daño grave o irreversible. |  |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:80` |
| `sgi_area_id` | Many2one | Área SGI |  |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:50` |
| `significant` | Boolean | Significativo | Nivel moderado o mayor, o con requisito legal aplicable. |  |  | compute `_compute_significance`, guardado |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:96` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_env_aspect.py:121` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_create_risk` | Crea (o abre) el riesgo ambiental donde viven las acciones de tratamiento. Probabilidad = frecuencia, impacto = severidad. |
| `action_evaluate` | Sella la revisión de hoy, programa la siguiente y deja el aspecto evaluado. Sirve igual para la evaluación inicial y para la revisión anual o por cambio (P-E01). |
| `action_set_borrador` | — |
| `action_set_obsoleto` | — |
