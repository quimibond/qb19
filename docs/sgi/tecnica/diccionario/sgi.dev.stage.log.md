<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.stage.log`

**Reloj por etapa del desarrollo de producto** (Model).

Paso de un desarrollo por una etapa: hora de inicio y fin, horas totales y horas de desarrollo (sin el tiempo con materia prima pendiente).

Orden: `date_start desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_project.py`, `addons/quimibond_sgi/models/sgi_dev_measure.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_end` | Datetime | Fin | Cuándo salió de la etapa; vacío mientras siga ahí. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:181` |
| `date_start` | Datetime | Inicio | Cuándo entró el proyecto a la etapa. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:179` |
| `hours_dev` | Float | Horas de desarrollo | Horas en la etapa sin contar la espera de materia prima. |  |  | compute `_compute_hours`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:189` |
| `hours_mp` | Float | Horas con materia prima pendiente | Parte de esas horas en que el proyecto esperaba materia prima. |  |  | compute `_compute_hours`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:187` |
| `hours_total` | Float | Horas en la etapa | Horas calendario entre la entrada y la salida (o ahora). |  |  | compute `_compute_hours`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:185` |
| `project_id` | Many2one |  | Proyecto de desarrollo. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:175` |
| `stage_id` | Many2one | Etapa | Etapa en la que estuvo el proyecto. | sí | `project.project.stage` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:177` |
| `stage_key` | Char | Clave de la etapa | Clave de la etapa de avance (solicitud, analisis, …, liberado); vacía si la etapa no es de desarrollo. Sirve para medir sin depender de ids. |  |  | compute `_compute_stage_key`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_measure.py:149` |
| `user_id` | Many2one | Lo pasó a la etapa | Usuario que movió el proyecto a esta etapa. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:183` |

