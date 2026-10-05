<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.measure.review`

**Revisión mensual de medición** (Model). Hereda de: `mail.thread`.

Revisión mensual de la evidencia de una actividad por el dueño del proceso.

Orden: `month desc, process_id, activity_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_measure_review.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:45` |
| `can_answer` | Boolean | Puede contestar |  |  |  | compute `_compute_can_answer`, sin guardar |  | `addons/quimibond_sgi/models/sgi_measure_review.py:75` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_measure_review.py:50` |
| `date_due` | Date | Vence | Días hábiles desde que se creó (parámetro quimibond_sgi.measure_review_business_days, 5). |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:57` |
| `done_criteria` | Text | Criterio de terminado |  |  |  | related `activity_id.done_criteria`, sin guardar |  | `addons/quimibond_sgi/models/sgi_measure_review.py:74` |
| `line_ids` | One2many | Muestras |  |  | `sgi.measure.review.line` |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:65` |
| `measure_domain` | Char | Filtro de evidencia | El filtro con el que se tomaron las muestras. |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:62` |
| `measure_model` | Char | Modelo de evidencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:61` |
| `measure_user_field` | Char | Campo de usuario |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:64` |
| `month` | Date | Mes | Primer día del mes revisado. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:52` |
| `name` | Char | Qué |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_measure_review.py:60` |
| `note` | Text | Qué no corresponde | Obligatoria con «No corresponde»: qué está mal (el filtro, el modelo, la fecha o quién aparece). |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:69` |
| `process_id` | Many2one | Proceso |  |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_measure_review.py:48` |
| `reviewed_by_id` | Many2one | Contestó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:72` |
| `reviewed_on` | Datetime | Contestada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:73` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:67` |
| `user_id` | Many2one | Revisa | Usuario del dueño del proceso (sin él, el Jefe MAST). | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_measure_review.py:54` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | «Sí, esto es lo que hago». |
| `action_not_matching` | «No corresponde»: pide la nota en un asistente. |
| `action_open_activity` | — |
