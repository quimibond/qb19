<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.lab.request`

**Solicitud de pruebas a laboratorio (desarrollo)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Solicitud de pruebas de laboratorio sobre renglones de la tabla de características.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `authorized_by_id` | Many2one | Autorizó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:138` |
| `company_id` | Many2one |  |  |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:146` |
| `date_authorized` | Datetime | Autorizada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:139` |
| `date_measured` | Datetime | Medida el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:140` |
| `date_requested` | Datetime | Solicitada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:137` |
| `hours_total` | Float | Horas del laboratorio | Horas calendario de la solicitud a la última medición. |  |  | compute `_compute_hours`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:143` |
| `kind` | Selection | Qué se mide | La muestra que mandó el cliente (análisis) o la corrida propia (paso 12). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:128` |
| `line_ids` | Many2many | Renglones a medir | Renglones de la tabla del proyecto que mide el laboratorio. |  | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:134` |
| `name` | Char | Solicitud |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:125` |
| `note` | Text | Observaciones | Instrucciones para el laboratorio; no capture aquí valores. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:145` |
| `pending_count` | Integer | Sin resultado | Renglones que aún no tienen valor medido. |  |  | compute `_compute_pending`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:141` |
| `project_id` | Many2one | Proyecto | Proyecto de desarrollo cuyos renglones se miden. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:126` |
| `requested_by_id` | Many2one | Solicitó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:136` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:131` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_authorize` | — |
| `action_cancel` | — |
| `action_request` | — |
