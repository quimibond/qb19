<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.lab.request`

**Solicitud de pruebas a laboratorio (desarrollo)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Solicitud de pruebas de laboratorio sobre renglones de la tabla de características.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`, `addons/quimibond_sgi/models/sgi_dev_process.py`.

## Campos (16)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `authorized_by_id` | Many2one | Autorizó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:158` |
| `company_id` | Many2one |  |  |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:166` |
| `date_authorized` | Datetime | Autorizada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:159` |
| `date_measured` | Datetime | Medida el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:160` |
| `date_requested` | Datetime | Solicitada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:157` |
| `date_verdict` | Datetime | Dictaminada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process.py:230` |
| `hours_total` | Float | Horas del laboratorio | Horas calendario de la solicitud a la última medición. |  |  | compute `_compute_hours`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:163` |
| `kind` | Selection | Qué se mide | La muestra que mandó el cliente (análisis) o la corrida propia (paso 12). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:145` |
| `line_ids` | Many2many | Renglones a medir | Renglones de la tabla del proyecto que mide el laboratorio. El laboratorio captura el valor en el renglón (abra la fila); el dictamen es de Diseño de Producto. La solicitud se cierra sola cuando todos los renglones tienen valor. |  | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:151` |
| `name` | Char | Solicitud |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:142` |
| `note` | Text | Observaciones | Instrucciones para el laboratorio; no capture aquí valores. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:165` |
| `pending_count` | Integer | Sin resultado | Renglones que aún no tienen valor medido. |  |  | compute `_compute_pending`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:161` |
| `project_id` | Many2one | Proyecto | Proyecto de desarrollo cuyos renglones se miden. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:143` |
| `requested_by_id` | Many2one | Solicitó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:156` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:148` |
| `verdict_by_id` | Many2one | Dictaminó (Diseño de Producto) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_process.py:229` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_authorize` | — |
| `action_cancel` | — |
| `action_request` | — |
| `action_verdict` | Dictamen de Diseño de Producto sobre los resultados medidos. Es el botón que aprueba C1.11. |
