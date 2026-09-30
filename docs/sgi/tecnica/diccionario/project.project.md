<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `project.project`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_dev_request.py`, `addons/quimibond_sgi/models/sgi_improvement.py`.

## Campos (21)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_dev_approved_by_id` | Many2one | Aprobó (Dirección de Operaciones) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:88` |
| `sgi_dev_currency_id` | Many2one | Moneda del precio |  |  | `res.currency` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:77` |
| `sgi_dev_customer_property` | Selection | Propiedad del cliente recibida |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:81` |
| `sgi_dev_date` | Date | Fecha de solicitud |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:68` |
| `sgi_dev_format_code` | Char | Formato |  |  |  | compute `_compute_sgi_dev_format_code`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_request.py:67` |
| `sgi_dev_line_ids` | One2many | Características del producto |  |  | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:86` |
| `sgi_dev_norms` | Char | Norma(s) a cumplir |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:79` |
| `sgi_dev_other` | Text | Otras características |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:85` |
| `sgi_dev_packaging` | Text | Datos en la etiqueta y empaque |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:80` |
| `sgi_dev_prepared_by_id` | Many2one | Elaboró (Diseño y Desarrollo) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:87` |
| `sgi_dev_requester` | Char | Nombre del solicitante |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:69` |
| `sgi_dev_sample_kg` | Float | Cantidad de la muestra (kg) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:73` |
| `sgi_dev_sample_m` | Float | Cantidad de la muestra (m) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:72` |
| `sgi_dev_spec` | Text | Especificación del cliente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:71` |
| `sgi_dev_target_price` | Monetary | Precio objetivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:76` |
| `sgi_dev_type` | Selection | Tipo de desarrollo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:66` |
| `sgi_dev_use` | Text | Descripción y uso del producto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:70` |
| `sgi_dev_volume` | Float | Volumen estimado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:74` |
| `sgi_dev_volume_uom` | Selection | Unidad del volumen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:75` |
| `sgi_is_ft` | Boolean | Proyecto FT (desarrollo) | Se marca solo cuando el nombre empieza con «FT-». Habilita la pestaña Solicitud de desarrollo. |  |  | compute `_compute_sgi_is_ft`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:63` |
| `sgi_is_improvement` | Boolean | Proyecto de mejora SGI |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_improvement.py:9` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_dev_load_lines` | Propone las características del tipo (solo agrega las que faltan). |
| `action_sgi_dev_print` | — |
| `sgi_dev_format_info` | 'F-P-D01-18 · Rev. 02' para el pie del PDF (clave y revisión vivas del documento ligado al mapeo). |
