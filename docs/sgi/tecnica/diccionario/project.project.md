<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `project.project`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_dev_characteristic.py`, `addons/quimibond_sgi/models/sgi_dev_request.py`, `addons/quimibond_sgi/models/sgi_improvement.py`.

## Campos (25)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_dev_approved_by_id` | Many2one | Aprobó (Dirección de Operaciones) | Persona de Dirección de Operaciones que aprueba la solicitud de desarrollo. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:73` |
| `sgi_dev_currency_id` | Many2one | Moneda del precio | Moneda del precio objetivo del desarrollo. |  | `res.currency` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:46` |
| `sgi_dev_customer_property` | Selection | Propiedad del cliente recibida | Qué entregó el cliente para el desarrollo (muestra, especificación, ambas o nada). Es propiedad del cliente y se resguarda. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:51` |
| `sgi_dev_date` | Date | Fecha de solicitud | Fecha en que se recibió la solicitud de desarrollo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:31` |
| `sgi_dev_deviation_count` | Integer | Fuera del control interno | Renglones cuya corrida cumple al cliente pero sale del margen interno: se embarca con aviso a Calidad y a Diseño de Procesos. |  |  | compute `_compute_sgi_dev_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:64` |
| `sgi_dev_format_code` | Char | Formato |  |  |  | compute `_compute_sgi_dev_format_code`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_request.py:30` |
| `sgi_dev_lab_pending_count` | Integer | Pendientes de laboratorio | Renglones marcados para medir en la muestra del cliente que aún no tienen resultado. |  |  | compute `_compute_sgi_dev_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:68` |
| `sgi_dev_line_count` | Integer | Características | Renglones de la tabla de características del proyecto. |  |  | compute `_compute_sgi_dev_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:59` |
| `sgi_dev_line_ids` | One2many | Características del producto |  |  | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:58` |
| `sgi_dev_norms` | Char | Norma(s) a cumplir |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:49` |
| `sgi_dev_other` | Text | Otras características |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:57` |
| `sgi_dev_out_of_spec_count` | Integer | No conformes en corrida | Renglones cuya corrida quedó fuera de lo que pide el cliente. |  |  | compute `_compute_sgi_dev_counts`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:61` |
| `sgi_dev_packaging` | Text | Datos en la etiqueta y empaque |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:50` |
| `sgi_dev_prepared_by_id` | Many2one | Elaboró (Diseño y Desarrollo) | Persona de Diseño y Desarrollo que elaboró la solicitud. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:71` |
| `sgi_dev_requester` | Char | Nombre del solicitante |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:33` |
| `sgi_dev_sample_kg` | Float | Cantidad de la muestra (kg) | Cantidad de muestra pedida, en kilogramos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:38` |
| `sgi_dev_sample_m` | Float | Cantidad de la muestra (m) | Cantidad de muestra pedida, en metros. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:36` |
| `sgi_dev_spec` | Text | Especificación del cliente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:35` |
| `sgi_dev_target_price` | Monetary | Precio objetivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:45` |
| `sgi_dev_type` | Selection | Tipo de desarrollo | Tipo de desarrollo que se solicita; define qué datos pide la solicitud. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:27` |
| `sgi_dev_use` | Text | Descripción y uso del producto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:34` |
| `sgi_dev_volume` | Float | Volumen estimado | Volumen mensual que el cliente estima comprar si el desarrollo se aprueba. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:40` |
| `sgi_dev_volume_uom` | Selection | Unidad del volumen | Unidad del volumen estimado: metros o kilogramos por mes. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_request.py:43` |
| `sgi_is_ft` | Boolean | Proyecto FT (desarrollo) | Se marca solo cuando el nombre empieza con «FT-». Habilita la pestaña Solicitud de desarrollo. |  |  | compute `_compute_sgi_is_ft`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_request.py:24` |
| `sgi_is_improvement` | Boolean | Proyecto de mejora SGI |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_improvement.py:9` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_dev_load_lines` | Propone las características del tipo desde el catálogo (``sgi.dev.characteristic.template``); solo agrega las que faltan. |
| `action_sgi_dev_print` | — |
| `sgi_dev_format_info` | 'F-P-D01-18 · Rev. 02' para el pie del PDF (clave y revisión vivas del documento ligado al mapeo). |
