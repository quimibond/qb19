<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.format.map`

**Formato SGI en documentos de Odoo** (Model).

Mapeo formato SGI ↔ documento de Odoo que lo sustituye.

Orden: `model_name, sequence, sgi_code, id`.

Archivos: `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_format_map_seed.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:148` |
| `document_alt_id` | Many2one | Documento alternativo | Formato que aplica cuando el registro está confirmado (solo ventas: cotización vs pedido; presupuesto vs pronóstico). |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:133` |
| `document_id` | Many2one | Documento controlado | Formato controlado que se imprime. La clave y la revisión salen de su revisión vigente, en vivo: si cambia la clave o sube la revisión, el pie cambia solo. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:127` |
| `is_general` | Boolean | General del modelo | Sin criterio: aplica a todo registro del modelo que no cumpla el criterio de otro mapeo. |  |  | compute `_compute_is_general`, guardado |  | `addons/quimibond_sgi/models/sgi_format_map.py:186` |
| `live_alt_label` | Char | Alternativa vigente |  |  |  | compute `_compute_live_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_format_map.py:147` |
| `live_label` | Char | Clave y revisión vigentes |  |  |  | compute `_compute_live_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_format_map.py:146` |
| `model_id` | Many2one | Modelo de Odoo | El documento de Odoo que sustituye al formato en Excel. Vacío en los formatos que el SGI usa por referencia desde un reporte (etiquetas, responsivas, pies). |  | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:121` |
| `model_name` | Char | Modelo técnico |  |  |  | related `model_id.model`, guardado |  | `addons/quimibond_sgi/models/sgi_format_map.py:126` |
| `note` | Char | Nota |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:149` |
| `picking_type_ids` | Many2many | Tipos de operación | Solo los registros de estos tipos de operación (transferencias, vales, órdenes de producción) imprimen este formato. Vacío = cualquier tipo. |  | `stock.picking.type` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:163` |
| `product_categ_ids` | Many2many | Categorías de producto | Solo los registros cuyo producto es de estas categorías (o de sus subcategorías) imprimen este formato. Vacío = cualquier producto. |  | `product.category` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:176` |
| `record_domain` | Char | Filtro adicional | Condición opcional sobre el registro, por ejemplo [('location_dest_id.usage', '=', 'supplier')] para las devoluciones a proveedor. Vacío = sin filtro. |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:181` |
| `selector_label` | Char | Cuándo aplica |  |  |  | compute `_compute_selector_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_format_map.py:190` |
| `sequence` | Integer | Prioridad | Si un registro cumple el criterio de más de un mapeo del mismo modelo, se usa el de número más bajo. El mapeo general (sin criterio) se usa solo cuando ningún otro aplica. |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:154` |
| `sgi_code` | Char | Clave al ligar | Clave con la que se sembró el mapeo (ej. F-P-A28-04). Solo sirve mientras no hay documento ligado; lo impreso sale del documento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:138` |
| `sgi_code_alt` | Char | Clave alternativa al ligar | Clave que aplica cuando el registro está confirmado (solo ventas: cotización vs pedido). Vacío = siempre la clave principal. |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:142` |
| `workcenter_ids` | Many2many | Centros de trabajo | Solo las órdenes con una operación en alguno de estos centros de trabajo imprimen este formato. Vacío = cualquier centro. |  | `mrp.workcenter` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:170` |

## Métodos públicos (9)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `sgi_footer_info` | 57.98.0 (I-01): {'label': 'F-P-A10-01 · Rev. 00', 'issue_date': date \| False} del pie «formato controlado»; False sin mapeo. La fecha de emisión es la del documento vigente ligado (sin fecha, el pie … |
| `sgi_footer_label` | V-M08 (57.44.0): clave y revisión del pie «formato controlado» de un reporte (``report/sgi_format_footer.xml``). Con ``ref``, el mapeo por referencia (``format_ref_*``); si no, el del modelo del regi… |
| `sgi_live_document` | Documento vigente que se imprime con este mapeo (vacío si no hay). |
| `sgi_live_label` | 'F-P-A28-12 · Rev. 03' \| 'F-P-A28-12' (sin vigente) \| False. |
| `sgi_live_parts` | (clave, revisión) vivas; revisión False si no hay vigente, y (False, False) si el mapeo no existe. |
| `sgi_ref_document` | — |
| `sgi_ref_label` | — |
| `sgi_ref_parts` | — |
