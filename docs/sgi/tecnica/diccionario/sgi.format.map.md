<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.format.map`

**Formato SGI en documentos de Odoo** (Model).

Mapeo formato SGI ↔ documento de Odoo que lo sustituye.

Orden: `sgi_code`.

Archivos: `addons/quimibond_sgi/models/sgi_format_map.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:62` |
| `document_alt_id` | Many2one | Documento alternativo | Formato que aplica cuando el registro está confirmado (solo ventas: cotización vs pedido; presupuesto vs pronóstico). |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:47` |
| `document_id` | Many2one | Documento controlado | Formato controlado que se imprime. La clave y la revisión salen de su revisión vigente, en vivo: si cambia la clave o sube la revisión, el pie cambia solo. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:41` |
| `live_alt_label` | Char | Alternativa vigente |  |  |  | compute `_compute_live_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_format_map.py:61` |
| `live_label` | Char | Clave y revisión vigentes |  |  |  | compute `_compute_live_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_format_map.py:60` |
| `model_id` | Many2one | Modelo de Odoo | El documento de Odoo que sustituye al formato en Excel. Vacío en los formatos que el SGI usa por referencia desde un reporte (etiquetas, responsivas, pies). |  | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:35` |
| `model_name` | Char | Modelo técnico |  |  |  | related `model_id.model`, guardado |  | `addons/quimibond_sgi/models/sgi_format_map.py:40` |
| `note` | Char | Nota |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:63` |
| `sgi_code` | Char | Clave al ligar | Clave con la que se sembró el mapeo (ej. F-P-A28-04). Solo sirve mientras no hay documento ligado; lo impreso sale del documento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:52` |
| `sgi_code_alt` | Char | Clave alternativa al ligar | Clave que aplica cuando el registro está confirmado (solo ventas: cotización vs pedido). Vacío = siempre la clave principal. |  |  |  |  | `addons/quimibond_sgi/models/sgi_format_map.py:56` |

## Métodos públicos (8)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `sgi_footer_label` | V-M08 (57.44.0): clave y revisión del pie «formato controlado» de un reporte (``report/sgi_format_footer.xml``). Con ``ref``, el mapeo por referencia (``format_ref_*``); si no, el del modelo del regi… |
| `sgi_live_document` | Documento vigente que se imprime con este mapeo (vacío si no hay). |
| `sgi_live_label` | 'F-P-A28-12 · Rev. 03' \| 'F-P-A28-12' (sin vigente) \| False. |
| `sgi_live_parts` | (clave, revisión) vivas; revisión False si no hay vigente, y (False, False) si el mapeo no existe. |
| `sgi_ref_document` | — |
| `sgi_ref_label` | — |
| `sgi_ref_parts` | — |
