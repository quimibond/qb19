<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.coa`

**Reporte de conformidad (CoA) del desarrollo** (Model). Hereda de: `mail.thread`.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_coa.py`.

## Campos (19)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_id` | Many2one | PDF emitido |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:62` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:38` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:49` |
| `issued_at` | Datetime | Emitido el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:61` |
| `issued_by_id` | Many2one | Emitió (Calidad) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:60` |
| `line_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:56` |
| `line_ids` | One2many | Renglones |  |  | `sgi.dev.coa.line` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:55` |
| `lot_id` | Many2one | Lote | Lote certificado. Vacío si los rollos del envío no llevan lote. |  | `stock.lot` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:44` |
| `name` | Char | Certificado |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:33` |
| `nonconforming_count` | Integer | No conformes |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:59` |
| `partner_id` | Many2one | Cliente |  |  |  | related `project_id.partner_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:37` |
| `pending_count` | Integer | Sin resultado | Renglones del certificado sin valor obtenido. |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:57` |
| `product_id` | Many2one | Artículo |  |  | `product.product` | compute `_compute_product_id`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:47` |
| `production_id` | Many2one | Orden de muestra |  |  | `mrp.production` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:42` |
| `project_id` | Many2one | Desarrollo | Proyecto cuya tabla de características alimenta el certificado. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:34` |
| `quantity_m` | Float | Metros certificados | Metros del lote que ampara el certificado (de los rollos del envío). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:50` |
| `roll_count` | Integer | Rollos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:52` |
| `shipment_id` | Many2one | Envío de muestra | Envío al que acompaña el certificado; el PDF se adjunta ahí al emitir. |  | `sgi.dev.shipment` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:39` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:53` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_emit` | — |
| `action_load_lines` | Carga los renglones «En certificado» de la tabla que aún no están, con el valor de la corrida. |
| `action_open_lines` | — |
| `action_print` | — |
| `create` | — |
| `unlink` | — |
