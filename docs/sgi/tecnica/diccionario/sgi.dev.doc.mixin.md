<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.doc.mixin`

**Documento del desarrollo impreso desde la tabla** (AbstractModel).

Lo común a los dos documentos: proyecto, artículo, revisión, estado y PDF emitido.

Archivos: `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_id` | Many2one | PDF emitido |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:64` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:59` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:62` |
| `line_count` | Integer |  |  |  |  | compute `_compute_line_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:65` |
| `partner_id` | Many2one | Cliente |  |  |  | related `project_id.partner_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:56` |
| `product_id` | Many2one | Artículo |  |  | `product.product` | compute `_compute_product_id`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:57` |
| `project_id` | Many2one | Desarrollo |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:54` |
| `revision` | Integer | Revisión del desarrollo | Revisión del proyecto con la que se tomó la tabla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:60` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:63` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_load_lines` | Toma (o retoma) los renglones de la tabla del proyecto: el documento en borrador refleja la tabla de hoy; emitido ya no cambia. |
| `action_print` | — |
| `create` | — |
| `unlink` | — |
