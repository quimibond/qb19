<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.sample.wizard.line`

**Orden de la corrida de muestra** (TransientModel).

57.154.0: una orden de la corrida de muestra (crudo, teñido o acabado).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_sample.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `bom_id` | Many2one | Lista de materiales | Solo listas del propio artículo. |  | `mrp.bom` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:456` |
| `location_dest_id` | Many2one | A dónde entra |  |  | `stock.location` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:465` |
| `operation_count` | Integer | Operaciones de la ruta |  |  |  | compute `_compute_operation_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:460` |
| `picking_type_id` | Many2one | Tipo de operación |  |  | `stock.picking.type` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:463` |
| `product_id` | Many2one | Artículo |  | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:453` |
| `product_qty` | Float | Cantidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:461` |
| `product_tmpl_id` | Many2one |  |  |  |  | related `product_id.product_tmpl_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:454` |
| `product_uom_id` | Many2one | Unidad |  |  |  | related `product_id.uom_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:455` |
| `qty_note` | Char | Cómo se calculó |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:462` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:451` |
| `stage` | Selection | Etapa |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:452` |
| `wizard_id` | Many2one |  |  | sí | `sgi.dev.sample.wizard` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:450` |

