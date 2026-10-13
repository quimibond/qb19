<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.sample.wizard.line`

**Orden de la corrida de muestra** (TransientModel).

57.154.0: una orden de la corrida de muestra (crudo, teñido o acabado).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_sample.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `bom_id` | Many2one | Lista de materiales | Solo listas del propio artículo. |  | `mrp.bom` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:392` |
| `location_dest_id` | Many2one | A dónde entra |  |  | `stock.location` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:401` |
| `operation_count` | Integer | Operaciones de la ruta |  |  |  | compute `_compute_operation_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:396` |
| `picking_type_id` | Many2one | Tipo de operación |  |  | `stock.picking.type` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:399` |
| `product_id` | Many2one | Artículo |  | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:389` |
| `product_qty` | Float | Cantidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:397` |
| `product_tmpl_id` | Many2one |  |  |  |  | related `product_id.product_tmpl_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:390` |
| `product_uom_id` | Many2one | Unidad |  |  |  | related `product_id.uom_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:391` |
| `qty_note` | Char | Cómo se calculó |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:398` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:387` |
| `stage` | Selection | Etapa |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:388` |
| `wizard_id` | Many2one |  |  | sí | `sgi.dev.sample.wizard` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:386` |

