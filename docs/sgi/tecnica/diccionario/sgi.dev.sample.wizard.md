<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.sample.wizard`

**Pedir la corrida de muestra del desarrollo** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_dev_sample.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `bom_id` | Many2one | Lista de materiales |  |  | `mrp.bom` | compute `_compute_bom_id`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:157` |
| `date_planned` | Datetime | Fecha deseada de máquina |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:162` |
| `location_dest_id` | Many2one | A dónde entra el sobrante |  | sí | `stock.location` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:166` |
| `note` | Text | Nota para Planeación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:168` |
| `picking_type_id` | Many2one | Tipo de operación |  | sí | `stock.picking.type` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:164` |
| `product_id` | Many2one | Artículo a fabricar | Crudo, teñido o acabado del proyecto. Por omisión el crudo: la corrida arranca en Tejido Desarrollo. | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:152` |
| `product_ids` | Many2many |  |  |  | `product.product` | compute `_compute_product_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:151` |
| `product_qty` | Float | Cantidad a fabricar |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:161` |
| `product_uom_id` | Many2one |  |  |  |  | related `product_id.uom_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:156` |
| `project_id` | Many2one |  |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:150` |
| `suggested_qty` | Float | Cantidad sugerida |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:159` |
| `suggestion_note` | Text | Por qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:160` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
| `default_get` | — |
