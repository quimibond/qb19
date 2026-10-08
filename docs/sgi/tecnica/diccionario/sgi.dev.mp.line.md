<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.mp.line`

**Materia prima de la muestra del desarrollo** (Model).

Materia prima que pide la muestra: lo que necesita la lista de materiales contra lo que hay.

Orden: `qty_missing desc, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_start.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `checked_at` | Datetime | Revisado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:67` |
| `product_id` | Many2one | Materia prima | Hoja de la lista de materiales (lo que se compra). | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:59` |
| `project_id` | Many2one |  |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:58` |
| `qty_available` | Float | Disponible | Existencia libre en los almacenes de la compañía al revisar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:64` |
| `qty_missing` | Float | Falta |  |  |  | compute `_compute_qty_missing`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_start.py:66` |
| `qty_needed` | Float | Necesaria | Lo que consume la muestra según la lista de materiales. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:62` |
| `requisition_id` | Many2one | Requisición | Requisición a Compras que pidió lo que falta. |  | `approval.request` |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:68` |
| `uom_id` | Many2one | Unidad |  | sí | `uom.uom` |  |  | `addons/quimibond_sgi/models/sgi_dev_start.py:61` |

