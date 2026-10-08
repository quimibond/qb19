<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.shipment.roll`

**Rollo avisado al cliente en un envío de muestra** (Model).

Orden: `shipment_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_shipment.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `length_m` | Float | Metros |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:433` |
| `lot_id` | Many2one | Lote | Lote del rollo en la ubicación de desarrollos. |  | `stock.lot` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:431` |
| `product_id` | Many2one |  |  |  |  | related `shipment_id.product_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:430` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:429` |
| `shipment_id` | Many2one | Envío |  | sí | `sgi.dev.shipment` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:428` |
| `weight_kg` | Float | Kilos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:435` |
| `width_m` | Float | Ancho (m) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:434` |

