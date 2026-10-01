<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `helpdesk.ticket`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_complaint.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_alert_id` | Many2one | No Conformidad | No conformidad que se generó desde esta reclamación con el botón «Generar NC». |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:94` |
| `sgi_disposition` | Selection | Disposición | Qué se hizo con el producto reclamado: devolución, reposición, nota de crédito o concesión al cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:85` |
| `sgi_is_complaint_team` | Boolean | Es reclamación (SGI) |  |  |  | related `team_id.sgi_is_complaint`, sin guardar |  | `addons/quimibond_sgi/models/sgi_complaint.py:74` |
| `sgi_lot_id` | Many2one | Lote | Lote del producto reclamado, para rastrear la producción de origen. |  | `stock.lot` |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:81` |
| `sgi_product_id` | Many2one | Producto | Producto que el cliente reclama. |  | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:79` |
| `sgi_qty_affected` | Float | Metros afectados | Metros de producto afectados según la reclamación. |  |  |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:83` |
| `sgi_sale_order_id` | Many2one | Pedido de venta | Pedido de venta del producto reclamado. |  | `sale.order` |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:77` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_generate_nc` | — |
