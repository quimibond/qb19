<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `helpdesk.ticket`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_complaint.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_alert_id` | Many2one | No Conformidad | No conformidad que se generó desde esta reclamación con el botón «Generar NC». |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:70` |
| `sgi_disposition` | Selection | Disposición | Qué se hizo con el producto reclamado: devolución, reposición, nota de crédito o concesión al cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:61` |
| `sgi_is_complaint_team` | Boolean | Es reclamación (SGI) |  |  |  | related `team_id.sgi_is_complaint`, sin guardar |  | `addons/quimibond_sgi/models/sgi_complaint.py:50` |
| `sgi_lot_id` | Many2one | Lote | Lote del producto reclamado, para rastrear la producción de origen. |  | `stock.lot` |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:57` |
| `sgi_product_id` | Many2one | Producto | Producto que el cliente reclama. |  | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:55` |
| `sgi_qty_affected` | Float | Metros afectados | Metros de producto afectados según la reclamación. |  |  |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:59` |
| `sgi_sale_order_id` | Many2one | Pedido de venta | Pedido de venta del producto reclamado. |  | `sale.order` |  |  | `addons/quimibond_sgi/models/sgi_complaint.py:53` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_generate_nc` | — |
