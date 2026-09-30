<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `account.move`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_kpi_account.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_payment_date` | Date | Fecha de pago | Fecha del último pago que dejó la factura pagada (S1-04: pagada en su vencimiento o antes). |  |  | compute `_compute_sgi_payment_date`, guardado |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:60` |
| `sgi_picking_ids` | Many2many | Entregas facturadas | Entregas (o recepciones) que esta factura cobra (S2.08). Se proponen desde las líneas del pedido; se pueden ajustar a mano. |  | `stock.picking` | compute `_compute_sgi_picking_ids`, guardado |  | `addons/quimibond_sgi/models/sgi_links.py:167` |

