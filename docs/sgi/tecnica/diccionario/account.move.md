<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `account.move`

Modelo de otra app que el SGI extiende.

S2.08: entregas que factura la factura. 57.95.0 (K-05): lo propuesto (entregas hechas de las líneas, sin guardar) va aparte de lo ajustado a mano (``sgi_picking_manual_ids``, guardado y no calculado, que solo llena ``write``). El cálculo d…

Archivos: `addons/quimibond_sgi/models/sgi_kpi_account.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_action_line_id` | Many2one | Acción de la reclamación | Causa y acción correctiva de la reclamación que pidió esta nota de crédito (C5.14). |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_links.py:195` |
| `sgi_payment_date` | Date | Fecha de pago | Fecha del último pago que dejó la factura pagada (S1-04: pagada en su vencimiento o antes). |  |  | compute `_compute_sgi_payment_date`, guardado |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:60` |
| `sgi_picking_ids` | Many2many | Entregas facturadas | Entregas (o recepciones) que esta factura cobra (S2.08). Se proponen desde las líneas del pedido; se pueden ajustar a mano. |  | `stock.picking` | compute `_compute_sgi_picking_ids`, guardado |  | `addons/quimibond_sgi/models/sgi_links.py:199` |
| `sgi_picking_manual` | Boolean | Entregas ajustadas a mano | Alguien cambió a mano las entregas facturadas: el sistema ya no las reemplaza con las propuestas. «Volver a las entregas propuestas» lo quita. |  |  |  |  | `addons/quimibond_sgi/models/sgi_links.py:209` |
| `sgi_picking_manual_ids` | Many2many | Entregas ajustadas a mano (guardadas) | Las entregas que alguien escribió a mano; las conserva aunque cambie lo propuesto. |  | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_links.py:213` |
| `sgi_picking_outdated` | Boolean | Entregas distintas de las propuestas | Las entregas guardadas no son las que el sistema propone hoy. |  |  | compute `_compute_sgi_picking_outdated`, sin guardar |  | `addons/quimibond_sgi/models/sgi_links.py:217` |
| `sgi_picking_proposed_ids` | Many2many | Entregas propuestas | Entregas hechas de las líneas de la factura: lo que el sistema propone. |  | `stock.picking` | compute `_compute_sgi_picking_proposed_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_links.py:205` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_picking_reset` | «Volver a las entregas propuestas»: quita el ajuste a mano y pone lo propuesto, aunque venga vacío (también quita lo que se conservaba sin propuesta). |
| `write` | — |
