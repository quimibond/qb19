<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `account.move.line`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_kpi_account.py`.

## Campos (1)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_po_price_diff` | Monetary | Diferencia vs orden de compra | (precio facturado − precio de la orden de compra) × cantidad (S1-05). |  |  | compute `_compute_sgi_po_price_diff`, guardado | account.group_account_readonly | `addons/quimibond_sgi/models/sgi_kpi_account.py:41` |

