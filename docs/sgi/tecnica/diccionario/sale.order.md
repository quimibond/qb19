<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sale.order`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_kpi_sales.py`, `addons/quimibond_sgi/models/sgi_sale_commitment.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `commitment_date` | Datetime |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_sale_commitment.py:17` |
| `sgi_coa_attachment_count` | Integer | # CoA |  |  |  | compute `_compute_sgi_coa_attachment_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_coa.py:221` |
| `sgi_coa_status` | Selection | CoA | El peor estado del CoA de sus salidas que lo requieren. |  |  | compute `_compute_sgi_coa_status`, guardado |  | `addons/quimibond_sgi/models/sgi_coa.py:218` |
| `sgi_commitment_set_at` | Datetime | Fecha compromiso registrada el | Cuándo se registró por primera vez la fecha compromiso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_sale_commitment.py:18` |
| `sgi_commitment_set_uid` | Many2one | Fecha compromiso registrada por | Quién registró por primera vez la fecha compromiso. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_sale_commitment.py:21` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
| `action_sgi_view_coa` | — |
| `create` | — |
| `write` | — |
