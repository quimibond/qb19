<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `purchase.order`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_sign_record.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_approval_request_id` | Many2one | Requisición aprobada | Solicitud de aprobación (requisición de compra) de la que salió esta orden (S1.09). Se llena sola al crear la orden desde la requisición. |  | `approval.request` |  |  | `addons/quimibond_sgi/models/sgi_links.py:145` |
| `sgi_nc_count` | Integer | # NC proveedor |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:129` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_open_ncs` | — |
| `button_confirm` | — |
