<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `mrp.production`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_dev_product.py`, `addons/quimibond_sgi/models/sgi_dev_start.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_nc_count` | Integer | # NC |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:107` |
| `sgi_release_picking_ids` | One2many | Traspasos a liberación | Traspasos (a liberación u otros) que salieron de esta orden (C4.19). |  | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_links.py:62` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
| `action_sgi_open_ncs` | — |
| `create` | — |
