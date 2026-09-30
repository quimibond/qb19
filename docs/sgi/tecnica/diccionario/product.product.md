<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `product.product`

Modelo de otra app que el SGI extiende.

El formulario de variante hereda la vista de la plantilla (mismo caso documentado en el smart button de PPAP): el botón de la spec también debe resolver en product.product.

Archivos: `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_sign_record.py`.

## Campos (3)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_can_sign` | Boolean |  |  |  |  | related `product_tmpl_id.sgi_can_sign`, sin guardar |  | `addons/quimibond_sgi/models/sgi_sign_record.py:111` |
| `sgi_ppap_count` | Integer | PPAP |  |  |  | compute `_compute_sgi_ppap_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_ppap.py:246` |
| `sgi_sign_count` | Integer |  |  |  |  | related `product_tmpl_id.sgi_sign_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_sign_record.py:110` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_open_spec` | — |
| `action_sgi_sign` | — |
| `action_sgi_view_sign_requests` | — |
| `action_view_sgi_ppap` | — |
