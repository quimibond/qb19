<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `product.template`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_kpi_sales.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_sign_record.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_control_plan_id` | Many2one | Plan de control | Plan de control vigente del artículo (C1.17). Se propone el último plan que lo incluye; se puede cambiar a mano. |  | `sgi.control.plan` | compute `_compute_sgi_control_plan_id`, guardado |  | `addons/quimibond_sgi/models/sgi_links.py:82` |
| `sgi_first_sale_date` | Date | Primer pedido confirmado | Fecha del primer pedido de venta confirmado con este producto (C1-04). |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_sales.py:28` |
| `sgi_packaging_notes` | Text | Manejo y empaque (C14-02) | Indicaciones de manejo y empaque del producto (sustituye F-P-C14-02). Visible para inspección y almacén. |  |  |  |  | `addons/quimibond_sgi/models/sgi_integration.py:216` |
| `sgi_ppap_count` | Integer | PPAP |  |  |  | compute `_compute_sgi_ppap_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_ppap.py:200` |
| `sgi_spec_document_id` | Many2one | Especificación (C04-06) | Documento controlado con la especificación del material (sustituye F-P-C04-06). La inspección de recepción valida contra esta spec. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_integration.py:210` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_open_spec` | — |
| `action_view_sgi_ppap` | — |
