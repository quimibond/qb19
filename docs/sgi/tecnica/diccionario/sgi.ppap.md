<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.ppap`

**PPAP - Proceso de Aprobación de Partes de Producción (P-C15)** (Model). Hereda de: `sgi.base.mixin`.

Expediente PPAP (P-C15) de un producto para un cliente, con sus elementos y la decisión del cliente.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_decision` | Date | Fecha de decisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:70` |
| `date_submitted` | Date | Fecha de envío |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:69` |
| `element_ids` | One2many | Elementos |  |  | `sgi.ppap.element` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:71` |
| `level` | Selection | Nivel |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:48` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:72` |
| `partner_id` | Many2one | Cliente |  | sí | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:44` |
| `product_tmpl_id` | Many2one | Producto |  | sí | `product.template` |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:46` |
| `reason` | Selection | Motivo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:55` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:62` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_approve` | — |
| `action_mark_enviado` | — |
| `action_reject` | — |
| `action_reset` | — |
| `action_set_interino` | — |
| `create` | — |
| `write` | — |
