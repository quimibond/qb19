<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.control.plan`

**Plan de control (P-C11)** (Model). Hereda de: `sgi.base.mixin`.

Plan de control (P-C11): puntos de control de calidad por producto y fase, con su AMEF. Estados borrador, vigente y obsoleto.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_control_plan.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `document_id` | Many2one | Especificación del cliente | Especificación del cliente en la que se basa el plan. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:100` |
| `fmea_count` | Integer | # AMEF |  |  |  | compute `_compute_fmea_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_control_plan.py:99` |
| `fmea_ids` | One2many | AMEF ligados |  |  | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:98` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:76` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:102` |
| `partner_id` | Many2one | Cliente | Cliente para el que se hace el plan de control. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:77` |
| `phase` | Selection | Fase | Fase del producto: prototipo, prelanzamiento o producción. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:82` |
| `point_count` | Integer | N° de puntos |  |  |  | compute `_compute_point_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_control_plan.py:97` |
| `point_ids` | One2many | Puntos de control |  |  | `quality.point` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:95` |
| `product_tmpl_ids` | Many2many | Productos | Productos que cubre el plan. |  | `product.template` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:80` |
| `revision` | Char | Revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:88` |
| `state` | Selection | Estado | Borrador mientras se arma; vigente cuando aplica; obsoleto cuando se sustituye. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:89` |

## Métodos públicos (8)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_orphan_points` | Puntos de calidad reales del piso que no pertenecen a ningún plan. |
| `action_set_borrador` | — |
| `action_set_obsoleto` | — |
| `action_set_vigente` | — |
| `action_view_fmeas` | — |
| `action_view_points` | — |
| `create` | — |
| `write` | — |
