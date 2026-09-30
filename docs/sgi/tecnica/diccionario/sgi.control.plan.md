<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.control.plan`

**Plan de control (P-C11)** (Model). Hereda de: `sgi.base.mixin`.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_control_plan.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `document_id` | Many2one | Especificación del cliente |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:88` |
| `fmea_count` | Integer | # AMEF |  |  |  | compute `_compute_fmea_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_control_plan.py:87` |
| `fmea_ids` | One2many | AMEF ligados |  |  | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:86` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:68` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:89` |
| `partner_id` | Many2one | Cliente |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:69` |
| `phase` | Selection | Fase |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:72` |
| `point_count` | Integer | N° de puntos |  |  |  | compute `_compute_point_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_control_plan.py:85` |
| `point_ids` | One2many | Puntos de control |  |  | `quality.point` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:83` |
| `product_tmpl_ids` | Many2many | Productos |  |  | `product.template` |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:71` |
| `revision` | Char | Revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:77` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_control_plan.py:78` |

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
