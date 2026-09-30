<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `project.task`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_improvement.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_area_id` | Many2one | Área SGI |  |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_improvement.py:28` |
| `sgi_bom_ids` | One2many | Listas de materiales |  |  | `mrp.bom` |  |  | `addons/quimibond_sgi/models/sgi_links.py:65` |
| `sgi_control_plan_ids` | One2many | Planes de control |  |  | `sgi.control.plan` |  |  | `addons/quimibond_sgi/models/sgi_links.py:67` |
| `sgi_dyd_link_count` | Integer | Ligas del desarrollo |  |  |  | compute `_compute_sgi_dyd_link_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_links.py:69` |
| `sgi_fmea_ids` | One2many | AMEF |  |  | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_links.py:64` |
| `sgi_improvement_type` | Selection | Tipo de mejora |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_improvement.py:22` |
| `sgi_is_improvement` | Boolean |  |  |  |  | related `project_id.sgi_is_improvement`, guardado |  | `addons/quimibond_sgi/models/sgi_improvement.py:21` |
| `sgi_ppap_ids` | One2many | PPAP |  |  | `sgi.ppap` |  |  | `addons/quimibond_sgi/models/sgi_links.py:68` |
| `sgi_process_id` | Many2one | Proceso SGI |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_improvement.py:29` |
| `sgi_production_ids` | One2many | Órdenes de muestra |  |  | `mrp.production` |  |  | `addons/quimibond_sgi/models/sgi_links.py:66` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `write` | — |
