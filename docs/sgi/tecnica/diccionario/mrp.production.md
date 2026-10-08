<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `mrp.production`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_dev_pilot.py`, `addons/quimibond_sgi/models/sgi_dev_product.py`, `addons/quimibond_sgi/models/sgi_dev_sample.py`, `addons/quimibond_sgi/models/sgi_dev_start.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_dev_issued_by_id` | Many2one | Orden emitida por (Planeación) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:269` |
| `sgi_dev_issued_date` | Datetime | Orden emitida el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:270` |
| `sgi_dev_project_id` | Many2one | Proyecto de desarrollo | Desarrollo cuya muestra fabrica esta orden; el sobrante queda ligado a él. |  | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:262` |
| `sgi_dev_run_validated_by_id` | Many2one | Corrida validada por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:271` |
| `sgi_dev_run_validated_date` | Datetime | Corrida validada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:272` |
| `sgi_dev_task_id` | Many2one | Tarea de la corrida |  |  | `project.task` |  |  | `addons/quimibond_sgi/models/sgi_dev_sample.py:265` |
| `sgi_nc_count` | Integer | # NC |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:107` |
| `sgi_release_picking_ids` | One2many | Traspasos a liberación | Traspasos (a liberación u otros) que salieron de esta orden (C4.19). |  | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_links.py:62` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
| `action_sgi_dev_validate_run` | Validación de la corrida de muestra por el supervisor del área (C1.10). La aprobación por puesto la pone la regla de Studio del SGI sobre este botón. |
| `action_sgi_open_ncs` | — |
| `create` | — |
| `write` | — |
