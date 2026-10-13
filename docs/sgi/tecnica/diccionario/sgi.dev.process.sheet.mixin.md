<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.process.sheet.mixin`

**Control del desarrollo sobre una ficha de proceso** (AbstractModel).

Capa de control del desarrollo sobre una ficha técnica de proceso (tejido, acabado).

Archivos: `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `production_id` | Many2one | Orden de la corrida | Orden de muestra con la que se corrieron los valores reales. |  | `mrp.production` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:96` |
| `project_id` | Many2one | Proyecto de desarrollo | Desarrollo de producto al que pertenece esta ficha. |  | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:93` |
| `sgi_measured_by_id` | Many2one | Midió (Laboratorio) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:103` |
| `sgi_measured_date` | Datetime | Medida el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:104` |
| `sgi_proposed_by_id` | Many2one | Propuso (Diseño y Desarrollo de Procesos) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:98` |
| `sgi_proposed_date` | Datetime | Propuesta el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:100` |
| `sgi_validated_by_id` | Many2one | Validó (Jefe de Manufactura) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:101` |
| `sgi_validated_date` | Datetime | Validada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:102` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_load_readings` | Un renglón por parámetro de la ficha que todavía no tiene el suyo. |
| `action_sgi_measure` | — |
| `action_sgi_propose` | — |
| `action_sgi_validate` | — |
| `create` | — |
