<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.link`

**Encadenamiento entre actividades** (Model).

Liga entre dos actividades de procedimiento: qué ENTREGABLE pasa de un paso al siguiente. Puede cruzar procesos (el pedido de Ventas alimenta el programa de Planeación): es el hilo conductor de la operación al nivel de paso, no solo entre …

Orden: `from_activity_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_process_procedure.py`, `addons/quimibond_sgi/models/sgi_deliverable.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `atorado_since` | Datetime | Atorado desde |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1558` |
| `chain_state` | Selection | Flujo | Fluye: ambos pasos con evidencia en su periodo. Atorado: el paso origen tiene evidencia pero el destino no — el entregable entró y no salió. Lo calcula el cron de medición. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1546` |
| `company_id` | Many2one | Empresa |  |  |  | related `from_activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1538` |
| `from_activity_id` | Many2one | Actividad origen | Actividad que entrega. | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1518` |
| `from_process_id` | Many2one | Proceso origen | Proceso de la actividad que entrega. |  |  | related `from_activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1530` |
| `input_id` | Many2one | Renglón «recibe» |  |  | `sgi.activity.input` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:530` |
| `is_cross_process` | Boolean | Cruza procesos | Se marca sola cuando el entregable pasa de un proceso a otro. |  |  | compute `_compute_cross`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1541` |
| `lag_days` | Float | Rezago (días) | Días que la última evidencia del paso destino va detrás de la del paso origen. 0 = el eslabón está al día. Si el «recibe» tiene plazo, son días hábiles desde que se entregó. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1553` |
| `max_days` | Integer | Plazo (días hábiles) | Días hábiles que tiene el entregable para llegar a la actividad que lo recibe. Pasado ese plazo, el eslabón se ve atorado. |  |  | related `input_id.max_days`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:533` |
| `name` | Char | Entregable / condición | Qué pasa de un paso al otro: el pedido confirmado, el programa semanal, el lote liberado… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1526` |
| `nc_alert_id` | Many2one | NC generada | No conformidad que se levantó por este eslabón atorado. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1559` |
| `to_activity_id` | Many2one | Actividad destino | Actividad que recibe. | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1522` |
| `to_process_id` | Many2one | Proceso destino | Proceso de la actividad que recibe. |  |  | related `to_activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1534` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
