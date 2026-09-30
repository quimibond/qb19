<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.link`

**Encadenamiento entre actividades** (Model).

Liga entre dos actividades de procedimiento: qué ENTREGABLE pasa de un paso al siguiente. Puede cruzar procesos (el pedido de Ventas alimenta el programa de Planeación): es el hilo conductor de la operación al nivel de paso, no solo entre …

Orden: `from_activity_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_process_procedure.py`, `addons/quimibond_sgi/models/sgi_deliverable.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `atorado_since` | Datetime | Atorado desde |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1516` |
| `chain_state` | Selection | Flujo | Fluye: ambos pasos con evidencia en su periodo. Atorado: el paso origen tiene evidencia pero el destino no — el entregable entró y no salió. Lo calcula el cron de medición. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1504` |
| `company_id` | Many2one | Empresa |  |  |  | related `from_activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1497` |
| `from_activity_id` | Many2one | Actividad origen |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1481` |
| `from_process_id` | Many2one | Proceso origen |  |  |  | related `from_activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1491` |
| `input_id` | Many2one | Renglón «recibe» |  |  | `sgi.activity.input` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:527` |
| `is_cross_process` | Boolean | Cruza procesos |  |  |  | compute `_compute_cross`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1500` |
| `lag_days` | Float | Rezago (días) | Días que la última evidencia del paso destino va detrás de la del paso origen. 0 = el eslabón está al día. Si el «recibe» tiene plazo, son días hábiles desde que se entregó. |  |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1511` |
| `max_days` | Integer | Plazo (días hábiles) |  |  |  | related `input_id.max_days`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:530` |
| `name` | Char | Entregable / condición | Qué pasa de un paso al otro: el pedido confirmado, el programa semanal, el lote liberado… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1487` |
| `nc_alert_id` | Many2one | NC generada |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1517` |
| `to_activity_id` | Many2one | Actividad destino |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1484` |
| `to_process_id` | Many2one | Proceso destino |  |  |  | related `to_activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process_procedure.py:1494` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
