<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.measure`

**Medición de indicador SGI** (Model).

Medición de un indicador en un periodo: valor, semáforo, evidencia y, si sale en rojo, causa y plan. Se calcula sola o se captura; el dueño la valida.

Orden: `period_date desc, indicator_id`.

Archivos: `addons/quimibond_sgi/models/sgi_indicator.py`, `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_indicator_detail.py`, `addons/quimibond_sgi/models/sgi_indicator_formula.py`, `addons/quimibond_sgi/models/sgi_indicator_ind2.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`, `addons/quimibond_sgi/models/sgi_indicator_trajectory.py`, `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi_revisado/models/sgi_calidad_pq.py`.

## Campos (36)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:115` |
| `alert_id` | Many2one | No Conformidad |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:959` |
| `captured_date` | Date | Capturada el | Día en que la medición pasó a «Capturado» (a mano o por el cálculo automático). El dueño del indicador tiene 3 días hábiles desde aquí para validarla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:106` |
| `cause` | Text | Causa | Por qué salió en rojo (I-4). |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:114` |
| `denominator` | Float | Denominador |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:286` |
| `detail_count` | Integer | Registros |  |  |  | compute `_compute_detail_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:294` |
| `detail_ids` | Text | Ids de los registros |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:293` |
| `detail_model` | Char | Modelo de los registros |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:292` |
| `direction` | Selection |  |  |  |  | related `indicator_id.direction`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:944` |
| `indicator_id` | Many2one | Indicador |  | sí | `sgi.indicator` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:935` |
| `indicator_status` | Selection | Estado del indicador |  |  |  | related `indicator_id.status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:296` |
| `note` | Text | Nota |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:953` |
| `numerator` | Float | Numerador |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:285` |
| `parallel_denominator` | Float | Denominador (fórmula) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:562` |
| `parallel_numerator` | Float | Numerador (fórmula) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:561` |
| `parallel_value` | Float | Valor de la fórmula | Lo que daría la fórmula configurada del indicador en este periodo, mientras el indicador sigue en su modo de código. Cuando coincidan un mes, se migra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:556` |
| `period_date` | Date | Periodo | Día 1 del mes medido. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:941` |
| `plan_done` | Boolean | Plan capturado |  |  |  | compute `_compute_plan`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:120` |
| `plan_due` | Date | Plan antes del | Día 10 del mes siguiente al periodo (si es inhábil, el hábil anterior). |  |  | compute `_compute_plan`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:117` |
| `plan_required` | Boolean | Requiere plan |  |  |  | compute `_compute_plan`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:116` |
| `range_max` | Float |  |  |  |  | related `indicator_id.range_max`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:228` |
| `range_min` | Float |  |  |  |  | related `indicator_id.range_min`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:227` |
| `sample_size` | Integer | Casos | Registros que forman la medición. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:287` |
| `semaphore` | Selection | Semáforo |  |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator.py:948` |
| `sgi_nc_suppressed` | Boolean | NC omitida (fuente apagada) | La medición ameritaba NC pero la fuente «Indicador en semáforo rojo» estaba desactivada. Se reintenta sola en cuanto se reactive. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:960` |
| `small_sample` | Boolean | Muestra chica | Menos casos que el mínimo (quimibond_sgi.indicator_min_sample): se mide, pero no abre NC. |  |  | compute `_compute_small_sample`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:288` |
| `source_info` | Char | Fuente del dato |  |  |  | related `indicator_id.source_info`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:939` |
| `source_type` | Selection | Origen del dato |  |  |  | related `indicator_id.source_type`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:937` |
| `split_ids` | One2many | Desglose |  |  | `sgi.indicator.measure.split` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:418` |
| `state` | Selection |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:283` |
| `target_acceptable` | Float | Aceptable |  |  |  | related `None`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:226` |
| `target_objective` | Float | Objetivo |  |  |  | related `None`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:225` |
| `uom` | Char | Unidad |  |  |  | related `indicator_id.uom`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:947` |
| `value` | Float | Valor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:943` |
| `value_is_pct` | Boolean |  |  |  |  | compute `_compute_value_is_pct`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:295` |
| `window_label` | Char | Ventana |  |  |  | related `indicator_id.window_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:113` |

## Métodos públicos (8)

| Método | Qué hace (docstring) |
|---|---|
| `action_capture` | — |
| `action_recompute_value` | Recalcula valor y detalle con el modo actual (sustituye al recálculo de solo valor). Solo mediciones no validadas: la validada es evidencia. |
| `action_reset` | — |
| `action_validate` | Una medición sin dato no se valida: no hay nada que confirmar y validarla la convertiría en un cero rojo. |
| `action_view_evidence` | Los modos de «indicadores 2» no caben en un dominio de fecha: su evidencia son los registros guardados en la medición (o los del periodo, calculados ahora, si la medición no los guardó). |
| `action_view_records` | Abre los registros guardados en la medición (la lista que explica el valor), no una consulta nueva. |
| `create` | — |
| `write` | — |
