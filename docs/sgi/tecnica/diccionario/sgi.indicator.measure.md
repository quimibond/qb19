<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.measure`

**Medición de indicador SGI** (Model). Hereda de: `mail.thread`.

Medición de un indicador en un periodo: valor, semáforo, evidencia y, si sale en rojo, causa y plan. Se calcula sola o se captura; el dueño la valida.

Orden: `period_date desc, indicator_id`.

Archivos: `addons/quimibond_sgi/models/sgi_indicator.py`, `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_indicator_detail.py`, `addons/quimibond_sgi/models/sgi_indicator_formula.py`, `addons/quimibond_sgi/models/sgi_indicator_health.py`, `addons/quimibond_sgi/models/sgi_indicator_ind2.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`, `addons/quimibond_sgi/models/sgi_indicator_trajectory.py`, `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi_revisado/models/sgi_calidad_pq.py`.

## Campos (38)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:127` |
| `alert_id` | Many2one | No conformidad | No conformidad levantada por esta medición en rojo. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:1052` |
| `captured_date` | Date | Capturada el | Día en que la medición pasó a «Capturado» (a mano o por el cálculo automático). El dueño del indicador tiene 3 días hábiles desde aquí para validarla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:114` |
| `cause` | Text | Causa | Por qué salió en rojo (I-4). |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:126` |
| `denominator` | Float | Denominador | Denominador del cálculo (la base contra la que se mide). |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:382` |
| `detail_count` | Integer | Registros |  |  |  | compute `_compute_detail_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:391` |
| `detail_ids` | Text | Ids de los registros |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:390` |
| `detail_model` | Char | Modelo de los registros |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:389` |
| `direction` | Selection |  | Sentido del indicador. |  |  | related `indicator_id.direction`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:1035` |
| `indicator_id` | Many2one | Indicador | Indicador medido. | sí | `sgi.indicator` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:1024` |
| `indicator_status` | Selection | Estado del indicador | Si el indicador es oficial o está a prueba. |  |  | related `indicator_id.status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:393` |
| `note` | Text | Nota |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:1045` |
| `numerator` | Float | Numerador | Numerador del cálculo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:381` |
| `parallel_denominator` | Float | Denominador (fórmula) | Denominador calculado con la fórmula configurable, para compararlo con el modo actual. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:586` |
| `parallel_numerator` | Float | Numerador (fórmula) | Numerador calculado con la fórmula configurable, para compararlo con el modo actual. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:583` |
| `parallel_value` | Float | Valor de la fórmula | Lo que daría la fórmula configurada del indicador en este periodo, mientras el indicador sigue en su modo de código. Cuando coincidan un mes, se migra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:578` |
| `period_date` | Date | Periodo | Día 1 del mes medido. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:1032` |
| `plan_done` | Boolean | Plan capturado | Indica que la medición en rojo ya tiene causa y plan. |  |  | compute `_compute_plan`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:133` |
| `plan_due` | Date | Plan antes del | Día 10 del mes siguiente al periodo (si es inhábil, el hábil anterior). |  |  | compute `_compute_plan`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:130` |
| `plan_required` | Boolean | Requiere plan | Indica que la medición está en rojo y pide causa y plan de acción. |  |  | compute `_compute_plan`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:128` |
| `range_max` | Float |  | Límite superior del rango del indicador. |  |  | related `indicator_id.range_max`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:240` |
| `range_min` | Float |  | Límite inferior del rango del indicador. |  |  | related `indicator_id.range_min`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:238` |
| `sample_size` | Integer | Casos | Registros que forman la medición. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:384` |
| `semaphore` | Selection | Semáforo | Verde, amarillo o rojo según el valor y las metas. Se calcula solo. |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator.py:1039` |
| `sgi_can_validate` | Boolean | Puede validar |  |  |  | compute `_compute_sgi_can_validate`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:1055` |
| `sgi_nc_suppressed` | Boolean | NC omitida (fuente apagada) | La medición ameritaba NC pero la fuente «Indicador en semáforo rojo» estaba desactivada. Se reintenta sola en cuanto se reactive. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:1057` |
| `sgi_validated_date` | Date | Validada el | Día en que la medición pasó a «Validado». Con él se mide si se validó a tiempo (3 días hábiles desde la captura). |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_health.py:457` |
| `small_sample` | Boolean | Muestra chica | Menos casos que el mínimo (quimibond_sgi.indicator_min_sample): se mide, pero no abre NC. |  |  | compute `_compute_small_sample`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:385` |
| `source_info` | Char | Fuente del dato |  |  |  | related `indicator_id.source_info`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:1030` |
| `source_type` | Selection | Origen del dato | Si el dato es automático o se captura. |  |  | related `indicator_id.source_type`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:1027` |
| `split_ids` | One2many | Desglose |  |  | `sgi.indicator.measure.split` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:419` |
| `state` | Selection |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:379` |
| `target_acceptable` | Float | Aceptable | Aceptable vigente en el periodo (con trayectoria, el del escalón). |  |  | related `None`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:235` |
| `target_objective` | Float | Objetivo | Objetivo vigente en el periodo (con trayectoria, el del escalón). |  |  | related `None`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:233` |
| `uom` | Char | Unidad |  |  |  | related `indicator_id.uom`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator.py:1038` |
| `value` | Float | Valor | Valor medido en el periodo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:1034` |
| `value_is_pct` | Boolean |  |  |  |  | compute `_compute_value_is_pct`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:392` |
| `window_label` | Char | Ventana |  |  |  | related `indicator_id.window_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:125` |

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
| `write` | 57.99.0: guarda el día en que la medición PASA a validada (SG-05). Re-validar una ya validada no lo mueve. |
