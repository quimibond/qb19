<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator`

**Indicador SGI (F-P-A10-03)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Indicador del SGI (F-P-A10-03): fórmula o modo de cálculo, metas, frecuencia y semáforo. El cron crea las mediciones del periodo; con ``nc_on_red`` un rojo levanta NC.

Orden: `code`.

Archivos: `addons/quimibond_sgi/models/sgi_indicator.py`, `addons/quimibond_sgi/models/sgi_activity_spec.py`, `addons/quimibond_sgi/models/sgi_business_line.py`, `addons/quimibond_sgi/models/sgi_direction_board.py`, `addons/quimibond_sgi/models/sgi_indicator_detail.py`, `addons/quimibond_sgi/models/sgi_indicator_formula.py`, `addons/quimibond_sgi/models/sgi_indicator_i3.py`, `addons/quimibond_sgi/models/sgi_indicator_ind2.py`, `addons/quimibond_sgi/models/sgi_indicator_p21.py`, `addons/quimibond_sgi/models/sgi_indicator_plan.py`, `addons/quimibond_sgi/models/sgi_indicator_trajectory.py`, `addons/quimibond_sgi_revisado/models/sgi_calidad_pq.py`.

## Campos (51)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:190` |
| `activity_id` | Many2one | Actividad medida | Para «% a tiempo»: la actividad cuyo cumplimiento semanal se toma. |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:832` |
| `baseline_date` | Date | Arranque desde | Fecha del valor de arranque; inicio de la trayectoria. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:88` |
| `baseline_value` | Float | Valor de arranque | El primer mes medido. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:823` |
| `calc_checked` | Datetime | Revisado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:66` |
| `calc_message` | Char | Motivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:65` |
| `calc_mode` | Selection |  |  |  |  |  |  | `addons/quimibond_sgi_revisado/models/sgi_calidad_pq.py:26` |
| `calc_status` | Selection | Último cálculo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:57` |
| `can_edit_formula` | Boolean |  |  |  |  | compute `_compute_can_edit_formula`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:348` |
| `code` | Char | Clave |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:78` |
| `critical` | Boolean | Crítico | Un solo periodo en rojo abre la NC (I-5). Sin marcar, hacen falta dos periodos seguidos en rojo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:50` |
| `deliverable_id` | Many2one | Entregable medido | Para «% completo»: el entregable cuyo filtro «ya está completo» se compara contra lo entregado. Vacío: el entregable con el que se mide la actividad. |  | `sgi.deliverable` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:835` |
| `direction` | Selection |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:81` |
| `formula` | Text | Fórmula | Cómo se calcula, en palabras: «Entregas completas en la fecha compromiso ÷ entregas del mes». Sale en el procedimiento impreso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:103` |
| `formula_text` | Text | Fórmula configurada |  |  |  | compute `_compute_has_formula`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:345` |
| `frequency` | Selection | Frecuencia |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:97` |
| `has_formula` | Boolean |  |  |  |  | compute `_compute_has_formula`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:344` |
| `has_trajectory` | Boolean |  |  |  |  | compute `_compute_has_trajectory`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:91` |
| `last_measure_id` | Many2one | Última medición |  |  | `sgi.indicator.measure` | compute `_compute_last_measure`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator.py:194` |
| `last_semaphore` | Selection | Último semáforo |  |  |  | compute `_compute_last_measure`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator.py:197` |
| `last_six` | Char | Últimos 6 periodos | Periodo: valor y semáforo (V verde, A amarillo, R rojo). |  |  | compute `_compute_last_six`, sin guardar |  | `addons/quimibond_sgi/models/sgi_direction_board.py:30` |
| `last_value` | Float | Último valor |  |  |  | compute `_compute_last_measure`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator.py:196` |
| `level` | Selection | Nivel | Dirección: va al Tablero de dirección. Proceso: lo sigue el dueño del proceso. Actividad: medición de un paso del procedimiento. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_direction_board.py:23` |
| `measure_from` | Date | Medir desde | Fecha desde la que hay dato confiable (el proceso entra a piloto o existe el campo que lo alimenta). Antes de ella no se crea medición. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:45` |
| `measure_ids` | One2many | Mediciones |  |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:192` |
| `measure_split` | Selection | Desglose | Además del total, una medición por equipo de ventas o por mercado (país del cliente contra el de la compañía). Solo con fórmula configurable y registros que lleguen al equipo o al cliente. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:266` |
| `measure_team_ids` | Many2many | Equipos a desglosar | Vacío = los equipos de venta de la compañía del indicador. |  | `crm.team` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:275` |
| `monthly_budget` | Float | Presupuesto mensual | Meta mensual para el cálculo de presupuesto de ventas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:182` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:79` |
| `nc_on_red` | Boolean | NC automática | Con el indicador oficial, abre una no conformidad por persistencia (I-5): dos periodos seguidos en rojo, o uno solo si el indicador es crítico. Nunca con muestra chica ni sin dato, y no duplica la NC mientras la anterior siga abierta. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:184` |
| `objective_id` | Many2one | Objetivo integral |  |  | `sgi.objective` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:89` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:80` |
| `range_max` | Float | Máximo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:84` |
| `range_min` | Float | Mínimo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:83` |
| `range_tolerance` | Float | Tolerancia | Fuera del rango pero dentro de esta distancia el semáforo es amarillo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:85` |
| `responsible_id` | Many2one | Responsable |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:88` |
| `sgi_area_id` | Many2one | Área SGI |  |  | `sgi.area` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:87` |
| `sgi_process_active` | Boolean | Proceso vigente | El proceso al que pertenece está activo. Sin proceso o con el proceso archivado, queda pendiente de proceso nuevo. |  |  | related `process_id.active`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator.py:83` |
| `source` | Text | De dónde sale el dato | Qué registros o documentos alimentan la fórmula: «Fecha compromiso del pedido contra fecha de la orden de entrega». |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:107` |
| `source_info` | Char | Fuente del dato |  |  |  | compute `_compute_source`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator.py:115` |
| `source_type` | Selection | Origen del dato |  |  |  | compute `_compute_source`, guardado |  | `addons/quimibond_sgi/models/sgi_indicator.py:111` |
| `spec_missing` | Char | Le falta |  |  |  | compute `_compute_spec_missing`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:828` |
| `split_support` | Char | Desglose posible |  |  |  | compute `_compute_split_support`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:279` |
| `status` | Selection | Estado del indicador | Nace en prueba. El dueño revisa una vez la lista de registros de una medición contra la realidad y lo pasa a oficial. Solo los oficiales pueden abrir una no conformidad. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_indicator_detail.py:38` |
| `step_ids` | One2many | Escalones |  |  | `sgi.indicator.step` |  |  | `addons/quimibond_sgi/models/sgi_indicator_trajectory.py:90` |
| `target_acceptable` | Float | Aceptable |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:96` |
| `target_date` | Date | Llegar a la meta el | Opcional: sin fecha, la meta es permanente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:825` |
| `target_objective` | Float | Objetivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:95` |
| `term_ids` | One2many | Términos de la fórmula |  |  | `sgi.indicator.term` |  |  | `addons/quimibond_sgi/models/sgi_indicator_formula.py:343` |
| `uom` | Char | Unidad | % , MXN, unidades, kg, m… |  |  |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:90` |
| `window_label` | Char | Ventana | Qué periodo de datos resume cada medición. |  |  | compute `_compute_window_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_indicator_plan.py:52` |

## Métodos públicos (11)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_trajectory` | Escalones trimestrales entre arranque y meta final. Los corregidos a mano se conservan; los demás se recalculan. |
| `action_recalculate_now` | Botón «Recalcular ahora»: el último periodo cerrado, guardando. |
| `action_set_official` | — |
| `action_set_trial` | — |
| `action_sgi_measures` | Mis indicadores → «Mediciones»: la lista de mediciones del indicador para capturar la pendiente (primero lo más reciente). |
| `action_sgi_recompute_pending_measures` | D-12 (57.5.0): botón «Recalcular mediciones pendientes» de la lista de indicadores, solo para el Administrador SGI. Con indicadores seleccionados recalcula solo esos; sin selección, todos. El cron di… |
| `action_view_trend` | La pregunta real de MAST frente a un KPI: ¿cómo viene la tendencia? Abre las mediciones del indicador en gráfica de línea por periodo. |
| `create` | — |
| `cron_missing_trajectories` | Paso del cron de indicadores: escalones para los que ya tienen fechas. |
| `sgi_recalculate` | Calcula el indicador en un periodo con su modo actual, sin esperar al cron. Por MCP: ``call_model_method('sgi.indicator', 'sgi_recalculate', [ids], {'period_date': '2026-08-01', 'save': True})``. |
| `write` | — |
