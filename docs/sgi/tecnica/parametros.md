<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# Parámetros del SGI

Valores de arranque de `sgi.config._SGI_DEFAULT_PARAMS` (`addons/quimibond_sgi/models/sgi_format_map.py`). Solo se crean si no existen: lo que se edite en Ajustes → Técnico → Parámetros del sistema nunca se pisa. Otros parámetros los siembran los satélites o se ponen a mano (p. ej. `quimibond_sgi.mast_user_id`, `quimibond_sgi.sgi_company_id`).

| Clave | De fábrica | Comentario en el código |
|---|---|---|
| `quimibond_sgi.vague_verbs` | `dar seguimiento,gestionar,coordinar,apoyar,asegurar,atender,ver,checar,manejar,controlar` | Verbos que revisa la especificación de actividades (sgi_activity_spec). |
| `quimibond_sgi.compare_verbs` | `verificar,comparar,revisar,validar,conciliar,inspeccionar` |  |
| `quimibond_sgi.nc_escalation_days` | `5` |  |
| `quimibond_sgi.pilot_days` | `60` | Días por omisión de una prueba piloto documental. |
| `quimibond_sgi.nc_escalation_days_external` | `3` |  |
| `quimibond_sgi.nc_days_containment` | `1` | NC-1/NC-3 (49.0.0): plazos por etapa en días hábiles desde que se abre la NC, días de gracia antes de escalar a MAST y días para la verificación de eficacia tras la última acción correctiva. |
| `quimibond_sgi.nc_days_root_cause` | `10` |  |
| `quimibond_sgi.nc_days_plan` | `15` |  |
| `quimibond_sgi.nc_escalation_mast_days` | `3` |  |
| `quimibond_sgi.nc_effectiveness_days` | `90` |  |
| `quimibond_sgi.nc_days_supplier_response` | `5` | NC-6: días hábiles que tiene el proveedor para contestar por el portal. |
| `quimibond_sgi.nc_recurrence_months` | `12` |  |
| `quimibond_sgi.action_escalation_manager_days` | `7` |  |
| `quimibond_sgi.action_escalation_director_days` | `15` |  |
| `quimibond_sgi.doc_review_notice_days` | `60` |  |
| `quimibond_sgi.doc_review_notice_days_final` | `30` |  |
| `quimibond_sgi.doc_ack_pending_days` | `7` |  |
| `quimibond_sgi.doc_pilot_notice_days` | `7` |  |
| `quimibond_sgi.fmea_npr_action` | `100` |  |
| `quimibond_sgi.risk_ryo_inmediata` | `16` |  |
| `quimibond_sgi.risk_ryo_media` | `9` |  |
| `quimibond_sgi.risk_ryo_intermedia` | `4` |  |
| `quimibond_sgi.supplier_weight_otd` | `0.7` |  |
| `quimibond_sgi.supplier_weight_quality` | `0.3` |  |
| `quimibond_sgi.supplier_nc_penalty` | `10.0` |  |
| `quimibond_sgi.supplier_otd_tolerance_days` | `1` | Días de gracia del OTD de proveedores (comparación por día calendario). |
| `quimibond_sgi.waste_location_ids` | `39,43` | 57.10.0 (A-020): quimibond_sgi.pesaje_tolerance_kg la siembra quimibond_sgi_pesaje. I-3: desperdicio en kg y compras de materia prima (sgi_indicator_i3.py). |
| `quimibond_sgi.waste_input_categ_ids` | `350,356` |  |
| `quimibond_sgi.raw_material_categ_id` | `318` |  |
| `quimibond_sgi.rework_picking_type_ids` | `106,107` | P-21: tipos de operación que cuentan como reproceso (MA-04). Re-proceso Tintorería (106) y Re-proceso Acabado (107); «Acabado producto en proceso» (151) entra cuando producción lo confirme. |
| `quimibond_sgi.finished_product_categ_ids` | `319` | 57.14.0 (indicadores 2): categorías de producto terminado de C1-04 (319 «Producto Terminado», con sus hijas). |
| `quimibond_sgi.monthly_measure_business_day` | `3` | I-6: día hábil del mes en que se miden los indicadores mensuales. |
| `quimibond_sgi.red_plan_due_day` | `10` | I-4: día del mes siguiente en que vence la causa y acción de un rojo. |
| `quimibond_sgi.indicator_min_sample` | `5` | I-2: mínimo de casos para que una medición cuente para NC. |
| `quimibond_sgi.release_block_enabled` | `True` | P-7: no surtir lotes sin liberar (models/sgi_release.py). |
| `quimibond_sgi.release_block_picking_type_ids` | `113,210` |  |
| `quimibond_sgi.unreleased_location_ids` | `324,44,36,246,45` |  |
| `quimibond_sgi.waste_subproduct_category` | `SubProducto` |  |
| `quimibond_sgi.coa_block_validation` | `False` | COA (sgi_coa): el bloqueo al validar una salida sin COA se enciende cuando el cambio se implemente formalmente; la excepción es del puesto Jefe de Calidad. |
| `quimibond_sgi.coa_exception_job_id` | `204` |  |
| `quimibond_sgi.monthly_sales_budget` | `0` |  |
| `quimibond_sgi.rh_user_id` | `0` |  |
| `quimibond_sgi.purchase_approval_category_id` | `0` |  |
| `quimibond_sgi.production_monthly_capacity` | `0` | Capacidad instalada mensual de producción (misma unidad que la producción, p.ej. kg) para el KPI MA-02. 0 = captura manual. |
| `quimibond_sgi.energy_partner_id` | `0` | Proveedor de energía (res.partner) para el KPI TR-03. 0 = sin configurar. |
| `quimibond_sgi.satisfaction_survey_id` | `0` | Encuesta que alimenta el KPI CA-02 (survey.survey). 0 = usar la sembrada del módulo. Permite re-apuntar al histórico archivado. |
| `quimibond_sgi.training_effectiveness_days` | `90` | 57.100.0 (N-13): días para evaluar la eficacia de la capacitación y encuesta opcional al jefe (survey.survey; 0 = sin encuesta). |
| `quimibond_sgi.training_effectiveness_survey_id` | `0` |  |
| `quimibond_sgi.ppap_sales_window_months` | `12` | 57.100.0 (N-14): meses de ventas que hacen «cliente del producto» en el ECO. |
| `quimibond_sgi.ai_enabled` | `False` | 57.100.0 (IA, puerta Q16): apagada hasta la autorización escrita de Jose. Proveedor Anthropic; modelo configurable; segundos de espera; si se mandan las 3 NC cerradas del mismo proceso. La llave (quimibond_sgi.ai_api_key) no se siembra: la captura un administrador. |
| `quimibond_sgi.ai_backend` | `anthropic` |  |
| `quimibond_sgi.ai_model` | `claude-opus-5-5` |  |
| `quimibond_sgi.ai_timeout` | `60` |  |
| `quimibond_sgi.ai_include_history` | `True` |  |
