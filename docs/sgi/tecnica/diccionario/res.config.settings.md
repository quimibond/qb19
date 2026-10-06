<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `res.config.settings`

Modelo de otra app que el SGI extiende.

Panel de Ajustes del SGI: la cara amigable de los parámetros.

Archivos: `addons/quimibond_sgi/models/sgi_epp_sign.py`, `addons/quimibond_sgi/models/sgi_my_procedure_sign.py`, `addons/quimibond_sgi/models/sgi_settings.py`, `addons/quimibond_sgi_pesaje/models/res_config_settings.py`.

## Campos (38)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_action_escalation_director_days` | Integer | Días hábiles para escalar una acción vencida a Dirección | Acción vencida por más de estos días → además, se avisa a Dirección. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:89` |
| `sgi_action_escalation_manager_days` | Integer | Días hábiles para escalar una acción vencida al jefe | Acción vencida por más de estos días → además del responsable, se avisa a su jefe directo (fallback Jefe MAST). |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:84` |
| `sgi_calibration_block_expired` | Boolean | Bloquear equipos con calibración vencida | Apagado (default, decisión de la tanda 2): el cron solo avisa con un resumen diario al Coordinador de Laboratorio y al Jefe de Calidad. Encendido: además marca «No usar» cada equipo vencido y manda el correo crítico. La inspección de calidad rechaza un equipo vencido en cualquier caso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:136` |
| `sgi_checklist_pin_required` | Boolean | PIN obligatorio para firmar checklists | Apagado (default): quien no tiene PIN firma la hoja y queda «(sin PIN registrado)». Encendido (D-08): sin PIN capturado en su ficha de empleado no se firma. Encender solo cuando RH haya capturado los PIN (el mismo del quiosco de asistencia). |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:144` |
| `sgi_dev_block_generic_sample_from` | Char | Bloquear «MUESTRA PILOTO» en órdenes nuevas desde (AAAA-MM-DD) | A partir de esta fecha no se crean ni confirman órdenes de fabricación con los artículos genéricos «MUESTRA PILOTO TEJIDO / TINTORERÍA»: las muestras de desarrollo se piden desde el proyecto con su artículo generado. Vacío: sin bloqueo (hay órdenes abiertas con ellos; la fecha la decide Dirección d… |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:15` |
| `sgi_dev_lab_authorizer_job_id` | Many2one | Puesto que autoriza pruebas de laboratorio de desarrollos | Puesto cuyas personas autorizan las solicitudes de pruebas de los proyectos de desarrollo. Por omisión, el Coordinador de Laboratorio y MP (se busca por nombre si el parámetro está vacío). El Jefe MAST siempre puede. |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_settings.py:23` |
| `sgi_doc_ack_pending_days` | Integer | Días hábiles para reclamar un acuse pendiente | Días hábiles que puede estar pendiente un acuse de lectura antes de avisar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:75` |
| `sgi_doc_pilot_notice_days` | Integer | Aviso de piloto por vencer (días) | Días antes del fin de una prueba piloto documental en que llega el aviso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:71` |
| `sgi_doc_review_notice_days` | Integer | Primer aviso de revisión documental (días) | Días antes de la próxima revisión de un documento en que llega el primer aviso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:63` |
| `sgi_doc_review_notice_days_final` | Integer | Segundo aviso de revisión documental (días) | Días antes de la próxima revisión de un documento en que llega el segundo aviso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:67` |
| `sgi_energy_partner_id` | Many2one | Proveedor de energía | KPI TR-03 (Consumo de energía): proveedor cuyas facturas del periodo suman el consumo. Sin configurar, la medición queda en 0 con nota. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_settings.py:175` |
| `sgi_epp_sign_template_id` | Many2one | Plantilla de Sign de la responsiva de EPP | Opcional: vacío, el SGI arma la plantilla sola (responsiva + hoja «Recibí el EPP»). Solo si se quiere otra, una plantilla hecha a mano con un solo firmante (el empleado). |  | `sign.template` |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:135` |
| `sgi_fmea_npr_action` | Integer | NPR que exige acción en el AMEF | Número de prioridad de riesgo a partir del cual un modo de falla del AMEF exige acción. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:94` |
| `sgi_mast_user_id` | Many2one | Jefe MAST y SGI | Recibe los avisos automáticos del SGI que no tienen dueño (parámetro quimibond_sgi.mast_user_id). Vacío: el usuario mas@quimibond.com o, si no existe, el primer miembro directo del grupo Jefe MAST y SGI. Al cambiarlo, los avisos abiertos se reasignan en la siguiente corrida de cada cron (no se dupl… |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_settings.py:151` |
| `sgi_monthly_sales_budget` | Float | Presupuesto mensual de ventas (MXN) | Alimenta los KPIs de cumplimiento y eficiencia del presupuesto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:129` |
| `sgi_mp_sign_required` | Boolean | «Mi procedimiento» se firma en Sign | Publicar deja la revisión en borrador y la manda a firmar (MAST elabora, el jefe directo aprueba); entra en vigor sola al firmarse. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_procedure_sign.py:137` |
| `sgi_nc_days_containment` | Integer | Plazo de contención de una NC (días hábiles) | Días hábiles, desde que se abre una NC, para registrar la contención. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:39` |
| `sgi_nc_days_plan` | Integer | Plazo del plan de acción de una NC (días hábiles) | Días hábiles, desde que se abre una NC, para registrar el plan de acción. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:47` |
| `sgi_nc_days_root_cause` | Integer | Plazo de causa raíz de una NC (días hábiles) | Días hábiles, desde que se abre una NC, para capturar la causa raíz. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:43` |
| `sgi_nc_days_supplier_response` | Integer | Días hábiles para que el proveedor conteste una NC (portal) | Días hábiles que tiene el proveedor para contestar una NC por el portal. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:55` |
| `sgi_nc_effectiveness_days` | Integer | Días para verificar la eficacia tras la última correctiva | Días después de terminar la última acción correctiva en que se pide verificar la eficacia. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:59` |
| `sgi_nc_escalation_days` | Integer | Días hábiles para escalar una NC sin acciones | NC interna sin acciones tras estos días → actividad al responsable y aviso a MAST. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:29` |
| `sgi_nc_escalation_days_external` | Integer | Días hábiles para escalar una NC externa/cliente | Las NC de auditoría externa y reclamaciones de cliente escalan más rápido que las internas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:34` |
| `sgi_nc_escalation_mast_days` | Integer | Días hábiles vencido un plazo de NC antes de escalar a MAST | Días hábiles que puede estar vencido un plazo de NC antes de escalar al Jefe MAST y SGI. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:51` |
| `sgi_nc_recurrence_months` | Integer | Ventana de reincidencia de NC (meses) | Una NC del SGI cuenta como reincidente si en este número de meses hubo otra NC del mismo proceso (misma cláusula pesa doble). |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:79` |
| `sgi_pesaje_tolerance_kg` | Float | Tolerancia de peso de rollo (kg) | Rollo confirmado fuera de esta tolerancia → alerta de calidad automática. |  |  |  |  | `addons/quimibond_sgi_pesaje/models/res_config_settings.py:10` |
| `sgi_production_monthly_capacity` | Float | Capacidad instalada mensual de producción | KPI MA-02 (Producido vs capacidad): capacidad mensual en la misma unidad que la producción (p.ej. kg). Para periodos no mensuales se prorratea por días. 0 = captura manual. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:169` |
| `sgi_purchase_approval_category_id` | Many2one | Categoría de requisiciones de compra | KPI CO-02 (Requisiciones): categoría de aprobación que cuenta como requisición de compra. Déjelo vacío para detectar automáticamente la(s) categoría(s) de tipo compra; configúrelo solo si hay varias. |  | `approval.category` |  |  | `addons/quimibond_sgi/models/sgi_settings.py:158` |
| `sgi_rh_user_id` | Many2one | Usuario de RH | Recibe las actividades automáticas de RH (competencias por vencer, DNC). |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_settings.py:133` |
| `sgi_risk_ryo_inmediata` | Integer | RyO: puntaje para atención Inmediata | Puntaje (probabilidad × impacto) desde el cual un riesgo es de atención inmediata. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:98` |
| `sgi_risk_ryo_intermedia` | Integer | RyO: puntaje para atención Intermedia | Puntaje (probabilidad × impacto) desde el cual un riesgo es de atención intermedia. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:106` |
| `sgi_risk_ryo_media` | Integer | RyO: puntaje para atención Media | Puntaje (probabilidad × impacto) desde el cual un riesgo es de atención media. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:102` |
| `sgi_satisfaction_survey_id` | Many2one | Encuesta de satisfacción (CA-02) | Encuesta cuyas respuestas alimentan el KPI CA-02. Sin configurar se usa la sembrada por el módulo. Útil para re-apuntar al histórico de respuestas (aunque esté archivado). |  | `survey.survey` |  |  | `addons/quimibond_sgi/models/sgi_settings.py:179` |
| `sgi_supplier_critical_categ_ids` | Many2many | Categorías de proveedores críticos | Materia prima y maquila: solo los proveedores que entregan productos de estas categorías (o los marcados como críticos en el contacto) entran a la evaluación trimestral. Vacío = la categoría de materia prima más las que se llaman «maquila». |  | `product.category` |  |  | `addons/quimibond_sgi/models/sgi_settings.py:163` |
| `sgi_supplier_nc_penalty` | Float | Puntos que descuenta cada NC al proveedor | Puntos que resta cada NC a la calificación de calidad del proveedor (sobre 100). |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:118` |
| `sgi_supplier_otd_tolerance_days` | Integer | Tolerancia OTD de proveedores (días) | Días de gracia sobre la fecha compromiso para contar una recepción como a tiempo (comparación por día calendario). |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:122` |
| `sgi_supplier_weight_otd` | Float | Peso de entregas a tiempo | Peso (0-1) de la puntualidad en la calificación del proveedor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:110` |
| `sgi_supplier_weight_quality` | Float | Peso de calidad | Peso de la calidad en la calificación del proveedor; el resto es la entrega a tiempo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_settings.py:114` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `get_values` | — |
| `set_values` | — |
