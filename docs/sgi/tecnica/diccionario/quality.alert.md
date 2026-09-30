<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `quality.alert`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_customer_reply.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_incident.py`, `addons/quimibond_sgi/models/sgi_indicator.py`, `addons/quimibond_sgi/models/sgi_kpi_quality.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_nonconformity.py`, `addons/quimibond_sgi/models/sgi_supplier_nc.py`.

## Campos (74)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_action_line_ids` | One2many | Correcciones y acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:131` |
| `sgi_approved_by` | Many2one | Aprobó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:126` |
| `sgi_approved_date` | Date | Fecha de aprobación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:127` |
| `sgi_cancel_reason` | Text | Motivo de cancelación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:156` |
| `sgi_cancel_requested_by` | Many2one | Cancelación solicitada por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:157` |
| `sgi_claimed_meters` | Float | Metros reclamados | Metros que el cliente reclama en esta NC (C5-01). |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_quality.py:24` |
| `sgi_classification` | Selection | Clasificación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:83` |
| `sgi_complaint_ticket_id` | Many2one | Reclamación ligada |  |  | `helpdesk.ticket` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:128` |
| `sgi_containment_done` | Boolean |  |  |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:149` |
| `sgi_containment_state` | Selection | Contención |  |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:143` |
| `sgi_customer_ack_date` | Date | Acuse de recibo al cliente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:54` |
| `sgi_customer_ack_due` | Date | Acusar recibo a más tardar |  |  |  | compute `_compute_sgi_customer_dues`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:52` |
| `sgi_customer_ack_on_time` | Boolean | Acuse a tiempo |  |  |  | compute `_compute_sgi_customer_on_time`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:61` |
| `sgi_customer_deadline` | Date | Plazo pedido por el cliente | Si el cliente fijó su propio plazo de respuesta, sustituye el de la línea. |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:55` |
| `sgi_customer_notice_date` | Date | Aviso escrito al cliente | Fecha del aviso vía Administración de ventas con lotes, cantidades y plan. |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:72` |
| `sgi_customer_notice_note` | Char | Cómo se avisó (referencia) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:75` |
| `sgi_customer_received_date` | Date | Reclamación recibida el | Día en que llegó la reclamación (el del ticket, si lo hay). Desde aquí corren los plazos. |  |  | compute `_compute_sgi_customer_received_date`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:48` |
| `sgi_customer_reply_required` | Boolean | Responde al cliente | Reclamación, scorecard o NC con cliente: se mide el acuse y la respuesta (C5.19). |  |  | compute `_compute_sgi_customer_reply_required`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:40` |
| `sgi_customer_response_date` | Date | Respuesta formal al cliente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:60` |
| `sgi_customer_response_due` | Date | Responder a más tardar |  |  |  | compute `_compute_sgi_customer_dues`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:58` |
| `sgi_customer_response_on_time` | Boolean | Respuesta a tiempo |  |  |  | compute `_compute_sgi_customer_on_time`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:63` |
| `sgi_deviation` | Text | Desviación detectada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:97` |
| `sgi_document_id` | Many2one | Documento ligado |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:165` |
| `sgi_due_containment` | Date | Contención vence |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:140` |
| `sgi_due_plan` | Date | Plan de acción vence |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:142` |
| `sgi_due_root_cause` | Date | Causa raíz vence |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:141` |
| `sgi_effectiveness_by` | Many2one | Eficacia verificada por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:107` |
| `sgi_effectiveness_date` | Date | Fecha de eficacia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:106` |
| `sgi_effectiveness_due` | Date | Verificar eficacia el | Se fija al terminar la última acción correctiva (90 días por omisión) y agenda la verificación al Jefe MAST. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:151` |
| `sgi_effectiveness_note` | Text | Verificación de eficacia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:105` |
| `sgi_external_ref` | Char | N° NCR externo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:129` |
| `sgi_fmea_id` | Many2one | AMEF ligado |  |  | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:164` |
| `sgi_folio` | Char | Folio SGI |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:63` |
| `sgi_followup_action` | Selection | Acción a seguir |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:119` |
| `sgi_followup_comments` | Text | Comentarios de seguimiento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:117` |
| `sgi_incident_id` | Many2one | Incidente SST de origen |  |  | `sgi.incident` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:9` |
| `sgi_indicator_measure_id` | Many2one | Medición de indicador |  |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:1314` |
| `sgi_is_recurrent` | Boolean | Reincidente |  |  |  | compute `_compute_sgi_recurrence`, guardado |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:176` |
| `sgi_ishikawa_notes` | Text | Notas Ishikawa (5-6M) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:104` |
| `sgi_lead_auditor_id` | Many2one | Auditor líder |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:93` |
| `sgi_lesson_captured` | Boolean | Lección aplicada a AMEF / plan de control / documento | Confírmelo cuando la lección aprendida de esta NC mayor ya se reflejó en el AMEF, el plan de control y/o el documento controlado correspondiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:111` |
| `sgi_maintenance_request_id` | Many2one | Solicitud de mantenimiento | Solicitud correctiva de la que salió esta NC (S5.06). |  | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_links.py:127` |
| `sgi_norm_clause_id` | Many2one | Requisito (cláusula) |  |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:88` |
| `sgi_origin_type` | Selection | Origen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:66` |
| `sgi_plan_state` | Selection | Plan de acción |  |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:147` |
| `sgi_process_id` | Many2one | Proceso detectado |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:94` |
| `sgi_recurrence_count` | Integer | Reincidencias | Casos previos del mismo proceso en la ventana de reincidencia (misma cláusula cuenta doble). |  |  | compute `_compute_sgi_recurrence`, guardado |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:172` |
| `sgi_requester_id` | Many2one | Solicitante |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:90` |
| `sgi_requester_job` | Char | Cargo del solicitante |  |  |  | related `sgi_requester_id.employee_id.job_title`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:91` |
| `sgi_required_capa` | Boolean | ¿Requirió acción correctiva? |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:118` |
| `sgi_responsible_ids` | Many2many | Responsables a contestar |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:95` |
| `sgi_risk_ids` | Many2many | Riesgos ligados |  |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:161` |
| `sgi_root_cause` | Text | Causa raíz |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:103` |
| `sgi_root_cause_state` | Selection | Causa raíz (plazo) |  |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:145` |
| `sgi_sale_team_id` | Many2one | Línea (equipo de venta) | Del pedido de la reclamación o, si no hay, del último pedido del cliente. Fija el plazo de respuesta. |  | `crm.team` | compute `_compute_sgi_sale_team_id`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:43` |
| `sgi_shipped_status` | Selection | ¿Producto ya embarcado? | C5.20: al abrir la NC revisar si hay lotes del mismo origen ya embarcados. |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:66` |
| `sgi_source_id` | Many2one | Fuente automática | Automatismo que levantó esta NC. Vacío si se capturó a mano. |  | `sgi.alert.source` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:79` |
| `sgi_stage_is_cancel` | Boolean |  |  |  |  | related `stage_id.sgi_is_cancel_stage`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:65` |
| `sgi_stage_is_closing` | Boolean |  |  |  |  | related `stage_id.sgi_is_closing_stage`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:64` |
| `sgi_supplier_action` | Text | Acción según el proveedor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:33` |
| `sgi_supplier_cause` | Text | Causa según el proveedor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:32` |
| `sgi_supplier_due_date` | Date | Respuesta antes del |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:30` |
| `sgi_supplier_id` | Many2one | Proveedor |  |  | `res.partner` | compute `_compute_sgi_supplier_id`, guardado |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:21` |
| `sgi_supplier_overdue` | Boolean |  |  |  |  | compute `_compute_sgi_supplier_overdue`, sin guardar |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:34` |
| `sgi_supplier_response_date` | Datetime | Contestada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:31` |
| `sgi_supplier_sent_date` | Date | Enviada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:29` |
| `sgi_supplier_state` | Selection | Respuesta del proveedor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:24` |
| `sgi_verified_by` | Many2one | Verificó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:124` |
| `sgi_verified_date` | Date | Fecha de verificación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:125` |
| `sgi_why_1` | Char | ¿Por qué? 1 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:98` |
| `sgi_why_2` | Char | ¿Por qué? 2 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:99` |
| `sgi_why_3` | Char | ¿Por qué? 3 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:100` |
| `sgi_why_4` | Char | ¿Por qué? 4 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:101` |
| `sgi_why_5` | Char | ¿Por qué? 5 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:102` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_cancel` | — |
| `action_sgi_escalate_to_nc` | Escala una alerta operativa de piso a una No Conformidad sistémica del SGI: la mueve al equipo NC Internas, le asigna folio y origen 'proceso', conservando producto/orden/picking. Las alertas rutinar… |
| `action_sgi_force_close` | — |
| `action_sgi_send_to_supplier` | Envía la NC al proveedor por el portal (correo con el enlace). |
| `create` | — |
| `sgi_auto_create` | Punto ÚNICO de entrada de las NC que levanta el sistema. |
| `write` | — |
