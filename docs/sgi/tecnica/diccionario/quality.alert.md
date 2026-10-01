<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `quality.alert`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_customer_reply.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_incident.py`, `addons/quimibond_sgi/models/sgi_indicator.py`, `addons/quimibond_sgi/models/sgi_kpi_quality.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_nonconformity.py`, `addons/quimibond_sgi/models/sgi_supplier_nc.py`.

## Campos (75)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_action_line_ids` | One2many | Correcciones y acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:156` |
| `sgi_approved_by` | Many2one | Aprobó | Persona que aprueba el cierre de la no conformidad. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:147` |
| `sgi_approved_date` | Date | Fecha de aprobación | Fecha en que se aprobó el cierre de la no conformidad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:149` |
| `sgi_cancel_reason` | Text | Motivo de cancelación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:201` |
| `sgi_cancel_requested_by` | Many2one | Cancelación solicitada por | Quién pidió cancelar la no conformidad. La cancelación la confirma el Jefe MAST y SGI. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:202` |
| `sgi_claimed_meters` | Float | Metros reclamados | Metros que el cliente reclama en esta NC (C5-01). |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_quality.py:24` |
| `sgi_classification` | Selection | Clasificación | Mayor, menor u observación. Una NC mayor manda un correo crítico al abrirse y exige aplicar la lección aprendida antes de cerrar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:87` |
| `sgi_complaint_ticket_id` | Many2one | Reclamación ligada | Reclamación de cliente de la que nació esta no conformidad. |  | `helpdesk.ticket` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:151` |
| `sgi_containment_done` | Boolean |  | Se marca sola cuando la NC ya tiene al menos una acción de contención registrada. |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:186` |
| `sgi_containment_state` | Selection | Contención | Plazo de la contención: hecha cuando hay una acción de contención; vencida si pasó la fecha sin ella. Se calcula al mostrarlo. |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:174` |
| `sgi_customer_ack_date` | Date | Acuse de recibo al cliente | Fecha en que se acusó recibo de la reclamación al cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:56` |
| `sgi_customer_ack_due` | Date | Acusar recibo a más tardar | Fecha límite para acusar recibo al cliente: días hábiles desde que llegó la reclamación (parámetro quimibond_sgi.complaint_ack_days). Se calcula sola. |  |  | compute `_compute_sgi_customer_dues`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:52` |
| `sgi_customer_ack_on_time` | Boolean | Acuse a tiempo | Indica si el acuse al cliente se dio a más tardar en su fecha límite. Se calcula sola. |  |  | compute `_compute_sgi_customer_on_time`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:68` |
| `sgi_customer_deadline` | Date | Plazo pedido por el cliente | Si el cliente fijó su propio plazo de respuesta, sustituye el de la línea. |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:58` |
| `sgi_customer_notice_date` | Date | Aviso escrito al cliente | Fecha del aviso vía Administración de ventas con lotes, cantidades y plan. |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:81` |
| `sgi_customer_notice_note` | Char | Cómo se avisó (referencia) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:84` |
| `sgi_customer_received_date` | Date | Reclamación recibida el | Día en que llegó la reclamación (el del ticket, si lo hay). Desde aquí corren los plazos. |  |  | compute `_compute_sgi_customer_received_date`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:48` |
| `sgi_customer_reply_required` | Boolean | Responde al cliente | Reclamación, scorecard o NC con cliente: se mide el acuse y la respuesta (C5.19). |  |  | compute `_compute_sgi_customer_reply_required`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:40` |
| `sgi_customer_response_date` | Date | Respuesta formal al cliente | Fecha en que se envió al cliente la respuesta formal (causa y acciones). |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:65` |
| `sgi_customer_response_due` | Date | Responder a más tardar | Fecha límite de la respuesta formal: la que pidió el cliente o los días hábiles de su equipo de ventas. Se calcula sola. |  |  | compute `_compute_sgi_customer_dues`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:61` |
| `sgi_customer_response_on_time` | Boolean | Respuesta a tiempo | Indica si la respuesta formal salió a más tardar en su fecha límite. Se calcula sola. |  |  | compute `_compute_sgi_customer_on_time`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:71` |
| `sgi_deadline_overdue` | Boolean | Plazo vencido | Algún plazo de la NC (contención, causa raíz o plan de acción) ya venció sin cumplirse. |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:190` |
| `sgi_deviation` | Text | Desviación detectada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:110` |
| `sgi_document_id` | Many2one | Documento ligado | Documento controlado relacionado con la NC (el que se incumplió o el que hay que cambiar). |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:214` |
| `sgi_due_containment` | Date | Contención vence | Fecha límite para registrar la contención, en días hábiles desde que se abrió la NC. La pone el sistema. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:165` |
| `sgi_due_plan` | Date | Plan de acción vence | Fecha límite para registrar el plan de acción (acción correctiva o preventiva con responsable y compromiso). La pone el sistema. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:171` |
| `sgi_due_root_cause` | Date | Causa raíz vence | Fecha límite para capturar la causa raíz, en días hábiles desde que se abrió la NC. La pone el sistema. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:168` |
| `sgi_effectiveness_by` | Many2one | Eficacia verificada por | Persona que verificó que las acciones fueron eficaces. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:122` |
| `sgi_effectiveness_date` | Date | Fecha de eficacia | Fecha de la verificación de eficacia. Se pide cuando terminan todas las acciones. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:119` |
| `sgi_effectiveness_due` | Date | Verificar eficacia el | Se fija al terminar la última acción correctiva (90 días por omisión) y agenda la verificación al Jefe MAST. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:196` |
| `sgi_effectiveness_note` | Text | Verificación de eficacia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:118` |
| `sgi_external_ref` | Char | N° NCR externo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:154` |
| `sgi_fmea_id` | Many2one | AMEF ligado | AMEF relacionado. Al cerrar una NC mayor, el aviso para actualizarlo se agenda sobre este AMEF. |  | `sgi.fmea` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:211` |
| `sgi_folio` | Char | Folio SGI |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:64` |
| `sgi_followup_action` | Selection | Acción a seguir | Consecuencia para los responsables. «Acción administrativa» pide al Coordinador de RH levantar el acta. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:137` |
| `sgi_followup_comments` | Text | Comentarios de seguimiento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:133` |
| `sgi_incident_id` | Many2one | Incidente SST de origen | Incidente o accidente de seguridad del que nació esta NC. |  | `sgi.incident` |  |  | `addons/quimibond_sgi/models/sgi_incident.py:9` |
| `sgi_indicator_measure_id` | Many2one | Medición de indicador |  |  | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_indicator.py:1412` |
| `sgi_is_recurrent` | Boolean | Reincidente | Se marca sola si el mismo proceso tuvo otra NC en los últimos meses (parámetro quimibond_sgi.nc_recurrence_months, 12 de fábrica). |  |  | compute `_compute_sgi_recurrence`, guardado |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:226` |
| `sgi_ishikawa_notes` | Text | Notas Ishikawa (5-6M) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:117` |
| `sgi_lead_auditor_id` | Many2one | Auditor líder | Auditor líder de la auditoría que detectó la NC. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:101` |
| `sgi_lesson_captured` | Boolean | Lección aplicada a AMEF / plan de control / documento | Confírmelo cuando la lección aprendida de esta NC mayor ya se reflejó en el AMEF, el plan de control y/o el documento controlado correspondiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:127` |
| `sgi_maintenance_request_id` | Many2one | Solicitud de mantenimiento | Solicitud correctiva de la que salió esta NC (S5.06). |  | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_links.py:127` |
| `sgi_norm_clause_id` | Many2one | Requisito (cláusula) | Cláusula de la norma que se incumplió. |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:94` |
| `sgi_origin_type` | Selection | Origen | De dónde viene la NC: proceso, auditoría, reclamación, indicador, incidente u otra fuente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:69` |
| `sgi_plan_state` | Selection | Plan de acción | Plazo del plan de acción: hecha cuando hay acción correctiva o preventiva con responsable y compromiso. Se calcula al mostrarlo. |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:182` |
| `sgi_process_id` | Many2one | Proceso detectado | Proceso en el que se detectó la NC. Su dueño recibe los escalamientos. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:103` |
| `sgi_recurrence_count` | Integer | Reincidencias | Casos previos del mismo proceso en la ventana de reincidencia (misma cláusula cuenta doble). |  |  | compute `_compute_sgi_recurrence`, guardado |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:222` |
| `sgi_requester_id` | Many2one | Solicitante | Persona que levanta la no conformidad. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:97` |
| `sgi_requester_job` | Char | Cargo del solicitante |  |  |  | related `sgi_requester_id.employee_id.job_title`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:99` |
| `sgi_required_capa` | Boolean | ¿Requirió acción correctiva? | Marque si la NC requirió acción correctiva además de la corrección inmediata. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:134` |
| `sgi_responsible_ids` | Many2many | Responsables a contestar | Personas que deben contestar la NC. La ven en Mis pendientes hasta que se cierre. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:106` |
| `sgi_risk_ids` | Many2many | Riesgos ligados | Riesgos del SGI relacionados con esta NC. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:207` |
| `sgi_root_cause` | Text | Causa raíz |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:116` |
| `sgi_root_cause_state` | Selection | Causa raíz (plazo) | Plazo de la causa raíz: hecha cuando está capturada; vencida si pasó la fecha. Se calcula al mostrarlo. |  |  | compute `_compute_sgi_deadline_states`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:178` |
| `sgi_sale_team_id` | Many2one | Línea (equipo de venta) | Del pedido de la reclamación o, si no hay, del último pedido del cliente. Fija el plazo de respuesta. |  | `crm.team` | compute `_compute_sgi_sale_team_id`, guardado |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:43` |
| `sgi_shipped_status` | Selection | ¿Producto ya embarcado? | C5.20: al abrir la NC revisar si hay lotes del mismo origen ya embarcados. |  |  |  |  | `addons/quimibond_sgi/models/sgi_customer_reply.py:75` |
| `sgi_source_id` | Many2one | Fuente automática | Automatismo que levantó esta NC. Vacío si se capturó a mano. |  | `sgi.alert.source` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:83` |
| `sgi_stage_is_cancel` | Boolean |  | Indica si la etapa actual es de cancelación. |  |  | related `stage_id.sgi_is_cancel_stage`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:67` |
| `sgi_stage_is_closing` | Boolean |  | Indica si la etapa actual es de cierre. |  |  | related `stage_id.sgi_is_closing_stage`, sin guardar |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:65` |
| `sgi_supplier_action` | Text | Acción según el proveedor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:43` |
| `sgi_supplier_cause` | Text | Causa según el proveedor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:42` |
| `sgi_supplier_due_date` | Date | Respuesta antes del | Fecha límite para que el proveedor conteste por el portal (días hábiles del parámetro quimibond_sgi.nc_days_supplier_response). |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:35` |
| `sgi_supplier_id` | Many2one | Proveedor | Proveedor responsable de la NC. Se toma del contacto si es proveedor; se puede cambiar. |  | `res.partner` | compute `_compute_sgi_supplier_id`, guardado |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:23` |
| `sgi_supplier_overdue` | Boolean |  | Indica que la NC se envió al proveedor y ya pasó su fecha de respuesta. |  |  | compute `_compute_sgi_supplier_overdue`, sin guardar |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:44` |
| `sgi_supplier_response_date` | Datetime | Contestada el | Fecha y hora en que el proveedor contestó por el portal. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:39` |
| `sgi_supplier_sent_date` | Date | Enviada el | Fecha en que la NC se envió al proveedor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:33` |
| `sgi_supplier_state` | Selection | Respuesta del proveedor | Situación de la respuesta del proveedor: sin enviar, enviada o contestada por el portal. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_nc.py:27` |
| `sgi_verified_by` | Many2one | Verificó | Persona que verifica la NC antes de su aprobación. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:144` |
| `sgi_verified_date` | Date | Fecha de verificación | Fecha de la verificación. |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:146` |
| `sgi_why_1` | Char | ¿Por qué? 1 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:111` |
| `sgi_why_2` | Char | ¿Por qué? 2 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:112` |
| `sgi_why_3` | Char | ¿Por qué? 3 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:113` |
| `sgi_why_4` | Char | ¿Por qué? 4 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:114` |
| `sgi_why_5` | Char | ¿Por qué? 5 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_nonconformity.py:115` |

## Métodos públicos (8)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_cancel` | — |
| `action_sgi_escalate_to_nc` | Escala una alerta operativa de piso a una No Conformidad sistémica del SGI: la mueve al equipo NC Internas, le asigna folio y origen 'proceso', conservando producto/orden/picking. Las alertas rutinar… |
| `action_sgi_force_close` | — |
| `action_sgi_send_to_supplier` | Envía la NC al proveedor por el portal (correo con el enlace). |
| `create` | — |
| `sgi_auto_create` | Punto ÚNICO de entrada de las NC que levanta el sistema. |
| `unlink` | 57.91.0 (K-02): una NC con folio es evidencia (ISO 10.2) y su folio no puede dejar hueco. Se cancela con «Cancelar NC». |
| `write` | — |
