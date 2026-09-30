<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `approval.request`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_doc_change.py`, `addons/quimibond_sgi/models/sgi_doc_change_sign.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_mp_change.py`.

## Campos (28)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_activity_id` | Many2one | Actividad del procedimiento | Actividad de «Mi procedimiento» a la que se propone el cambio. |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:67` |
| `sgi_affected_process_ids` | Many2many | Procesos afectados |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:48` |
| `sgi_applied` | Boolean | Cambio aplicado al documento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:49` |
| `sgi_change_attachment_id` | Many2one | Archivo de la revisión nueva | El archivo que se mandó a firmar: es el que se publica al aprobarse. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:61` |
| `sgi_change_kind` | Selection | Tipo de cambio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:31` |
| `sgi_changes` | Text | Descripción de cambios |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:47` |
| `sgi_current_revision` | Integer | Revisión vigente |  |  |  | related `sgi_document_id.sgi_revision`, sin guardar |  | `addons/quimibond_sgi/models/sgi_doc_change.py:40` |
| `sgi_document_id` | Many2one | Documento afectado |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:30` |
| `sgi_is_doc_change` | Boolean |  |  |  |  | related `category_id.sgi_is_doc_change`, guardado |  | `addons/quimibond_sgi/models/sgi_doc_change.py:24` |
| `sgi_is_moc` | Boolean |  |  |  |  | related `category_id.sgi_is_moc`, guardado |  | `addons/quimibond_sgi/models/sgi_doc_change.py:25` |
| `sgi_moc_risk_note` | Text | Evaluación de riesgos del cambio | 45001 §8.1.3 / 9001 §6.3: riesgos del cambio para calidad, ambiente y SST, y cómo se controlan. Obligatoria para aprobar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:26` |
| `sgi_mp_apply_scheduled` | Boolean | Cambio aplicado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:75` |
| `sgi_mp_change_type` | Selection | Tipo de propuesta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:70` |
| `sgi_mp_diff_html` | Html | Qué cambia |  |  |  | related `sgi_mp_proposal_id.diff_snapshot`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change.py:73` |
| `sgi_mp_proposal_id` | Many2one | Propuesta |  |  | `sgi.activity.change` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:71` |
| `sgi_new_document_id` | Many2one | Revisión publicada |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:52` |
| `sgi_new_revision` | Integer | Nueva revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:42` |
| `sgi_pilot` | Boolean | Prueba piloto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:43` |
| `sgi_pilot_end` | Date | Fin de piloto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:45` |
| `sgi_pilot_start` | Date | Inicio de piloto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:44` |
| `sgi_reason` | Text | Motivo del cambio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:46` |
| `sgi_sign_archived` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:64` |
| `sgi_sign_notified_state` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:65` |
| `sgi_sign_progress` | Char | Firmas |  |  |  | compute `_compute_sgi_sign_progress`, sin guardar |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:59` |
| `sgi_sign_request_id` | Many2one | Firma (Sign) |  |  | `sign.request` |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:55` |
| `sgi_sign_required` | Boolean |  |  |  |  | related `category_id.sgi_sign_required`, sin guardar |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:54` |
| `sgi_sign_state` | Selection | Estado de la firma |  |  |  | related `sgi_sign_request_id.state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:57` |
| `sgi_what_changes` | Selection | ¿Qué se modifica? |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:36` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_approve` | — |
| `action_confirm` | — |
| `action_create_purchase_orders` | approvals_purchase crea las órdenes desde las líneas; aquí se les deja escrita la requisición de la que salieron. |
| `action_sgi_create_document` | Alta documental aprobada: abre el formulario del documento nuevo con el contexto que lo liga de vuelta a esta solicitud (trazabilidad del alta — antes el documento se creaba suelto en la app Document… |
| `action_sgi_send_to_sign` | «Reenviar a firma»: la firma anterior se canceló o venció. |
| `create` | — |
| `write` | — |
