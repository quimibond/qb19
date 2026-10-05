<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `approval.request`

Modelo de otra app que el SGI extiende.

La solicitud de cambio del MIID es una solicitud de cambio documental de siempre con la huella y la foto de los datos con que se generó su PDF.

Archivos: `addons/quimibond_sgi/models/sgi_doc_change.py`, `addons/quimibond_sgi/models/sgi_doc_change_sign.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_miid.py`, `addons/quimibond_sgi/models/sgi_mp_change.py`.

## Campos (32)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_activity_id` | Many2one | Actividad del procedimiento | Actividad de «Mi procedimiento» a la que se propone el cambio. |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:67` |
| `sgi_affected_process_ids` | Many2many | Procesos afectados | Procesos a los que afecta el cambio del documento. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:62` |
| `sgi_applied` | Boolean | Cambio aplicado al documento | Se marca cuando el cambio aprobado ya se aplicó al documento (nueva revisión). |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:64` |
| `sgi_change_attachment_id` | Many2one | Archivo de la revisión nueva | El archivo que se mandó a firmar: es el que se publica al aprobarse. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:64` |
| `sgi_change_kind` | Selection | Tipo de cambio | Alta de un documento nuevo, modificación de uno existente o baja. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:38` |
| `sgi_changes` | Text | Descripción de cambios |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:61` |
| `sgi_current_revision` | Integer | Revisión vigente | Revisión vigente del documento antes del cambio. |  |  | related `sgi_document_id.sgi_revision`, sin guardar |  | `addons/quimibond_sgi/models/sgi_doc_change.py:49` |
| `sgi_document_id` | Many2one | Documento afectado | Documento controlado que se modifica o se da de baja. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:36` |
| `sgi_is_doc_change` | Boolean |  | Se marca sola cuando la categoría es de cambio documental del SGI. |  |  | related `category_id.sgi_is_doc_change`, guardado |  | `addons/quimibond_sgi/models/sgi_doc_change.py:27` |
| `sgi_is_moc` | Boolean |  | Se marca sola cuando la categoría es de gestión del cambio. |  |  | related `category_id.sgi_is_moc`, guardado |  | `addons/quimibond_sgi/models/sgi_doc_change.py:30` |
| `sgi_miid_blocked_note` | Text | Último aviso de candados del MIID | Lo que detiene la aprobación del MIID aunque las firmas estén completas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:1017` |
| `sgi_miid_generated` | Datetime | MIID generado el | Cuándo se generó el PDF del MIID que lleva esta solicitud. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:1015` |
| `sgi_miid_hash` | Char | Huella del MIID | Huella de los datos con que se generó el PDF del MIID de esta solicitud. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:1011` |
| `sgi_miid_snapshot` | Text | Datos del MIID | Foto de los datos con que se generó el PDF del MIID. |  |  |  |  | `addons/quimibond_sgi/models/sgi_miid.py:1013` |
| `sgi_moc_risk_note` | Text | Evaluación de riesgos del cambio | 45001 §8.1.3 / 9001 §6.3: riesgos del cambio para calidad, ambiente y SST, y cómo se controlan. Obligatoria para aprobar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:32` |
| `sgi_mp_apply_scheduled` | Boolean | Cambio aplicado | Se marca cuando la propuesta aprobada ya se aplicó a la actividad. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:78` |
| `sgi_mp_change_type` | Selection | Tipo de propuesta | Qué propone la persona: agregar una actividad, cambiar esta o quitarla. |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:70` |
| `sgi_mp_diff_html` | Html | Qué cambia |  |  |  | related `sgi_mp_proposal_id.diff_snapshot`, sin guardar |  | `addons/quimibond_sgi/models/sgi_mp_change.py:76` |
| `sgi_mp_proposal_id` | Many2one | Propuesta | Propuesta de cambio a la actividad que se aprueba con esta solicitud. |  | `sgi.activity.change` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:73` |
| `sgi_new_document_id` | Many2one | Revisión publicada | Documento de la revisión nueva, cuando el cambio trajo el archivo. La revisión anterior queda obsoleta. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:69` |
| `sgi_new_revision` | Integer | Nueva revisión | Número de la revisión que tendrá el documento al aplicar el cambio. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:52` |
| `sgi_pilot` | Boolean | Prueba piloto | Marque si el cambio se prueba primero en piloto antes de quedar vigente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:55` |
| `sgi_pilot_end` | Date | Fin de piloto | Fecha en que termina la prueba piloto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:59` |
| `sgi_pilot_start` | Date | Inicio de piloto | Fecha en que empieza la prueba piloto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:58` |
| `sgi_reason` | Text | Motivo del cambio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:60` |
| `sgi_sign_archived` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:67` |
| `sgi_sign_notified_state` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:68` |
| `sgi_sign_progress` | Char | Firmas |  |  |  | compute `_compute_sgi_sign_progress`, sin guardar |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:62` |
| `sgi_sign_request_id` | Many2one | Firma (Sign) | Solicitud de firma en Sign ligada a esta aprobación. |  | `sign.request` |  |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:56` |
| `sgi_sign_required` | Boolean |  | Indica si la categoría exige firma en Sign para aprobar. |  |  | related `category_id.sgi_sign_required`, sin guardar |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:54` |
| `sgi_sign_state` | Selection | Estado de la firma | Estado de la firma en Sign. |  |  | related `sgi_sign_request_id.state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_doc_change_sign.py:59` |
| `sgi_what_changes` | Selection | ¿Qué se modifica? | Si cambia solo el formato (presentación) o el contenido del documento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_doc_change.py:44` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_approve` | Candados del MIID (Q16, Q17). Con el botón: error claro. Desde Sign (``sgi_sign_sync``; el cron diario no tiene savepoint por solicitud): no se levanta nada, la solicitud del MIID se salta, se anota … |
| `action_confirm` | — |
| `action_create_purchase_orders` | approvals_purchase crea las órdenes desde las líneas; aquí se les deja escrita la requisición de la que salieron. |
| `action_sgi_create_document` | Alta documental aprobada: abre el formulario del documento nuevo con el contexto que lo liga de vuelta a esta solicitud (trazabilidad del alta — antes el documento se creaba suelto en la app Document… |
| `action_sgi_send_to_sign` | «Reenviar a firma»: la firma anterior se canceló o venció. |
| `create` | — |
| `write` | — |
