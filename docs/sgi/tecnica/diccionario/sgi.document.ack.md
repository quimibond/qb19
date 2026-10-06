<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.document.ack`

**Acuse de lectura de documento SGI** (Model). Hereda de: `sgi.pin.signature.mixin`.

Acuse de lectura de un documento controlado por empleado. Nace al publicar el documento para los puestos aplicables; se cierra al leer o al firmar en Sign.

Orden: `document_id, employee_id`.

Archivos: `addons/quimibond_sgi/models/sgi_document.py`, `addons/quimibond_sgi/models/sgi_sign_elearning.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `ack_date` | Datetime | Fecha de acuse | Fecha y hora en que la persona firmó de leído. |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:1406` |
| `document_id` | Many2one | Documento | Documento que se debe leer. | sí | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_document.py:1394` |
| `employee_id` | Many2one | Empleado | Persona que debe leer el documento. | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_document.py:1397` |
| `sgi_code` | Char | Clave |  |  |  | related `document_id.sgi_code`, guardado |  | `addons/quimibond_sgi/models/sgi_document.py:1396` |
| `sign_request_id` | Many2one | Solicitud de firma |  |  | `sign.request` |  |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:125` |
| `sign_state` | Selection | Firma electrónica |  |  |  | related `sign_request_id.state`, guardado |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:130` |
| `state` | Selection | Estado | Pendiente hasta que la persona firma «Leído y entendido». Los pendientes aparecen en Mis pendientes. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:1400` |
| `user_id` | Many2one | Usuario |  |  | `res.users` | related `employee_id.user_id`, guardado |  | `addons/quimibond_sgi/models/sgi_document.py:1399` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_read` | — |
| `action_view_file` | Leer antes de firmar: abre el PDF/enlace del documento del acuse. |
| `create` | — |
| `write` | — |
