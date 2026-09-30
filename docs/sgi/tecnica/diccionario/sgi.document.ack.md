<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.document.ack`

**Acuse de lectura de documento SGI** (Model).

Acuse de lectura de un documento controlado por empleado. Nace al publicar el documento para los puestos aplicables; se cierra al leer o al firmar en Sign.

Orden: `document_id, employee_id`.

Archivos: `addons/quimibond_sgi/models/sgi_document.py`, `addons/quimibond_sgi/models/sgi_sign_elearning.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `ack_date` | Datetime | Fecha de acuse |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:1254` |
| `document_id` | Many2one | Documento |  | sí | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_document.py:1246` |
| `employee_id` | Many2one | Empleado |  | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_document.py:1248` |
| `sgi_code` | Char | Clave |  |  |  | related `document_id.sgi_code`, guardado |  | `addons/quimibond_sgi/models/sgi_document.py:1247` |
| `sign_request_id` | Many2one | Solicitud de firma |  |  | `sign.request` |  |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:123` |
| `sign_state` | Selection | Firma electrónica |  |  |  | related `sign_request_id.state`, guardado |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:128` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_document.py:1250` |
| `user_id` | Many2one | Usuario |  |  | `res.users` | related `employee_id.user_id`, guardado |  | `addons/quimibond_sgi/models/sgi_document.py:1249` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_read` | — |
| `action_view_file` | Leer antes de firmar: abre el PDF/enlace del documento del acuse. |
| `create` | — |
| `write` | — |
