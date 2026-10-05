<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.epp.delivery`

**Responsiva de entrega de EPP (S03-02)** (Model). Hereda de: `mail.thread`, `sgi.pin.signature.mixin`.

Responsiva de entrega de EPP a un empleado (S03-02), con renglones y firma en Sign.

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_epp.py`, `addons/quimibond_sgi/models/sgi_epp_sign.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `can_sign` | Boolean |  | Indica si usted puede firmar esta responsiva. |  |  | compute `_compute_can_sign`, sin guardar |  | `addons/quimibond_sgi/models/sgi_epp.py:49` |
| `date` | Date | Fecha de entrega | Fecha en que se entregó el equipo. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:33` |
| `delivered_by_id` | Many2one | Entregó | Persona que entregó el equipo. | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_epp.py:38` |
| `employee_id` | Many2one | Empleado | Empleado que recibe el equipo de protección. | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_epp.py:24` |
| `items` | Text |  |  |  |  | compute `_compute_items`, guardado |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:39` |
| `job_id` | Many2one | Puesto al entregar | Puesto del empleado al momento de la entrega. |  | `hr.job` | compute `_compute_job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_epp.py:30` |
| `line_ids` | One2many | Renglones de EPP |  |  | `sgi.epp.delivery.line` |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:38` |
| `name` | Char | Folio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:23` |
| `note` | Text | Observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:41` |
| `sign_request_id` | Many2one | Solicitud de firma (Sign) | Solicitud de firma electrónica de la responsiva. |  | `sign.request` |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:40` |
| `sign_state` | Selection | Firma electrónica | Estado de la firma electrónica. |  |  | related `sign_request_id.state`, guardado |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:43` |
| `signed_date` | Datetime | Firmada el | Fecha y hora en que el empleado firmó. |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:47` |
| `state` | Selection | Estado | Entregada sin firmar hasta que el empleado firma la responsiva. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:42` |
| `user_id` | Many2one | Usuario |  |  |  | related `employee_id.user_id`, guardado |  | `addons/quimibond_sgi/models/sgi_epp.py:29` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_sign_request` | — |
| `action_send_sign_request` | Crea la solicitud de firma para el empleado (una viva por responsiva). |
| `action_sign` | El empleado firma desde su ficha o desde Mi procedimiento. |
| `create` | — |
| `sgi_format_info` | — |
| `write` | — |
