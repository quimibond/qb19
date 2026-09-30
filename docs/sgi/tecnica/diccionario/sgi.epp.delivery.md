<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.epp.delivery`

**Responsiva de entrega de EPP (S03-02)** (Model). Hereda de: `mail.thread`.

Responsiva de entrega de EPP a un empleado (S03-02), con renglones y firma en Sign.

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_epp.py`, `addons/quimibond_sgi/models/sgi_epp_sign.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `can_sign` | Boolean |  |  |  |  | compute `_compute_can_sign`, sin guardar |  | `addons/quimibond_sgi/models/sgi_epp.py:42` |
| `date` | Date | Fecha de entrega |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:30` |
| `delivered_by_id` | Many2one | Entregó |  | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_epp.py:34` |
| `employee_id` | Many2one | Empleado |  | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_epp.py:23` |
| `items` | Text |  |  |  |  | compute `_compute_items`, guardado |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:39` |
| `job_id` | Many2one | Puesto al entregar |  |  | `hr.job` | compute `_compute_job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_epp.py:28` |
| `line_ids` | One2many | Renglones de EPP |  |  | `sgi.epp.delivery.line` |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:38` |
| `name` | Char | Folio |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:22` |
| `note` | Text | Observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:36` |
| `sign_request_id` | Many2one | Solicitud de firma (Sign) |  |  | `sign.request` |  |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:40` |
| `sign_state` | Selection | Firma electrónica |  |  |  | related `sign_request_id.state`, guardado |  | `addons/quimibond_sgi/models/sgi_epp_sign.py:42` |
| `signed_date` | Datetime | Firmada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:41` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_epp.py:37` |
| `user_id` | Many2one | Usuario |  |  |  | related `employee_id.user_id`, guardado |  | `addons/quimibond_sgi/models/sgi_epp.py:27` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_sign_request` | — |
| `action_send_sign_request` | Crea la solicitud de firma para el empleado (una viva por responsiva). |
| `action_sign` | El empleado firma desde su ficha o desde Mi procedimiento. |
| `create` | — |
| `sgi_format_info` | — |
| `write` | — |
