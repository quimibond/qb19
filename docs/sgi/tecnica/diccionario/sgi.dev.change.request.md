<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.change.request`

**Solicitud de modificación del proyecto de desarrollo** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_change.py`.

## Campos (27)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `applied_at` | Datetime | Aplicada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:64` |
| `applied_by_id` | Many2one | Aplicó los cambios |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:63` |
| `approved_at` | Datetime | Firmada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:62` |
| `approved_by_id` | Many2one | Aprobó (Dirección de Operaciones) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:61` |
| `attachment_id` | Many2one | PDF firmado |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:65` |
| `cause` | Text | Por qué no se obtuvo el resultado | Causa observada. Los 5 porqués llevan a la causa raíz. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:47` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:37` |
| `date` | Date | Fecha |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:40` |
| `description` | Text | Qué se modifica | El cambio al producto o al proceso, en palabras del cliente o del resultado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:46` |
| `dev_stage_ids` | Many2many |  |  |  | `project.project.stage` | compute `_compute_dev_stage_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_change.py:57` |
| `name` | Char | Solicitud |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_change.py:33` |
| `origin` | Selection | Motivo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:41` |
| `partner_id` | Many2one | Cliente |  |  |  | related `project_id.partner_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_change.py:36` |
| `project_id` | Many2one | Desarrollo |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:34` |
| `requested_by_id` | Many2one | Elaboró (Diseño de Producto) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:58` |
| `revision` | Integer | Revisión que abre | Revisión del desarrollo que queda abierta con esta modificación. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:44` |
| `root_cause` | Text | Causa raíz |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:53` |
| `sequence_number` | Integer | Número | Consecutivo dentro del desarrollo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:38` |
| `shipment_id` | Many2one | Respuesta del cliente que la origina |  |  | `sgi.dev.shipment` |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:42` |
| `solution` | Text | Solución | Qué se va a cambiar para obtener el resultado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:54` |
| `stage_ids` | Many2many | Fases afectadas |  |  | `project.project.stage` |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:55` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:60` |
| `why_1` | Char | ¿Por qué? 1 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:48` |
| `why_2` | Char | ¿Por qué? 2 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:49` |
| `why_3` | Char | ¿Por qué? 3 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:50` |
| `why_4` | Char | ¿Por qué? 4 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:51` |
| `why_5` | Char | ¿Por qué? 5 |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_change.py:52` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_apply` | — |
| `action_approve` | Firma de Dirección de Operaciones: abre la revisión del desarrollo si la respuesta del cliente no la abrió ya, regresa el proyecto a «Muestra» y guarda el PDF. |
| `action_back_to_draft` | — |
| `action_print` | — |
| `create` | — |
| `unlink` | — |
