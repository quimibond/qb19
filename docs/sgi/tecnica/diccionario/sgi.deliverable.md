<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.deliverable`

**Entregable SGI (lo que pasa de una actividad a otra)** (Model).

Orden: `name`.

Archivos: `addons/quimibond_sgi/models/sgi_deliverable.py`, `addons/quimibond_sgi/models/sgi_activity_spec.py`, `addons/quimibond_sgi/models/sgi_sign_record.py`.

## Campos (23)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `acceptance_criteria` | Text | Criterio de aceptación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:114` |
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:115` |
| `boundary` | Selection | Frontera del mapa | Entrada externa: llega de fuera del mapa (cliente, SAT, proveedor, una falla); nadie del mapa la entrega. Salida final: sale del mapa; nadie del mapa la recibe. Marcado así, no es un cabo suelto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:142` |
| `code` | Char | Código | Clave corta para la carga por API (C2-PEDIDO, C3-PROGRAMA…). |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:87` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:116` |
| `complete_criteria` | Text | Completo significa | Lo mismo en palabras; se imprime en el procedimiento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:689` |
| `complete_domain` | Char | Filtro: está completo | Además de existir, cuándo cuenta como completo, ej. [('client_order_ref', '!=', False), ('commitment_date', '!=', False)]. Vacío: lo que existe cuenta como completo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:684` |
| `consumer_activity_ids` | Many2many | Actividades que lo reciben |  |  | `sgi.process.activity` | compute `_compute_consumers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:124` |
| `consumer_process_ids` | Many2many | Procesos que lo reciben |  |  | `sgi.process` | compute `_compute_processes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:130` |
| `document_id` | Many2one | Formato | Formato controlado en el que viaja, si aplica. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:92` |
| `input_line_ids` | One2many | Lo reciben |  |  | `sgi.activity.input` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:123` |
| `measure_date_field` | Char | Campo de fecha | Cuándo quedó entregado: date_order, date_done, invoice_date… |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:107` |
| `measure_domain` | Char | Filtro: ya está entregado | Qué registros cuentan como entregados, ej. [('state', '=', 'sale')]. |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:104` |
| `measure_user_field` | Char | Campo de usuario | Quién lo entregó (create_uid, user_id…): mide si lo hizo el puesto que debía. |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:110` |
| `measured_activity_ids` | One2many | Se miden con él |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:126` |
| `name` | Char | Entregable | Qué pasa de un paso al otro: «Pedido confirmado». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:90` |
| `odoo_model_id` | Many2one | Modelo de Odoo | Dónde vive el entregable en Odoo (sale.order, stock.picking…). Con modelo, la actividad que lo entrega se puede medir con él. |  | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:96` |
| `odoo_model_name` | Char | Modelo técnico |  |  |  | related `odoo_model_id.model`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:100` |
| `orphan` | Selection | Cabo suelto | Un entregable que alguien recibe pero nadie entrega (o al revés) es un hueco en la cadena, salvo que esté marcado como frontera. |  |  | compute `_compute_processes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:132` |
| `producer_activity_ids` | Many2many | Lo entregan |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:120` |
| `producer_process_ids` | Many2many | Procesos que lo entregan |  |  | `sgi.process` | compute `_compute_processes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:128` |
| `require_signed` | Boolean | Exige firmado | Cuenta como completo solo si el registro tiene una solicitud de Sign firmada ligada a él (REG-1). |  |  |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:170` |
| `survey_id` | Many2one | Encuesta | El entregable es una respuesta terminada de esta encuesta (REG-2). |  | `survey.survey` |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:173` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_links` | — |
| `create` | — |
| `unlink` | — |
| `write` | — |
