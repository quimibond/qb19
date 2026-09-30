<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.deliverable`

**Entregable SGI (lo que pasa de una actividad a otra)** (Model).

Entregable: lo que una actividad produce y otra recibe. Si apunta a un modelo de Odoo con dominio, la actividad se mide sola (``complete_domain`` y campos de fecha y usuario).

Orden: `name`.

Archivos: `addons/quimibond_sgi/models/sgi_deliverable.py`, `addons/quimibond_sgi/models/sgi_activity_spec.py`, `addons/quimibond_sgi/models/sgi_deliverable_models.py`, `addons/quimibond_sgi/models/sgi_sign_record.py`.

## Campos (23)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `acceptance_criteria` | Text | Criterio de aceptación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:117` |
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:118` |
| `boundary` | Selection | Frontera del mapa | Entrada externa: llega de fuera del mapa (cliente, SAT, proveedor, una falla); nadie del mapa la entrega. Salida final: sale del mapa; nadie del mapa la recibe. Marcado así, no es un cabo suelto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:148` |
| `code` | Char | Código | Clave corta para la carga por API (C2-PEDIDO, C3-PROGRAMA…). |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:90` |
| `company_id` | Many2one | Empresa |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:119` |
| `complete_criteria` | Text | Completo significa | Lo mismo en palabras; se imprime en el procedimiento. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:696` |
| `complete_domain` | Char | Filtro: está completo | Además de existir, cuándo cuenta como completo, ej. [('client_order_ref', '!=', False), ('commitment_date', '!=', False)]. Vacío: lo que existe cuenta como completo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:691` |
| `consumer_activity_ids` | Many2many | Actividades que lo reciben |  |  | `sgi.process.activity` | compute `_compute_consumers`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:128` |
| `consumer_process_ids` | Many2many | Procesos que lo reciben | Procesos cuyas actividades reciben este entregable. |  | `sgi.process` | compute `_compute_processes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:135` |
| `document_id` | Many2one | Formato | Formato controlado en el que viaja, si aplica. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:95` |
| `input_line_ids` | One2many | Lo reciben |  |  | `sgi.activity.input` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:127` |
| `measure_date_field` | Char | Campo de fecha | Cuándo quedó entregado: date_order, date_done, invoice_date… |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:110` |
| `measure_domain` | Char | Filtro: ya está entregado | Qué registros cuentan como entregados, ej. [('state', '=', 'sale')]. |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:107` |
| `measure_user_field` | Char | Campo de usuario | Quién lo entregó (create_uid, user_id…): mide si lo hizo el puesto que debía. |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:113` |
| `measured_activity_ids` | One2many | Se miden con él |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:130` |
| `name` | Char | Entregable | Qué pasa de un paso al otro: «Pedido confirmado». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:93` |
| `odoo_model_id` | Many2one | Modelo de Odoo | Dónde vive el entregable en Odoo (sale.order, stock.picking…). Con modelo, la actividad que lo entrega se puede medir con él. |  | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:99` |
| `odoo_model_name` | Char | Modelo técnico |  |  |  | related `odoo_model_id.model`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:103` |
| `orphan` | Selection | Cabo suelto | Un entregable que alguien recibe pero nadie entrega (o al revés) es un hueco en la cadena, salvo que esté marcado como frontera. |  |  | compute `_compute_processes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:138` |
| `producer_activity_ids` | Many2many | Lo entregan | Actividades que producen este entregable. |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:123` |
| `producer_process_ids` | Many2many | Procesos que lo entregan | Procesos cuyas actividades producen este entregable. |  | `sgi.process` | compute `_compute_processes`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:132` |
| `require_signed` | Boolean | Exige firmado | Cuenta como completo solo si el registro tiene una solicitud de Sign firmada ligada a él (REG-1). |  |  |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:174` |
| `survey_id` | Many2one | Encuesta | El entregable es una respuesta terminada de esta encuesta (REG-2). |  | `survey.survey` |  |  | `addons/quimibond_sgi/models/sgi_sign_record.py:177` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_links` | — |
| `create` | — |
| `unlink` | — |
| `write` | — |
