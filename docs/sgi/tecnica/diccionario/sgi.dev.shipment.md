<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.shipment`

**Envío de muestra de desarrollo al cliente** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_shipment.py`, `addons/quimibond_sgi/models/sgi_dev_coa.py`.

## Campos (37)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_ids` | Many2many | Documentos que acompañan | CoA del lote y lo demás que viaja con la muestra; se adjuntan al correo. |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:104` |
| `carrier_id` | Many2one | Paquetería | De la lista «Paquetería» (Ajustes → SGI → Listas del desarrollo de producto). |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:93` |
| `coa_count` | Integer |  |  |  |  | compute `_compute_coa_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:249` |
| `coa_ids` | One2many | Reportes de conformidad |  |  | `sgi.dev.coa` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:248` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:81` |
| `date_shipped` | Date | Fecha de envío |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:96` |
| `followup_date` | Datetime | Último seguimiento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:111` |
| `medium` | Selection | Medio de entrega |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:90` |
| `medium_label` | Char | Medio (texto) | Etiqueta del medio para el correo al cliente. |  |  | compute `_compute_medium_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:91` |
| `name` | Char | Envío |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:76` |
| `notified_by_id` | Many2one | Aviso enviado por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:110` |
| `notified_date` | Datetime | Aviso al cliente enviado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:109` |
| `partner_id` | Many2one | Cliente |  |  |  | related `project_id.partner_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:80` |
| `picking_id` | Many2one | Baja de almacén | Traslado de salida (Baja de Muestras) creado desde este envío. |  | `stock.picking` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:97` |
| `picking_state` | Selection | Estado de la baja |  |  |  | related `picking_id.state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:99` |
| `product_id` | Many2one | Artículo | El artículo de la orden o, si no hay orden, el del proyecto. |  | `product.product` | compute `_compute_product_id`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:85` |
| `production_id` | Many2one | Orden de muestra | Corrida de la que salen los rollos. |  | `mrp.production` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:82` |
| `project_id` | Many2one | Desarrollo | Proyecto de desarrollo cuya muestra se envía. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:77` |
| `reject_reason_id` | Many2one | Motivo del rechazo | De la lista «Motivo de rechazo del cliente». |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:124` |
| `response` | Selection | Respuesta del cliente |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:113` |
| `response_contact_id` | Many2one | Quién respondió |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:116` |
| `response_date` | Date | Fecha de la respuesta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:114` |
| `response_file` | Binary | Evidencia de la respuesta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:119` |
| `response_filename` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:120` |
| `response_medium` | Selection | Medio de la respuesta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:115` |
| `response_note` | Text | Cambios que pide | Solo si pide cambios: qué ajusta; la especificación se corrige en la tabla del proyecto y queda como revisión. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:121` |
| `response_ref` | Char | Referencia de la respuesta | Asunto del correo, número de orden de compra… |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:117` |
| `response_registered_at` | Datetime | Respuesta registrada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:127` |
| `response_registered_by_id` | Many2one | Respuesta registrada por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:128` |
| `roll_count` | Integer | Rollos |  |  |  | compute `_compute_totals`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:101` |
| `roll_ids` | One2many | Rollos avisados |  |  | `sgi.dev.shipment.roll` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:100` |
| `shipped_at` | Datetime | Envío registrado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:107` |
| `shipped_by_id` | Many2one | Envío registrado por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:108` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:87` |
| `total_kg` | Float | Kilos |  |  |  | compute `_compute_totals`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:103` |
| `total_m` | Float | Metros |  |  |  | compute `_compute_totals`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:102` |
| `tracking_ref` | Char | Guía | Número de guía de la paquetería. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_shipment.py:95` |

## Métodos públicos (8)

| Método | Qué hace (docstring) |
|---|---|
| `action_create_picking` | Crea la baja (salida) con el artículo y la cantidad de los rollos; se valida en Inventario. |
| `action_open_mail` | Correo listo con rollos, medio y guía; lo manda Administración de Ventas. |
| `action_open_picking` | — |
| `action_register_response` | — |
| `action_sgi_dev_coa` | Un certificado por lote de los rollos avisados (uno solo si no llevan lote), con los metros y rollos de ese lote; abre el que falta por emitir o la lista. |
| `action_ship` | Registra el envío. Si hay baja ligada, se registra al validarla; aquí solo si ya está hecha. |
| `cron_sgi_dev_shipment_followup` | — |
| `message_post` | — |
