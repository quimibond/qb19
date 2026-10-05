<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `stock.picking`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_kpi_sales.py`, `addons/quimibond_sgi/models/sgi_links.py`, `addons/quimibond_sgi/models/sgi_release.py`, `addons/quimibond_sgi/models/sgi_sign_record.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_acuse_attachment_ids` | One2many | Acuses firmados |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_links.py:312` |
| `sgi_acuse_count` | Integer | Acuses |  |  |  | compute `_compute_sgi_acuse_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_links.py:314` |
| `sgi_coa_attachment_ids` | Many2many | CoA | Certificados de análisis del embarque (uno por producto). |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:114` |
| `sgi_coa_date` | Datetime | CoA adjuntado | Fecha y hora en que se adjuntó el CoA. |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:118` |
| `sgi_coa_sent_date` | Datetime | CoA enviado al cliente | Fecha y hora en que se envió el CoA al cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:123` |
| `sgi_coa_status` | Selection | Estado del CoA | No aplica, pendiente, adjunto o enviado. Se calcula solo. |  |  | compute `_compute_sgi_coa_status`, guardado |  | `addons/quimibond_sgi/models/sgi_coa.py:126` |
| `sgi_coa_uid` | Many2one | CoA adjuntado por | Quién adjuntó el CoA. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:120` |
| `sgi_delivery_picking_id` | Many2one | Entrega que surte | Orden de entrega del mismo pedido que este traslado interno surte: la abierta con la fecha programada más próxima, o la última si todas están hechas. Se puede corregir a mano. |  | `stock.picking` | compute `_compute_sgi_delivery_picking`, guardado |  | `addons/quimibond_sgi/models/sgi_release.py:49` |
| `sgi_export_crossing_datetime` | Datetime | Cruce (exportación) | Fecha y hora en que la mercancía cruzó la frontera (C2-05). |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_sales.py:51` |
| `sgi_export_file_closed_date` | Date | Expediente cerrado | Fecha en que quedó completo el expediente de exportación (C2-05). |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_sales.py:54` |
| `sgi_nc_count` | Integer | # NC |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:11` |
| `sgi_production_id` | Many2one | Orden de producción | Orden de producción de la que sale este traspaso (C4.19). Se propone desde los movimientos o el documento origen; se puede fijar a mano. |  | `mrp.production` | compute `_compute_sgi_production_id`, guardado |  | `addons/quimibond_sgi/models/sgi_links.py:307` |
| `sgi_quality_approved_at` | Datetime | Aprobado por Calidad | Última aprobación de un control de calidad de este traslado (C5-03). |  |  | compute `_compute_sgi_quality_approved`, guardado |  | `addons/quimibond_sgi/models/sgi_kpi_sales.py:57` |
| `sgi_release_hours` | Float | Horas hasta la aprobación | Horas entre la entrega de Inspección (creación del traslado) y la aprobación de Calidad (C5-03). |  |  | compute `_compute_sgi_quality_approved`, guardado |  | `addons/quimibond_sgi/models/sgi_kpi_sales.py:60` |
| `sgi_requires_coa` | Boolean | Requiere CoA | Salida a un cliente que pide certificado de análisis en cada embarque. |  |  | compute `_compute_sgi_requires_coa`, guardado |  | `addons/quimibond_sgi/models/sgi_coa.py:104` |
| `sgi_return_alert_id` | Many2one | NC de devolución |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_integration.py:32` |
| `sgi_seal_number` | Char | Sello de embarque | Número de sello del transporte. Se imprime en la remisión. Sustituye F-IT-P-A07-01-07/08. |  |  |  |  | `addons/quimibond_sgi/models/sgi_integration.py:37` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_attach_acuse` | — |
| `action_sgi_attach_coa` | — |
| `action_sgi_open_acuses` | — |
| `action_sgi_open_ncs` | — |
| `button_validate` | — |
