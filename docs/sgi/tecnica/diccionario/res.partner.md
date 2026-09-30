<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `res.partner`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_supplier_eval.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_coa_recipient_ids` | Many2many | Reciben el COA | Contactos a quienes se manda el COA. Vacío: el contacto de la entrega. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:59` |
| `sgi_complaint_count` | Integer | # Reclamaciones |  |  |  | compute `_compute_sgi_partner_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:304` |
| `sgi_eval_count` | Integer | # Evaluaciones |  |  |  | compute `_compute_sgi_eval_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:49` |
| `sgi_eval_ids` | One2many | Evaluaciones SGI |  |  | `sgi.supplier.eval` |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:48` |
| `sgi_last_eval_date` | Date | Última evaluación | Fecha de la última evaluación de desempeño del proveedor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:46` |
| `sgi_nc_count` | Integer | # NC |  |  |  | compute `_compute_sgi_partner_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:305` |
| `sgi_ppap_count` | Integer | PPAP |  |  |  | compute `_compute_sgi_ppap_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_ppap.py:184` |
| `sgi_requires_coa` | Boolean | Requiere COA en cada embarque | Cada salida a este cliente (o a sus direcciones de entrega) debe llevar el certificado de análisis adjunto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:55` |
| `sgi_supplier_approved_by` | Many2one | Aprobado por | Quién aprobó al proveedor. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:34` |
| `sgi_supplier_approved_date` | Date | Fecha de aprobación | Fecha en que se aprobó al proveedor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:37` |
| `sgi_supplier_class` | Selection | Clasificación SGI | Sin datos: en el periodo evaluado no hubo recepciones con fecha compromiso ni NC; no hay con qué calificarlo (no es «Baja»). |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:20` |
| `sgi_supplier_critical` | Boolean | Proveedor crítico | Materia prima o maquila: entra a la evaluación trimestral aunque en el periodo no haya comprado productos de las categorías críticas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:40` |
| `sgi_supplier_score` | Float | Calificación SGI | Calificación de la última evaluación trimestral del proveedor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:44` |
| `sgi_supplier_status` | Selection | Aprobación SGI (8.4.1) | Aprobación inicial del proveedor (9001 8.4.1). Vacío: fuera del SGI. Bloqueado: no se pueden confirmar órdenes de compra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:27` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_approve_supplier` | — |
| `action_sgi_block_supplier` | — |
| `action_sgi_open_complaints` | — |
| `action_sgi_open_evals` | — |
| `action_sgi_open_ncs` | — |
| `action_view_sgi_ppap` | — |
| `write` | — |
