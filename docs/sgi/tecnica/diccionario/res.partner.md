<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `res.partner`

Modelo de otra app que el SGI extiende.

57.96.0 (N-06, 45001 8.1.4): evaluación SST del contratista.

Archivos: `addons/quimibond_sgi/models/sgi_coa.py`, `addons/quimibond_sgi/models/sgi_integration.py`, `addons/quimibond_sgi/models/sgi_ppap.py`, `addons/quimibond_sgi/models/sgi_supplier_eval.py`, `addons/quimibond_sgi/models/sgi_work_permit.py`.

## Campos (19)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_coa_recipient_ids` | Many2many | Reciben el CoA | Contactos a quienes se manda el CoA. Vacío: el contacto de la entrega. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_coa.py:68` |
| `sgi_complaint_count` | Integer | # Reclamaciones |  |  |  | compute `_compute_sgi_partner_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:321` |
| `sgi_eval_count` | Integer | # Evaluaciones |  |  |  | compute `_compute_sgi_eval_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:51` |
| `sgi_eval_ids` | One2many | Evaluaciones SGI |  |  | `sgi.supplier.eval` |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:50` |
| `sgi_last_eval_date` | Date | Última evaluación | Fecha de la última evaluación de desempeño del proveedor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:48` |
| `sgi_nc_count` | Integer | # NC |  |  |  | compute `_compute_sgi_partner_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_integration.py:322` |
| `sgi_ppap_count` | Integer | PPAP |  |  |  | compute `_compute_sgi_ppap_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_ppap.py:195` |
| `sgi_requires_coa` | Boolean | Requiere CoA en cada embarque | Cada salida a este cliente (o a sus direcciones de entrega) debe llevar el certificado de análisis adjunto. |  |  |  |  | `addons/quimibond_sgi/models/sgi_coa.py:64` |
| `sgi_requires_contingency` | Boolean | Exige plan de contingencia | El cliente pide un plan de contingencia de suministro. |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:203` |
| `sgi_requires_ppap` | Boolean | Exige PPAP ante cambios | Todo cambio de ingeniería de un producto que se le vende pide PPAP: el SGI marca «Requiere PPAP» en el ECO y genera un PPAP por cliente al aplicarlo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:199` |
| `sgi_sst_eval_note` | Text | Qué se revisó (SST) | REPSE, SUA, constancias DC-3, inducción de seguridad, seguro… |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:431` |
| `sgi_sst_eval_valid_until` | Date | Evaluación SST vigente hasta | Hasta cuándo vale la evaluación de seguridad y salud del contratista. El permiso de trabajo la revisa. La registra el Jefe MAST. |  |  |  |  | `addons/quimibond_sgi/models/sgi_work_permit.py:427` |
| `sgi_supplier_approved_by` | Many2one | Aprobado por | Quién aprobó al proveedor. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:36` |
| `sgi_supplier_approved_date` | Date | Fecha de aprobación | Fecha en que se aprobó al proveedor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:39` |
| `sgi_supplier_class` | Selection | Clasificación SGI | Sin datos: en el periodo evaluado no hubo recepciones con fecha compromiso ni NC; no hay con qué calificarlo (no es «Baja»). |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:22` |
| `sgi_supplier_critical` | Boolean | Proveedor crítico | Materia prima o maquila: entra a la evaluación trimestral aunque en el periodo no haya comprado productos de las categorías críticas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:42` |
| `sgi_supplier_score` | Float | Calificación SGI | Calificación de la última evaluación trimestral del proveedor. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:46` |
| `sgi_supplier_status` | Selection | Aprobación SGI (8.4.1) | Aprobación inicial del proveedor (9001 8.4.1). Vacío: fuera del SGI. Bloqueado: no se pueden confirmar órdenes de compra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:29` |
| `sgi_user_can_eval_sst` | Boolean |  | Usted es Jefe MAST: puede registrar la evaluación SST del contratista. |  |  | compute `_compute_sgi_user_can_eval_sst`, sin guardar |  | `addons/quimibond_sgi/models/sgi_work_permit.py:435` |

## Métodos públicos (8)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_approve_supplier` | — |
| `action_sgi_block_supplier` | — |
| `action_sgi_open_complaints` | — |
| `action_sgi_open_evals` | — |
| `action_sgi_open_ncs` | — |
| `action_view_sgi_ppap` | — |
| `create` | — |
| `write` | — |
