<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.supplier.eval`

**Evaluación de proveedor SGI (8.4)** (Model).

Orden: `date_to desc, partner_id`.

Archivos: `addons/quimibond_sgi/models/sgi_supplier_eval.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_from` | Date | Desde |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:111` |
| `date_to` | Date | Hasta |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:112` |
| `nc_count` | Integer | # NC |  |  |  | compute `_compute_metrics`, guardado |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:118` |
| `notes` | Text | Notas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:122` |
| `otd_has_data` | Boolean | Con datos de entrega | Hubo recepciones con fecha compromiso en el periodo. Sin ellas el OTD no se calcula (no cuenta como 0 %) y la calificación usa solo la calidad. |  |  | compute `_compute_metrics`, guardado |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:114` |
| `otd_pct` | Float | OTD % |  |  |  | compute `_compute_metrics`, guardado |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:113` |
| `partner_id` | Many2one | Proveedor |  | sí | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:109` |
| `score` | Float | Calificación |  |  |  | compute `_compute_metrics`, guardado |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:119` |
| `supplier_class` | Selection | Clasificación |  |  |  | compute `_compute_metrics`, guardado |  | `addons/quimibond_sgi/models/sgi_supplier_eval.py:120` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_apply_to_partner` | — |
| `action_recompute` | Recalcula OTD/NC/score con los datos de HOY: las métricas son computes almacenados cuyos depends (partner/fechas) no cambian cuando llegan recepciones o NCs posteriores a la creación de la evaluación. |
