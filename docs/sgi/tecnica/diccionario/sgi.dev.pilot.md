<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.pilot`

**Pilotaje del desarrollo (primeros lotes y estudio de habilidad)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_pilot.py`.

## Campos (18)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_id` | Many2one | PDF del estudio |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:119` |
| `closed_at` | Datetime | Cerrado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:118` |
| `closed_by_id` | Many2one | Cerró |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:117` |
| `company_id` | Many2one |  |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:102` |
| `conforming_count` | Integer | Lotes conformes |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:113` |
| `cpk_min` | Float | Cpk mínimo | Del parámetro; 0: el estudio informa y no dictamina. |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:115` |
| `critical_count` | Integer | Características críticas |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:114` |
| `lot_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:112` |
| `lot_ids` | One2many | Lotes |  |  | `sgi.dev.pilot.lot` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:109` |
| `lots_required` | Integer | Lotes del pilotaje | Cuántos lotes conformes cierran el pilotaje (brief: tres). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:105` |
| `name` | Char | Pilotaje |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:97` |
| `partner_id` | Many2one | Cliente |  |  |  | related `project_id.partner_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:100` |
| `product_id` | Many2one | Artículo |  | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:101` |
| `project_id` | Many2one | Desarrollo |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:98` |
| `reading_ids` | One2many | Lecturas |  |  | `sgi.dev.pilot.reading` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:110` |
| `readings_required` | Integer | Lecturas por lote | Lecturas por característica y lote. 0: no se exige (el brief no lo define). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:107` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:103` |
| `study_ids` | One2many | Estudio de habilidad |  |  | `sgi.dev.pilot.study` |  |  | `addons/quimibond_sgi/models/sgi_dev_pilot.py:111` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_close` | — |
| `action_compute_study` | — |
| `action_print` | — |
