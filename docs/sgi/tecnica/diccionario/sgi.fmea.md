<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.fmea`

**AMEF - Análisis de Modo y Efecto de Falla (P-C10)** (Model). Hereda de: `sgi.base.mixin`.

AMEF de proceso o de diseño (P-C10) con sus líneas y NPR máximo; ligado al plan de control. No pasa a vigente con NPR alto sin acción.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_fmea.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `control_plan_id` | Many2one | Plan de control | Plan de control que materializa los controles de este AMEF (cadena IATF PFMEA → plan de control). |  | `sgi.control.plan` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:39` |
| `date` | Date | Fecha | Fecha del AMEF. |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:44` |
| `fmea_type` | Selection | Tipo | De proceso (PFMEA) o de diseño (DFMEA). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:27` |
| `line_ids` | One2many | Modos de falla |  |  | `sgi.fmea.line` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:52` |
| `max_npr` | Integer | NPR máximo | El NPR más alto de sus modos de falla. Se calcula solo. |  |  | compute `_compute_max_npr`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:53` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:26` |
| `process_id` | Many2one | Proceso | Proceso que analiza el AMEF. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:34` |
| `product_tmpl_id` | Many2one | Producto | Producto que analiza el AMEF. |  | `product.template` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:32` |
| `revision` | Char | Revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:43` |
| `sgi_nc_count` | Integer | # NC ligadas |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_fmea.py:57` |
| `sgi_nc_ids` | One2many | NC ligadas |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:56` |
| `state` | Selection | Estado | Borrador, vigente u obsoleto. No pasa a vigente con un NPR alto sin acción. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:46` |
| `team_ids` | Many2many | Equipo AMEF | Personas que hicieron el AMEF. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:45` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_borrador` | — |
| `action_set_obsoleto` | — |
| `action_set_vigente` | — |
| `action_view_sgi_ncs` | — |
| `write` | — |
