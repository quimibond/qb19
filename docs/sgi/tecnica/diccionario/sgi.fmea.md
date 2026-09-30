<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.fmea`

**AMEF - Análisis de Modo y Efecto de Falla (P-C10)** (Model). Hereda de: `sgi.base.mixin`.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_fmea.py`, `addons/quimibond_sgi/models/sgi_links.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `control_plan_id` | Many2one | Plan de control | Plan de control que materializa los controles de este AMEF (cadena IATF PFMEA → plan de control). |  | `sgi.control.plan` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:30` |
| `date` | Date | Fecha |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:35` |
| `fmea_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:21` |
| `line_ids` | One2many | Modos de falla |  |  | `sgi.fmea.line` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:42` |
| `max_npr` | Integer | NPR máximo |  |  |  | compute `_compute_max_npr`, guardado |  | `addons/quimibond_sgi/models/sgi_fmea.py:43` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:20` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:26` |
| `product_tmpl_id` | Many2one | Producto |  |  | `product.template` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:25` |
| `revision` | Char | Revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:34` |
| `sgi_nc_count` | Integer | # NCs ligadas |  |  |  | compute `_compute_sgi_nc_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_fmea.py:46` |
| `sgi_nc_ids` | One2many | NCs ligadas |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:45` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:37` |
| `team_ids` | Many2many | Equipo AMEF |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_fmea.py:36` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_borrador` | — |
| `action_set_obsoleto` | — |
| `action_set_vigente` | — |
| `action_view_sgi_ncs` | — |
| `write` | — |
