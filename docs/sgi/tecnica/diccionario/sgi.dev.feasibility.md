<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.feasibility`

**Checklist de factibilidad del desarrollo** (Model).

Renglón del checklist de factibilidad de un proyecto: sí, no o no aplica, con observación.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `answer` | Selection | Respuesta | Sí, no o no aplica. Pendiente mientras nadie conteste. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:261` |
| `answered_by_id` | Many2one | Contestó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:264` |
| `auto` | Selection | Lo contesta Odoo |  |  |  | related `item_id.auto`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:260` |
| `item_id` | Many2one | Recurso | Recurso del catálogo que se verifica. | sí | `sgi.dev.feasibility.item` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:257` |
| `observation` | Char | Observación | Texto libre; no capture aquí valores. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:263` |
| `project_id` | Many2one |  |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:256` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:259` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `write` | — |
