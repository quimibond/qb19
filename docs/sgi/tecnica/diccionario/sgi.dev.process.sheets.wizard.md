<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.process.sheets.wizard`

**Fichas de proceso del desarrollo** (TransientModel).

57.155.0: las fichas de proceso de los artículos del proyecto (crudo, teñido, acabado); abre la que existe o crea la que falta con el artículo ya puesto.

Archivos: `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py`.

## Campos (2)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `line_ids` | One2many | Fichas |  |  | `sgi.dev.process.sheets.wizard.line` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:322` |
| `project_id` | Many2one |  |  | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:321` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `default_get` | — |
