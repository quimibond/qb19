<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.similar`

**Artículos parecidos al desarrollo** (TransientModel).

Asistente: artículos de línea y desarrollos anteriores parecidos a lo que pide el cliente.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `ancho` | Integer | Ancho pedido (cm) | Ancho nominal que pide el cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:70` |
| `line_ids` | One2many | Parecidos |  |  | `sgi.dev.similar.line` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:71` |
| `peso` | Integer | Peso pedido (g/m²) | Masa nominal que pide el cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:69` |
| `project_id` | Many2one |  | Proyecto de desarrollo que se compara. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:67` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_refresh` | — |
