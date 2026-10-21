<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.similar`

**Artículos parecidos al desarrollo** (TransientModel).

Asistente: artículos de línea y desarrollos anteriores parecidos a lo que pide el cliente.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `ancho` | Integer | Ancho pedido (cm) | Ancho nominal que pide el cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:84` |
| `line_ids` | One2many | Parecidos | Ordenados por cercanía en peso, ancho, composición, dibujo y galga (leídos del código). «Dentro de tolerancia» puede ser producto de línea; con una sola diferencia es producto nuevo. Diseño de Producto confirma con «Usar como base». |  | `sgi.dev.similar.line` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:85` |
| `peso` | Integer | Peso pedido (g/m²) | Masa nominal que pide el cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:83` |
| `project_id` | Many2one |  | Proyecto de desarrollo que se compara. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:81` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_refresh` | — |
