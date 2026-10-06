<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.similar.line`

**Artículo parecido** (TransientModel).

Un artículo parecido con su distancia y si cumple la tolerancia del cliente.

Orden: `score, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `ancho` | Integer | Ancho (cm) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:95` |
| `galga` | Integer | Galga |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:96` |
| `note` | Char | Diferencias |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:103` |
| `peso` | Integer | Peso (g/m²) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:94` |
| `product_id` | Many2one | Artículo |  | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:91` |
| `score` | Float | Distancia | Menor es más parecido: % de diferencia en peso y ancho más castigo por composición, dibujo y galga distintos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:97` |
| `source` | Selection | Origen | Artículo de línea o de un desarrollo anterior. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:92` |
| `within_tolerance` | Boolean | Dentro de tolerancia | Peso y ancho dentro de lo que pide el cliente, con la misma composición, dibujo y galga: puede ser producto de línea. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:100` |
| `wizard_id` | Many2one |  |  | sí | `sgi.dev.similar` |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:90` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_use_as_base` | — |
