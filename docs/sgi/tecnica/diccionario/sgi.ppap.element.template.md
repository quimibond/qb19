<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.ppap.element.template`

**Elemento PPAP (catálogo AIAG)** (Model).

Catálogo de elementos PPAP (AIAG); ``is_psw`` marca la carta de garantía.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_ppap.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:27` |
| `is_psw` | Boolean | Es PSW (elemento 18) | Marca el elemento que es la carta de garantía de partes (PSW). |  |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:25` |
| `name` | Char | Elemento |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:24` |
| `sequence` | Integer | N° |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_ppap.py:23` |

