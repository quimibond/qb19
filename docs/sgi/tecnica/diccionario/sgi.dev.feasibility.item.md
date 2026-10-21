<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.feasibility.item`

**Recurso del checklist de factibilidad** (Model).

Recurso o pregunta del checklist de factibilidad, por línea (catálogo; nace vacío).

Orden: `line, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Los recursos archivados no se cargan en proyectos nuevos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:295` |
| `auto` | Selection | Lo contesta Odoo | Si Odoo puede contestar o sugerir el renglón: existencia de materia prima (lista de materiales contra existencias); con la ficha de la máquina, las circulares que hoy corren la galga del artículo, la producción real de esas circulares contra el consumo mensual, los jets que aceptan la tela por su r… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:288` |
| `line` | Selection | Línea | Línea de producción cuyo checklist incluye el recurso. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:283` |
| `name` | Char | Recurso o pregunta | Qué se verifica (máquina, materia prima, laboratorio, personal…). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:286` |
| `sequence` | Integer |  | Orden en el checklist. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:285` |

