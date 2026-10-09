<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.feasibility.item`

**Recurso del checklist de factibilidad** (Model).

Recurso o pregunta del checklist de factibilidad, por línea (catálogo; nace vacío).

Orden: `line, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_analysis.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Los recursos archivados no se cargan en proyectos nuevos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:274` |
| `auto` | Selection | Lo contesta Odoo | Si Odoo puede contestar el renglón solo: existencia de materia prima (lista de materiales contra existencias) o capacidad de máquina (llega con el cotizador nuevo). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:271` |
| `line` | Selection | Línea | Línea de producción cuyo checklist incluye el recurso. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:266` |
| `name` | Char | Recurso o pregunta | Qué se verifica (máquina, materia prima, laboratorio, personal…). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:269` |
| `sequence` | Integer |  | Orden en el checklist. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_analysis.py:268` |

