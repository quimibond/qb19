<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.option`

**Opción de lista del desarrollo de producto** (Model).

Opción de lista para el proyecto de desarrollo (aplicación, mercado, laminado, requisito legal, motivo de no factibilidad).

Orden: `kind, sequence, name, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_project.py`, `addons/quimibond_sgi/models/sgi_dev_start_approval.py`, `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Las opciones archivadas no se proponen en proyectos nuevos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:154` |
| `kind` | Selection | Lista | A qué lista pertenece la opción. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:150` |
| `line_key` | Selection | Línea | Solo para «Información requerida al cliente»: la línea a la que aplica el documento. Vacío: las dos líneas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_start_approval.py:147` |
| `name` | Char | Opción | Texto de la opción tal como se elige. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:153` |
| `name_en` | Char | Opción en inglés | Para los documentos bilingües al cliente (instrucciones de cuidado). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:45` |
| `sequence` | Integer |  | Orden dentro de su lista. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:152` |

