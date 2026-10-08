<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.option`

**Opción de lista del desarrollo de producto** (Model).

Opción de lista para el proyecto de desarrollo (aplicación, mercado, laminado, requisito legal, motivo de no factibilidad).

Orden: `kind, sequence, name, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_project.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  | Las opciones archivadas no se proponen en proyectos nuevos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:128` |
| `kind` | Selection | Lista | A qué lista pertenece la opción. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:124` |
| `name` | Char | Opción | Texto de la opción tal como se elige. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:127` |
| `sequence` | Integer |  | Orden dentro de su lista. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:126` |

