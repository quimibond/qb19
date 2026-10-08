<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.mp.wait`

**Espera de materia prima del desarrollo** (Model).

Periodo en que un desarrollo esperó materia prima: detiene el reloj de desarrollo.

Orden: `date_start desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_project.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_end` | Datetime | Hasta | Cuándo llegó la materia prima; vacío mientras siga pendiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:211` |
| `date_start` | Datetime | Desde | Cuándo se detectó que faltaba materia prima. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:209` |
| `hours` | Float | Horas | Horas calendario de espera (hasta ahora si sigue pendiente). |  |  | compute `_compute_hours`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_project.py:212` |
| `note` | Char | Qué faltó | Materia prima que se esperaba (texto breve). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:214` |
| `project_id` | Many2one |  | Proyecto de desarrollo. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:207` |

