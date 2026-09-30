<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.machine.sheet.yarn`

**Hilo de la ficha de proceso por máquina** (Model).

Hilo de la ficha de proceso por máquina (tipo, título, porcentaje y consumo).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_machine_sheet.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `consumption` | Char | cm/vuelta – longitud (m) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:126` |
| `feed` | Char | Disposición del hilo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:122` |
| `fiber` | Selection | Fibra |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:120` |
| `pct` | Float | % de hilo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:127` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:119` |
| `sheet_id` | Many2one |  |  | sí | `sgi.machine.sheet` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:118` |
| `title` | Char | Título de hilo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:124` |
| `twist` | Char | Torsión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:125` |
| `yarn_type` | Char | Tipo de hilo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:123` |

