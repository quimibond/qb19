<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.machine.sheet.yarn`

**Hilo de la ficha de proceso por máquina** (Model).

Hilo de la ficha de proceso por máquina (tipo, título, porcentaje y consumo).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_machine_sheet.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `consumption` | Char | cm/vuelta – longitud (m) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:137` |
| `feed` | Char | Disposición del hilo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:133` |
| `fiber` | Selection | Fibra |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:131` |
| `pct` | Float | % de hilo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:138` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:130` |
| `sheet_id` | Many2one |  |  | sí | `sgi.machine.sheet` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:129` |
| `title` | Char | Título de hilo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:135` |
| `twist` | Char | Torsión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:136` |
| `yarn_type` | Char | Tipo de hilo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:134` |

