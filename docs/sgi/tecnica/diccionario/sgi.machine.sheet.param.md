<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.machine.sheet.param`

**Parámetro de la ficha de proceso por máquina** (Model).

Parámetro de proceso de una ficha por máquina (especificación, tolerancia y unidad).

Orden: `section, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_machine_sheet.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `name` | Char | Condición |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:150` |
| `section` | Selection | Sección |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:149` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:148` |
| `sheet_id` | Many2one |  |  | sí | `sgi.machine.sheet` |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:147` |
| `spec` | Char | Especificación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:151` |
| `tolerance` | Char | Tolerancia (±) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:152` |
| `unit` | Char | Unidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_machine_sheet.py:153` |

