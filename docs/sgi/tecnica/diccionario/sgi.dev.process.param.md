<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.process.param`

**Parámetro de la ficha de proceso** (Model).

Parámetro de la ficha: propuesto por Diseño de Procesos, real del supervisor, ajuste y motivo.

Orden: `pass_number, section, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_process_sheet.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `adjustment` | Integer | Número de ajuste | Cuántas veces se ajustó este parámetro en la corrida. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:363` |
| `kind` | Selection |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:356` |
| `name` | Char | Parámetro |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:354` |
| `pass_number` | Integer | Pase | Pase de rama (acabado); 0 en tintorería. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:350` |
| `proposed_bool` | Boolean | Propuesto (sí) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:361` |
| `proposed_label` | Char |  |  |  |  | compute `_compute_labels`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:366` |
| `proposed_num` | Float | Propuesto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:357` |
| `proposed_text` | Char | Propuesto (texto) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:359` |
| `real_bool` | Boolean | Real (sí) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:362` |
| `real_label` | Char |  |  |  |  | compute `_compute_labels`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:367` |
| `real_num` | Float | Real |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:358` |
| `real_text` | Char | Real (texto) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:360` |
| `reason_id` | Many2one | Motivo del ajuste | Motivo de lista (SGI → Desarrollos → Opciones, lista «Motivo de ajuste»). |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:364` |
| `section` | Selection |  |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:351` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:353` |
| `sheet_id` | Many2one |  |  | sí | `sgi.dev.process.sheet` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:349` |
| `unit` | Char | Unidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:355` |

