<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.finish.route`

**Paso de la ruta de acabado** (Model).

Paso de la ruta de proceso de acabado (hasta 10).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_process_sheet.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `name` | Char | Paso |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:412` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:411` |
| `sheet_id` | Many2one |  |  | sí | `sgi.dev.process.sheet` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:410` |
| `workcenter_id` | Many2one | Centro de trabajo |  |  | `mrp.workcenter` |  |  | `addons/quimibond_sgi/models/sgi_dev_process_sheet.py:413` |

