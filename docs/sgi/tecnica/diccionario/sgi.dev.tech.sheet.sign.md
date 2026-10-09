<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.tech.sheet.sign`

**Firma de la ficha técnica interna** (Model).

Orden: `sheet_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date` | Datetime | Fecha de firma |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:266` |
| `job_id` | Many2one | Puesto |  | sí | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:264` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:263` |
| `sheet_id` | Many2one | Ficha |  | sí | `sgi.dev.tech.sheet` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:262` |
| `user_id` | Many2one | Firmó |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:265` |

