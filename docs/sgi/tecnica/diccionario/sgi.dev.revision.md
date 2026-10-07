<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.revision`

**Revisión del desarrollo de producto** (Model).

Renglón de la bitácora de revisiones de un desarrollo: qué cambió, cuándo y quién lo pidió.

Orden: `date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_project.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `characteristic_id` | Many2one | Característica | Renglón de la tabla de características que cambió. |  | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:145` |
| `characteristic_name` | Char | Característica (texto) | Nombre de la característica, por si el renglón se borra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:147` |
| `date` | Datetime | Fecha | Cuándo se registró el cambio. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:139` |
| `new_value` | Char | Valor nuevo | Especificación después del cambio. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:150` |
| `note` | Char | Observaciones | Texto libre; no capture aquí valores. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:154` |
| `old_value` | Char | Valor anterior | Especificación antes del cambio. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:149` |
| `project_id` | Many2one |  | Proyecto de desarrollo al que pertenece la revisión. | sí | `project.project` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:136` |
| `quotation_ref` | Char | Cotización asociada | Referencia de la cotización que recoge el cambio (la liga directa llega con el cotizador nuevo). |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:151` |
| `requested_by` | Selection | Lo pidió | Quién pidió el cambio: el cliente o alguien de Quimibond. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:143` |
| `revision` | Integer | Revisión | Número de revisión del proyecto al que corresponde el cambio. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:138` |
| `user_id` | Many2one | Registró | Usuario que registró el cambio en Odoo. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_project.py:141` |

