<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.coa.line`

**Renglón del reporte de conformidad** (Model).

Orden: `coa_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_coa.py`.

## Campos (15)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `characteristic_id` | Many2one | Renglón de la tabla |  | sí | `sgi.dev.characteristic` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:180` |
| `coa_id` | Many2one | Certificado |  | sí | `sgi.dev.coa` |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:178` |
| `direction` | Selection |  |  |  |  | related `characteristic_id.direction`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:188` |
| `has_value` | Boolean |  |  |  |  | compute `_compute_result`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:193` |
| `kind` | Selection |  |  |  |  | related `characteristic_id.kind`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:185` |
| `method` | Char | Método / norma |  |  |  | related `characteristic_id.method`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:187` |
| `name` | Char | Característica |  |  |  | related `characteristic_id.name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:183` |
| `name_en` | Char | Characteristic |  |  |  | related `characteristic_id.caracteristica_id.name_en`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:184` |
| `position` | Selection |  |  |  |  | related `characteristic_id.position`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:189` |
| `result` | Selection | Resultado | Contra la especificación del cliente; el control interno no cuenta aquí. |  |  | compute `_compute_result`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:194` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:179` |
| `spec_label` | Char | Especificación del cliente |  |  |  | related `characteristic_id.spec_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:190` |
| `text` | Char | Obtenido (texto) | Para características cualitativas. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:192` |
| `unit` | Char |  |  |  |  | related `characteristic_id.unit`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:186` |
| `value` | Float | Obtenido | Valor obtenido en el lote. |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_coa.py:191` |

