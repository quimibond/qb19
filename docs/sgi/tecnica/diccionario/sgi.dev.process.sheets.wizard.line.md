<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.process.sheets.wizard.line`

**Ficha de proceso de un artículo del desarrollo** (TransientModel).

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `acabado_id` | Many2one | Ficha de acabado |  |  | `ficha.tecnica.acabado` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:349` |
| `can_create` | Boolean |  |  |  |  | compute `_compute_status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:351` |
| `has_sheet` | Boolean |  |  |  |  | compute `_compute_status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:350` |
| `product_id` | Many2one | Artículo |  | sí | `product.product` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:347` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:345` |
| `stage` | Selection | Etapa |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:346` |
| `status` | Char | Estado |  |  |  | compute `_compute_status`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:352` |
| `tejido_id` | Many2one | Ficha de tejido |  |  | `ficha.tecnica.tejido` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:348` |
| `wizard_id` | Many2one |  |  | sí | `sgi.dev.process.sheets.wizard` |  |  | `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py:344` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_open` | Abre la ficha; si falta, la ficha nueva con el artículo, el proyecto y (en acabado) la ficha de tejido del crudo ya puestos. |
