<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dev.customer.spec`

**Especificaciones del producto para el cliente** (Model). Hereda de: `mail.thread`, `sgi.dev.doc.mixin`.

Orden: `id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `care_ids` | Many2many | Instrucciones de cuidado | De la lista «Instrucción de cuidado» (nombre en español e inglés). |  | `sgi.dev.option` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:269` |
| `emitted_at` | Datetime | Emitida el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:273` |
| `emitted_by_id` | Many2one | Emitió (Ventas) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:272` |
| `line_ids` | One2many | Características |  |  | `sgi.dev.customer.spec.line` |  |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:266` |
| `main_use` | Text | Uso principal / Main use | El de la solicitud del proyecto; se captura una sola vez. |  |  | related `project_id.sgi_dev_use`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:267` |
| `name` | Char | Especificaciones |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_dev_tech_sheet.py:265` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_emit` | — |
