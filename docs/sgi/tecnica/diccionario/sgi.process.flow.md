<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.process.flow`

**Flujo entre procesos SGI** (Model).

Flujo entre dos procesos: qué pasa de uno a otro y, si es un documento de Odoo, de qué modelo.

Orden: `from_process_id, name`.

Archivos: `addons/quimibond_sgi/models/sgi_process.py`, `addons/quimibond_sgi/models/sgi_deliverable.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `acceptance_criteria` | Text | Criterio de aceptación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:562` |
| `company_id` | Many2one | Empresa |  |  |  | related `from_process_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process.py:567` |
| `document_id` | Many2one | Formato de entrega | Formato con el que se entrega lo que pasa entre los procesos. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_process.py:560` |
| `from_process_id` | Many2one | Proceso origen | Proceso que entrega. | sí | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process.py:556` |
| `name` | Char | Entregable |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:555` |
| `odoo_model_id` | Many2one | Modelo Odoo que lo materializa | Modelo de Odoo donde queda el registro de lo que pasa entre procesos. |  | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_process.py:563` |
| `odoo_model_name` | Char | Modelo técnico |  |  |  | related `odoo_model_id.model`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:566` |
| `to_process_id` | Many2one | Proceso destino | Proceso que recibe. | sí | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process.py:558` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_records` | Abre los registros vivos del modelo Odoo que materializa el flujo. |
| `unlink` | — |
| `write` | — |
