<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.process.flow`

**Flujo entre procesos SGI** (Model).

Orden: `from_process_id, name`.

Archivos: `addons/quimibond_sgi/models/sgi_process.py`, `addons/quimibond_sgi/models/sgi_deliverable.py`.

## Campos (8)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `acceptance_criteria` | Text | Criterio de aceptación |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:545` |
| `company_id` | Many2one | Empresa |  |  |  | related `from_process_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_process.py:548` |
| `document_id` | Many2one | Formato de entrega |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_process.py:544` |
| `from_process_id` | Many2one | Proceso origen |  | sí | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process.py:542` |
| `name` | Char | Entregable |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_process.py:541` |
| `odoo_model_id` | Many2one | Modelo Odoo que lo materializa |  |  | `ir.model` |  |  | `addons/quimibond_sgi/models/sgi_process.py:546` |
| `odoo_model_name` | Char | Modelo técnico |  |  |  | related `odoo_model_id.model`, sin guardar |  | `addons/quimibond_sgi/models/sgi_process.py:547` |
| `to_process_id` | Many2one | Proceso destino |  | sí | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_process.py:543` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_records` | Abre los registros vivos del modelo Odoo que materializa el flujo. |
| `unlink` | — |
| `write` | — |
