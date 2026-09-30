<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `maintenance.request`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_checklist.py`, `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_integration.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_alert_id` | Many2one | NC generada |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_integration.py:149` |
| `sgi_checklist_date` | Date | Día del checklist |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:186` |
| `sgi_checklist_done_at` | Datetime | Terminado el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:190` |
| `sgi_checklist_employee_id` | Many2one | Lo llenó |  |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:188` |
| `sgi_checklist_line_ids` | One2many | Hoja de checklist |  |  | `sgi.checklist.line` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:187` |
| `sgi_checklist_state` | Selection | Checklist |  |  |  | compute `_compute_sgi_checklist_state`, guardado |  | `addons/quimibond_sgi/models/sgi_checklist.py:191` |
| `sgi_checklist_template_id` | Many2one | Checklist SGI |  |  | `sgi.checklist.template` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:184` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_sgi_checklist_finish` | Abre la firma de quien llenó la hoja (empleado + PIN). |
| `action_sgi_create_correctives` | Una solicitud correctiva por punto con falla (una sola vez). |
| `action_sgi_raise_nc` | Levanta una NC (equipo NC Internas) desde una solicitud correctiva, pre-llenada con el equipo/máquina y la descripción de la falla. |
