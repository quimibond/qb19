<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.checklist.finish`

**Terminar checklist: quién lo llenó** (TransientModel).

Asistente para terminar una hoja de checklist: quién la llenó y, si está encendido el parámetro, su PIN de empleado.

Archivos: `addons/quimibond_sgi/models/sgi_checklist.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `allowed_employee_ids` | Many2many |  | Personas que pueden firmar esta hoja: las de «Quién lo llena» en la plantilla. |  | `hr.employee` | compute `_compute_allowed_employee_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_checklist.py:264` |
| `employee_id` | Many2one | ¿Quién lo llenó? | Elija a la persona que llenó la hoja. | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:267` |
| `pin` | Char | PIN del empleado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:270` |
| `request_id` | Many2one |  | Hoja de checklist que se termina. | sí | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:262` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
