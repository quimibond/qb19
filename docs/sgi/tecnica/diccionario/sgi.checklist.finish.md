<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.checklist.finish`

**Terminar checklist: quién lo llenó** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_checklist.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `allowed_employee_ids` | Many2many |  |  |  | `hr.employee` | compute `_compute_allowed_employee_ids`, sin guardar |  | `addons/quimibond_sgi/models/sgi_checklist.py:243` |
| `employee_id` | Many2one | ¿Quién lo llenó? |  | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:244` |
| `pin` | Char | PIN del empleado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:246` |
| `request_id` | Many2one |  |  | sí | `maintenance.request` |  |  | `addons/quimibond_sgi/models/sgi_checklist.py:242` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
