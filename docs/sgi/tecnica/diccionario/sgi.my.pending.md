<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.my.pending`

**Mis pendientes (SGI)** (TransientModel).

Orden: `state_rank, date_due, id`.

Archivos: `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi/models/sgi_archived_filters.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_due` | Date | Vence |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:191` |
| `employee_id` | Many2one | Persona |  |  | `hr.employee.public` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:187` |
| `kind` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:188` |
| `name` | Char | Qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:189` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:190` |
| `res_id` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:195` |
| `res_model` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:194` |
| `state` | Selection | Estado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:192` |
| `state_rank` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:193` |
| `user_id` | Many2one | Usuario |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:186` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_open` | Abre el registro de origen (la acción, la NC, la medición…). |
| `action_open_mine` | Inicio → Mis pendientes: los del usuario actual. Sin empleado ligado, los renglones van solo con el usuario. |
| `action_validate_measure` | «Validar» desde el renglón (I-006). Valida quien abre la lista, con sus permisos: solo el dueño del indicador o el Jefe MAST pueden (``_sgi_check_validate_access``). |
