<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.my.pending`

**Mis pendientes (SGI)** (TransientModel).

Mis pendientes: bandeja de cada persona con actividades atrasadas, firmas, acuses, capturas, validaciones y aprobaciones. Se recalcula al abrirla; no guarda historia.

Orden: `state_rank, date_due, id`.

Archivos: `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi/models/sgi_archived_filters.py`.

## Campos (15)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `can_mark_exec` | Boolean |  | Quien abre la lista puede marcar el avance. |  |  | compute `_compute_can_mark_exec`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:227` |
| `date_due` | Date | Vence | Fecha en que vence el pendiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:207` |
| `employee_id` | Many2one | Persona | Persona a la que le toca el pendiente. |  | `hr.employee.public` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:199` |
| `exec_manual` | Boolean |  | Actividad de registro manual: «Ir» abre su registro. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:225` |
| `exec_note` | Char | Nota de avance |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:224` |
| `exec_state` | Selection | Avance | Actividades del periodo: pendiente o en proceso (con nota de avance). «En proceso» no quita el atraso. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:218` |
| `is_mine` | Boolean |  | El renglón es de quien abre la lista. |  |  | compute `_compute_is_mine`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:215` |
| `kind` | Selection | Tipo | Qué hay que hacer: acción, NC, medición, validación, acuse, firma, aprobación… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:201` |
| `name` | Char | Qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:204` |
| `process_id` | Many2one | Proceso | Proceso del pendiente. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:205` |
| `res_id` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:212` |
| `res_model` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:211` |
| `state` | Selection | Estado | Atrasado, por vencer o al día. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:208` |
| `state_rank` | Integer |  | Orden para mostrar primero lo atrasado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:210` |
| `user_id` | Many2one | Usuario |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:198` |

## Métodos públicos (10)

| Método | Qué hace (docstring) |
|---|---|
| `action_done_notice` | 57.92.0 (U-03): «Hecho» marca hecho el aviso (la actividad nativa), solo si es de quien abre la lista. |
| `action_exec_done` | 57.103.0: «Hecho» desde el renglón (evidencia si es de registro manual o si el sistema no vio el registro). |
| `action_exec_progress` | 57.103.0: «En proceso» desde el renglón (nota de avance y fecha estimada). |
| `action_open` | Abre el registro de origen (la acción, la NC, la medición…). |
| `action_open_home` | 57.98.0 (U-07, Q20): acción del menú raíz «SGI». Dirección abre en el Tablero; los demás, en Mis pendientes (como antes). No toca la acción de inicio de ningún usuario. |
| `action_open_mine` | Inicio → Mis pendientes: los del usuario actual. Sin empleado ligado, los renglones van solo con el usuario. |
| `action_sgi_report` | 57.98.0 (U-07): SGI → Reportar abre la ficha NUEVA de una NC, un incidente o una queja, con su equipo del SGI. Sin el equipo (base sin datos) avisa en lugar de abrir Calidad o Soporte completos. |
| `action_sign_ack` | 57.92.0 (U-05): «Leído y entendido» desde el renglón. El candado de identidad de ``sgi.document.ack`` decide si quien abre la lista puede firmar: el propio empleado o el Jefe MAST. |
| `action_validate_measure` | «Validar» desde el renglón (I-006). Valida quien abre la lista, con sus permisos: solo el dueño del indicador o el Jefe MAST pueden (``_sgi_check_validate_access``). |
| `action_validate_selected` | 57.92.0 (U-02): «Validar seleccionadas». Ignora los renglones que no son mediciones. Valida, con los permisos de quien abre la lista, solo las mediciones que puede validar (``sgi_can_validate``: dueñ… |
