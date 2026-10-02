<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.my.pending`

**Mis pendientes (SGI)** (TransientModel).

Mis pendientes: bandeja de cada persona con actividades atrasadas, firmas, acuses, capturas, validaciones y aprobaciones. Se recalcula al abrirla; no guarda historia.

Orden: `state_rank, date_due, id`.

Archivos: `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi/models/sgi_archived_filters.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `date_due` | Date | Vence | Fecha en que vence el pendiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:204` |
| `employee_id` | Many2one | Persona | Persona a la que le toca el pendiente. |  | `hr.employee.public` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:196` |
| `is_mine` | Boolean |  | El renglón es de quien abre la lista. |  |  | compute `_compute_is_mine`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:212` |
| `kind` | Selection | Tipo | Qué hay que hacer: acción, NC, medición, validación, acuse, firma, aprobación… | sí |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:198` |
| `name` | Char | Qué |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:201` |
| `process_id` | Many2one | Proceso | Proceso del pendiente. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:202` |
| `res_id` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:209` |
| `res_model` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:208` |
| `state` | Selection | Estado | Atrasado, por vencer o al día. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:205` |
| `state_rank` | Integer |  | Orden para mostrar primero lo atrasado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:207` |
| `user_id` | Many2one | Usuario |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_my_pending.py:195` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_done_notice` | 57.92.0 (U-03): «Hecho» marca hecho el aviso (la actividad nativa), solo si es de quien abre la lista. |
| `action_open` | Abre el registro de origen (la acción, la NC, la medición…). |
| `action_open_mine` | Inicio → Mis pendientes: los del usuario actual. Sin empleado ligado, los renglones van solo con el usuario. |
| `action_sign_ack` | 57.92.0 (U-05): «Leído y entendido» desde el renglón. El candado de identidad de ``sgi.document.ack`` decide si quien abre la lista puede firmar: el propio empleado o el Jefe MAST. |
| `action_validate_measure` | «Validar» desde el renglón (I-006). Valida quien abre la lista, con sus permisos: solo el dueño del indicador o el Jefe MAST pueden (``_sgi_check_validate_access``). |
| `action_validate_selected` | 57.92.0 (U-02): «Validar seleccionadas». Ignora los renglones que no son mediciones. Valida, con los permisos de quien abre la lista, solo las mediciones que puede validar (``sgi_can_validate``: dueñ… |
