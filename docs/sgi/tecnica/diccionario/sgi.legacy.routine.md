<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.legacy.routine`

**Rutina del procedimiento anterior** (Model). Hereda de: `mail.thread`.

Orden: `procedure_code, n, id`.

Archivos: `addons/quimibond_sgi/models/sgi_legacy_routine.py`.

## Campos (22)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:158` |
| `activity_ids` | Many2many | Actividades que la cubren | Actividades del proceso nuevo que hacen esta rutina. |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:122` |
| `activity_numbers` | Char | Numerales | Numerales de las actividades, para buscar y exportar. |  |  | compute `_compute_activity_numbers`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:127` |
| `company_id` | Many2one | Empresa |  |  |  | related `procedure_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:157` |
| `decision` | Selection | Decisión | Qué se hará con la pendiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:142` |
| `decision_deadline` | Date | Fecha compromiso | Las 38 pendientes de hoy se deciden a más tardar el 16 de octubre de 2026. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:147` |
| `decision_owner_id` | Many2one | Responsable de decidir | Quién cierra la pendiente (dueño del proceso o MAST). |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:144` |
| `frequency` | Char | Frecuencia anterior | Cada cuándo se hacía (texto del procedimiento; no es el vencimiento de Odoo). |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:108` |
| `has_archived_activity` | Boolean | Con actividad archivada |  |  |  | compute `_compute_activity_numbers`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:130` |
| `n` | Integer | N.º | Número de la rutina dentro del procedimiento anterior, tal como viene en el análisis. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:103` |
| `name` | Char | Rutina | Qué se hacía en el sistema anterior, en una línea. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:106` |
| `previous_owner` | Char | Responsable anterior | Puesto que la hacía según el procedimiento del Dropbox. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:111` |
| `procedure_code` | Char | Clave anterior | Clave del procedimiento en el Dropbox (P-A02). Solo aquí y en el buscador. |  |  | compute `_compute_procedure_code`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:100` |
| `procedure_id` | Many2one | Procedimiento anterior | Procedimiento del Dropbox del que sale esta rutina (su PDF queda como histórico). | sí | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:95` |
| `procedure_migration_state` | Selection | Estado del procedimiento |  |  |  | related `procedure_id.sgi_migration_state`, sin guardar |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:140` |
| `process_id` | Many2one | Proceso nuevo | Proceso que sustituye al procedimiento, o su proceso actual si todavía no lo sustituye. |  | `sgi.process` | compute `_compute_process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:136` |
| `reason` | Text | Motivo | Cómo la cubre la actividad, por qué la hace Odoo solo, por qué se eliminó o por qué está pendiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:132` |
| `resolved_date` | Date | Resuelta el | Cuándo dejó de estar pendiente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:154` |
| `resolved_uid` | Many2one | Resuelta por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:156` |
| `review_note` | Text | Comentario de revisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:153` |
| `review_state` | Selection | Revisión | Revisión del dueño del proceso sobre el análisis. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:150` |
| `state` | Selection | Estado | Cubierta: una actividad de Odoo la hace. La hace Odoo: Odoo la hace solo o era redundante. Eliminada: se dejó a propósito. Pendiente: nadie la cubre todavía. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine.py:114` |

## Métodos públicos (6)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_activities` | — |
| `action_open_procedure` | — |
| `create` | — |
| `load_routines` | Carga el análisis rutina por rutina. Formato en docs/audit/12-transicion.md §3.9:: |
| `unlink` | — |
| `write` | — |
