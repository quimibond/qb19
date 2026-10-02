<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `hr.employee.public`

Modelo de otra app que el SGI extiende.

Mi equipo: el mismo semáforo por persona.

Archivos: `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_mp_ack_state` | Selection | Mi procedimiento | Si la persona ya firmó de leído su Mi procedimiento vigente. |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:952` |
| `sgi_mp_acks_pending` | Integer | Firmas pendientes | Acuses de lectura pendientes de la persona (Mi procedimiento y demás documentos). |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:948` |
| `sgi_mp_late` | Integer | Atrasadas | Actividades del puesto (ejecuta o aprueba) que hoy están atrasadas. |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:941` |
| `sgi_mp_ok` | Integer | Al día |  |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:944` |
| `sgi_mp_pending_late` | Integer | Pendientes atrasados | Pendientes atrasados de la persona en Mis pendientes. En los filtros se usa el resumen guardado (de la noche o de la última vez que se abrió su lista). |  |  | compute `_compute_sgi_mp_pending`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:1038` |
| `sgi_mp_pending_state` | Selection | Semáforo | El peor estado de sus pendientes: atrasada, por vencer o al día. En los filtros se usa el resumen guardado (de la noche o de la última vez que se abrió su lista). |  |  | compute `_compute_sgi_mp_pending`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:1043` |
| `sgi_mp_pending_total` | Integer | Pendientes | Total de pendientes de la persona en Mis pendientes. En los filtros se usa el resumen guardado (de la noche o de la última vez que se abrió su lista). |  |  | compute `_compute_sgi_mp_pending`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:1033` |
| `sgi_mp_total` | Integer | Actividades | Número de actividades del SGI que le tocan a la persona. |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:946` |
| `sgi_mp_unmeasured` | Integer | Sin medición automática |  |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:945` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_my_team` | Inicio → Mi equipo: la lista nativa acotada al equipo. |
| `action_sgi_open_my_procedure` | — |
| `action_sgi_open_pending` | — |
| `action_sgi_print_my_procedure` | Imprimir desde la ficha pública: el PDF del puesto para esta persona. |
| `action_sgi_team_pending` | «Pendientes del equipo»: una lista, agrupada por persona. |
