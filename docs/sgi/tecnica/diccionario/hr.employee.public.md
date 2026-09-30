<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `hr.employee.public`

Modelo de otra app que el SGI extiende.

Mi equipo: el mismo semáforo por persona.

Archivos: `addons/quimibond_sgi/models/sgi_my_pending.py`, `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_mp_ack_state` | Selection | Mi procedimiento |  |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:873` |
| `sgi_mp_acks_pending` | Integer | Firmas pendientes | Acuses de lectura pendientes de la persona (Mi procedimiento y demás documentos). |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:869` |
| `sgi_mp_late` | Integer | Atrasadas | Actividades del puesto (ejecuta o aprueba) que hoy están atrasadas. |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:863` |
| `sgi_mp_ok` | Integer | Al día |  |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:866` |
| `sgi_mp_pending_late` | Integer | Pendientes atrasados |  |  |  | compute `_compute_sgi_mp_pending`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:678` |
| `sgi_mp_pending_state` | Selection | Semáforo | El peor estado de sus pendientes: atrasada, por vencer o al día. |  |  | compute `_compute_sgi_mp_pending`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:681` |
| `sgi_mp_pending_total` | Integer | Pendientes |  |  |  | compute `_compute_sgi_mp_pending`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_pending.py:675` |
| `sgi_mp_total` | Integer | Actividades |  |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:868` |
| `sgi_mp_unmeasured` | Integer | Sin medición automática |  |  |  | compute `_compute_sgi_mp_stats`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:867` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_my_team` | Inicio → Mi equipo: la lista nativa acotada al equipo. |
| `action_sgi_open_my_procedure` | — |
| `action_sgi_open_pending` | — |
| `action_sgi_print_my_procedure` | Imprimir desde la ficha pública: el PDF del puesto para esta persona. |
| `action_sgi_team_pending` | «Pendientes del equipo»: una lista, agrupada por persona. |
