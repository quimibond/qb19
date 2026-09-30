<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.exec.stat`

**Ejecuciones de una actividad SGI por semana y usuario** (Model).

Ejecuciones de una actividad por semana y usuario (cuántos registros del entregable hizo cada quien). Lo llena la medición de actividades; sirve para ver quién la ejecuta de verdad.

Orden: `period_start desc, activity_id, count desc`.

Archivos: `addons/quimibond_sgi/models/sgi_exec_stat.py`, `addons/quimibond_sgi/models/sgi_business_line.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad | Actividad medida. | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:28` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:57` |
| `count` | Integer | Ejecuciones | Número de registros de la actividad hechos por el usuario en la semana. |  |  |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:55` |
| `department_id` | Many2one | Departamento | Departamento del puesto de quien ejecutó. |  |  | related `job_id.department_id`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:246` |
| `employee_id` | Many2one | Empleado | Empleado del usuario al momento de medir. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:42` |
| `exec_class` | Selection | Clase | Vacía cuando el ejecutor de la actividad es un rol relativo (solicitante, quien detecta…): no hay contra quién comparar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:51` |
| `family_id` | Many2one | Familia | Familia de puestos por la que la persona tiene la actividad. |  | `sgi.job.family` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:48` |
| `job_id` | Many2one | Puesto | Puesto del empleado al momento de medir. |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:45` |
| `period_start` | Date | Semana | Lunes de la semana medida. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:36` |
| `process_id` | Many2one | Proceso | Proceso de la actividad. |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:32` |
| `sale_team_ids` | Many2many | Aplica a (equipo de ventas) | Equipos de ventas a los que aplica la actividad. Se calcula solo. |  | `crm.team` | compute `_compute_sale_team_ids`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:249` |
| `team_filter_id` | Many2one | Equipo (con las generales) | Filtro por equipo de ventas que incluye las actividades generales. |  |  | related `activity_id.team_filter_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:253` |
| `user_id` | Many2one | Usuario | Usuario que hizo los registros. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:39` |

