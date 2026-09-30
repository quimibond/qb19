<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.exec.stat`

**Ejecuciones de una actividad SGI por semana y usuario** (Model).

Orden: `period_start desc, activity_id, count desc`.

Archivos: `addons/quimibond_sgi/models/sgi_exec_stat.py`, `addons/quimibond_sgi/models/sgi_business_line.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:26` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:50` |
| `count` | Integer | Ejecuciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:49` |
| `department_id` | Many2one | Departamento |  |  |  | related `job_id.department_id`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:244` |
| `employee_id` | Many2one | Empleado | Empleado del usuario al momento de medir. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:37` |
| `exec_class` | Selection | Clase | Vacía cuando el ejecutor de la actividad es un rol relativo (solicitante, quien detecta…): no hay contra quién comparar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:45` |
| `family_id` | Many2one | Familia |  |  | `sgi.job.family` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:43` |
| `job_id` | Many2one | Puesto | Puesto del empleado al momento de medir. |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:40` |
| `period_start` | Date | Semana | Lunes de la semana medida. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:32` |
| `process_id` | Many2one | Proceso |  |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:29` |
| `sale_team_ids` | Many2many | Aplica a (equipo de ventas) |  |  | `crm.team` | compute `_compute_sale_team_ids`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:246` |
| `team_filter_id` | Many2one | Equipo (con las generales) |  |  |  | related `activity_id.team_filter_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:249` |
| `user_id` | Many2one | Usuario |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_exec_stat.py:35` |

