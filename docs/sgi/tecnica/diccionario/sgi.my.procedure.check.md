<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.my.procedure.check`

**Mi procedimiento: revisión previa a publicar** (TransientModel).

Revisión previa a publicar, con listas nativas (antes HTML).

Archivos: `addons/quimibond_sgi/models/sgi_my_procedure_screen.py`.

## Campos (5)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `duplicate_job_ids` | Many2many | Puestos duplicados | Puestos con el mismo nombre normalizado. Un empleado en el duplicado sin roles abre su procedimiento y lo ve vacío. |  | `hr.job` | compute `_compute_result`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:844` |
| `job_without_roles_employee_ids` | Many2many | Empleados en un puesto sin roles | Personas cuyo puesto no tiene ninguna actividad del SGI. |  | `hr.employee.public` | compute `_compute_result`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:852` |
| `no_job_employee_ids` | Many2many | Empleados sin puesto | Personas sin puesto: no tienen Mi procedimiento. |  | `hr.employee.public` | compute `_compute_result`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:848` |
| `ready_count` | Integer | Puestos listos para publicar | Puestos con roles y con personas. |  |  | compute `_compute_result`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:841` |
| `roles_without_people_job_ids` | Many2many | Puestos con roles pero sin personas | Puestos con actividades del SGI pero sin nadie que los ocupe. |  | `hr.job` | compute `_compute_result`, sin guardar |  | `addons/quimibond_sgi/models/sgi_my_procedure_screen.py:856` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_open` | Administración SGI → Firmas de lectura → Publicar Mi procedimiento. |
| `action_publish_all` | — |
