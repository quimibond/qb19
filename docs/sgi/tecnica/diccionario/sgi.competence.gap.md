<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.competence.gap`

**Brecha de competencia (DNC)** (Model).

Vista SQL: brechas entre las competencias esperadas del puesto (hr.job.skill) y las que tiene el empleado (hr.employee.skill).

Orden: `department_id, employee_id`.

Archivos: `addons/quimibond_sgi/models/sgi_competence.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `current_level_id` | Many2one | Nivel actual | Nivel que hoy tiene el empleado en la competencia. |  | `hr.skill.level` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:50` |
| `current_progress` | Integer | % actual |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence.py:53` |
| `department_id` | Many2one | Departamento | Departamento del empleado. |  | `hr.department` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:40` |
| `employee_id` | Many2one | Empleado | Empleado con la brecha. |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:38` |
| `gap` | Integer | Brecha (%) | Diferencia entre el nivel requerido y el actual, en porcentaje. |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence.py:54` |
| `job_id` | Many2one | Puesto | Puesto que requiere la competencia. |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:42` |
| `required_level_id` | Many2one | Nivel requerido | Nivel que el puesto requiere. |  | `hr.skill.level` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:48` |
| `required_progress` | Integer | % requerido |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence.py:52` |
| `skill_id` | Many2one | Competencia | Competencia con brecha. |  | `hr.skill` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:44` |
| `skill_type_id` | Many2one | Tipo | Tipo de competencia. |  | `hr.skill.type` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:46` |

