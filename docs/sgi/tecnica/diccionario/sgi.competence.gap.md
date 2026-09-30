<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.competence.gap`

**Brecha de competencia (DNC)** (Model).

Vista SQL: brechas entre las competencias esperadas del puesto (hr.job.skill) y las que tiene el empleado (hr.employee.skill).

Orden: `department_id, employee_id`.

Archivos: `addons/quimibond_sgi/models/sgi_competence.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `current_level_id` | Many2one | Nivel actual |  |  | `hr.skill.level` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:44` |
| `current_progress` | Integer | % actual |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence.py:46` |
| `department_id` | Many2one | Departamento |  |  | `hr.department` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:39` |
| `employee_id` | Many2one | Empleado |  |  | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:38` |
| `gap` | Integer | Brecha (%) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence.py:47` |
| `job_id` | Many2one | Puesto |  |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:40` |
| `required_level_id` | Many2one | Nivel requerido |  |  | `hr.skill.level` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:43` |
| `required_progress` | Integer | % requerido |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence.py:45` |
| `skill_id` | Many2one | Competencia |  |  | `hr.skill` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:41` |
| `skill_type_id` | Many2one | Tipo |  |  | `hr.skill.type` |  |  | `addons/quimibond_sgi/models/sgi_competence.py:42` |

