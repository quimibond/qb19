<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.training.effectiveness`

**Eficacia de la capacitación** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Eficacia de la capacitación (ISO 9001 7.2 c): a los 90 días de otorgar una competencia por examen o curso, el jefe inmediato dice si fue eficaz.

Orden: `due_date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_competence_grant.py`.

## Campos (16)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `channel_id` | Many2one | Curso |  |  | `slide.channel` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:287` |
| `company_id` | Many2one | Empresa |  |  |  | related `employee_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:275` |
| `due_date` | Date | Evaluar a más tardar | Día en que vence la evaluación de eficacia (90 días después de otorgarla; parámetro quimibond_sgi.training_effectiveness_days). | sí |  |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:291` |
| `employee_id` | Many2one | Empleado | Persona que recibió la competencia. | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:268` |
| `employee_name` | Char | Persona | Nombre de la persona que recibió la competencia. |  |  | related `employee_id.name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:273` |
| `evaluated_by` | Many2one | Evaluada por |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:309` |
| `evaluated_date` | Date | Evaluada el |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:308` |
| `evaluator_id` | Many2one | Evalúa | Jefe inmediato; si no tiene usuario, el responsable del departamento, RH o el Jefe MAST. | sí | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:295` |
| `granted_date` | Date | Otorgada el | Día en que la persona recibió la competencia. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:289` |
| `origin` | Selection | Origen | Cómo se otorgó la competencia: curso terminado o examen aprobado. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:282` |
| `result_note` | Text | Comentario | Qué se observó en el trabajo de la persona. Obligatorio si no fue eficaz. |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:305` |
| `skill_id` | Many2one | Competencia | Competencia que se otorgó. | sí | `hr.skill` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:277` |
| `skill_level_id` | Many2one | Nivel | Nivel otorgado. |  | `hr.skill.level` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:280` |
| `state` | Selection | Resultado | Pendiente hasta que quien evalúa dice si la capacitación fue eficaz. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:299` |
| `survey_id` | Many2one | Examen |  |  | `survey.survey` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:288` |
| `survey_input_id` | Many2one | Encuesta al jefe | Invitación a la encuesta de eficacia (opcional; parámetro quimibond_sgi.training_effectiveness_survey_id). |  | `survey.user_input` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:310` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_mark_effective` | — |
| `action_mark_ineffective` | — |
| `unlink` | — |
| `write` | — |
