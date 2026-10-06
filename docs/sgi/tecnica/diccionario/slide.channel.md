<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `slide.channel`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_competence_grant.py`, `addons/quimibond_sgi/models/sgi_sign_elearning.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_skill_id` | Many2one | Competencia SGI que otorga | Al terminar el curso, el empleado recibe esta competencia al nivel indicado (cierra la brecha en la DNC). |  | `hr.skill` |  |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:156` |
| `sgi_skill_level_id` | Many2one | Nivel que otorga | Nivel de competencia que obtiene quien termina el curso. |  | `hr.skill.level` |  |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:163` |
| `sgi_skill_type_id` | Many2one | Tipo de competencia | Tipo de la competencia que otorga el curso. |  |  | related `sgi_skill_id.skill_type_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_sign_elearning.py:160` |
| `sgi_skill_validity_months` | Integer | Vigencia (meses) | Meses que dura la competencia que otorga el curso. 0: no vence. |  |  |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:82` |

