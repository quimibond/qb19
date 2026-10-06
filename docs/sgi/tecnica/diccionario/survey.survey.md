<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `survey.survey`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi/models/sgi_competence_grant.py`.

## Campos (3)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_skill_id` | Many2one | Competencia SGI que otorga | Quien aprueba esta certificación recibe la competencia, con la vigencia de la certificación. |  | `hr.skill` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:46` |
| `sgi_skill_level_id` | Many2one | Nivel que otorga | Nivel de competencia que obtiene quien aprueba. |  | `hr.skill.level` |  |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:53` |
| `sgi_skill_type_id` | Many2one | Tipo de competencia | Tipo de la competencia que otorga el examen. |  |  | related `sgi_skill_id.skill_type_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_competence_grant.py:50` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `write` | I-7: ligar un examen con una competencia es del Jefe MAST, por el servidor y con sudo (no necesita permisos de la app Encuestas). |
