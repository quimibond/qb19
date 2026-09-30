<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.change.role`

**Quién hace la actividad (propuesta)** (Model).

Orden: `change_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_mp_change.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `after_days` | Integer | Escala a los (días hábiles) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:563` |
| `change_id` | Many2one |  |  | sí | `sgi.activity.change` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:541` |
| `condition` | Char | Condición |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:564` |
| `family_id` | Many2one | Familia de puestos |  |  | `sgi.job.family` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:561` |
| `job_id` | Many2one | Puesto |  |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:560` |
| `relative_role` | Selection | Rol relativo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:562` |
| `role` | Selection | Rol |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:557` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:542` |
| `target_type` | Selection | Asignado a |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:558` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
