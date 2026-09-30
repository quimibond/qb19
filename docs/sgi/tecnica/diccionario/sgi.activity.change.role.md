<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.change.role`

**Quién hace la actividad (propuesta)** (Model).

Renglón «quién hace» de una propuesta de cambio (``sgi.activity.change``): puesto, familia o rol relativo con su papel. Al aprobarse la propuesta pasa a ``sgi.activity.role``.

Orden: `change_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_mp_change.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `after_days` | Integer | Escala a los (días hábiles) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:588` |
| `change_id` | Many2one |  |  | sí | `sgi.activity.change` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:566` |
| `condition` | Char | Condición |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:589` |
| `family_id` | Many2one | Familia de puestos |  |  | `sgi.job.family` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:586` |
| `job_id` | Many2one | Puesto |  |  | `hr.job` |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:585` |
| `relative_role` | Selection | Rol relativo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:587` |
| `role` | Selection | Rol |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:582` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:567` |
| `target_type` | Selection | Asignado a |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_mp_change.py:583` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `create` | — |
| `unlink` | — |
| `write` | — |
