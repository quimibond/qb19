<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.nc.step`

**Registrar el paso que toca de una NC** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_pending_actions.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_id` | Many2one | No conformidad |  | sí | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:35` |
| `date_commit` | Date | Compromiso | Fecha en que la acción queda hecha. |  |  |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:42` |
| `name` | Char | Qué se hace | Descripción de la acción de contención o correctiva. |  |  |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:39` |
| `plazos` | Char | Plazos de la NC |  |  |  | compute `_compute_plazos`, sin guardar |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:45` |
| `responsible_id` | Many2one | Responsable | Quién hace la acción. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:40` |
| `root_cause` | Text | Causa raíz | Por qué pasó (5 porqués, Ishikawa). |  |  |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:44` |
| `step` | Selection | Paso | Contención: atiende el efecto ya. Causa raíz: por qué pasó. Plan de acción: la acción correctiva con responsable y compromiso que ataca la causa. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_pending_actions.py:36` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
