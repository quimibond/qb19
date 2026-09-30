<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `studio.approval.rule`

Modelo de otra app que el SGI extiende.

Archivos: `addons/quimibond_sgi_studio/models/sgi_approval_studio.py`, `addons/quimibond_sgi_studio/models/studio_approval_rule_archive.py`.

## Campos (1)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `sgi_role_id` | Many2one | Rol SGI que aprueba | Renglón «Aprueba» de la actividad del procedimiento que mantiene esta regla. |  | `sgi.activity.role` |  |  | `addons/quimibond_sgi_studio/models/sgi_approval_studio.py:17` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `write` | — |
