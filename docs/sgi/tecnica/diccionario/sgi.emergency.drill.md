<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.emergency.drill`

**Simulacro de emergencia** (Model). Hereda de: `sgi.base.mixin`.

Simulacro de un plan de emergencia: programado, realizado o cancelado, con resultado, hallazgos y acciones.

Orden: `date_planned desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_emergency.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:138` |
| `date_done` | Date | Fecha realizada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:129` |
| `date_planned` | Date | Fecha programada |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:127` |
| `duration_minutes` | Integer | Duración (min) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:131` |
| `findings` | Text | Hallazgos / observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:137` |
| `participants_count` | Integer | Participantes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:130` |
| `plan_id` | Many2one | Plan de emergencia |  | sí | `sgi.emergency.plan` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:124` |
| `plan_type` | Selection |  |  |  |  | related `plan_id.plan_type`, guardado |  | `addons/quimibond_sgi/models/sgi_emergency.py:126` |
| `result` | Selection | Resultado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:132` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:140` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_cancelado` | — |
| `action_set_programado` | — |
| `action_set_realizado` | — |
| `write` | — |
