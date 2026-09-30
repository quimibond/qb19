<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.emergency.drill`

**Simulacro de emergencia** (Model). Hereda de: `sgi.base.mixin`.

Simulacro de un plan de emergencia: programado, realizado o cancelado, con resultado, hallazgos y acciones.

Orden: `date_planned desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_emergency.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:151` |
| `date_done` | Date | Fecha realizada | Fecha en que se hizo el simulacro. |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:140` |
| `date_planned` | Date | Fecha programada | Fecha en que se programa el simulacro. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:137` |
| `duration_minutes` | Integer | Duración (min) | Duración del simulacro en minutos. |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:143` |
| `findings` | Text | Hallazgos / observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:150` |
| `participants_count` | Integer | Participantes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:142` |
| `plan_id` | Many2one | Plan de emergencia | Plan de emergencia que se practica. | sí | `sgi.emergency.plan` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:133` |
| `plan_type` | Selection |  | Tipo de emergencia del plan. |  |  | related `plan_id.plan_type`, guardado |  | `addons/quimibond_sgi/models/sgi_emergency.py:136` |
| `result` | Selection | Resultado | Resultado del simulacro. Con observaciones o no satisfactorio, registre acciones. |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:144` |
| `state` | Selection | Estado | Programado, realizado o cancelado. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:153` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_cancelado` | — |
| `action_set_programado` | — |
| `action_set_realizado` | — |
| `write` | — |
