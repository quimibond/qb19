<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.emergency.drill`

**Simulacro de emergencia** (Model). Hereda de: `sgi.base.mixin`.

Orden: `date_planned desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_emergency.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Acciones |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:134` |
| `date_done` | Date | Fecha realizada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:125` |
| `date_planned` | Date | Fecha programada |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:123` |
| `duration_minutes` | Integer | Duración (min) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:127` |
| `findings` | Text | Hallazgos / observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:133` |
| `participants_count` | Integer | Participantes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:126` |
| `plan_id` | Many2one | Plan de emergencia |  | sí | `sgi.emergency.plan` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:120` |
| `plan_type` | Selection |  |  |  |  | related `plan_id.plan_type`, guardado |  | `addons/quimibond_sgi/models/sgi_emergency.py:122` |
| `result` | Selection | Resultado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:128` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:136` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_cancelado` | — |
| `action_set_programado` | — |
| `action_set_realizado` | — |
| `write` | — |
