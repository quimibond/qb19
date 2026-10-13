<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.execution.mark`

**Marcar avance de una actividad** (TransientModel).

Asistente de «En proceso», «Hecha» y «No aplica este periodo».

Archivos: `addons/quimibond_sgi/models/sgi_activity_execution.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_ids` | Many2many | Archivos de evidencia |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:564` |
| `date_due` | Date | Vence |  |  |  | related `execution_id.date_due`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:555` |
| `date_estimated` | Date | Fecha estimada | Cuándo espera terminarla (opcional). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:562` |
| `done_criteria` | Text |  |  |  |  | related `execution_id.done_criteria`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:556` |
| `evidence_note` | Text | Evidencia | Folio, documento o registro que demuestra que se hizo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:559` |
| `execution_count` | Integer | Renglones |  |  |  | compute `_compute_execution_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:548` |
| `execution_id` | Many2one | Actividad del periodo |  |  | `sgi.activity.execution` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:543` |
| `execution_ids` | Many2many | Periodos |  |  | `sgi.activity.execution` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:546` |
| `mode` | Selection | Marcar como |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:549` |
| `na_reason` | Text | Por qué no aplica |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:561` |
| `name` | Char | Qué |  |  |  | related `execution_id.name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:554` |
| `needs_evidence` | Boolean |  |  |  |  | related `execution_id.needs_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:557` |
| `progress_note` | Text | Nota de avance | Qué lleva y qué le falta. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:558` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
