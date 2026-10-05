<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.execution.mark`

**Marcar avance de una actividad** (TransientModel).

Asistente de «En proceso», «Hecha» y «No aplica este periodo».

Archivos: `addons/quimibond_sgi/models/sgi_activity_execution.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `attachment_ids` | Many2many | Archivos de evidencia |  |  | `ir.attachment` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:436` |
| `date_due` | Date | Vence |  |  |  | related `execution_id.date_due`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:427` |
| `date_estimated` | Date | Fecha estimada | Cuándo espera terminarla (opcional). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:434` |
| `done_criteria` | Text |  |  |  |  | related `execution_id.done_criteria`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:428` |
| `evidence_note` | Text | Evidencia | Folio, documento o registro que demuestra que se hizo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:431` |
| `execution_id` | Many2one | Actividad del periodo |  | sí | `sgi.activity.execution` |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:419` |
| `mode` | Selection | Marcar como |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:421` |
| `na_reason` | Text | Por qué no aplica |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:433` |
| `name` | Char | Qué |  |  |  | related `execution_id.name`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:426` |
| `needs_evidence` | Boolean |  |  |  |  | related `execution_id.needs_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:429` |
| `progress_note` | Text | Nota de avance | Qué lleva y qué le falta. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_execution.py:430` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_confirm` | — |
