<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit.checklist.line`

**Pregunta del checklist de auditoría (por actividad)** (Model).

AU-1 (50.0.0): el checklist de la auditoría sale del proceso, una pregunta por actividad («¿se cumple C2.17 … en plazo y con evidencia?»), con acceso a los registros reales del entregable. Cada respuesta no conforme crea (y mantiene) su ha…

Orden: `audit_id, sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:170` |
| `answer` | Selection | Respuesta |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:696` |
| `audit_id` | Many2one | Auditoría |  | sí | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:686` |
| `can_open_records` | Boolean |  |  |  |  | compute `_compute_can_open_records`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:695` |
| `deliverables` | Char | Entregable |  |  |  | compute `_compute_question`, guardado |  | `addons/quimibond_sgi/models/sgi_audit.py:694` |
| `evidence` | Text | Evidencia | Qué registros se revisaron y qué se encontró. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:702` |
| `executor` | Char | Quién la ejecuta |  |  |  | compute `_compute_question`, guardado |  | `addons/quimibond_sgi/models/sgi_audit.py:693` |
| `finding_id` | Many2one | Hallazgo |  |  | `sgi.audit.finding` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:703` |
| `norm_clause_ids` | Many2many | Requisitos | Puntos de la norma que se auditan con esta pregunta. |  | `sgi.norm.clause` | compute `_compute_norm_clause_ids`, guardado |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:171` |
| `process_id` | Many2one | Proceso |  |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_audit.py:691` |
| `question` | Char | Pregunta |  |  |  | compute `_compute_question`, guardado |  | `addons/quimibond_sgi/models/sgi_audit.py:692` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:688` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_records` | Los registros recientes del entregable de la actividad: la evidencia que el auditor revisa. |
| `create` | — |
| `write` | — |
