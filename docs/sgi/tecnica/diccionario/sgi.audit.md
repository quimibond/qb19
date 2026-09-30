<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit`

**Auditoría interna (P-G03)** (Model). Hereda de: `sgi.base.mixin`.

Orden: `date_planned desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`.

## Campos (24)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `audit_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:180` |
| `auditee_ids` | Many2many | Auditados |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:197` |
| `auditor_ids` | Many2many | Equipo auditor |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:195` |
| `checklist_answered_count` | Integer |  |  |  |  | compute `_compute_checklist_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:226` |
| `checklist_count` | Integer |  |  |  |  | compute `_compute_checklist_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:225` |
| `checklist_line_ids` | One2many | Checklist |  |  | `sgi.audit.checklist.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:223` |
| `checklist_nonconforming_count` | Integer |  |  |  |  | compute `_compute_checklist_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:227` |
| `closing_minutes` | Text | Minuta de cierre | Resumen presentado al auditado: hallazgos, conclusión, plazos. Sustituye el formato F-P-G03-06. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:209` |
| `conclusion` | Text | Conclusión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:202` |
| `date_end` | Date | Fin real |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:201` |
| `date_planned` | Date | Fecha planificada |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:199` |
| `date_start` | Date | Inicio real |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:200` |
| `external_report_ref` | Char | N° de reporte externo | Número del reporte de auditoría del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:190` |
| `finding_count` | Integer | # Hallazgos |  |  |  | compute `_compute_finding_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:221` |
| `finding_ids` | One2many | Hallazgos |  |  | `sgi.audit.finding` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:220` |
| `lead_auditor_id` | Many2one | Auditor líder |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:194` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_audit.py:178` |
| `norm_ids` | Many2many | Normas |  |  | `sgi.norm` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:192` |
| `opening_minutes` | Text | Minuta de apertura | Acuerdos de la reunión de apertura: alcance confirmado, agenda, criterios. Sustituye el formato F-P-G03-05. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:205` |
| `partner_id` | Many2one | Cliente / proveedor |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:189` |
| `process_ids` | Many2many | Procesos auditados |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:193` |
| `program_line_id` | Many2one | Línea de programa |  |  | `sgi.audit.program.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:179` |
| `report_document_id` | Many2one | Informe archivado |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:229` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:213` |

## Métodos públicos (10)

| Método | Qué hace (docstring) |
|---|---|
| `action_close` | — |
| `action_draft` | — |
| `action_evaluate_auditors` | Abre la encuesta de evaluación del comportamiento del auditor (sustituye F-IT-P-G03-01-01): el auditado la contesta al terminar la auditoría y el resultado queda en Encuestas, consultable por MAST. L… |
| `action_generate_checklist` | Además de una pregunta por actividad, si la auditoría dice qué normas cubre: una pregunta por cada punto de esas normas que no tiene ninguna actividad en el SGI (idempotente). |
| `action_open_findings` | — |
| `action_open_report_document` | — |
| `action_plan` | — |
| `action_report` | — |
| `action_start` | — |
| `write` | — |
