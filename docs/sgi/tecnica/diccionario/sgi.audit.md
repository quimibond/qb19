<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit`

**Auditoría interna (P-G03)** (Model). Hereda de: `sgi.base.mixin`.

Auditoría interna o a proveedor/cliente (P-G03): planeación, checklist generado del proceso, hallazgos y cierre con informe. Nace del programa anual o a mano.

Orden: `date_planned desc, folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`.

## Campos (24)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `audit_type` | Selection | Tipo | Interna, externa de certificación, de un cliente a Quimibond o de Quimibond a un proveedor. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:190` |
| `auditee_ids` | Many2many | Auditados | Personas auditadas. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:214` |
| `auditor_ids` | Many2many | Equipo auditor | Auditores que acompañan al auditor líder. Deben ser independientes del proceso auditado. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:210` |
| `checklist_answered_count` | Integer |  |  |  |  | compute `_compute_checklist_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:246` |
| `checklist_count` | Integer |  |  |  |  | compute `_compute_checklist_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:245` |
| `checklist_line_ids` | One2many | Checklist |  |  | `sgi.audit.checklist.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:243` |
| `checklist_nonconforming_count` | Integer |  |  |  |  | compute `_compute_checklist_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:247` |
| `closing_minutes` | Text | Minuta de cierre | Resumen presentado al auditado: hallazgos, conclusión, plazos. Sustituye el formato F-P-G03-06. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:228` |
| `conclusion` | Text | Conclusión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:221` |
| `date_end` | Date | Fin real | Fecha en que terminó la auditoría. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:220` |
| `date_planned` | Date | Fecha planificada | Fecha en que se planea realizar la auditoría. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:217` |
| `date_start` | Date | Inicio real | Fecha en que empezó la auditoría. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:219` |
| `external_report_ref` | Char | N° de reporte externo | Número del reporte de auditoría del cliente. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:202` |
| `finding_count` | Integer | # Hallazgos |  |  |  | compute `_compute_finding_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:241` |
| `finding_ids` | One2many | Hallazgos |  |  | `sgi.audit.finding` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:240` |
| `lead_auditor_id` | Many2one | Auditor líder | Responsable de la auditoría. No puede ser dueño de un proceso auditado. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:207` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_audit.py:187` |
| `norm_ids` | Many2many | Normas | Normas contra las que se audita. |  | `sgi.norm` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:204` |
| `opening_minutes` | Text | Minuta de apertura | Acuerdos de la reunión de apertura: alcance confirmado, agenda, criterios. Sustituye el formato F-P-G03-05. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:224` |
| `partner_id` | Many2one | Cliente / proveedor | Cliente que audita a Quimibond o proveedor al que se audita. |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:200` |
| `process_ids` | Many2many | Procesos auditados | Procesos que cubre la auditoría. De ellos se genera el checklist. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:205` |
| `program_line_id` | Many2one | Línea de programa | Renglón del programa anual del que nació esta auditoría. |  | `sgi.audit.program.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:188` |
| `report_document_id` | Many2one | Informe archivado | Informe de la auditoría archivado en Documentos al cerrarla. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:249` |
| `state` | Selection | Estado | Borrador, planificada, en ejecución, informe o cerrada. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:232` |

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
