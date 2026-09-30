<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit.finding`

**Hallazgo de auditoría** (Model).

Orden: `audit_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_id` | Many2one | No Conformidad |  |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:487` |
| `audit_id` | Many2one | Auditoría |  | sí | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:467` |
| `checklist_line_id` | Many2one | Pregunta del checklist |  |  | `sgi.audit.checklist.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:477` |
| `description` | Text | Descripción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:480` |
| `disposition` | Selection | Disposición |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:482` |
| `evidence` | Text | Evidencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:481` |
| `finding_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:469` |
| `norm_clause_id` | Many2one | Cláusula |  |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:476` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:479` |
| `reason_no_action` | Text | Justificación sin acción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:488` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_nc` | — |
| `unlink` | — |
