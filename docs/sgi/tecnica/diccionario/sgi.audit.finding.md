<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit.finding`

**Hallazgo de auditoría** (Model).

Hallazgo de una auditoría con cláusula y evidencia. Si la disposición lo pide, «Generar NC» crea la NC ligada.

Orden: `audit_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`.

## Campos (10)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_id` | Many2one | No Conformidad | No conformidad generada desde este hallazgo. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:531` |
| `audit_id` | Many2one | Auditoría | Auditoría a la que pertenece el hallazgo. | sí | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:505` |
| `checklist_line_id` | Many2one | Pregunta del checklist |  |  | `sgi.audit.checklist.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:518` |
| `description` | Text | Descripción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:522` |
| `disposition` | Selection | Disposición | Qué se hace con el hallazgo: generar NC, registrar una mejora o no hacer nada (con motivo). Sin disposición la auditoría no se cierra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:524` |
| `evidence` | Text | Evidencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:523` |
| `finding_type` | Selection | Tipo | Conformidad, observación, no conformidad menor o mayor, u oportunidad de mejora. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:508` |
| `norm_clause_id` | Many2one | Cláusula | Cláusula de la norma a la que se refiere el hallazgo. |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:516` |
| `process_id` | Many2one | Proceso | Proceso en el que se encontró el hallazgo. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:520` |
| `reason_no_action` | Text | Justificación sin acción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:533` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_nc` | — |
| `unlink` | — |
