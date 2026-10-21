<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit.finding`

**Hallazgo de auditoría** (Model).

Hallazgo de una auditoría con cláusula y evidencia. Si la disposición lo pide, «Generar NC» crea la NC ligada.

Orden: `audit_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `alert_id` | Many2one | No conformidad | No conformidad generada desde este hallazgo. |  | `quality.alert` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:585` |
| `audit_id` | Many2one | Auditoría | Auditoría a la que pertenece el hallazgo. | sí | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:559` |
| `checklist_line_id` | Many2one | Pregunta del checklist |  |  | `sgi.audit.checklist.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:572` |
| `description` | Text | Descripción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:576` |
| `disposition` | Selection | Disposición | Qué se hace con el hallazgo: generar NC, registrar una mejora o no hacer nada (con motivo). Sin disposición la auditoría no se cierra. |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:578` |
| `evidence` | Text | Evidencia |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:577` |
| `finding_type` | Selection | Tipo | Conformidad, observación, no conformidad menor o mayor, u oportunidad de mejora. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:562` |
| `norm_clause_id` | Many2one | Cláusula | Cláusula de la norma a la que se refiere el hallazgo. |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:570` |
| `process_id` | Many2one | Proceso | Proceso en el que se encontró el hallazgo. |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:574` |
| `reason_no_action` | Text | Justificación sin acción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:587` |
| `sgi_is_locked` | Boolean | Cerrado (solo lectura) |  |  |  | compute `_compute_sgi_is_locked`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:603` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_generate_nc` | — |
| `create` | — |
| `unlink` | — |
| `write` | — |
