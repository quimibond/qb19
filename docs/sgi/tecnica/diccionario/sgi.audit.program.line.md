<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit.program.line`

**Línea del programa de auditorías** (Model).

Orden: `planned_month, id`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `audit_id` | Many2one | Auditoría |  |  | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:143` |
| `audit_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:129` |
| `lead_auditor_id` | Many2one | Auditor líder |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:137` |
| `norm_ids` | Many2many | Normas |  |  | `sgi.norm` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:136` |
| `partner_id` | Many2one | Cliente / proveedor |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:135` |
| `planned_month` | Selection | Mes planificado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:128` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:127` |
| `program_id` | Many2one | Programa |  | sí | `sgi.audit.program` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:125` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:138` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_create_audit` | — |
