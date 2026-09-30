<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit.program.line`

**Línea del programa de auditorías** (Model).

Renglón del programa anual: qué proceso, en qué mes y con qué auditor líder. «Crear auditoría» genera la ``sgi.audit``.

Orden: `planned_month, id`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `audit_id` | Many2one | Auditoría |  |  | `sgi.audit` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:150` |
| `audit_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:136` |
| `lead_auditor_id` | Many2one | Auditor líder |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:144` |
| `norm_ids` | Many2many | Normas |  |  | `sgi.norm` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:143` |
| `partner_id` | Many2one | Cliente / proveedor |  |  | `res.partner` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:142` |
| `planned_month` | Selection | Mes planificado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:135` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:134` |
| `program_id` | Many2one | Programa |  | sí | `sgi.audit.program` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:132` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:145` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_create_audit` | — |
