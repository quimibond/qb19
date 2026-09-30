<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.spec.gap`

**Faltante de especificación de una actividad SGI** (Model).

Orden: `severity, code, activity_id`.

Archivos: `addons/quimibond_sgi/models/sgi_activity_spec.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:138` |
| `code` | Selection | Faltante |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:143` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:149` |
| `message` | Char | Detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:148` |
| `process_id` | Many2one | Proceso |  |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:141` |
| `severity` | Selection | Severidad |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:144` |

