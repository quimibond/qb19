<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.approval.subject`

**Asunto de una categoría de Aprobaciones** (Model).

Orden: `category_id, sequence, name`.

Archivos: `addons/quimibond_sgi/models/sgi_approval_subject.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_subject.py:31` |
| `activity_id` | Many2one | Actividad |  |  |  | related `role_id.activity_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_approval_subject.py:30` |
| `category_id` | Many2one | Categoría |  | sí | `approval.category` |  |  | `addons/quimibond_sgi/models/sgi_approval_subject.py:23` |
| `name` | Char | Asunto | Lo que se pide, en pocas palabras: «Propuesta de pago semanal». | sí |  |  |  | `addons/quimibond_sgi/models/sgi_approval_subject.py:20` |
| `role_id` | Many2one | Aprobación del SGI | Rol «Aprueba» de la actividad: sus personas aprueban la solicitud y la medición de la actividad cuenta las solicitudes de este asunto. |  | `sgi.activity.role` |  |  | `addons/quimibond_sgi/models/sgi_approval_subject.py:25` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_approval_subject.py:22` |

