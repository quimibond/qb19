<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.process.stage`

**Etapa de un proceso SGI** (Model).

Etapa de un proceso; agrupa sus actividades.

Orden: `process_id, sequence, code, id`.

Archivos: `addons/quimibond_sgi/models/sgi_deliverable.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_count` | Integer | Núm. de actividades |  |  |  | compute `_compute_activity_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_deliverable.py:41` |
| `activity_ids` | One2many | Actividades |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:40` |
| `code` | Char | Letra | A, B, C… (opcional). |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:38` |
| `company_id` | Many2one | Empresa |  |  |  | related `process_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_deliverable.py:42` |
| `name` | Char | Etapa |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:39` |
| `process_id` | Many2one | Proceso |  | sí | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:35` |
| `sequence` | Integer |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_deliverable.py:37` |

