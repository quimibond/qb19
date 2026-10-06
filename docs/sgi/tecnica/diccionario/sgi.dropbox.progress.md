<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dropbox.progress`

**Avance de la transición del Dropbox** (Model).

Avance de la transición: un renglón por proceso activo.

Orden: `process_code, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dropbox_views.py`.

## Campos (20)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activities_new` | Integer | Actividades nuevas (sin antecedente) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:269` |
| `company_id` | Many2one | Empresa |  |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:253` |
| `docs_incomplete` | Integer | Documentos incompletos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:268` |
| `docs_migrated` | Integer | Migrados con liga |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:265` |
| `docs_paper` | Integer | Siguen como documento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:266` |
| `docs_pending` | Integer | Documentos pendientes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:267` |
| `docs_total` | Integer | Documentos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:264` |
| `procedures_control` | Integer | Control operacional |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:256` |
| `procedures_pending` | Integer | Procedimientos con pendientes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:255` |
| `procedures_replaced` | Integer | Procedimientos sustituidos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:254` |
| `process_code` | Char | Clave |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:252` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:251` |
| `progress_pct` | Float | % migrado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:270` |
| `routines_covered` | Integer | Cubiertas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:258` |
| `routines_eliminated` | Integer | Eliminadas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:260` |
| `routines_pending` | Integer | Rutinas pendientes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:261` |
| `routines_replaced` | Integer | La hace Odoo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:259` |
| `routines_resolved_pct` | Float | % rutinas resuelto |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:263` |
| `routines_total` | Integer | Rutinas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:257` |
| `routines_undecided` | Integer | Pendientes sin decisión |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:262` |

## Métodos públicos (5)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_documents` | — |
| `action_open_new_activities` | — |
| `action_open_pending_routines` | — |
| `action_open_procedures` | — |
| `action_open_routines` | — |
