<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.dropbox.key`

**Clave anterior del Dropbox** (Model).

Buscador por clave anterior: cada renglón es una clave vieja y dice qué es hoy y dónde vive.

Orden: `key, id`.

Archivos: `addons/quimibond_sgi/models/sgi_dropbox_views.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad archivada |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:76` |
| `activity_numbers` | Char | Numerales |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:86` |
| `destination` | Char | Dónde vive en Odoo |  |  |  | compute `_compute_destination`, sin guardar |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:87` |
| `doc_state` | Selection | Estado del documento |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:80` |
| `document_id` | Many2one | Documento |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:74` |
| `key` | Char | Clave anterior |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:71` |
| `kind` | Selection | Qué es |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:72` |
| `migration_state` | Selection | Estado de migración |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:78` |
| `odoo_menu_id` | Many2one | Menú de Odoo |  |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:84` |
| `point_id` | Many2one | Worksheet |  |  | `quality.point` |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:85` |
| `process_id` | Many2one | Proceso |  |  | `sgi.process` |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:77` |
| `routine_id` | Many2one | Rutina |  |  | `sgi.legacy.routine` |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:75` |
| `routine_state` | Selection | Estado de la rutina |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:79` |
| `title` | Char | Título |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_dropbox_views.py:73` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_previous` | «Ver el anterior»: el documento (o la actividad archivada). |
| `action_open_target` | «Abrir en Odoo»: menú → su acción; worksheet → el punto de calidad; rutina → sus actividades; procedimiento → el proceso que lo sustituye (o su proceso actual). |
