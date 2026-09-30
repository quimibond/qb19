<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.catalog.load.wizard`

**Cargar catálogo SGI** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_load_wizard.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `change_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:39` |
| `dry_run_ok` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:34` |
| `error_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:38` |
| `line_ids` | One2many | Resultado |  |  | `sgi.catalog.load.wizard.line` |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:36` |
| `payload_file` | Binary | Archivo JSON |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:27` |
| `payload_filename` | Char | Nombre del archivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:28` |
| `payload_text` | Text | JSON |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:26` |
| `state` | Selection | Estado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:29` |
| `summary` | Text | Resumen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:35` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_load` | — |
| `action_test` | — |
