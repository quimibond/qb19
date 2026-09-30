<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.mapa.load.wizard`

**Cargar mapa de procesos SGI** (TransientModel).

Archivos: `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `change_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:66` |
| `confirm` | Boolean | Entiendo que se escribe en la base |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:58` |
| `counts` | Char | El mapa trae |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:61` |
| `dry_run_ok` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:59` |
| `error_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:64` |
| `line_ids` | One2many | Resultado |  |  | `sgi.mapa.load.wizard.line` |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:62` |
| `payload_file` | Binary | Archivo JSON |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:50` |
| `payload_filename` | Char | Nombre del archivo |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:51` |
| `source` | Selection | Qué mapa |  | sí |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:46` |
| `state` | Selection |  |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:52` |
| `summary` | Text | Resumen |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:60` |
| `tested_hash` | Char |  |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:57` |
| `warning_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:65` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_export` | Descarga el mapa de ESTA base (export_payload) como JSON. |
| `action_load` | — |
| `action_test` | — |
| `create` | — |
