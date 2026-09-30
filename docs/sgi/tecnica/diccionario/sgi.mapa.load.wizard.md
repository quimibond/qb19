<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.mapa.load.wizard`

**Cargar mapa de procesos SGI** (TransientModel).

Asistente «Cargar mapa de procesos» de ``quimibond_sgi_mapa``: prueba y carga el JSON del módulo o uno subido, y descarga el de la base.

Archivos: `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `change_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:68` |
| `confirm` | Boolean | Entiendo que se escribe en la base |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:60` |
| `counts` | Char | El mapa trae |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:63` |
| `dry_run_ok` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:61` |
| `error_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:66` |
| `line_ids` | One2many | Resultado |  |  | `sgi.mapa.load.wizard.line` |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:64` |
| `payload_file` | Binary | Archivo JSON |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:52` |
| `payload_filename` | Char | Nombre del archivo |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:53` |
| `source` | Selection | Qué mapa |  | sí |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:48` |
| `state` | Selection |  |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:54` |
| `summary` | Text | Resumen |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:62` |
| `tested_hash` | Char |  |  |  |  |  |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:59` |
| `warning_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi_mapa/models/sgi_mapa_wizard.py:67` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_export` | Descarga el mapa de ESTA base (export_payload) como JSON. |
| `action_load` | — |
| `action_test` | — |
| `create` | — |
