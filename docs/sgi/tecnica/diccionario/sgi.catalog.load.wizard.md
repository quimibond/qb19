<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.catalog.load.wizard`

**Cargar catálogo SGI** (TransientModel).

Asistente «Cargar catálogo»: recibe el JSON de ``sgi.process.load_payload``, lo prueba sin escribir y luego lo carga. Solo Administrador SGI.

Archivos: `addons/quimibond_sgi/models/sgi_load_wizard.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `change_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:43` |
| `dry_run_ok` | Boolean |  | Indica que la prueba salió sin errores y ya se puede cargar. |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:37` |
| `error_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:42` |
| `line_ids` | One2many | Resultado |  |  | `sgi.catalog.load.wizard.line` |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:40` |
| `payload_file` | Binary | Archivo JSON |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:29` |
| `payload_filename` | Char | Nombre del archivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:30` |
| `payload_text` | Text | JSON |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:28` |
| `state` | Selection | Estado | Captura, probado (sin escribir nada) o cargado. |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:31` |
| `summary` | Text | Resumen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_load_wizard.py:39` |

## Métodos públicos (2)

| Método | Qué hace (docstring) |
|---|---|
| `action_load` | — |
| `action_test` | — |
