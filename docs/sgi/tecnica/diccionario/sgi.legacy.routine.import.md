<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.legacy.routine.import`

**Importar rutinas del Dropbox** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_legacy_routine_import.py`.

## Campos (11)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `archive_missing` | Boolean | Archivar las rutinas que ya no vienen | Del mismo procedimiento. Se archivan, nunca se borran. |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:147` |
| `confirm` | Boolean | Entiendo que se escribe en la base |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:156` |
| `dry_run_ok` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:157` |
| `error_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:161` |
| `file` | Binary | Libro (XLSX o CSV) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:145` |
| `filename` | Char | Nombre del archivo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:146` |
| `line_ids` | One2many | Resultado |  |  | `sgi.legacy.routine.import.line` |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:159` |
| `state` | Selection |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:150` |
| `summary` | Text | Resumen |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:158` |
| `tested_hash` | Char |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:155` |
| `warning_count` | Integer |  |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_legacy_routine_import.py:162` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_load` | — |
| `action_test` | — |
| `create` | — |
