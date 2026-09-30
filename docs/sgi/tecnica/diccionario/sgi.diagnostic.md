<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.diagnostic`

**Diagnóstico de configuración y adopción del SGI** (TransientModel).

Archivos: `addons/quimibond_sgi/models/sgi_diagnostic.py`, `addons/quimibond_sgi_revisado/models/sgi_calidad_pq.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `bad_count` | Integer | Fallas |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:45` |
| `date` | Date | Fecha |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:43` |
| `line_ids` | One2many | Hallazgos |  |  | `sgi.diagnostic.line` |  |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:44` |
| `ok_count` | Integer | En orden |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:47` |
| `summary` | Text | Resumen |  |  |  | compute `_compute_summary`, sin guardar |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:48` |
| `warn_count` | Integer | Avisos |  |  |  | compute `_compute_counts`, sin guardar |  | `addons/quimibond_sgi/models/sgi_diagnostic.py:46` |

## Métodos públicos (3)

| Método | Qué hace (docstring) |
|---|---|
| `action_refresh` | — |
| `action_run` | Menú Diagnóstico del SGI: corre el diagnóstico y abre la lista nativa de hallazgos (fallas primero, agrupadas por sección). |
| `create` | — |
