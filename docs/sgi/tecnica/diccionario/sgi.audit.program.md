<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit.program`

**Programa anual de auditorías (P-G03)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Programa anual de auditorías (P-G03). Lo arma MAST (puede sugerir renglones), se aprueba y de cada renglón nace la auditoría.

Orden: `year desc`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `coverage_gap_count` | Integer | Procesos sin programar en 3 años |  |  |  | compute `_compute_coverage_gap`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:66` |
| `coverage_gap_ids` | Many2many | Sin programar en 3 años | Subprocesos sin renglón en este programa ni en los de los dos años anteriores registrados en Odoo. |  | `sgi.process` | compute `_compute_coverage_gap`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:62` |
| `line_count` | Integer | Auditorías programadas | Renglones del programa. |  |  | compute `_compute_progress`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:55` |
| `line_done_count` | Integer | Auditorías hechas | Renglones cuya auditoría ya se cerró. |  |  | compute `_compute_progress`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:57` |
| `line_ids` | One2many | Líneas |  |  | `sgi.audit.program.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:53` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_audit.py:42` |
| `progress_pct` | Float | Avance | Auditorías cerradas entre auditorías programadas, en %. |  |  | compute `_compute_progress`, sin guardar |  | `addons/quimibond_sgi/models/sgi_audit.py:59` |
| `state` | Selection | Estado | Borrador mientras se arma; aprobado cuando se autoriza (desde ahí se avisa cada auditoría); cerrado al terminar el año. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:46` |
| `year` | Integer | Año | Año que cubre el programa. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:43` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_approve` | 4.4: solo MAST aprueba, y cada auditoría interna del programa lleva su auditor líder (en 2026 las 14 líneas estaban sin auditor). |
| `action_close` | — |
| `action_draft` | — |
| `action_suggest_lines` | AU-5 (53.0.0): programa sugerido. Una línea por subproceso, repartidos por trimestre; los procesos con NC abiertas o indicadores en rojo, dos veces al año. Solo agrega los que aún no están. 57.93.0 (… |
