<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.audit.program`

**Programa anual de auditorías (P-G03)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Programa anual de auditorías (P-G03). Lo arma MAST (puede sugerir renglones), se aprueba y de cada renglón nace la auditoría.

Orden: `year desc`.

Archivos: `addons/quimibond_sgi/models/sgi_audit.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `line_ids` | One2many | Líneas |  |  | `sgi.audit.program.line` |  |  | `addons/quimibond_sgi/models/sgi_audit.py:48` |
| `name` | Char | Nombre |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_audit.py:40` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:43` |
| `year` | Integer | Año |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_audit.py:41` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_approve` | 4.4: solo MAST aprueba, y cada auditoría interna del programa lleva su auditor líder (en 2026 las 14 líneas estaban sin auditor). |
| `action_close` | — |
| `action_draft` | — |
| `action_suggest_lines` | AU-5 (53.0.0): programa sugerido. Una línea por proceso vigente o en piloto (subprocesos), repartidos por trimestre; los procesos con NC abiertas o indicadores en rojo, dos veces al año. Solo agrega … |
