<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.norm`

**Norma ISO** (Model).

Norma de referencia (ISO 9001, 14001, 45001 y las que se capturen) con sus cláusulas.

Orden: `code`.

Archivos: `addons/quimibond_sgi/models/sgi_norm.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`.

## Campos (6)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:21` |
| `clause_count` | Integer | Cláusulas en Odoo |  |  |  | compute `_compute_clause_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm.py:16` |
| `clause_ids` | One2many | Cláusulas |  |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_norm.py:14` |
| `code` | Char | Clave |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:12` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:13` |
| `scope` | Text | Alcance certificado | Alcance del certificado de esta norma, como lo imprime el Manual del SGI (1.2). Vacío: la norma no sale en el alcance. |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:18` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_uncovered_clauses` | — |
