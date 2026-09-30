<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.norm`

**Norma ISO** (Model).

Norma de referencia (ISO 9001, 14001, 45001 y las que se capturen) con sus cláusulas.

Orden: `code`.

Archivos: `addons/quimibond_sgi/models/sgi_norm.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`.

## Campos (4)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:14` |
| `clause_ids` | One2many | Cláusulas |  |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_norm.py:13` |
| `code` | Char | Clave |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:11` |
| `name` | Char | Nombre |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:12` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_uncovered_clauses` | — |
