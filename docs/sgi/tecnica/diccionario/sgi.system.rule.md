<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.system.rule`

**Regla del sistema del SGI** (Model).

57.162.0: regla que el sistema hace cumplir con candados y avisos (no es una actividad). La liga a sus cláusulas y a la pantalla donde se ve; el Manual del SGI (5.2) imprime estas reglas.

Orden: `sequence, id`.

Archivos: `addons/quimibond_sgi/models/sgi_norm.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:84` |
| `clause_ids` | Many2many | Cláusulas |  |  | `sgi.norm.clause` |  |  | `addons/quimibond_sgi/models/sgi_norm.py:78` |
| `clause_label` | Char | Numeral | Los numerales de sus cláusulas, sin repetir (p. ej. 6.1, 8.2). |  |  | compute `_compute_clause_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm.py:80` |
| `menu_id` | Many2one | Pantalla en Odoo |  |  | `ir.ui.menu` |  |  | `addons/quimibond_sgi/models/sgi_norm.py:83` |
| `name` | Char | Tema |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:77` |
| `sequence` | Integer | Orden |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:76` |
| `text` | Text | Regla |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:82` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_menu` | — |
