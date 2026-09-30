<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.norm.clause`

**Cláusula de norma ISO** (Model).

Cláusula de una norma; «Cumple con» liga actividades y procesos a ella y se ve cuáles no tienen cobertura.

Orden: `norm_id, code`.

Archivos: `addons/quimibond_sgi/models/sgi_norm.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_count` | Integer | Actividades |  |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:50` |
| `activity_ids` | Many2many | Actividades que lo cumplen |  |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:45` |
| `alert_count` | Integer | NC |  |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:51` |
| `code` | Char | Numeral |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:30` |
| `covered` | Boolean | Con actividad |  |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:52` |
| `name` | Char | Requisito |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:31` |
| `norm_id` | Many2one | Norma |  | sí | `sgi.norm` |  |  | `addons/quimibond_sgi/models/sgi_norm.py:29` |
| `process_ids` | Many2many | Procesos que lo cumplen |  |  | `sgi.process` | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:48` |
| `short_label` | Char | Punto |  |  |  | compute `_compute_short_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:44` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_alerts` | — |
