<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.norm.clause`

**Cláusula de norma ISO** (Model).

Cláusula de una norma; «Cumple con» liga actividades y procesos a ella y se ve cuáles no tienen cobertura.

Orden: `norm_id, code`.

Archivos: `addons/quimibond_sgi/models/sgi_norm.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_count` | Integer | Actividades |  |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:52` |
| `activity_ids` | Many2many | Actividades que lo cumplen | Actividades que cumplen con esta cláusula («Cumple con»). |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:45` |
| `alert_count` | Integer | NC |  |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:53` |
| `code` | Char | Numeral |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:31` |
| `covered` | Boolean | Con actividad | Indica si al menos una actividad cumple con la cláusula. |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:54` |
| `name` | Char | Requisito |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:32` |
| `norm_id` | Many2one | Norma | Norma a la que pertenece la cláusula. | sí | `sgi.norm` |  |  | `addons/quimibond_sgi/models/sgi_norm.py:29` |
| `process_ids` | Many2many | Procesos que lo cumplen | Procesos cuyas actividades cumplen con la cláusula. |  | `sgi.process` | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:49` |
| `short_label` | Char | Punto |  |  |  | compute `_compute_short_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:44` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_alerts` | — |
