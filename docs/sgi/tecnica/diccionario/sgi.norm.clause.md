<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.norm.clause`

**Cláusula de norma ISO** (Model).

Cláusula de una norma; «Cumple con» liga actividades y procesos a ella y se ve cuáles no tienen cobertura.

Orden: `norm_id, code`.

Archivos: `addons/quimibond_sgi/models/sgi_norm.py`, `addons/quimibond_sgi/models/sgi_norm_compliance.py`.

## Campos (14)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_count` | Integer | Actividades |  |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:52` |
| `activity_ids` | Many2many | Actividades que lo cumplen | Actividades que cumplen con esta cláusula («Cumple con»). |  | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:45` |
| `alert_count` | Integer | NC |  |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:53` |
| `code` | Char | Numeral |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:43` |
| `coverage` | Selection | Cobertura | Cómo se cumple el punto: una actividad del SGI, una regla que el sistema hace cumplir (sin actividad) o no aplica a PNTQ. |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:61` |
| `covered` | Boolean | Cubierto | El punto lo cumple al menos una actividad o una regla del sistema, o no aplica (con su justificación). |  |  | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:57` |
| `exclusion` | Boolean | No aplica | El requisito (o un inciso suyo) no aplica a PNTQ. Sale en el Manual del SGI (1.3) con su justificación. |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:47` |
| `exclusion_detail` | Char | Inciso que no aplica | Inciso o numeral de detalle, p. ej. «8.4.1 b)». Vacío: toda la cláusula. |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:50` |
| `exclusion_reason` | Text | Justificación de la no aplicabilidad |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:52` |
| `name` | Char | Requisito |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_norm.py:44` |
| `norm_id` | Many2one | Norma | Norma a la que pertenece la cláusula. | sí | `sgi.norm` |  |  | `addons/quimibond_sgi/models/sgi_norm.py:41` |
| `process_ids` | Many2many | Procesos que lo cumplen | Procesos cuyas actividades cumplen con la cláusula. |  | `sgi.process` | compute `_compute_sgi_evidence`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:49` |
| `short_label` | Char | Punto |  |  |  | compute `_compute_short_label`, sin guardar |  | `addons/quimibond_sgi/models/sgi_norm_compliance.py:44` |
| `system_rule_ids` | Many2many | Reglas del sistema | Lo que el sistema hace cumplir sin una actividad (candados y avisos). |  | `sgi.system.rule` |  |  | `addons/quimibond_sgi/models/sgi_norm.py:53` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_alerts` | — |
