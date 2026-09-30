<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.emergency.plan`

**Plan de emergencia (ISO 14001/45001 8.2)** (Model). Hereda de: `sgi.base.mixin`.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_emergency.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `document_id` | Many2one | Plan documentado |  |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:35` |
| `drill_count` | Integer | # Simulacros |  |  |  | compute `_compute_drill_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_emergency.py:51` |
| `drill_frequency_months` | Integer | Frecuencia de simulacro (meses) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:39` |
| `drill_ids` | One2many | Simulacros |  |  | `sgi.emergency.drill` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:46` |
| `last_drill_date` | Date | Último simulacro |  |  |  | compute `_compute_drill_dates`, guardado |  | `addons/quimibond_sgi/models/sgi_emergency.py:52` |
| `location` | Char | Ubicación / zona |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:32` |
| `name` | Char | Escenario de emergencia |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:22` |
| `next_drill_date` | Date | Próximo simulacro |  |  |  | compute `_compute_drill_dates`, guardado |  | `addons/quimibond_sgi/models/sgi_emergency.py:54` |
| `plan_type` | Selection | Tipo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:23` |
| `responsible_id` | Many2one | Responsable (brigada) |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:33` |
| `risk_ids` | Many2many | Riesgos ligados (IPER/ambiental) |  |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:37` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:41` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_borrador` | — |
| `action_set_obsoleto` | — |
| `action_set_vigente` | — |
| `action_view_drills` | — |
