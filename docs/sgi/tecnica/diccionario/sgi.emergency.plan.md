<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.emergency.plan`

**Plan de emergencia (ISO 14001/45001 8.2)** (Model). Hereda de: `sgi.base.mixin`.

Plan de emergencia (14001/45001 8.2) con su frecuencia de simulacros; el cron avisa cuando toca el siguiente.

Orden: `folio desc`.

Archivos: `addons/quimibond_sgi/models/sgi_emergency.py`.

## Campos (12)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `document_id` | Many2one | Plan documentado | Documento controlado con el plan de emergencia. |  | `documents.document` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:43` |
| `drill_count` | Integer | # Simulacros |  |  |  | compute `_compute_drill_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_emergency.py:63` |
| `drill_frequency_months` | Integer | Frecuencia de simulacro (meses) | Cada cuántos meses se hace un simulacro de este plan. |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:49` |
| `drill_ids` | One2many | Simulacros |  |  | `sgi.emergency.drill` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:58` |
| `last_drill_date` | Date | Último simulacro | Fecha del último simulacro realizado. Se calcula sola. |  |  | compute `_compute_drill_dates`, guardado |  | `addons/quimibond_sgi/models/sgi_emergency.py:64` |
| `location` | Char | Ubicación / zona |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:39` |
| `name` | Char | Escenario de emergencia |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:28` |
| `next_drill_date` | Date | Próximo simulacro | Fecha en que toca el siguiente simulacro, según la frecuencia. Se calcula sola. |  |  | compute `_compute_drill_dates`, guardado |  | `addons/quimibond_sgi/models/sgi_emergency.py:67` |
| `plan_type` | Selection | Tipo | Tipo de emergencia que atiende el plan. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:29` |
| `responsible_id` | Many2one | Responsable (brigada) | Responsable del plan (brigada). Recibe los avisos de simulacros. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:40` |
| `risk_ids` | Many2many | Riesgos ligados (IPER/ambiental) | Riesgos de seguridad o aspectos ambientales que atiende el plan. |  | `sgi.risk` |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:46` |
| `state` | Selection | Estado | Borrador, vigente u obsoleto. Solo los vigentes llevan simulacros. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_emergency.py:52` |

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `action_set_borrador` | — |
| `action_set_obsoleto` | — |
| `action_set_vigente` | — |
| `action_view_drills` | — |
