<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.objective`

**Objetivo Integral SGI** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Objetivo integral del año con sus indicadores y su salud.

Orden: `target_year, name`.

Archivos: `addons/quimibond_sgi/models/sgi_objective.py`.

## Campos (9)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `action_line_ids` | One2many | Plan de acción (6.2.2) |  |  | `sgi.action.line` |  |  | `addons/quimibond_sgi/models/sgi_objective.py:38` |
| `active` | Boolean |  |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_objective.py:48` |
| `description` | Text | Descripción |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_objective.py:27` |
| `health` | Selection | Salud agregada | Peor color entre los procesos de sus indicadores Y el último semáforo de cada indicador. |  |  | compute `_compute_health`, sin guardar |  | `addons/quimibond_sgi/models/sgi_objective.py:40` |
| `indicator_count` | Integer | # Indicadores |  |  |  | compute `_compute_indicator_count`, sin guardar |  | `addons/quimibond_sgi/models/sgi_objective.py:35` |
| `indicator_ids` | One2many | Indicadores |  |  | `sgi.indicator` |  |  | `addons/quimibond_sgi/models/sgi_objective.py:34` |
| `name` | Char | Objetivo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_objective.py:26` |
| `policy_id` | Many2one | Política integral | Política de la que se despliega este objetivo (cascada ISO). |  | `sgi.policy` |  |  | `addons/quimibond_sgi/models/sgi_objective.py:28` |
| `target_year` | Integer | Año meta | Año en que se debe cumplir el objetivo. |  |  |  |  | `addons/quimibond_sgi/models/sgi_objective.py:33` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_open_indicators` | — |
