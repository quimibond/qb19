<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.week.stat`

**Cumplimiento semanal de una actividad SGI** (Model).

Aplicables, hechas, completas, a tiempo y vencidas abiertas, por actividad y semana. Va aparte de ``sgi.activity.exec.stat`` (que tiene un renglón por usuario): repetir estos totales en cada renglón de usuario los multiplicaría al sumar en…

Orden: `period_start desc, activity_id`.

Archivos: `addons/quimibond_sgi/models/sgi_activity_spec.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad | Actividad medida. | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1127` |
| `applicable_count` | Integer | Aplicables |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1139` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1157` |
| `complete_count` | Integer | Completas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1141` |
| `completeness_rate` | Float | % completas | Porcentaje de registros de la semana que cumplen el criterio de completo. Se calcula solo. |  |  | compute `_compute_rates`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1149` |
| `done_count` | Integer | Hechas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1140` |
| `exec_channel` | Selection | Canal | Dónde se hace el trabajo. |  |  | related `activity_id.exec_channel`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1134` |
| `late_open_count` | Integer | Vencidas abiertas | Entradas aplicables sin salida y con el plazo vencido al cierre de la semana. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1146` |
| `on_time_count` | Integer | A tiempo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1145` |
| `on_time_rate` | Float | % a tiempo | Porcentaje de registros de la semana hechos a tiempo. Se calcula solo. |  |  | compute `_compute_rates`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1153` |
| `period_start` | Date | Semana | Lunes de la semana medida. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1137` |
| `process_id` | Many2one | Proceso | Proceso de la actividad. |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1131` |
| `timed_count` | Integer | Con plazo medible | Hechas cuya entrada se pudo ligar (o con vencimiento periódico). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:1142` |

