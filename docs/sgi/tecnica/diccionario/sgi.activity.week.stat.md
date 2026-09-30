<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.activity.week.stat`

**Cumplimiento semanal de una actividad SGI** (Model).

Aplicables, hechas, completas, a tiempo y vencidas abiertas, por actividad y semana. Va aparte de ``sgi.activity.exec.stat`` (que tiene un renglón por usuario): repetir estos totales en cada renglón de usuario los multiplicaría al sumar en…

Orden: `period_start desc, activity_id`.

Archivos: `addons/quimibond_sgi/models/sgi_activity_spec.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `activity_id` | Many2one | Actividad |  | sí | `sgi.process.activity` |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:969` |
| `applicable_count` | Integer | Aplicables |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:977` |
| `company_id` | Many2one | Empresa |  |  |  | related `activity_id.company_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:993` |
| `complete_count` | Integer | Completas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:979` |
| `completeness_rate` | Float | % completas |  |  |  | compute `_compute_rates`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:987` |
| `done_count` | Integer | Hechas |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:978` |
| `exec_channel` | Selection | Canal |  |  |  | related `activity_id.exec_channel`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:974` |
| `late_open_count` | Integer | Vencidas abiertas | Entradas aplicables sin salida y con el plazo vencido al cierre de la semana. |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:984` |
| `on_time_count` | Integer | A tiempo |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:983` |
| `on_time_rate` | Float | % a tiempo |  |  |  | compute `_compute_rates`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:990` |
| `period_start` | Date | Semana |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:976` |
| `process_id` | Many2one | Proceso |  |  |  | related `activity_id.process_id`, guardado |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:972` |
| `timed_count` | Integer | Con plazo medible | Hechas cuya entrada se pudo ligar (o con vencimiento periódico). |  |  |  |  | `addons/quimibond_sgi/models/sgi_activity_spec.py:980` |

