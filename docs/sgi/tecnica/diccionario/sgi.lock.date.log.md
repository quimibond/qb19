<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.lock.date.log`

**Bitácora de fechas de bloqueo contable** (Model).

Bitácora de cambios a las fechas de bloqueo contable, para medir el cierre a tiempo.

Orden: `moved_at desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_kpi_account.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `business_day` | Integer | Día hábil del mes | Número de día hábil del mes (calendario del SGI) en que se movió (S3-01). |  |  | compute `_compute_business_day`, guardado |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:97` |
| `company_id` | Many2one | Compañía |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:88` |
| `date_after` | Date | Después | Fecha de bloqueo después del cambio. |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:92` |
| `date_before` | Date | Antes | Fecha de bloqueo antes del cambio. |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:91` |
| `lock_field` | Selection | Fecha de bloqueo | Qué fecha de bloqueo contable se movió. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:89` |
| `moved_at` | Datetime | Movida el | Fecha y hora del cambio. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:93` |
| `user_id` | Many2one | Quién | Quién movió la fecha de bloqueo. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:95` |

