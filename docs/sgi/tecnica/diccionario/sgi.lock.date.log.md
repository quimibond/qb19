<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.lock.date.log`

**Bitácora de fechas de bloqueo contable** (Model).

Orden: `moved_at desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_kpi_account.py`.

## Campos (7)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `business_day` | Integer | Día hábil del mes | Número de día hábil del mes (calendario del SGI) en que se movió (S3-01). |  |  | compute `_compute_business_day`, guardado |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:91` |
| `company_id` | Many2one | Compañía |  | sí | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:85` |
| `date_after` | Date | Después |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:88` |
| `date_before` | Date | Antes |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:87` |
| `lock_field` | Selection | Fecha de bloqueo |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:86` |
| `moved_at` | Datetime | Movida el |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:89` |
| `user_id` | Many2one | Quién |  |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_kpi_account.py:90` |

