<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.staff.efficiency.line`

**Calificación mensual de un empleado** (Model).

Calificación mensual de un empleado (eficiencia, calidad, orden, asistencia) y su importe.

Orden: `employee_id`.

Archivos: `addons/quimibond_sgi/models/sgi_staff_efficiency.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `amount` | Monetary | A pagar |  |  |  | compute `_compute_amounts`, guardado | _MONEY_GROUPS | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:204` |
| `attendance_pct` | Float | Asistencia (máx. 5.5 %) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:199` |
| `currency_id` | Many2one |  |  |  |  | related `sheet_id.currency_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:209` |
| `department_id` | Many2one | Área |  |  |  | related `employee_id.department_id`, guardado |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:194` |
| `efficiency_pct` | Float | Eficiencia (máx. 2 %) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:201` |
| `efficiency_ratio` | Float | Esperado / real |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:208` |
| `employee_id` | Many2one | Empleado |  | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:192` |
| `expected_minutes` | Float | Tiempo esperado (min) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:207` |
| `housekeeping_pct` | Float | Orden y limpieza (máx. 2 %) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:200` |
| `job_id` | Many2one | Puesto |  |  |  | related `employee_id.job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:193` |
| `note` | Char | Observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:210` |
| `quality_pct` | Float | Calidad (máx. 2 %) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:202` |
| `real_minutes` | Float | Tiempo real (min) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:206` |
| `sheet_id` | Many2one | Hoja mensual |  | sí | `sgi.staff.efficiency` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:191` |
| `total_pct` | Float | Total (%) |  |  |  | compute `_compute_total_pct`, guardado |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:203` |
| `wage_daily` | Monetary | Salario diario |  |  |  | compute `_compute_wage_daily`, guardado | _MONEY_GROUPS | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:195` |
| `wage_monthly` | Monetary | Salario mensual (×30) |  |  |  | compute `_compute_wage_monthly`, sin guardar | _MONEY_GROUPS | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:197` |

