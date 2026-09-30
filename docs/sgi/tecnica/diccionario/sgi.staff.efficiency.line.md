<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.staff.efficiency.line`

**Calificación mensual de un empleado** (Model).

Calificación mensual de un empleado (eficiencia, calidad, orden, asistencia) y su importe.

Orden: `employee_id`.

Archivos: `addons/quimibond_sgi/models/sgi_staff_efficiency.py`.

## Campos (17)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `amount` | Monetary | A pagar |  |  |  | compute `_compute_amounts`, guardado | _MONEY_GROUPS | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:221` |
| `attendance_pct` | Float | Asistencia (máx. 5.5 %) | Porcentaje por asistencia, hasta 5.5 %. |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:210` |
| `currency_id` | Many2one |  |  |  |  | related `sheet_id.currency_id`, sin guardar |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:226` |
| `department_id` | Many2one | Área | Área del empleado. |  |  | related `employee_id.department_id`, guardado |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:204` |
| `efficiency_pct` | Float | Eficiencia (máx. 2 %) | Porcentaje por eficiencia, hasta 2 %. |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:214` |
| `efficiency_ratio` | Float | Esperado / real |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:225` |
| `employee_id` | Many2one | Empleado | Empleado calificado. | sí | `hr.employee` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:200` |
| `expected_minutes` | Float | Tiempo esperado (min) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:224` |
| `housekeeping_pct` | Float | Orden y limpieza (máx. 2 %) | Porcentaje por orden y limpieza, hasta 2 %. |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:212` |
| `job_id` | Many2one | Puesto | Puesto del empleado. |  |  | related `employee_id.job_id`, guardado |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:202` |
| `note` | Char | Observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:227` |
| `quality_pct` | Float | Calidad (máx. 2 %) | Porcentaje por calidad, hasta 2 %. |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:216` |
| `real_minutes` | Float | Tiempo real (min) |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:223` |
| `sheet_id` | Many2one | Hoja mensual | Hoja mensual a la que pertenece la calificación. | sí | `sgi.staff.efficiency` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:198` |
| `total_pct` | Float | Total (%) | Suma de asistencia, orden y limpieza, eficiencia y calidad. Se calcula sola. |  |  | compute `_compute_total_pct`, guardado |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:218` |
| `wage_daily` | Monetary | Salario diario |  |  |  | compute `_compute_wage_daily`, guardado | _MONEY_GROUPS | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:206` |
| `wage_monthly` | Monetary | Salario mensual (×30) |  |  |  | compute `_compute_wage_monthly`, sin guardar | _MONEY_GROUPS | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:208` |

