<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.staff.efficiency`

**Eficiencias de personal (F-P-A01-32/34)** (Model). Hereda de: `mail.activity.mixin`, `mail.thread`.

Hoja mensual de eficiencias de personal de un área: carga empleados, calcula y la recibe RH. Los importes solo los ve el grupo de salarios.

Orden: `period_date desc, id desc`.

Archivos: `addons/quimibond_sgi/models/sgi_staff_efficiency.py`.

## Campos (13)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `amount_total` | Monetary | Importe total |  |  |  | compute `_compute_amount_total`, sin guardar | _MONEY_GROUPS | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:66` |
| `company_id` | Many2one |  |  |  | `res.company` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:69` |
| `currency_id` | Many2one |  |  |  | `res.currency` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:68` |
| `department_id` | Many2one | Área | Área de la hoja. |  | `hr.department` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:52` |
| `employee_count` | Integer | Número de empleados |  |  |  | compute `_compute_totals`, sin guardar |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:65` |
| `line_ids` | One2many | Empleados |  |  | `sgi.staff.efficiency.line` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:64` |
| `name` | Char |  |  |  |  | compute `_compute_name`, guardado |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:48` |
| `note` | Text | Observaciones |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:70` |
| `period_date` | Date | Mes | Mes que se califica. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:49` |
| `prepared_by_id` | Many2one | Elaboró (jefe de área) | Jefe de área que elabora la hoja. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:54` |
| `received_by_id` | Many2one | Recibió (coordinador de RH) | Coordinador de RH que recibe la hoja. |  | `res.users` |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:56` |
| `received_date` | Datetime | Recibida por RH el | Fecha y hora en que RH recibió la hoja. |  |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:58` |
| `state` | Selection | Estado | Borrador mientras se captura; cerrado por el jefe de área; recibido por RH. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_staff_efficiency.py:60` |

## Métodos públicos (7)

| Método | Qué hace (docstring) |
|---|---|
| `action_close` | El jefe de área cierra su hoja y RH la recibe (C4.25 → S4.35). |
| `action_compute_efficiency` | Eficiencia = tiempo esperado de las órdenes de trabajo / tiempo real registrado por el empleado en el mes. Propone el % (tope 2 %). |
| `action_load_employees` | Agrega los empleados activos del área que aún no están en la hoja. |
| `action_receive` | RH recibe la hoja cerrada: queda quién y cuándo, y se cierra su aviso. |
| `action_reopen` | — |
| `sgi_format_info` | — |
| `sgi_show_money` | El PDF lleva salarios e importes solo para «Salarios de eficiencias» (entrega 4). |
