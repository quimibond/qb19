<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.measure.split`

**Desglose de medición de indicador (equipo o mercado)** (Model).

Un renglón del desglose (equipo de ventas o mercado) dentro de la medición del periodo. La oficial (NC, semáforo del proceso, tablero, RxD) sigue siendo el total.

Orden: `measure_id, market, team_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_business_line.py`.

## Campos (15)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `denominator` | Float | Denominador | Denominador del desglose. |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:444` |
| `detail_ids` | Text | Registros del detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:459` |
| `detail_model` | Char | Modelo del detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:458` |
| `indicator_id` | Many2one | Indicador | Indicador medido. |  |  | related `measure_id.indicator_id`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:432` |
| `label` | Char | Renglón |  |  |  | compute `_compute_label`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:441` |
| `market` | Selection | Mercado | Mercado del desglose: nacional, exportación o cliente sin país. |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:439` |
| `measure_id` | Many2one | Medición |  | sí | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:430` |
| `numerator` | Float | Numerador | Numerador del desglose. |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:443` |
| `period_date` | Date | Periodo | Periodo de la medición. |  |  | related `measure_id.period_date`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:435` |
| `sample_size` | Integer | Casos | Número de casos del desglose. |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:445` |
| `semaphore` | Selection | Semáforo | Semáforo del desglose. Se calcula solo. |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:452` |
| `state` | Selection | Estado | Calculado o sin dato. | sí |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:446` |
| `team_id` | Many2one | Equipo de ventas | Equipo de ventas del desglose. |  | `crm.team` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:437` |
| `uom` | Char | Unidad |  |  |  | related `indicator_id.uom`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:451` |
| `value` | Float | Valor | Valor del desglose. |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:442` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_evidence` | — |
