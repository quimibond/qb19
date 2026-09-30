<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.measure.split`

**Desglose de medición de indicador (equipo o mercado)** (Model).

Un renglón del desglose (equipo de ventas o mercado) dentro de la medición del periodo. La oficial (NC, semáforo del proceso, tablero, RxD) sigue siendo el total.

Orden: `measure_id, market, team_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_business_line.py`.

## Campos (15)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `denominator` | Float | Denominador |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:435` |
| `detail_ids` | Text | Registros del detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:448` |
| `detail_model` | Char | Modelo del detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:447` |
| `indicator_id` | Many2one | Indicador |  |  |  | related `measure_id.indicator_id`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:427` |
| `label` | Char | Renglón |  |  |  | compute `_compute_label`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:432` |
| `market` | Selection | Mercado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:431` |
| `measure_id` | Many2one | Medición |  | sí | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:425` |
| `numerator` | Float | Numerador |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:434` |
| `period_date` | Date | Periodo |  |  |  | related `measure_id.period_date`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:429` |
| `sample_size` | Integer | Casos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:436` |
| `semaphore` | Selection | Semáforo |  |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:442` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:437` |
| `team_id` | Many2one | Equipo de ventas |  |  | `crm.team` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:430` |
| `uom` | Char | Unidad |  |  |  | related `indicator_id.uom`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:441` |
| `value` | Float | Valor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:433` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_evidence` | — |
