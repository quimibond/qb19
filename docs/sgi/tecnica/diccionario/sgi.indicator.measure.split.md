<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.indicator.measure.split`

**Desglose de medición de indicador (equipo o mercado)** (Model).

Un renglón del desglose (equipo de ventas o mercado) dentro de la medición del periodo. La oficial (NC, semáforo del proceso, tablero, RxD) sigue siendo el total.

Orden: `measure_id, market, team_id, id`.

Archivos: `addons/quimibond_sgi/models/sgi_business_line.py`.

## Campos (15)

| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |
|---|---|---|---|---|---|---|---|---|
| `denominator` | Float | Denominador |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:440` |
| `detail_ids` | Text | Registros del detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:453` |
| `detail_model` | Char | Modelo del detalle |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:452` |
| `indicator_id` | Many2one | Indicador |  |  |  | related `measure_id.indicator_id`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:432` |
| `label` | Char | Renglón |  |  |  | compute `_compute_label`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:437` |
| `market` | Selection | Mercado |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:436` |
| `measure_id` | Many2one | Medición |  | sí | `sgi.indicator.measure` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:430` |
| `numerator` | Float | Numerador |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:439` |
| `period_date` | Date | Periodo |  |  |  | related `measure_id.period_date`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:434` |
| `sample_size` | Integer | Casos |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:441` |
| `semaphore` | Selection | Semáforo |  |  |  | compute `_compute_semaphore`, guardado |  | `addons/quimibond_sgi/models/sgi_business_line.py:447` |
| `state` | Selection | Estado |  | sí |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:442` |
| `team_id` | Many2one | Equipo de ventas |  |  | `crm.team` |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:435` |
| `uom` | Char | Unidad |  |  |  | related `indicator_id.uom`, sin guardar |  | `addons/quimibond_sgi/models/sgi_business_line.py:446` |
| `value` | Float | Valor |  |  |  |  |  | `addons/quimibond_sgi/models/sgi_business_line.py:438` |

## Métodos públicos (1)

| Método | Qué hace (docstring) |
|---|---|
| `action_view_evidence` | — |
