# Plan de Expansión Comercial — Índice

Proyecto: **"Plan de expansión comercial"** de Productora de No Tejidos Quimibond (PNTQ).

La estructura separa lo que ya pasó de lo que sigue:

- **Análisis retrospectivos** (uno por ejercicio cerrado) — qué pasó, por qué, y qué
  lección deja. Sin objetivos: los años pasados no se planean, se entienden.
  - [`ANALISIS_2022.md`](ANALISIS_2022.md) — Año récord post-pandemia; concentración extrema en un cliente ancla.
  - [`ANALISIS_2023.md`](ANALISIS_2023.md) — Contracción por superpeso; mejor margen y cobranza de la serie.
  - [`ANALISIS_2024.md`](ANALISIS_2024.md) — Rebote por nearshoring; cambio de cliente #1; el margen empieza a caer.
  - [`ANALISIS_2025.md`](ANALISIS_2025.md) — Récord frágil: margen mínimo, aranceles, máxima concentración.
- **[`ESTRATEGIA_2026_2028.md`](ESTRATEGIA_2026_2028.md)** — **El plan.** Diagnóstico
  consolidado, tesis estratégica, objetivos SMART, OKRs 2026, Balanced Scorecard con
  metas, Canvas objetivo, escenarios con disparadores, 3 horizontes, reingeniería,
  liderazgo en costos, estrategia fiscal y gobernanza.

Todos los números provienen de la base sincronizada (Supabase, espejo de Odoo, solo
lectura) y fueron validados contra Odoo directamente (mismos totales al peso).

## ¿Addon SGI o proyecto separado?

**Recomendación: no meterlo como código nuevo al addon SGI.** Separar en tres capas:

1. **El plan y los análisis (contenido)** → viven aquí en `docs/plan_expansion/`
   y/o como páginas compartibles. No requieren modelos de Odoo.
2. **El seguimiento operativo (KPIs, objetivos, revisión periódica)** → hecho en el
   addon `quimibond_sgi` **sin modelos nuevos**: los KPIs del plan están sembrados
   como indicadores automáticos EX-01…EX-07 (nuevos `calc_mode` en `sgi.indicator`,
   medidos por el cron mensual con semáforo y evidencia) colgados del objetivo
   "Expansión comercial rentable y diversificada (2028)" en `sgi.objective`.
   Encaja con ISO 9001 cláusulas 4 y 6.2.
3. **El análisis vivo con datos (margen, concentración, clientes nuevos)** →
   `quimibond-intelligence` (frontend en Vercel sobre Supabase), donde un tablero
   "Plan de expansión" se actualiza solo.

Un addon nuevo solo se justificaría para capturar en Odoo estructuras que el SGI no
tiene (p. ej. un canvas editable o escenarios) — hoy no hace falta para arrancar.

## Comparativo 2022–2025 (datos reales)

| Métrica | 2022 | 2023 | 2024 | 2025 |
|---|---|---|---|---|
| Ventas facturadas (MDP, sin IVA) | 162.0 | 153.6 | 176.3 | 182.0 |
| Crecimiento vs año previo | — | −5.2% | +14.8% | +3.3% |
| Notas de crédito (MDP) | 3.1 | 3.5 | 1.6 | 1.2 |
| Compras (MDP) | 127.5 | 114.2 | 137.6 | 154.0 |
| Compras / Ventas | 78.7% | 74.4% | 78.1% | 84.6% |
| Margen s/órdenes de venta (Odoo) | 9.6% | 10.6% | 7.5% | 5.6% |
| Facturas emitidas | 3,646 | 3,180 | 2,843 | 2,561 |
| Clientes activos | 490 | 427 | 337 | 327 |
| Clientes nuevos | ~240* | 126 | 79 | 61 |
| Ticket promedio por orden (miles MXN) | 51.3 | 53.8 | 68.8 | 79.2 |
| Días promedio de cobro (DSO) | 60 | 46 | 50 | 49 |
| Facturación en USD | 66% | 66% | 66% | 71% |
| Concentración top 2 clientes | 45.6% | 36.0% | 34.0% | 36.9% |
| Concentración top 8 clientes | ~68% | ~67% | ~68% | ~70% |

\* El histórico en Odoo arranca en 2021, por lo que "clientes nuevos 2022" está sobreestimado.

**La historia que cuentan los cuatro años:** PNTQ vende más con menos clientes, menos
facturas y menos margen. El crecimiento 2024–2025 es de *profundización* (tickets más
grandes en los mismos clientes exportadores), no de *expansión*. Las tres palancas del
plan: (1) recuperar la generación de clientes nuevos, (2) defender margen vía mezcla y
costos, y (3) reducir el riesgo de concentración. El detalle vive en la
[Estrategia 2026–2028](ESTRATEGIA_2026_2028.md).

### Notas sobre los datos

- Fuente: tablas `odoo_invoices`, `odoo_invoice_lines`, `odoo_sale_orders`, `companies`
  (compañía Odoo ID 1 = PNTQ), facturas en estado `posted`. Montos en MXN sin IVA.
  Totales anuales validados contra Odoo vía MCP (`account.move`, `amount_untaxed_signed`).
- El margen % viene del campo `margin` de las órdenes de venta de Odoo y depende de la
  calidad de los costos capturados; úsese como tendencia, no como cifra contable.
- Las compras (`in_invoice`) incluyen todo gasto facturado, no solo materia prima.
