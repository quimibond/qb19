# Plan de Expansión Comercial — Índice

Proyecto: **"Plan de expansión comercial"** de Productora de No Tejidos Quimibond (PNTQ).

Cuatro documentos anuales, uno por ejercicio, que aplican los marcos estratégicos
(KPIs, SMART, OKR, 5 Fuerzas de Porter, PESTLE, Balanced Scorecard, Cadena de Valor,
Business Model Canvas, Planificación de Escenarios, Modelo de 3 Horizontes,
Reingeniería de Procesos, Liderazgo en Costos) y el bloque fiscal (elección de régimen,
deducciones autorizadas, declaración anual, cumplimiento normativo, defensa del
contribuyente / PRODECON) con los **datos reales de cada año** extraídos de la base
sincronizada (Supabase, espejo de Odoo — solo lectura):

- [`PLAN_2022.md`](PLAN_2022.md) — Año récord post-pandemia; concentración extrema en un cliente ancla.
- [`PLAN_2023.md`](PLAN_2023.md) — Contracción por superpeso; mejor margen y cobranza; diversificación forzada.
- [`PLAN_2024.md`](PLAN_2024.md) — Rebote por nearshoring; cambio de cliente #1; margen empieza a caer.
- [`PLAN_2025.md`](PLAN_2025.md) — Récord de ventas con margen mínimo; aranceles y revisión T-MEC; base de este plan.

## ¿Addon SGI o proyecto separado?

**Recomendación: no meterlo como código nuevo al addon SGI.** Separar en tres capas:

1. **El plan (análisis y documentos)** → es contenido, no código. Vive aquí en `docs/plan_expansion/`
   y/o como páginas compartibles. No requiere modelos de Odoo.
2. **El seguimiento operativo (KPIs, objetivos, revisión periódica)** → el addon `quimibond_sgi`
   **ya tiene** los modelos para esto: `sgi.objective`, `sgi.indicator`, `sgi.context`,
   `sgi.risk` y `sgi.sales_budget`. Los objetivos SMART/OKR del plan se cargan como
   registros en esos modelos existentes — **sin crear modelos nuevos** (recordar la regla
   del CI: modelo nuevo sin bump de versión = error, y el riesgo de ramas superconjunto).
   Además encaja con ISO 9001 cláusulas 4 (contexto) y 6.2 (objetivos), que el SGI ya cubre.
3. **El análisis vivo con datos (dashboards por año, concentración, márgenes)** →
   `quimibond-intelligence` (frontend en Vercel sobre Supabase), que ya tiene los 21 modelos
   sincronizados. Ahí es donde un tablero "Plan de expansión" se actualiza solo.

Un addon nuevo solo se justificaría si se quisiera capturar en Odoo estructuras que el SGI
no tiene (p. ej. un canvas editable o escenarios) — y hoy no hace falta para arrancar.

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
grandes en los mismos clientes exportadores), no de *expansión*. Un plan de expansión
comercial tiene tres palancas claras: (1) recuperar la generación de clientes nuevos
(61/año vs 126 en 2023), (2) defender margen vía mezcla y costos (compras ya son 84.6%
de ventas), y (3) reducir el riesgo de concentración (top 2 = 37%, y 71% de la
facturación expuesta a política comercial de EUA).

### Notas sobre los datos

- Fuente: tablas `odoo_invoices`, `odoo_invoice_lines`, `odoo_sale_orders`, `companies`
  (compañía Odoo ID 1 = PNTQ), facturas en estado `posted`. Montos en MXN sin IVA.
- El margen % viene del campo `margin` de las órdenes de venta de Odoo y depende de la
  calidad de los costos capturados; úsese como tendencia, no como cifra contable.
- Las compras (`in_invoice`) incluyen todo gasto facturado, no solo materia prima.
