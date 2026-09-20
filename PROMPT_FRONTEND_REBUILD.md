# PROMPT: Quimibond Intelligence — Frontend + Agent System Rebuild

## Quien eres
Eres el CTO de Quimibond Intelligence, un sistema de inteligencia empresarial para una empresa textil mexicana (Quimibond). Tu trabajo es reconstruir el frontend y el sistema de agentes AI desde casi cero para que sea un **sistema predictivo que anticipa problemas antes de que ocurran** y presenta evidencia accionable al CEO y managers.

El CEO usa esto PRINCIPALMENTE desde su celular a las 7am. Cada insight debe tener evidencia clickeable (facturas, emails, pedidos) y la persona exacta responsable de actuar.

## RPCs listos para usar (YA en produccion, probados con datos reales)

### 1. Dashboard — UNA sola llamada
```typescript
const { data } = await supabase.rpc('get_dashboard_kpis')
// Retorna:
// revenue: { this_month, last_month, ytd }  (todo MXN)
// collections: { total_overdue_mxn, overdue_count, expected_collections_30d, clients_at_risk }
// cash: { cash_mxn, cash_usd, total_mxn, runway_days }
// insights: { new_count, urgent_count, acted_this_month, acceptance_rate }
// predictions: { reorders_overdue, reorders_lost, reorders_at_risk_mxn, payments_at_risk, payments_improving }
// operations: { otd_rate, pending_deliveries, late_deliveries, manufacturing_active, overdue_activities }
```

### 2. Company 360 — UNA sola llamada
```typescript
const { data } = await supabase.rpc('company_evidence_pack', { p_company_id: 6033 })
// Retorna: financials, orders, communication, deliveries, activities, history, predictions
// predictions incluye:
//   payment: { predicted_payment_date, payment_risk, payment_trend, avg_days_to_pay, median_days_to_pay, avg_recent_6m }
//   reorder: { predicted_next_order, days_overdue_reorder, reorder_status, avg_cycle_days, salesperson_name/email }
//   ltv_health: { churn_risk_score, overdue_risk_score, customer_status, trend_pct }
//   cashflow: { expected_collection, collection_probability, total_receivable }
```

### 3. Agent briefings — para orchestrate
```typescript
const { data } = await supabase.rpc('get_director_briefing', { p_director: 'comercial', p_max_companies: 5 })
// Retorna: evidence_packs (array de company_evidence_pack), agent_feedback, instructions
```

### 4. Campos `_mxn` BACKFILLED — ya NO necesitas toMxn()
Todas las 28,371 facturas, 31K order_lines, 42K invoice_lines, 12K sale_orders,
5.6K purchase_orders YA tienen `_mxn` poblado (MXN=1:1, USD=rate Banxico anual, EUR=rate anual).
Elimina las llamadas a `toMxn()` de invoices.ts y usa `amount_total_mxn` directo.

### 5. Views contables (queries directas, no RPC)
```typescript
// P&L mensual
const { data: pl } = await supabase.from('pl_estado_resultados').select('*').gte('period', '2025-01').order('period', { ascending: false })
// → period, ingresos, costo_ventas, gastos_operativos, utilidad_bruta, utilidad_operativa, otros_neto

// CFO snapshot (1 row)
const { data: cfo } = await supabase.from('cfo_dashboard').select('*').single()
// → efectivo_disponible, deuda_tarjetas, posicion_neta, cuentas_por_cobrar, cuentas_por_pagar, cartera_vencida, ventas_30d, cobros_30d, clientes_morosos

// Working capital (1 row)
const { data: wc } = await supabase.from('working_capital').select('*').single()
// → capital_de_trabajo, ratio_liquidez, ratio_prueba_acida

// Runway (1 row)
const { data: runway } = await supabase.from('financial_runway').select('*').single()
// → cash_mxn, burn_rate_daily, runway_days_net, runway_days_cash_only

// Anomalias contables
const { data: anomalies } = await supabase.from('accounting_anomalies').select('*').order('amount', { ascending: false })
// → anomaly_type, severity, description, company_id, company_name, amount

// Balance por banco
const { data: banks } = await supabase.from('odoo_bank_balances').select('*').gt('current_balance', 0).order('current_balance', { ascending: false })
```

### 6. IMPORTANTE: Multi-company
Odoo tiene 8 empresas pero solo nos importa company_id=1 (PRODUCTORA DE NO TEJIDOS QUIMIBOND).
El sync ya filtra por company_id=1. Los datos en Supabase son SOLO de Quimibond.
Despues del proximo deploy, chart_of_accounts bajara de 1,557 a ~200, y los balances seran solo de Quimibond.

### 7. Datos reales (auditados y corregidos 14-abr-2026, todo MXN):
```
Revenue:      $3.4M este mes | $18.4M marzo | $67M YTD
P&L marzo:    Ingresos $18.4M | Costo $12.3M | Utilidad bruta $6.1M (33.1%) | Operativa $3.3M
Cartera:      CxC $66.2M | Vencida $51.2M | Cobro esperado 30d $22.9M
Cash:         MXN $1.5M | USD $865K | Total MXN $16.6M | Deuda TJ $3.0M | Neto $13.6M
Runway:       72 dias (con cobros) | 38 dias (solo cash) | Burn rate $359K/dia
Working Cap:  $51.6M | Liquidez 2.66 | Prueba acida 0.53
CxP:          $28.2M pendiente | Pagos prov 30d $18.2M
Anomalias:    CONTITECH excede credito 41% ($7M vs $5M) | 3 facturas posible duplicado $614K
Insights:     21 nuevos | 42 urgentes | 56% acceptance rate
Predicciones: 191 clientes "lost" | 26 reorden vencido | $88M en riesgo
Operaciones:  148 entregas tarde | 76 manufactura activa | 5,539 actividades vencidas
```

---

## Stack obligatorio
- **Next.js 15** (App Router, RSC)
- **React 19** (Server Components default, Client solo para interactividad)
- **TypeScript 5.8** estricto
- **Tailwind CSS 4** semantic tokens (NO raw colors)
- **shadcn/ui** — CADA componente visual construido sobre shadcn
- **Supabase** JS client (ya configurado en `src/lib/supabase-server.ts`)
- **Recharts** para graficas responsive
- **Claude Sonnet** via `src/lib/claude.ts` (ya existe con retry logic)

---

## PARTE 1: AGENTES INTELIGENTES PREDICTIVOS

### Filosofia: El agente debe saber que va a pasar ANTES de que pase

No queremos agentes que digan "la factura X esta vencida" (eso ya lo se). Queremos agentes que digan:

> "CONTITECH va a pagar la factura INV/2026/02/0144 dentro de 8 dias (su patron historico es pagar a los 21 dias mediana). Pero INV/2026/01/0022 lleva 62 dias — **excede su patron normal** (avg 79d). Gilberto Lopez (gilberto@quimibond.com) no ha tenido comunicacion con el contacto Carina Yazmin (carina.yazmin.donjuan.davila@continental.com) en 97 dias. **Accion: Gilberto debe llamar a Carina hoy** antes de que la factura entre a zona critica el viernes."

### RPC disponible: `get_director_briefing(director_slug, max_companies)`

Esta funcion YA existe en Supabase y retorna para cada director:
- **Top 5 empresas** que necesitan atencion (seleccionadas por riesgo + impacto)
- Para cada empresa, un **evidence_pack** completo con:

```typescript
{
  company_id: number,
  company_name: string,
  tier: "strategic" | "important" | "standard",
  rfc: string,
  credit_limit: number,
  
  financials: {
    total_invoiced_12m: number,      // MXN siempre
    total_overdue_mxn: number,
    overdue_invoices: [{              // CADA factura individual
      name: "INV/2026/02/0144",
      amount_mxn: 31900,
      days_overdue: 50,
      due_date: "2026-02-19"
    }],
    avg_days_to_pay: number,         // patron historico
    credit_notes_12m: number,
    payables_overdue_mxn: number     // lo que NOSOTROS les debemos
  },
  
  orders: {
    total_orders_12m: number,
    last_order_date: string,
    days_since_last_order: number,
    avg_order_mxn: number,
    revenue_trend: {
      last_3m: number,               // para detectar caida
      prev_3m: number
    },
    salesperson: "Gilberto Lopez",   // PERSONA responsable
    salesperson_email: "gilberto@quimibond.com",
    top_products: [{                  // que les vendemos
      product: "PES CREP 90 g/m2",
      ref: "WP4032BL152",
      total_mxn: 4488858
    }]
  },
  
  communication: {
    total_emails: number,
    last_email_date: string,
    days_since_last_email: number,   // ALERTA si > 30
    unanswered_threads: number,      // threads sin respuesta nuestra
    recent_threads: [{
      subject: string,
      last_sender: string,
      hours_waiting: number,
      has_our_reply: boolean
    }],
    key_contacts: [{                  // contactos del CLIENTE
      name: "Carina Yazmin",
      email: "carina.yazmin@continental.com"
    }]
  },
  
  deliveries: {
    total_deliveries_90d: number,
    late_deliveries: number,
    otd_rate: number,
    pending_shipments: number,
    late_details: [{ name, scheduled, origin }]
  },
  
  activities: {
    total_pending: number,
    overdue: number,
    overdue_detail: [{
      type: "To Do",
      summary: string,
      assigned_to: "Guadalupe Guerrero",  // PERSONA
      deadline: "2023-07-20"
    }]
  },
  
  history: {
    recent_insights: [{               // para NO repetir
      title: string,
      state: "acted_on" | "expired",
      category: string,
      created: string
    }],
    health_trend: [{ date, score }]   // 6 puntos recientes
  }

  predictions: {
    payment: {                         // de payment_predictions matview
      avg_days_to_pay: number,
      median_days_to_pay: number,
      avg_recent_6m: number,           // tendencia reciente
      avg_older: number,               // tendencia historica
      payment_trend: "mejorando" | "empeorando",
      predicted_payment_date: string,
      payment_risk: "NORMAL: dentro de patron" | "ALTO: fuera de patron" | "CRITICO: excede maximo",
      pending_count: number,
      total_pending: number,
      fastest_payment: number,         // dias
      slowest_payment: number
    } | null,
    reorder: {                         // de client_reorder_predictions matview
      avg_cycle_days: number,
      stddev_days: number,
      last_order_date: string,
      days_since_last: number,
      predicted_next_order: string,    // fecha predicha
      days_overdue_reorder: number,    // dias tarde vs prediccion
      reorder_status: "on_track" | "overdue" | "lost",
      avg_order_value: number,
      total_revenue: number,
      salesperson_name: string,
      salesperson_email: string,
      top_product_ref: string
    } | null,
    ltv_health: {                      // de customer_ltv_health matview
      churn_risk_score: number,        // 0-100
      overdue_risk_score: number,      // 0-100
      trend_pct: number,              // % cambio vs trimestres anteriores
      customer_status: "active" | "cooling" | "at_risk" | "churned"
    } | null,
    cashflow: {                        // de cashflow_projection matview
      expected_collection: number,     // MXN esperado a cobrar
      collection_probability: number,  // 0-1
      total_receivable: number         // total pendiente
    } | null
  }
}
```

### RPC: `company_evidence_pack(company_id)` 
Retorna el mismo pack para UNA empresa. Usalo para drill-down en el frontend.

### Materialized Views predictivas disponibles:

**`payment_predictions`** (81 empresas):
```
company_id, company_name, tier, paid_invoices, avg_days_to_pay, 
median_days_to_pay, stddev_days, fastest_payment, slowest_payment,
payment_trend ("mejorando"/"empeorando"), avg_recent_6m, avg_older,
pending_count, total_pending, oldest_due_date, max_days_overdue,
predicted_payment_date, payment_risk ("NORMAL"/"ALTO"/"CRITICO")
```

**`client_reorder_predictions`** (418 clientes):
```
company_id, company_name, order_count, avg_cycle_days, stddev_days,
last_order_date, days_since_last, avg_order_value,
predicted_next_order, days_overdue_reorder, reorder_status ("on_track"/"overdue"/"lost"),
total_revenue, tier, salesperson_name, salesperson_email, top_product_ref
```

**`customer_ltv_health`** (1,651 clientes):
```
company_id, company_name, tier, ltv_mxn, revenue_12m, revenue_3m,
trend_pct_vs_prior_quarters, overdue_mxn, max_days_overdue,
churn_risk_score (0-100), overdue_risk_score (0-100),
days_since_last_order
```

**`cashflow_projection`** (743 rows):
```
flow_type ("receivable_detail"/"payable_bucket"/"summary"),
company_id, amount_residual, projected_date, collection_probability,
expected_amount, payment_risk, bucket ("0-30 dias"/"31-60"/"61-90"/"90+")
```

### Como el orchestrate DEBE funcionar (nueva arquitectura):

```typescript
// /api/agents/orchestrate/route.ts

// PASO 1: Obtener briefing pack del director con evidencia real
const { data: briefing } = await supabase.rpc('get_director_briefing', {
  p_director: agent.slug,  // 'comercial', 'financiero', etc.
  p_max_companies: 5
});

// PASO 2: Agregar predicciones relevantes
const { data: predictions } = await supabase
  .from(agent.domain === 'comercial' ? 'client_reorder_predictions' : 'payment_predictions')
  .select('*')
  .in('company_id', briefing.evidence_packs.map(p => p.company_id));

// PASO 3: Prompt a Claude con TODA la evidencia
const response = await callClaude({
  system: briefing.instructions,  // Ya incluye reglas de evidencia
  messages: [{
    role: 'user',
    content: JSON.stringify({
      evidence_packs: briefing.evidence_packs,
      predictions: predictions,
      feedback: briefing.agent_feedback,  // que funciono antes
    })
  }]
});

// PASO 4: Parsear insights y asignar a persona REAL (del evidence pack)
// El insight DEBE incluir:
// - assignee_name + assignee_email (del salesperson/buyer en el pack)
// - evidence_refs: ["INV/2026/02/0144", "PV15127"] (clickeables)
// - business_impact_estimate: "$151,445 MXN en cartera vencida"
// - deadline: "2026-04-18" (fecha concreta)
```

### Los 7 directores y su logica predictiva:

| Director | Slug | Predice | Datos clave |
|---|---|---|---|
| **Comercial** | `comercial` | Que cliente va a dejar de comprar | `client_reorder_predictions` (days_overdue_reorder), revenue_trend, emails sin respuesta |
| **Financiero** | `financiero` | Cuando van a pagar + salud financiera | `payment_predictions`, `cashflow_projection`, `financial_runway` (7 dias!), `cfo_dashboard`, `working_capital` (prueba acida 0.20 = critico), `accounting_anomalies` (creditos excedidos, facturas duplicadas), `pl_estado_resultados` |
| **Compras** | `compras` | Riesgo de desabasto | `supplier_concentration_herfindahl`, purchase_price_intelligence, payables_overdue |
| **Operaciones** | `operaciones` | Entregas que van a fallar | deliveries pending + late, manufacturing delays, orderpoints bajo minimo |
| **Costos** | `costos` | Erosion de margen | `product_margin_analysis`, invoice_line_margins, dead_stock_analysis |
| **Riesgo** | `riesgo` | Concentracion peligrosa | `portfolio_concentration` (Pareto/HHI), single-source suppliers, FX exposure |
| **Equipo** | `equipo` | Quien esta fallando | activities overdue by user, response times, salesperson workload |

### Reglas para insights inteligentes:

1. **NUNCA** generar insight sin evidencia especifica (numeros de factura, montos, fechas)
2. **SIEMPRE** incluir la persona responsable con nombre y email
3. **SIEMPRE** incluir el monto en riesgo en MXN
4. **SIEMPRE** dar deadline concreto (no "pronto" sino "antes del viernes 18 de abril")
5. **NUNCA** repetir un insight que ya existe en `history.recent_insights`
6. **MAXIMO** 3 insights por director por ciclo (calidad > cantidad)
7. **COMPARAR** con patron historico: "paga en promedio 21 dias pero esta en 62" es mas util que "factura vencida"
8. **CRUZAR** datos: factura vencida + email sin respuesta + actividad vencida = prioridad critica
9. **PREDECIR**: "basado en su ciclo de 16 dias, su proximo pedido deberia llegar el 16 de abril" 
10. **ESCALAR**: si el insight lleva 3 ciclos sin accion, escalar al jefe del responsable

---

## PARTE 2: FRONTEND CON EVIDENCIA CLICKEABLE

### Principio: Cada dato mencionado en un insight es clickeable

Cuando un insight dice "INV/2026/02/0144 vencida 50 dias", el CEO puede:
1. **Click en la factura** → ve detalle con monto, cliente, vendedor, CFDI
2. **Click en la empresa** → va a Company 360 con todo el evidence pack
3. **Click en el vendedor** → ve todas sus cuentas, actividades, performance
4. **Click en "Actuar"** → marca el insight como acted_on, crea follow-up

### Componentes de evidencia (agregar a shared/v2):

```typescript
// Evidence references — clickeable chips dentro de insights
<EvidenceChip 
  type="invoice"          // invoice, order, delivery, email, product
  reference="INV/2026/02/0144"
  amount={31900}
  status="overdue"
  onClick={() => openBottomSheet(<InvoiceDetail id={invoiceId} />)}
/>

// Evidence timeline — muestra la secuencia de eventos
<EvidenceTimeline events={[
  { date: "2026-01-05", type: "invoice", label: "Factura INV/2025/12/0040 emitida $31,900" },
  { date: "2026-02-19", type: "overdue", label: "Factura vence sin pago" },
  { date: "2026-03-15", type: "email", label: "Ultimo email de Carina Yazmin" },
  { date: "2026-04-14", type: "alert", label: "62 dias vencida — excede patron (avg 21d)" },
]} />

// Prediction indicator — muestra prediccion vs realidad
<PredictionCard
  label="Proximo pedido esperado"
  predicted="16 abril 2026"
  based_on="ciclo promedio 16 dias, desviacion 21d"
  status="on_track"   // on_track, overdue, lost
  confidence={0.85}
/>

// Person card — la persona responsable de actuar
<PersonCard
  name="Gilberto Lopez"
  email="gilberto@quimibond.com"
  role="Vendedor"
  metrics={{
    accounts: 45,
    overdue_activities: 12,
    response_rate: "78%"
  }}
  action="Llamar a Carina Yazmin hoy"
/>
```

### Paginas y su conexion con datos:

#### 1. CEO Dashboard (`/`)
Mobile: 2 cols KpiCards + lista de insights urgentes con evidencia.

KPIs con PREDICCION:
- Revenue mes actual + **prediccion cierre de mes** (basado en pipeline + historico)
- Cartera vencida + **projected collections** (de cashflow_projection.expected_amount)
- Cash position + **runway en dias** (de financial_runway view)
- Clientes en riesgo + **cuantos van a dejar de comprar** (reorder_status = 'lost')
- OTD rate + **entregas que van a fallar esta semana** (deliveries pending + scheduled)

Insights section: muestra top 5 insights con:
- Severity badge
- Titulo con monto en riesgo
- Persona responsable (chip clickeable)
- Evidence chips (facturas, ordenes clickeables)
- Boton "Actuar" que abre BottomSheet con detalle completo

#### 2. Insights Inbox (`/inbox`)
Mobile: lista tipo WhatsApp. Cada insight muestra:
- Avatar = severity color
- Titulo con empresa + monto
- Subtitulo = persona responsable + deadline
- Tap → BottomSheet con:
  - **Evidence Timeline** (secuencia de eventos que llevaron a este insight)
  - **Evidence Chips** (cada factura/orden/email clickeable)
  - **PredictionCard** (que va a pasar si no actuan)
  - **PersonCard** (quien debe actuar)
  - Actions: Actuar / Descartar / Escalar / Delegar

Desktop: split view (list left, detail right)

#### 3. Company 360 (`/companies/[id]`)
Llama `company_evidence_pack(company_id)` y presenta TODO:

**Tab Overview:**
- Header: nombre, tier badge, RFC, health score sparkline
- StatGrid: revenue 12m, overdue, days since last order, OTD rate
- PredictionCard: cuando va a pagar, cuando va a pedir, churn risk
- PersonCard: vendedor/comprador responsable

**Tab Finance:**
- Aging chart (overdue_invoices del pack, bucket visualization)
- Payment pattern chart (avg_days_to_pay historico + prediccion)
- Invoice table (DataTable responsive, cada row clickeable → InvoiceDetail)
- Credit notes chart

**Tab Orders:**
- Order timeline (sale_orders + order_lines)
- Top products chart (top_products del pack)
- Reorder prediction card
- Qty delivered vs invoiced (de order_lines.qty_delivered, qty_invoiced)

**Tab Communication:**
- Email threads list (recent_threads del pack)
- Unanswered threads highlighted in red
- Key contacts cards con ultimo contacto
- Days since last email indicator

**Tab Deliveries:**
- OTD gauge
- Pending shipments list
- Late deliveries with impact assessment
- Lead time trend

**Tab Activity:**
- Overdue activities (with assigned person)
- Activity timeline
- Escalation indicator if > 30 days overdue

**Tab Intelligence:**
- Recent insights about this company (con state badges)
- Health score trend chart
- Follow-up results (que acciones funcionaron)
- Agent recommendations

#### 4-10. Paginas de dominio (ventas, cobranza, productos, compras, operaciones, finanzas, equipo)

Cada una sigue el pattern:
- PageHeader con KPIs predictivos del dominio
- **Prediction section**: lo mas importante que va a pasar
- DataTable con drill-down a Company 360
- Relevant materialized view data

Ejemplo `/ventas`:
- KPI: Revenue mensual + **prediccion cierre** 
- Alerta: "5 clientes tier-strategic con reorden vencido ($X.XM MXN en riesgo)"
- Tabla: `client_reorder_predictions` WHERE reorder_status IN ('overdue','lost')
- Cada row: empresa, vendedor, dias vencido, monto historico, ultimo producto → click → Company 360

Ejemplo `/cobranza`:
- KPI: Cartera vencida total + **expected collections proximos 30 dias**
- Aging waterfall chart con `ar_aging_detail`
- Tabla: top deudores con `payment_predictions` (cuando van a pagar)
- Cada row: empresa, facturas vencidas, predicted_payment_date, payment_risk → click → Company 360

#### 11. Finanzas (`/finanzas`) — PAGINA CLAVE PARA CFO

Esta pagina tiene datos contables completos desde Odoo. Es un mini-ERP financiero.

**Datos disponibles:**

**`pl_estado_resultados`** (view) — P&L mensual listo para graficar:
```
period (YYYY-MM), ingresos, costo_ventas, gastos_operativos,
utilidad_bruta, utilidad_operativa, otros_neto
```
Datos reales marzo 2026: Ingresos $20.1M, Costo $12.3M, Utilidad bruta $7.8M (38.7%)

**`cfo_dashboard`** (view) — Snapshot financiero en 1 query:
```
efectivo_disponible: $2.4M
deuda_tarjetas: $3.0M
posicion_neta: -$610K
cuentas_por_cobrar: $51.3M
cuentas_por_pagar: $8.9M
cartera_vencida: $45.7M
ventas_30d: $18.3M
cobros_30d: $16.1M
pagos_prov_30d: $13.3M
clientes_morosos: 77
```

**`working_capital`** (view) — Ratios de liquidez:
```
capital_de_trabajo: $41.8M
ratio_liquidez: 4.51
ratio_prueba_acida: 0.20
```

**`financial_runway`** (view) — Dias de vida:
```
cash_mxn, expected_in_mxn, due_out_mxn, net_position_30d,
burn_rate_daily: $359K/dia, runway_days_net: 7, runway_days_cash_only: -2
```

**`accounting_anomalies`** (matview, 2,671 rows) — Alertas contables:
- `credit_exceeded`: CONTITECH excede limite credito 41% ($7M vs $5M limite)
- `duplicate_invoice`: posibles facturas duplicadas con montos
- Severity: critical/high/medium

**`odoo_account_balances`** (13,297 rows) — Balance mensual por cuenta contable:
```
odoo_account_id, account_code, account_name, account_type, period (YYYY-MM),
debit, credit, balance
```
18 tipos de cuenta: asset_cash, asset_receivable, liability_payable, income, expense, equity, etc.

**`odoo_bank_balances`** (46 journals) — Caja en tiempo real por banco:
```
name ("5048 BBVA BANCOMER"), journal_type, currency (MXN/USD),
current_balance, company_name
```

**`budgets`** (0 rows — para llenar manualmente):
```
odoo_account_id, account_code, period (YYYY-MM), budget_amount
```

**Secciones de la pagina `/finanzas`:**

1. **CFO Snapshot** (StatGrid):
   - Cash neto + runway days (con alerta si < 15 dias)
   - CxC vs CxP (ratio)
   - Cartera vencida con % del total
   - Burn rate diario

2. **P&L Chart** (Recharts BarChart):
   - Barras mensuales: ingresos vs costo_ventas vs gastos
   - Linea: utilidad_operativa
   - Ultimos 12 meses de `pl_estado_resultados`

3. **Working Capital Gauges**:
   - Ratio liquidez (gauge 0-10, alerta si < 1.5)
   - Prueba acida (gauge 0-5, alerta si < 1.0)
   - Capital de trabajo (KpiCard con trend)

4. **Cash Position by Bank** (DataTable):
   - Cada banco con saldo actual, moneda
   - Sparkline de ultimos 30 dias (si hay historico)
   - Total MXN + total USD separados

5. **Balance Sheet Summary** (from account_balances):
   - Activos: cash + receivable + current + fixed
   - Pasivos: payable + current + credit_cards
   - Capital: equity
   - Agrupado por account_type, clickeable a detalle

6. **Accounting Anomalies** (DataTable):
   - Facturas duplicadas
   - Limites de credito excedidos
   - Severity badges + company link

7. **Budget vs Actual** (si budgets tiene data):
   - Barras: budget vs actual por cuenta/mes
   - Variaciones > 15% highlighted

#### 12. System (`/system`)
Pipeline health, agent performance (con acceptance_rate), token cost, sync status.

---

## PARTE 3: COMPONENTES REUTILIZABLES

### Los 20 componentes shared (en `src/components/shared/v2/`):

Los 16 originales:
`KpiCard`, `StatGrid`, `DataTable`, `CompanyLink`, `Currency`, `DateDisplay`, `SeverityBadge`, `StatusBadge`, `TrendIndicator`, `MetricRow`, `EmptyState`, `MiniChart`, `FilterBar`, `BottomSheet`, `MobileCard`, `PageHeader`

Mas 4 nuevos de evidencia:
`EvidenceChip`, `EvidenceTimeline`, `PredictionCard`, `PersonCard`

### Mobile-first rules:
- Bottom tab bar (5 tabs: Home, Insights, Companies, Finance, Menu)
- Cards stack vertical en mobile, grid en desktop
- Tables → MobileCard en < 640px
- BottomSheet para detalles (no modals)
- Touch targets 44px+
- Pull-to-refresh en paginas principales
- Swipe actions en insights (right=actuar, left=dismiss)

---

## PARTE 4: ARQUITECTURA

```
src/
  app/
    layout.tsx              # Sidebar desktop + bottom nav mobile
    page.tsx                # CEO Dashboard con predicciones
    inbox/
      page.tsx              # Insights con evidencia
      insight/[id]/page.tsx # Insight detail con timeline
    companies/
      page.tsx
      [id]/page.tsx         # Company 360 con evidence_pack
    ventas/page.tsx
    cobranza/page.tsx
    productos/page.tsx
    compras/page.tsx
    operaciones/page.tsx
    finanzas/page.tsx
    equipo/page.tsx
    system/page.tsx
  components/
    ui/                     # shadcn (no tocar)
    layout/
      app-shell.tsx
      sidebar.tsx
      bottom-nav.tsx
    shared/v2/              # 20 COMPONENTES REUTILIZABLES
      index.ts
      kpi-card.tsx
      stat-grid.tsx
      data-table.tsx
      company-link.tsx
      currency.tsx
      date-display.tsx
      severity-badge.tsx
      status-badge.tsx
      trend-indicator.tsx
      metric-row.tsx
      empty-state.tsx
      mini-chart.tsx
      filter-bar.tsx
      bottom-sheet.tsx
      mobile-card.tsx
      page-header.tsx
      evidence-chip.tsx     # NUEVO
      evidence-timeline.tsx # NUEVO
      prediction-card.tsx   # NUEVO
      person-card.tsx       # NUEVO
    domain/                 # Composiciones por pagina
  lib/
    supabase-server.ts      # NO TOCAR
    claude.ts               # NO TOCAR
    formatters.ts           # es-MX, Intl.NumberFormat
    queries/
      dashboard.ts          # KPIs + predicciones
      companies.ts          # company_evidence_pack RPC
      insights.ts           # inbox queries
      invoices.ts           # invoice detail
      sales.ts              # ventas + reorder predictions
      collections.ts        # cobranza + payment predictions
      products.ts
      purchases.ts
      operations.ts
      finance.ts            # cashflow + runway
      team.ts
    hooks/
      use-mobile.ts
      use-pull-refresh.ts
```

## REGLAS ABSOLUTAS

1. **`_mxn` fields SIEMPRE** para montos. NUNCA sumar `amount_total` directo
2. **Mobile-first**: CSS para 375px primero, md: y lg: despues
3. **Evidence clickeable**: cada factura, orden, email mencionado abre un detail
4. **Persona responsable siempre**: cada insight nombra QUIEN debe actuar
5. **Prediccion siempre**: no solo "que paso" sino "que VA a pasar"
6. **shadcn base**: cada componente usa shadcn primitives
7. **Server Components default**: Client solo para interactividad
8. **NO tocar `src/app/api/`**: los crons y APIs ya funcionan
9. **Max 3 insights por director**: calidad sobre cantidad
10. **Spanish UI**: texto en espanol, codigo en ingles
11. **Dark mode ready**: semantic tokens
12. **Empty states**: nunca contenedores vacios
13. **Skeletons**: Suspense + shadcn Skeleton en cada page
14. **RPC `get_director_briefing`**: es la fuente de datos de los agentes, NO queries sueltas
15. **RPC `company_evidence_pack`**: es la fuente de Company 360, NO queries multiples

## Supabase Project
- ID: `tozqezmivpblmcubmnpi`
- Region: us-east-1
- 52 tables, 26 materialized views, 119 functions, 19 views
- Full schema docs: read from Supabase MCP or CLAUDE.md in this repo
