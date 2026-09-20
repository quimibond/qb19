# PLAN: Cerebro Quimibond v2 — Rediseño Arquitectónico

## Estado Actual (Diagnóstico)

### Lo que funciona
- Pipeline Gmail → Claude → Supabase → Briefing: funcional, 1509 emails procesados
- Knowledge Graph: 116 entities, 81 facts, 42 relationships
- Health Scores: 281 registros calculados
- 40 RPCs en Supabase, MV contact_360, 22 triggers, HNSW vector indexes
- Frontend Next.js con dashboard, contactos, alertas, chat RAG

### Lo que NO funciona — El problema central

**Los datos están fragmentados y desconectados:**

| Métrica | Valor | Impacto |
|---------|-------|---------|
| Contactos externos sin `odoo_partner_id` | 160/160 (100%) | El brain no sabe NADA de Odoo |
| Contactos sin `entity_id` (KG) | 141/160 (88%) | Sin link al Knowledge Graph |
| Contactos sin `role` | 147/160 (92%) | No sabe quién es quién |
| Companies sin `entity_id` | 88/120 (73%) | Empresas sin KG |
| Companies sin `odoo_partner_id` | 106/120 (88%) | Sin datos ERP |
| Entities sin `odoo_id` | 70/116 (60%) | KG desconectado de Odoo |

**Resultado:** El cerebro tiene piezas de información pero no las puede conectar.
Un contacto puede tener emails, pero no se vincula a su partner de Odoo, ni a su entity del KG, ni a los facts sobre él.

### Problemas Arquitectónicos Raíz

1. **3 fuentes de identidad sin resolver**: `contacts.email`, `entities.canonical_name`, `odoo.res.partner.id` — no hay reconciliación robusta
2. **Pipeline batch-only**: Solo se ejecuta 1x/día a las 19:00. No hay sync incremental real-time
3. **Claude como cuello de botella**: Todo el análisis pasa por un solo pipeline serial
4. **KG superficial**: Facts como texto libre sin estructura, sin embeddings en facts, sin temporal reasoning
5. **Sin feedback loop real**: prediction_outcomes tiene 0 rows, feedback_signals tiene 3
6. **No hay priorización inteligente**: Todas las alertas se tratan igual
7. **Datos de Odoo son snapshot**: Se copian una vez, no hay sync bidireccional

---

## Visión: El Cerebro que Transforma la Empresa

### Principio #1: Una sola fuente de verdad por entidad
Cada persona y empresa debe tener UN registro maestro que conecte TODO.

### Principio #2: Tiempo real, no batch diario
Los insights deben llegar cuando se necesitan, no 24h después.

### Principio #3: El cerebro aprende y se adapta
Feedback explícito + implícito → mejores alertas, mejores scores, mejores acciones.

### Principio #4: Accionable > Informativo
Cada insight debe tener un "next step" concreto y asignado.

---

## Fase 1: Resolver la Identidad (URGENTE — Semana 1-2)

### 1.1 Entity Resolution Service

El problema central es que hay 3 registros para la misma persona/empresa que no están conectados. Necesitamos un servicio de resolución que:

```
email → contact → entity → odoo_partner
        ↓                    ↓
      company ←→ entity ←→ odoo_company
```

**Nuevo: `resolve_identity()` RPC en Supabase:**
```sql
-- Dado un email, conecta contact ↔ entity ↔ odoo
-- Dado un nombre de empresa, conecta company ↔ entity ↔ odoo
-- Usa fuzzy matching para nombres
-- Ejecuta en cada insert/update de contacts/entities
```

**Acciones concretas:**
1. Crear RPC `resolve_all_identities()` que recorra TODOS los contactos y:
   - Busque entity por email exacto → link entity_id
   - Busque entity por canonical_name → link entity_id
   - Busque company por canonical_name → link company_id
   - Si no existe entity, créala desde el contact

2. Crear trigger `trg_auto_resolve_identity` en contacts (INSERT/UPDATE):
   - Auto-link entity_id si encuentra match por email
   - Auto-link company_id si encuentra match por company name

3. **Odoo side**: Mejorar `_link_odoo_ids` para:
   - Usar fuzzy search (ILIKE + similarity) no solo exact match
   - Buscar por multiple emails del mismo partner
   - Buscar por VAT/RFC si disponible
   - Ejecutar SIEMPRE, no solo para `odoo_partner_id=is.null`

### 1.2 Company Domain Resolution

Muchas companies no tienen domain. Extraer domain del email de sus contactos:

```sql
UPDATE companies co SET domain = (
  SELECT split_part(c.email, '@', 2)
  FROM contacts c WHERE c.company_id = co.id
  AND c.email NOT LIKE '%gmail%' AND c.email NOT LIKE '%hotmail%'
  LIMIT 1
) WHERE co.domain IS NULL;
```

### 1.3 Role Detection from Email Analysis

El 92% de contactos no tiene role. Claude ya analiza emails — agregar extracción de roles:

```python
# En el análisis por cuenta, extraer:
# - Rol inferido (compras, ventas, dirección, logística, calidad)
# - Decision power (por firma de email, tono, tipo de decisiones)
# - Relationship pattern (gatekeeper, decision-maker, influencer)
```

---

## Fase 2: De Batch a Incremental (Semana 2-4)

### 2.1 Supabase Realtime Hooks

En lugar de un pipeline monolítico 1x/día:

```
Gmail Push Notification → Webhook → Edge Function → Supabase
                                         ↓
                                  Auto-classify email
                                  Auto-extract facts
                                  Auto-update health score
                                  Auto-trigger alerts
```

**Implementación pragmática (sin webhooks aún):**
- Odoo cron cada 30 min (no cada 24h) para sync incremental
- Separar el pipeline en micro-pipelines independientes:
  1. `sync_emails` — cada 30 min
  2. `analyze_new_emails` — después de sync_emails
  3. `update_scores` — cada 2h
  4. `enrich_companies` — cada 6h
  5. `generate_briefing` — 1x/día

### 2.2 Tablas Nuevas para Event Sourcing

```sql
CREATE TABLE events (
  id bigserial PRIMARY KEY,
  event_type text NOT NULL,  -- 'email_received', 'alert_created', 'score_changed', 'odoo_sync'
  entity_type text,          -- 'contact', 'company', 'thread'
  entity_id bigint,
  payload jsonb NOT NULL,
  created_at timestamptz DEFAULT now()
);
CREATE INDEX idx_events_type_date ON events(event_type, created_at DESC);
CREATE INDEX idx_events_entity ON events(entity_type, entity_id);
```

Esto permite:
- Timeline de actividad por contacto/empresa
- Detección de patrones temporales
- Audit trail completo
- "¿Qué pasó con este cliente en los últimos 30 días?"

### 2.3 Webhook de Odoo → Supabase (bidireccional)

```python
# En Odoo, hook post-write en res.partner:
def write(self, vals):
    result = super().write(vals)
    if any(f in vals for f in ['credit_limit', 'invoice_ids', ...]):
        self._push_to_supabase(vals)
    return result
```

---

## Fase 3: Knowledge Graph Profundo (Semana 3-5)

### 3.1 Reestructurar Facts

Actualmente: facts son texto libre sin estructura.
Propuesta: facts tipados con estructura semántica.

```sql
-- Agregar columnas a facts:
ALTER TABLE facts ADD COLUMN subject_entity_id bigint REFERENCES entities(id);
ALTER TABLE facts ADD COLUMN object_entity_id bigint REFERENCES entities(id);
ALTER TABLE facts ADD COLUMN predicate text;  -- 'compra', 'reclama', 'solicita', 'negocia'
ALTER TABLE facts ADD COLUMN value_numeric numeric;  -- para facts cuantitativos
ALTER TABLE facts ADD COLUMN value_unit text;         -- 'USD', 'tons', 'days'
ALTER TABLE facts ADD COLUMN valid_from date;
ALTER TABLE facts ADD COLUMN valid_until date;
```

**Ejemplo:**
```
Antes:  "Cliente Textiles del Norte compró 500 tons de tela en marzo"
Después: subject=Textiles del Norte, predicate=compra, object=tela,
         value_numeric=500, value_unit=tons, valid_from=2026-03-01
```

### 3.2 Entity Types Expandidos

Actualmente: `person`, `company`
Propuesta: agregar `product`, `topic`, `event`, `location`, `process`

```sql
-- Esto permite:
-- "¿Qué clientes compran Tela Quimibond 400?"
-- "¿Qué eventos afectan nuestra cadena de suministro?"
-- "¿Qué procesos internos causan más retrasos?"
```

### 3.3 Temporal Reasoning

```sql
CREATE TABLE entity_timeline (
  id bigserial PRIMARY KEY,
  entity_id bigint REFERENCES entities(id),
  event_type text NOT NULL,      -- 'status_change', 'score_change', 'interaction', 'alert'
  event_date timestamptz NOT NULL,
  old_value jsonb,
  new_value jsonb,
  source text,                   -- 'email', 'odoo', 'claude', 'manual'
  created_at timestamptz DEFAULT now()
);
```

Permite preguntar: "¿Cómo evolucionó la relación con Textiles del Norte en los últimos 6 meses?"

---

## Fase 4: Inteligencia Accionable (Semana 4-6)

### 4.1 Sistema de Playbooks

En lugar de alertas genéricas, definir playbooks por tipo de situación:

```sql
CREATE TABLE playbooks (
  id bigserial PRIMARY KEY,
  name text NOT NULL,
  trigger_conditions jsonb NOT NULL,  -- cuándo se activa
  steps jsonb NOT NULL,               -- qué hacer, en orden
  owner_role text,                    -- quién lo ejecuta
  sla_hours int,                      -- en cuánto tiempo
  is_active boolean DEFAULT true
);

-- Ejemplo:
-- name: "Cliente en riesgo de churn"
-- trigger: health_score < 40 AND trend = 'declining' AND is_customer = true
-- steps: [
--   {action: "Revisar últimos 5 emails", assignee: "account_manager"},
--   {action: "Llamar al contacto principal", sla: "24h"},
--   {action: "Preparar propuesta de retención", sla: "48h"},
--   {action: "Reportar a dirección si no responde", sla: "72h"}
-- ]
```

### 4.2 Alert Intelligence

Actualmente: todas las alertas se tratan igual.
Propuesta: scoring de alertas por impacto de negocio.

```sql
ALTER TABLE alerts ADD COLUMN business_value_at_risk numeric;
ALTER TABLE alerts ADD COLUMN urgency_score numeric;
ALTER TABLE alerts ADD COLUMN recommended_playbook_id bigint REFERENCES playbooks(id);
```

El `business_value_at_risk` se calcula desde `companies.lifetime_value` + pipeline CRM.
Esto permite: "Alerta: Riesgo de perder a Textiles del Norte ($2.3M/año)"

### 4.3 Accountability Engine

Mejorar el tracking de acciones:

```sql
CREATE TABLE action_escalations (
  id bigserial PRIMARY KEY,
  action_id bigint REFERENCES action_items(id),
  escalation_level int DEFAULT 1,
  escalated_to text,
  escalated_at timestamptz DEFAULT now(),
  reason text
);
```

Flujo: Acción asignada → SLA vencido → Escalamiento automático → Notificación

---

## Fase 5: El Cerebro que Aprende (Semana 5-8)

### 5.1 Feedback Loop Real

Actualmente: 0 prediction outcomes, 3 feedback signals.

**Edge Function para capturar feedback implícito:**
```typescript
// Cuando el usuario:
// - Ignora una alerta > 48h → feedback negativo implícito
// - Resuelve una alerta < 4h → feedback positivo
// - Marca acción como completada → feedback positivo
// - Cambia prioridad de acción → feedback sobre priorización
```

### 5.2 Claude con Memoria Contextual

Actualmente: Claude recibe un prompt frío cada día.
Propuesta: memoria acumulativa por cuenta/contacto.

```sql
CREATE TABLE claude_memory (
  id bigserial PRIMARY KEY,
  context_type text NOT NULL,  -- 'account', 'contact', 'company', 'general'
  context_id text,             -- email o company_id
  memory_text text NOT NULL,
  importance float DEFAULT 0.5,
  created_at timestamptz DEFAULT now(),
  expires_at timestamptz,
  embedding vector(1024)
);
```

En cada análisis, Claude recibe sus "memorias" previas sobre esa cuenta:
- "La última vez que analicé a este cliente, detecté tensión por entregas tardías"
- "Este contacto suele escalar cuando no recibe respuesta en 24h"

### 5.3 Score Calibration

```sql
-- Cada semana, comparar predicciones vs realidad:
-- Si el score predijo riesgo y el cliente se fue → reward +1
-- Si el score predijo estabilidad y el cliente se fue → penalizar weights
-- Ajustar pesos de health score automáticamente
```

---

## Fase 6: Comunicación Inteligente (Semana 6-10)

### 6.1 Template Engine para Respuestas

El cerebro no solo detecta problemas, sugiere respuestas:

```sql
CREATE TABLE response_templates (
  id bigserial PRIMARY KEY,
  scenario text NOT NULL,        -- 'late_delivery_apology', 'price_negotiation', 'follow_up'
  language text DEFAULT 'es',
  tone text DEFAULT 'formal',
  template_text text NOT NULL,
  variables jsonb,               -- [{name: 'client_name'}, {name: 'delay_days'}]
  effectiveness_score float,     -- basado en feedback
  times_used int DEFAULT 0
);
```

### 6.2 Communication Scoring Interno

No solo medir comunicación con externos, también internos:

```sql
-- ¿Quién responde más rápido internamente?
-- ¿Qué departamento tiene mejor tasa de respuesta?
-- ¿Qué emails internos se ignoran consistentemente?
-- ¿Hay silos de comunicación entre departamentos?
```

### 6.3 Briefings Personalizados

En lugar de 1 briefing para todos:
- **Briefing del Director**: KPIs, riesgos financieros, decisiones pendientes
- **Briefing de Ventas**: Oportunidades, clientes en riesgo, pipeline
- **Briefing de Logística**: Entregas pendientes, problemas de calidad
- **Briefing de Compras**: Proveedores con problemas, negociaciones abiertas

---

## Arquitectura Target

```
                    ┌─────────────┐
                    │   FRONTEND  │
                    │  (Next.js)  │
                    └──────┬──────┘
                           │ Supabase Realtime + REST
                    ┌──────┴──────┐
                    │  SUPABASE   │
                    │ (PostgreSQL │
                    │  + pgvector │
                    │  + RPC)     │
                    └──┬───┬───┬──┘
                       │   │   │
           ┌───────────┘   │   └───────────┐
           │               │               │
    ┌──────┴──────┐ ┌──────┴──────┐ ┌──────┴──────┐
    │  ODOO 19    │ │  CLAUDE API │ │  GMAIL API  │
    │  (ERP)      │ │  (Analysis) │ │  (Emails)   │
    │  - Partners │ │  - Extract  │ │  - Read     │
    │  - Sales    │ │  - Analyze  │ │  - Push     │
    │  - Invoice  │ │  - Score    │ │  - Watch    │
    │  - Delivery │ │  - Profile  │ │             │
    │  - CRM      │ │  - Memory   │ │             │
    └─────────────┘ └─────────────┘ └─────────────┘

    Sync: bidireccional    Sync: event-driven    Sync: incremental
    Freq: real-time hooks  Freq: per-event       Freq: cada 30 min
```

---

## Prioridades de Implementación

### Sprint 1 (Esta semana): Identity Resolution
- [ ] RPC `resolve_all_identities()` — conectar los 160 contactos huérfanos
- [ ] Trigger auto-resolve en contacts INSERT/UPDATE
- [ ] Mejorar `_link_odoo_ids` con fuzzy matching
- [ ] Domain extraction para companies
- [ ] **Meta: 0% de contactos sin entity_id y company_id**

### Sprint 2 (Semana 2): Pipeline Micro-Services
- [ ] Separar pipeline monolítico en 5 crons independientes
- [ ] Reducir ciclo de sync de 24h a 30min para emails
- [ ] Event sourcing table + triggers

### Sprint 3 (Semana 3): KG Profundo
- [ ] Structured facts (predicate, subject, object)
- [ ] Entity types expandidos
- [ ] Role detection en análisis Claude
- [ ] Entity timeline

### Sprint 4 (Semana 4): Actionable Intelligence
- [ ] Playbooks engine
- [ ] Alert business value scoring
- [ ] Action escalation
- [ ] Feedback loop real (implícito + explícito)

### Sprint 5 (Semana 5): Communication Intelligence
- [ ] Claude memory contextual
- [ ] Briefings personalizados por rol
- [ ] Response templates
- [ ] Communication scoring interno

---

## Métricas de Éxito

| Métrica | Hoy | Meta Sprint 1 | Meta Final |
|---------|-----|---------------|------------|
| Contactos con entity_id | 12% | 90% | 99% |
| Contactos con odoo_id | 0% | 60% | 95% |
| Companies con odoo_id | 12% | 70% | 95% |
| Contactos con role | 8% | 40% | 85% |
| Cycle time de insights | 24h | 2h | 30min |
| Prediction accuracy | 0% (sin data) | — | 70% |
| Action completion rate | desconocido | medible | >60% |
| Alerts with business value | 0% | 50% | 90% |
| Feedback signals/semana | 0.4 | 10 | 50+ |
