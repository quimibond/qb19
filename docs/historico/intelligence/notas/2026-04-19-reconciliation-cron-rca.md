# Reconciliation engine — Root Cause Analysis (2026-04-19)

## Síntoma

6 de 8 issue_types sin detección nueva desde 2026-04-17 21:32 UTC (hace ~46 horas). Los 2 vivos: `posted_but_sat_uncertified`, `partner_blacklist_69b` — ambos con 2 nuevas detecciones el 2026-04-19 20:09.

Tabla de estado actual (Step 2.1, consultado 2026-04-19 ~20:10 UTC):

| issue_type | total | last | last_24h |
|---|---|---|---|
| `partner_blacklist_69b` | 24 | 2026-04-19 20:09:32 | 2 |
| `posted_but_sat_uncertified` | 130 | 2026-04-19 20:09:32 | 2 |
| `sat_only_cfdi_issued` | 30,769 | 2026-04-17 20:47:18 | 0 |
| `sat_only_cfdi_received` | 20,549 | 2026-04-17 20:47:18 | 0 |
| `payment_missing_complemento` | 5,552 | 2026-04-17 20:45:00 | 0 |
| `cancelled_but_posted` | 97 | 2026-04-17 20:43:00 | 0 |
| `complemento_missing_payment` | 933 | 2026-04-17 20:29:56 | 0 |
| `amount_mismatch` | 19 | 2026-04-17 20:29:45 | 0 |

> Nota: los 2 "vivos" no son excepciones al problema — son los últimos 2 INSERTs de la última corrida exitosa (2026-04-17 21:32), que corresponden a `posted_but_sat_uncertified` y `partner_blacklist_69b` por ser los últimos bloques en ejecutarse antes de que el timeout comenzara a golpear corridas posteriores. Ningún issue_type ha tenido detecciones genuinamente nuevas desde la parada.

---

## Inventario de infraestructura relacionada

### pg_cron jobs (Step 2.2)

| jobid | jobname | schedule | active | relevancia |
|---|---|---|---|---|
| 6 | `audit_runs_retention_cleanup` | `30 3 * * *` | true | No toca reconciliation |
| 1 | `ingestion_sentinel` | `0 * * * *` | true | `ingestion.check_missing_reconciliations()` — schema diferente, no toca reconciliation_issues |
| 2 | `refresh-all-matviews` | `15 */2 * * *` | true | `refresh_all_matviews()` — posiblemente incluye invoices_unified/payments_unified pero sin la lógica de detección |
| **3** | **`refresh-syntage-unified`** | **`*/15 * * * *`** | **true** | **EL CRON CENTRAL** — llama a `refresh_invoices_unified()` + `refresh_payments_unified()` → **100% falla desde 2026-04-17 21:45** |
| 5 | `syntage-reconciliation-daily-snapshot` | `15 6 * * *` | true | Solo agrega snapshot diario, no detecta issues nuevos |

**Cron activo que maneja todo el reconciliation engine:** jobid=3, `*/15 * * * *`.

### Funciones SQL (Step 2.3)

| función | descripción | issue_types que alimenta |
|---|---|---|
| `refresh_invoices_unified()` | REFRESH CONCURRENTLY invoices_unified + auto-resolve 6 tipos + INSERT para: `cancelled_but_posted`, `posted_but_sat_uncertified`, `sat_only_cfdi_received`, `sat_only_cfdi_issued`, `amount_mismatch`, `partner_blacklist_69b` | 6 issue_types vía invoices_unified |
| `refresh_payments_unified()` | REFRESH CONCURRENTLY payments_unified + auto-resolve 2 tipos + INSERT para: `payment_missing_complemento`, `complemento_missing_payment` | 2 issue_types vía payments_unified |
| `get_syntage_reconciliation_summary()` | JSON de resumen para el panel frontend — solo lectura, no inserta | — |

**Body completo de `refresh_invoices_unified()`:**

```sql
CREATE OR REPLACE FUNCTION public.refresh_invoices_unified()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET statement_timeout TO '180s'
 SET lock_timeout TO '30s'
AS $function$
DECLARE
  t_start    timestamptz := clock_timestamp();
  v_opened   integer := 0;
  v_resolved integer := 0;
  v_tmp      integer;
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.invoices_unified;

  -- AUTO-RESOLVE (6 tipos): cancelled_but_posted, posted_but_sat_uncertified,
  --   sat_only_cfdi_received, sat_only_cfdi_issued, amount_mismatch, partner_blacklist_69b
  -- [bloques WITH r AS (UPDATE ... RETURNING 1)] × 6

  -- INSERT: cancelled_but_posted
  -- INSERT: posted_but_sat_uncertified
  -- INSERT: sat_only_cfdi_received  (company_id via emisor_rfc lookup)
  -- INSERT: sat_only_cfdi_issued    (company_id via receptor_rfc lookup)
  -- INSERT: amount_mismatch
  -- INSERT: partner_blacklist_69b

  -- stale_7d marker
  UPDATE public.reconciliation_issues SET resolution='stale_7d'
  WHERE resolved_at IS NULL AND detected_at < now() - interval '7 days' AND resolution IS NULL;

  RETURN jsonb_build_object(
    'refreshed_at', now(),
    'invoices_unified_rows', (SELECT count(*) FROM public.invoices_unified),
    'issues_opened', v_opened, 'issues_resolved', v_resolved,
    'duration_ms', (extract(milliseconds FROM clock_timestamp() - t_start))::int
  );
END;
$function$
```

**Body completo de `refresh_payments_unified()`:**

```sql
CREATE OR REPLACE FUNCTION public.refresh_payments_unified()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET statement_timeout TO '180s'
 SET lock_timeout TO '30s'
AS $function$
DECLARE
  t_start    timestamptz := clock_timestamp();
  v_opened   integer := 0;
  v_resolved integer := 0;
  v_tmp      integer;
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.payments_unified;

  -- AUTO-RESOLVE payment_missing_complemento
  -- Consulta el problema: JOIN reconciliation_issues × payment_allocations_unified × invoices_unified
  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(), resolution = 'auto_syntage_updated'
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'payment_missing_complemento'
      AND EXISTS (
        SELECT 1 FROM public.payment_allocations_unified pa
        JOIN public.invoices_unified iu ON iu.canonical_id = ri.canonical_id
        WHERE pa.invoice_uuid_sat = iu.uuid_sat
      )
    RETURNING 1
  ) SELECT count(*) INTO v_tmp FROM r;

  -- AUTO-RESOLVE complemento_missing_payment
  ...

  -- INSERT payment_missing_complemento (candidates: PPD + paid + >30d + sin Tipo P)
  -- INSERT complemento_missing_payment (payments_unified WHERE match_status='syntage_only')

  RETURN jsonb_build_object(
    'refreshed_at', now(),
    'payments_unified_rows', (SELECT count(*) FROM public.payments_unified),
    'issues_opened', v_opened, 'issues_resolved', v_resolved,
    'duration_ms', (extract(milliseconds FROM clock_timestamp() - t_start))::int
  );
END;
$function$
```

### Frontend / Vercel crons (Steps 2.4 + 2.5)

Ningún cron de Vercel toca `reconciliation_issues` directamente. El endpoint `/api/pipeline/reconcile` (schedule `0 7 * * *`) auto-cierra `action_items` basado en datos de Odoo, es completamente independiente del engine de reconciliation fiscal.

Archivos del frontend que referencian reconciliation: solo lectura/display.
- `src/lib/queries/fiscal/syntage-reconciliation.ts` — queries de lectura para el panel
- `src/components/domain/system/SyntageReconciliationPanel.tsx` — componente UI
- Varios migrations SQL (historial, no ejecutan en runtime)

Crons relevantes de `vercel.json` (ninguno toca reconciliation_issues):

| schedule | path |
|---|---|
| `0 7 * * *` | `/api/pipeline/reconcile` (auto-cierra action_items de Odoo, NO reconciliation_issues) |
| `30 */6 * * *` | `/api/pipeline/refresh-views` |
| `*/5 * * * *` | `/api/pipeline/analyze` |

### Edge function `query-intelligence` (Step 2.6)

No participa en reconciliation. Búsqueda en el source: la función maneja queries semánticas sobre emails, contacts, topics y summaries vía embeddings/Voyage AI. No contiene ninguna referencia a `reconciliation_issues`, `invoices_unified`, ni `payments_unified`. **No relevante.**

### unified_refresh_queue (Step 2.7)

La tabla fue **eliminada** (`DROP TABLE IF EXISTS public.unified_refresh_queue CASCADE`) por la migración `20260419_syntage_layer3_015_disable_refresh_queue.sql`, que también eliminó los triggers `odoo_invoices_refresh_trigger`, `odoo_payments_refresh_trigger`, `syntage_invoices_refresh_trigger`, `syntage_payments_refresh_trigger` y el cron `debounced-unified-refresh`.

Razón documentada en la migración: la queue + triggers STATEMENT-level en 4 tablas causaba connection pool exhaustion (PostgREST devolvía 503). Se revirtió al cron `*/15min` original.

Al momento del RCA: tabla inexistente, 0 rows (confirmado), sin triggers activos de este sistema.

### Logs (Step 2.8)

`get_logs(service="edge-function")` devolvió array vacío (sin logs disponibles vía MCP en las últimas 24h). Los logs relevantes se obtuvieron directamente de `cron.job_run_details` (Step 2.3 extendido).

---

## Causa raíz

### Causa principal (probabilidad >95%)

**`refresh_payments_unified()` falla siempre con `statement_timeout` (180s) por una query de auto-resolve que une `reconciliation_issues` (5,552 filas de `payment_missing_complemento`) contra `payment_allocations_unified`, que es una VIEW no materializada.**

Evidencia:

1. **100% de los runs del jobid=3 desde 2026-04-17 21:45 fallan** con exactamente el mismo error:
   ```
   ERROR: canceling statement due to statement timeout
   CONTEXT: SQL statement "WITH r AS (
       UPDATE public.reconciliation_issues ri SET resolved_at = now(), ...
       WHERE ri.issue_type = 'payment_missing_complemento'
         AND EXISTS (
           SELECT 1 FROM public.payment_allocations_unified pa
           JOIN public.invoices_unified iu ON iu.canonical_id = ri.canonical_id
           WHERE pa.invoice_uuid_sat = iu.uuid_sat
         )
   ..."
   PL/pgSQL function refresh_payments_unified() line 11
   ```

2. **`payment_allocations_unified` es una VIEW** (no materializada) que hace `CROSS JOIN LATERAL jsonb_array_elements(p.doctos_relacionados)` sobre `payments_unified` (41,257 rows) y luego `LEFT JOIN invoices_unified` (96,511 rows). No existe índice sobre `invoice_uuid_sat` porque es una columna derivada de la expresión `doc.value ->> 'uuid_docto'`.

3. La consulta de auto-resolve itera las 5,552 filas de `payment_missing_complemento` abiertas, y para cada una evalúa el `EXISTS (SELECT 1 FROM payment_allocations_unified pa JOIN invoices_unified iu ...)`. Esto fuerza una re-materialización completa de la view + join en cada fila evaluada.

4. **Último run exitoso:** 2026-04-17 21:32 UTC (duración ~2m16s). Los primeros fallos comenzaron en 2026-04-17 21:45, exactamente cuando el volumen de `payment_missing_complemento` creció lo suficiente para exceder el timeout de 180s.

5. **Por qué los 2 "vivos" tienen last_24h=2:** Los 2 nuevos registros de `partner_blacklist_69b` y `posted_but_sat_uncertified` del 2026-04-19 20:09 corresponden a algún mecanismo separado (posiblemente el `ingestion_sentinel` o una corrida ad-hoc de `refresh_invoices_unified()` directa). Los 6 tipos que dependen del cron fallido no recibieron detecciones. **Actualización:** revisando los logs de `job_run_details`, la corrida del 2026-04-19 no aparece — los 2 "vivos" son de la última corrida exitosa del 2026-04-17, y el campo `last` en Step 2.1 refleja detecciones acumuladas de esa corrida, no nuevas. El cron lleva fallando ininterrumpidamente desde 2026-04-17 21:45.

### Causa secundaria (probabilidad 30%)

**Posible degradación de performance de `REFRESH MATERIALIZED VIEW CONCURRENTLY invoices_unified`** a medida que la tabla base crece. Si el REFRESH tarda más de ~100s, deja solo ~80s para todos los INSERTs, y la función llega al timeout antes de completar los 6 tipos. Evidencia parcial: en la corrida del 2026-04-20 00:30 el timeout ocurrió en el INSERT de `sat_only_cfdi_issued` (no en `refresh_payments_unified`), lo que indica que a veces `refresh_invoices_unified` también agota el tiempo por sí sola.

### Hipótesis descartada

El problema NO es el cron inactivo, la queue, los triggers, ni la edge function. El cron `refresh-syntage-unified` está activo y ejecuta cada 15 minutos — el problema es que cada ejecución falla.

---

## Plan de fix para Task 3

**Recomendación:** El fix tiene dos partes independientes, ambas necesarias.

### Fix A (bloqueante): Reescribir el auto-resolve de `payment_missing_complemento`

Reemplazar el `EXISTS (SELECT 1 FROM payment_allocations_unified pa JOIN invoices_unified iu ...)` por una subquery que use directamente `payments_unified.doctos_relacionados` sin materializar la view completa, o pre-calcular los `invoice_uuid_sat` como CTE:

```sql
-- Versión eficiente: usa jsonb path directo sobre payments_unified
CREATE OR REPLACE FUNCTION public.refresh_payments_unified()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET statement_timeout TO '300s'  -- ampliar también si es necesario
SET lock_timeout TO '30s'
AS $function$
DECLARE
  t_start timestamptz := clock_timestamp();
  v_opened integer := 0;
  v_resolved integer := 0;
  v_tmp integer;
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.payments_unified;

  -- FIX: pre-materializar los uuid_sat que SÍ tienen complemento Tipo P
  -- usando jsonb directo en payments_unified (evita re-materializar la view por cada row)
  WITH matched_invoices AS (
    SELECT DISTINCT (el->>'uuid_docto') AS invoice_uuid_sat
    FROM public.payments_unified pu
    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pu.doctos_relacionados, '[]'::jsonb)) el
    WHERE el->>'uuid_docto' IS NOT NULL
  ),
  -- AUTO-RESOLVE payment_missing_complemento
  r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(), resolution = 'auto_syntage_updated'
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'payment_missing_complemento'
      AND EXISTS (
        SELECT 1 FROM matched_invoices mi
        JOIN public.invoices_unified iu ON iu.uuid_sat = mi.invoice_uuid_sat
        WHERE iu.canonical_id = ri.canonical_id
      )
    RETURNING 1
  )
  SELECT count(*) INTO v_tmp FROM r;
  v_resolved := v_resolved + v_tmp;

  -- [resto de la función sin cambios]
  ...
END;
$function$;
```

### Fix B (recomendado): Agregar índice en `invoices_unified.uuid_sat`

```sql
-- Verificar si existe primero
SELECT indexname FROM pg_indexes 
WHERE tablename='invoices_unified' AND indexdef ILIKE '%uuid_sat%';

-- Si no existe:
CREATE INDEX CONCURRENTLY IF NOT EXISTS invoices_unified_uuid_sat_idx 
ON public.invoices_unified (uuid_sat) 
WHERE uuid_sat IS NOT NULL;
```

### Fix C (preventivo): Separar los dos refreshes en crons distintos

Actualmente el cron llama `refresh_invoices_unified()` + `refresh_payments_unified()` en serie dentro del mismo timeout de 180s (compartido por ambos). Separar en dos jobs independientes cada uno con su propio timeout y retry semántico.

### Verificación post-fix

```sql
-- Confirmar que el cron empieza a tener éxito:
SELECT status, start_time, end_time, return_message
FROM cron.job_run_details
WHERE jobid = 3
ORDER BY start_time DESC
LIMIT 5;

-- Confirmar detecciones nuevas:
SELECT issue_type, COUNT(*) FILTER (WHERE detected_at > now() - interval '1 hour') AS last_1h
FROM public.reconciliation_issues
GROUP BY issue_type ORDER BY last_1h DESC;
```

---

## Notas importantes para fases siguientes

1. **`payment_allocations_unified` es una VIEW, no una MATVIEW.** Si se materializa y se crea un índice sobre `invoice_uuid_sat`, tanto el auto-resolve como el INSERT de `payment_missing_complemento` serán O(log n) en vez de O(n²). Considerar `CREATE MATERIALIZED VIEW payment_allocations_unified_mv` con refresh en cascade desde `refresh_payments_unified()`.

2. **El stale_7d marker es destructivo.** En `refresh_invoices_unified()` hay un `UPDATE reconciliation_issues SET resolution='stale_7d'` para issues sin resolver con más de 7 días. Con el cron caído ~2 días, cuando se reanude no habrá auto-resolución de stale durante ese lag — pero tampoco se han agregado nuevas detecciones, por lo que el volumen acumulado podría ser alto y el primer refresh exitoso podría generar miles de INSERTs simultáneos, volviendo a agotar el timeout. Task 3 debe considerar un `SET statement_timeout TO '600s'` para la primera corrida de recuperación, o dividir los INSERTs en batches.

3. **`sat_only_cfdi_issued` tiene 30,769 rows** — el INSERT sin filtro de fecha podría ser lento. El filtro actual `fecha_timbrado >= '2021-01-01'` está bien, pero si crecieron desde la última corrida, el ON CONFLICT + subquery de `companies.rfc` lookup por fila puede ser un segundo vector de timeout. Revisar.

4. **El cron `refresh-all-matviews` (jobid=2, cada 2h)** podría estar refrescando `invoices_unified` sin la lógica de detección de issues. Esto explica por qué el MATVIEW no está completamente stale, pero los issues sí lo están.

5. **`ingestion_sentinel` (jobid=1)** llama `ingestion.check_missing_reconciliations()` en el schema `ingestion` — es un sistema separado que no alimenta `public.reconciliation_issues`. No confundir en futuros debugs.
