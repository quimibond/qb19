-- Fix A: rewrite del auto-resolve payment_missing_complemento en refresh_payments_unified().
-- Root cause (RCA): docs/superpowers/notes/2026-04-19-reconciliation-cron-rca.md
--
-- El bloque original hacía:
--   UPDATE reconciliation_issues ri ... WHERE EXISTS (
--     SELECT 1 FROM payment_allocations_unified pa
--     JOIN invoices_unified iu ON iu.canonical_id = ri.canonical_id
--     WHERE pa.invoice_uuid_sat = iu.uuid_sat
--   )
-- payment_allocations_unified es una VIEW que se re-materializa por cada fila de ri
-- (5,552 filas abiertas * costo full-scan de la view = O(n²) → timeout 180s).
--
-- Fix: pre-materializar el set de invoice_uuid_sat matcheados extrayendo uuid_docto
-- de payments_unified.doctos_relacionados (jsonb array) una sola vez en un CTE,
-- luego hacer el JOIN contra ese set. O(n) en lugar de O(n²).
--
-- Adicionalmente: statement_timeout ampliado de 180s a 600s para mayor resiliencia.
--
-- Cron jobid=3 (refresh-syntage-unified) pausado durante este DDL.

CREATE OR REPLACE FUNCTION public.refresh_payments_unified()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET statement_timeout TO '600s'
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
  -- FIX (2026-04-19, RCA: docs/superpowers/notes/2026-04-19-reconciliation-cron-rca.md):
  -- pre-materializar uuid_docto de todos los complementos antes del JOIN
  -- para evitar re-materializar payment_allocations_unified por cada row.
  WITH matched_invoices AS (
    SELECT DISTINCT (el->>'uuid_docto') AS invoice_uuid_sat
    FROM public.payments_unified pu
    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pu.doctos_relacionados, '[]'::jsonb)) el
    WHERE el->>'uuid_docto' IS NOT NULL
  ),
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
  ) SELECT count(*) INTO v_tmp FROM r;
  v_resolved := v_resolved + v_tmp;

  -- AUTO-RESOLVE complemento_missing_payment
  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(), resolution = 'auto_odoo_updated'
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'complemento_missing_payment'
      AND NOT EXISTS (
        SELECT 1 FROM public.payments_unified pu
        WHERE pu.canonical_payment_id = ri.canonical_id
          AND pu.match_status = 'syntage_only'
      )
    RETURNING 1
  ) SELECT count(*) INTO v_tmp FROM r;
  v_resolved := v_resolved + v_tmp;

  -- INSERT payment_missing_complemento · severity high · FILTRADO: PPD + paid + ≥30d
  WITH candidates AS (
    SELECT iu.canonical_id, iu.uuid_sat, iu.odoo_invoice_id, iu.odoo_company_id, iu.company_id,
           iu.odoo_ref, iu.receptor_nombre, iu.emisor_nombre,
           iu.odoo_amount_total, iu.due_date, iu.invoice_date, iu.emisor_rfc, iu.receptor_rfc
    FROM public.invoices_unified iu
    WHERE iu.payment_state = 'paid'
      AND iu.metodo_pago = 'PPD'
      AND iu.invoice_date < (now() - interval '30 days')::date
      AND iu.uuid_sat IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM public.payment_allocations_unified pa
        WHERE pa.invoice_uuid_sat = iu.uuid_sat
      )
  ), ins AS (
    INSERT INTO public.reconciliation_issues
      (issue_type, canonical_id, uuid_sat, odoo_invoice_id, odoo_company_id, company_id,
       description, severity, metadata)
    SELECT
      'payment_missing_complemento',
      c.canonical_id, c.uuid_sat, c.odoo_invoice_id, c.odoo_company_id, c.company_id,
      format('Factura %s marcada paid (PPD) hace >30d, sin complemento Tipo P en SAT',
             c.odoo_ref),
      'high',
      jsonb_build_object(
        'counterparty_rfc', COALESCE(c.emisor_rfc, c.receptor_rfc),
        'detected_via', 'uuid',
        'days_overdue', (CURRENT_DATE - c.invoice_date),
        'amount_due', c.odoo_amount_total
      )
    FROM candidates c
    ON CONFLICT (issue_type, canonical_id) WHERE resolved_at IS NULL DO NOTHING
    RETURNING 1
  ) SELECT count(*) INTO v_tmp FROM ins;
  v_opened := v_opened + v_tmp;

  -- INSERT complemento_missing_payment · severity high
  WITH ins AS (
    INSERT INTO public.reconciliation_issues
      (issue_type, canonical_id, uuid_sat, odoo_invoice_id, odoo_payment_id, odoo_company_id, company_id,
       description, severity, metadata)
    SELECT
      'complemento_missing_payment',
      pu.canonical_payment_id, pu.uuid_complemento, NULL, NULL, pu.odoo_company_id, pu.company_id,
      format('Complemento Tipo P %s ($%s) no tiene pago matcheable en Odoo',
             pu.uuid_complemento, pu.monto),
      'high',
      jsonb_build_object(
        'counterparty_rfc', COALESCE(pu.rfc_emisor_cta_ord, pu.rfc_emisor_cta_ben),
        'detected_via', 'num_operacion',
        'amount', pu.monto,
        'fecha_pago', pu.fecha_pago
      )
    FROM public.payments_unified pu
    WHERE pu.match_status = 'syntage_only'
    ON CONFLICT (issue_type, canonical_id) WHERE resolved_at IS NULL DO NOTHING
    RETURNING 1
  ) SELECT count(*) INTO v_tmp FROM ins;
  v_opened := v_opened + v_tmp;

  RETURN jsonb_build_object(
    'refreshed_at', now(),
    'payments_unified_rows', (SELECT count(*) FROM public.payments_unified),
    'issues_opened', v_opened,
    'issues_resolved', v_resolved,
    'duration_ms', (extract(milliseconds FROM clock_timestamp() - t_start))::int
  );
END;
$function$
;

-- Log this change
INSERT INTO public.schema_changes (change_type, table_name, description, sql_executed, triggered_by, success, created_at)
VALUES (
  'function_replace',
  'reconciliation_issues',
  'Fix A (phase-0): rewrite auto-resolve payment_missing_complemento — CTE materializada evita O(n²) en payment_allocations_unified. Timeout ampliado 180s→600s. RCA: docs/superpowers/notes/2026-04-19-reconciliation-cron-rca.md',
  'CREATE OR REPLACE FUNCTION public.refresh_payments_unified() ... (see migration 20260419120300)',
  'fase-0-contencion / Task 3',
  true,
  now()
);
