-- Phase 1: auto-resolve payment_missing_complemento via trigger en syntage_invoice_payments.
-- SCHEMA NOTE: syntage_invoice_payments PK is syntage_id text (not id bigint).

CREATE OR REPLACE FUNCTION public.resolve_payment_missing_complemento_for_syntage_payment(
  p_syntage_id text
) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v_resolved integer := 0;
  v_docto_uuids text[];
BEGIN
  SELECT array_agg(DISTINCT el->>'uuid_docto')
  INTO v_docto_uuids
  FROM public.syntage_invoice_payments sip,
       LATERAL jsonb_array_elements(COALESCE(sip.doctos_relacionados, '[]'::jsonb)) el
  WHERE sip.syntage_id = p_syntage_id
    AND el->>'uuid_docto' IS NOT NULL;

  IF v_docto_uuids IS NULL OR cardinality(v_docto_uuids) = 0 THEN
    RETURN 0;
  END IF;

  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(),
        resolution = format('auto_syntage_complemento_received (syntage_id=%s)', p_syntage_id)
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'payment_missing_complemento'
      AND EXISTS (
        SELECT 1 FROM public.odoo_invoices oi
        WHERE oi.id = ri.odoo_invoice_id
          AND oi.cfdi_uuid = ANY (v_docto_uuids)
      )
    RETURNING 1
  )
  SELECT count(*) INTO v_resolved FROM r;

  RETURN v_resolved;
END;
$$;

CREATE OR REPLACE FUNCTION public.trg_autoresolve_payment_missing_complemento()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM public.resolve_payment_missing_complemento_for_syntage_payment(NEW.syntage_id);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_autoresolve_payment_missing_complemento_insert ON public.syntage_invoice_payments;
CREATE TRIGGER trg_autoresolve_payment_missing_complemento_insert
AFTER INSERT ON public.syntage_invoice_payments
FOR EACH ROW EXECUTE FUNCTION public.trg_autoresolve_payment_missing_complemento();

DROP TRIGGER IF EXISTS trg_autoresolve_payment_missing_complemento_update ON public.syntage_invoice_payments;
CREATE TRIGGER trg_autoresolve_payment_missing_complemento_update
AFTER UPDATE OF doctos_relacionados ON public.syntage_invoice_payments
FOR EACH ROW EXECUTE FUNCTION public.trg_autoresolve_payment_missing_complemento();

-- Backfill: iterate per payment row
DO $$
DECLARE
  total int := 0;
  this_run int;
  p record;
BEGIN
  FOR p IN SELECT syntage_id FROM public.syntage_invoice_payments
           WHERE doctos_relacionados IS NOT NULL
             AND jsonb_array_length(doctos_relacionados) > 0
  LOOP
    SELECT public.resolve_payment_missing_complemento_for_syntage_payment(p.syntage_id) INTO this_run;
    total := total + COALESCE(this_run, 0);
  END LOOP;

  INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
  VALUES (
    gen_random_uuid(),
    'phase_1_autoresolve_payment_missing_complemento_backfill',
    'ok',
    'supabase',
    'migration',
    jsonb_build_object('resolved_count', total, 'migration', '20260420100100'),
    now()
  );
END $$;
