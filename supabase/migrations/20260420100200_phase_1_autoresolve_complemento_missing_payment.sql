-- Phase 1: auto-resolve complemento_missing_payment via trigger en odoo_payment_invoice_links.
-- SCHEMA NOTE: odoo_payment_invoice_links uses odoo_invoice_id (not invoice_id).

CREATE OR REPLACE FUNCTION public.resolve_complemento_missing_payment_for_link(
  p_invoice_id bigint
) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE v_resolved integer := 0;
BEGIN
  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(),
        resolution = format('auto_odoo_payment_linked (invoice_id=%s)', p_invoice_id)
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'complemento_missing_payment'
      AND ri.odoo_invoice_id = p_invoice_id
    RETURNING 1
  )
  SELECT count(*) INTO v_resolved FROM r;
  RETURN v_resolved;
END;
$$;

CREATE OR REPLACE FUNCTION public.trg_autoresolve_complemento_missing_payment()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM public.resolve_complemento_missing_payment_for_link(NEW.odoo_invoice_id);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_autoresolve_complemento_missing_payment_insert ON public.odoo_payment_invoice_links;
CREATE TRIGGER trg_autoresolve_complemento_missing_payment_insert
AFTER INSERT ON public.odoo_payment_invoice_links
FOR EACH ROW EXECUTE FUNCTION public.trg_autoresolve_complemento_missing_payment();

-- Backfill
DO $$
DECLARE v_count int;
BEGIN
  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(),
        resolution = 'auto_odoo_payment_linked (backfill phase_1)'
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'complemento_missing_payment'
      AND EXISTS (
        SELECT 1 FROM public.odoo_payment_invoice_links opl
        WHERE opl.odoo_invoice_id = ri.odoo_invoice_id
      )
    RETURNING 1
  )
  SELECT count(*) INTO v_count FROM r;

  INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
  VALUES (gen_random_uuid(), 'phase_1_autoresolve_complemento_missing_payment_backfill', 'ok', 'supabase', 'migration',
    jsonb_build_object('resolved_count', v_count), now());
END $$;
