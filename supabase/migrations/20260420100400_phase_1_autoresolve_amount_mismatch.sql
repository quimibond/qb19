-- Phase 1: auto-resolve amount_mismatch cuando los totales reconcilian.
-- syntage_invoices.total confirmed (numeric). Match via ri.uuid_sat = si.uuid.

CREATE OR REPLACE FUNCTION public.resolve_amount_mismatch_for_invoice(p_odoo_invoice_id bigint)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE v_resolved integer := 0;
BEGIN
  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(),
        resolution = 'auto_amount_reconciled (totals converge within $0.01)'
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'amount_mismatch'
      AND ri.odoo_invoice_id = p_odoo_invoice_id
      AND EXISTS (
        SELECT 1 FROM public.odoo_invoices oi
        JOIN public.syntage_invoices si ON si.uuid = oi.cfdi_uuid
        WHERE oi.id = p_odoo_invoice_id
          AND ABS(COALESCE(oi.amount_total, 0) - COALESCE(si.total, 0)) < 0.01
      )
    RETURNING 1
  )
  SELECT count(*) INTO v_resolved FROM r;
  RETURN v_resolved;
END;
$$;

CREATE OR REPLACE FUNCTION public.trg_autoresolve_amount_mismatch_odoo()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.amount_total IS DISTINCT FROM COALESCE(OLD.amount_total, -1) THEN
    PERFORM public.resolve_amount_mismatch_for_invoice(NEW.id);
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.trg_autoresolve_amount_mismatch_syntage()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_oi_id bigint;
BEGIN
  IF NEW.total IS DISTINCT FROM COALESCE(OLD.total, -1) THEN
    SELECT id INTO v_oi_id FROM public.odoo_invoices WHERE cfdi_uuid = NEW.uuid LIMIT 1;
    IF v_oi_id IS NOT NULL THEN
      PERFORM public.resolve_amount_mismatch_for_invoice(v_oi_id);
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_autoresolve_amount_mismatch_odoo_update ON public.odoo_invoices;
CREATE TRIGGER trg_autoresolve_amount_mismatch_odoo_update
AFTER UPDATE OF amount_total ON public.odoo_invoices
FOR EACH ROW EXECUTE FUNCTION public.trg_autoresolve_amount_mismatch_odoo();

DROP TRIGGER IF EXISTS trg_autoresolve_amount_mismatch_syntage_update ON public.syntage_invoices;
CREATE TRIGGER trg_autoresolve_amount_mismatch_syntage_update
AFTER UPDATE OF total ON public.syntage_invoices
FOR EACH ROW EXECUTE FUNCTION public.trg_autoresolve_amount_mismatch_syntage();

-- Backfill
DO $$
DECLARE v_count int;
BEGIN
  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(),
        resolution = 'auto_amount_reconciled (backfill phase_1)'
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'amount_mismatch'
      AND EXISTS (
        SELECT 1 FROM public.odoo_invoices oi
        JOIN public.syntage_invoices si ON si.uuid = oi.cfdi_uuid
        WHERE oi.id = ri.odoo_invoice_id
          AND ABS(COALESCE(oi.amount_total, 0) - COALESCE(si.total, 0)) < 0.01
      )
    RETURNING 1
  )
  SELECT count(*) INTO v_count FROM r;

  INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
  VALUES (gen_random_uuid(), 'phase_1_autoresolve_amount_mismatch_backfill', 'ok', 'supabase', 'migration',
    jsonb_build_object('resolved_count', v_count), now());
END $$;
