-- Phase 1: auto-resolve cancelled_but_posted via triggers syntage+odoo.
-- SCHEMA NOTE: syntage_invoices uses estado_sat (not estatus).
-- Match issues via ri.uuid_sat = si.uuid (cfdi_uuid often null in odoo_invoices).

CREATE OR REPLACE FUNCTION public.resolve_cancelled_but_posted(
  p_cfdi_uuid text, p_odoo_invoice_id bigint, p_reason text
) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE v_resolved integer := 0;
BEGIN
  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(),
        resolution = format('auto_cancellation_reconciled: %s', p_reason)
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'cancelled_but_posted'
      AND (
        (p_cfdi_uuid IS NOT NULL AND ri.uuid_sat = p_cfdi_uuid)
        OR (p_odoo_invoice_id IS NOT NULL AND ri.odoo_invoice_id = p_odoo_invoice_id)
      )
    RETURNING 1
  )
  SELECT count(*) INTO v_resolved FROM r;
  RETURN v_resolved;
END;
$$;

-- Trigger on syntage_invoices: resolve when estado_sat becomes vigente (cancellation reversed)
CREATE OR REPLACE FUNCTION public.trg_autoresolve_cancelled_syntage()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.estado_sat = 'vigente' AND COALESCE(OLD.estado_sat, '') <> 'vigente' THEN
    PERFORM public.resolve_cancelled_but_posted(NEW.uuid, NULL,
      format('syntage_invoices.estado_sat changed to vigente (was %s)', COALESCE(OLD.estado_sat, 'NULL')));
  END IF;
  RETURN NEW;
END;
$$;

-- Trigger on odoo_invoices: resolve when state becomes cancel (Odoo caught up)
CREATE OR REPLACE FUNCTION public.trg_autoresolve_cancelled_odoo()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state = 'cancel' AND COALESCE(OLD.state, '') <> 'cancel' THEN
    PERFORM public.resolve_cancelled_but_posted(NEW.cfdi_uuid, NEW.id,
      format('odoo_invoices.state changed to cancel (was %s)', COALESCE(OLD.state, 'NULL')));
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_autoresolve_cancelled_syntage_update ON public.syntage_invoices;
CREATE TRIGGER trg_autoresolve_cancelled_syntage_update
AFTER UPDATE OF estado_sat ON public.syntage_invoices
FOR EACH ROW EXECUTE FUNCTION public.trg_autoresolve_cancelled_syntage();

DROP TRIGGER IF EXISTS trg_autoresolve_cancelled_odoo_update ON public.odoo_invoices;
CREATE TRIGGER trg_autoresolve_cancelled_odoo_update
AFTER UPDATE OF state ON public.odoo_invoices
FOR EACH ROW EXECUTE FUNCTION public.trg_autoresolve_cancelled_odoo();

-- Backfill: resolve if odoo.state='cancel' OR syntage.estado_sat='vigente' (matched via uuid_sat)
DO $$
DECLARE v_count int;
BEGIN
  WITH r AS (
    UPDATE public.reconciliation_issues ri
    SET resolved_at = now(),
        resolution = 'auto_cancellation_reconciled (backfill phase_1)'
    WHERE ri.resolved_at IS NULL
      AND ri.issue_type = 'cancelled_but_posted'
      AND (
        EXISTS (
          SELECT 1 FROM public.odoo_invoices oi
          WHERE oi.id = ri.odoo_invoice_id AND oi.state = 'cancel'
        )
        OR EXISTS (
          SELECT 1 FROM public.syntage_invoices si
          WHERE si.uuid = ri.uuid_sat AND si.estado_sat = 'vigente'
        )
      )
    RETURNING 1
  )
  SELECT count(*) INTO v_count FROM r;

  INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
  VALUES (gen_random_uuid(), 'phase_1_autoresolve_cancelled_but_posted_backfill', 'ok', 'supabase', 'migration',
    jsonb_build_object('resolved_count', v_count), now());
END $$;
