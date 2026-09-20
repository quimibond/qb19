-- Phase 1 final invariant snapshot (2026-04-20)
-- Values pre-computed individually to avoid cross-table NOT IN timeout on syntage_invoices.
-- syntage_match_pct computed via NOT EXISTS correlated subquery (more efficient).
INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
VALUES (
  gen_random_uuid(),
  'phase_1_final',
  'ok',
  'supabase',
  'final',
  '{"reconciliation_issues_open_total":44660,"reconciliation_issues_open_payment_missing_complemento":730,"reconciliation_issues_open_complemento_missing_payment":22754,"reconciliation_issues_open_cancelled_but_posted":97,"reconciliation_issues_open_amount_mismatch":21,"syntage_match_pct_odoo_to_syntage":77.82,"company_profile_column_count":40,"autoresolve_triggers_count":7}'::jsonb,
  now()
);
