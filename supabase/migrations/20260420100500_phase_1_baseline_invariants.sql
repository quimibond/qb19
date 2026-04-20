INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
SELECT
  gen_random_uuid(),
  'phase_1_baseline',
  'ok',
  'supabase',
  'baseline',
  jsonb_build_object(
    'reconciliation_issues_open_total', (SELECT COUNT(*) FROM public.reconciliation_issues WHERE resolved_at IS NULL),
    'reconciliation_issues_open_payment_missing_complemento', (SELECT COUNT(*) FROM public.reconciliation_issues WHERE resolved_at IS NULL AND issue_type='payment_missing_complemento'),
    'reconciliation_issues_open_complemento_missing_payment', (SELECT COUNT(*) FROM public.reconciliation_issues WHERE resolved_at IS NULL AND issue_type='complemento_missing_payment'),
    'reconciliation_issues_open_cancelled_but_posted', (SELECT COUNT(*) FROM public.reconciliation_issues WHERE resolved_at IS NULL AND issue_type='cancelled_but_posted'),
    'reconciliation_issues_open_amount_mismatch', (SELECT COUNT(*) FROM public.reconciliation_issues WHERE resolved_at IS NULL AND issue_type='amount_mismatch'),
    'syntage_gap_cfdis', (SELECT COUNT(*) FROM public.odoo_invoices WHERE cfdi_uuid IS NOT NULL AND cfdi_uuid NOT IN (SELECT uuid FROM public.syntage_invoices)),
    'odoo_invoices_with_cfdi', (SELECT COUNT(*) FROM public.odoo_invoices WHERE cfdi_uuid IS NOT NULL),
    'syntage_match_pct_odoo_to_syntage', (
      SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE cfdi_uuid IN (SELECT uuid FROM public.syntage_invoices)) / NULLIF(COUNT(*) FILTER (WHERE cfdi_uuid IS NOT NULL), 0), 2)
      FROM public.odoo_invoices
    ),
    'company_profile_columns', (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='company_profile'),
    'company_profile_kind', (SELECT CASE WHEN relkind='m' THEN 'matview' WHEN relkind='v' THEN 'view' ELSE relkind::text END FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname='public' AND c.relname='company_profile')
  ),
  now();
