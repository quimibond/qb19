-- Phase 0 baseline: captura métricas clave ANTES de ejecutar fixes.
-- Se re-ejecuta al final de la fase para validar.
-- Schema real de audit_runs: invariant_key, details, severity in (ok|warn|error),
-- source in (odoo|supabase), model NOT NULL, run_id uuid NOT NULL.

INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
SELECT
  gen_random_uuid(),
  'phase_0_baseline',
  'ok',
  'supabase',
  'baseline',
  jsonb_build_object(
    'odoo_invoices_total', (SELECT COUNT(*) FROM public.odoo_invoices),
    'cfdi_uuid_groups_dupes', (
      SELECT COUNT(*) FROM (
        SELECT cfdi_uuid FROM public.odoo_invoices
        WHERE cfdi_uuid IS NOT NULL
        GROUP BY cfdi_uuid HAVING COUNT(*) > 1
      ) x
    ),
    'cfdi_uuid_extra_rows', (
      SELECT COALESCE(SUM(n - 1), 0) FROM (
        SELECT COUNT(*) AS n FROM public.odoo_invoices
        WHERE cfdi_uuid IS NOT NULL
        GROUP BY cfdi_uuid HAVING COUNT(*) > 1
      ) x
    ),
    'reconciliation_issues_open', (
      SELECT COUNT(*) FROM public.reconciliation_issues WHERE resolved_at IS NULL
    ),
    'reconciliation_issue_types_active_last_24h', (
      SELECT jsonb_agg(DISTINCT issue_type)
      FROM public.reconciliation_issues
      WHERE detected_at > now() - interval '24 hours'
    ),
    'invoices_unified_rows', (SELECT COUNT(*) FROM public.invoices_unified),
    'payments_unified_rows', (SELECT COUNT(*) FROM public.payments_unified),
    'odoo_snapshots_max_created', (SELECT MAX(created_at) FROM public.odoo_snapshots),
    'odoo_crm_leads_rows', (SELECT COUNT(*) FROM public.odoo_crm_leads),
    'journal_flow_profile_rows', (SELECT COUNT(*) FROM public.journal_flow_profile)
  ),
  now();
