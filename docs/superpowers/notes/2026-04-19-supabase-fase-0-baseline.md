# Fase 0 — Baseline (2026-04-19)

```json
{
  "odoo_crm_leads_rows": 20,
  "odoo_invoices_total": 27748,
  "cfdi_uuid_extra_rows": 3774,
  "invoices_unified_rows": 96511,
  "payments_unified_rows": 41257,
  "cfdi_uuid_groups_dupes": 1547,
  "journal_flow_profile_rows": 16,
  "odoo_snapshots_max_created": "2026-04-19T05:30:55.316753+00:00",
  "reconciliation_issues_open": 27643,
  "reconciliation_issue_types_active_last_24h": ["partner_blacklist_69b", "posted_but_sat_uncertified"]
}
```

## Expectativas post-fase (según spec)

- cfdi_uuid_groups_dupes: 1547 → 0
- cfdi_uuid_extra_rows: 3774 → 0
- invoices_unified_rows: ~96000 → ~153700 (odoo post-dedup + syntage)
- odoo_snapshots_max_created: 2026-04-19 05:30 → última hora
- reconciliation_issue_types_active_last_24h: ["posted_but_sat_uncertified", "partner_blacklist_69b"] → los 8 tipos

## Schema real de audit_runs (descubierto 2026-04-19)

- `invariant_key` (no `invariant`)
- `details` jsonb (no `detail`)
- `severity` CHECK IN ('ok','warn','error') — no 'info'
- `source` CHECK IN ('odoo','supabase')
- `model` text NOT NULL (sin default) — uso 'baseline'
- `run_id` uuid NOT NULL (sin default) — uso gen_random_uuid()

Este schema debe usarse en todos los INSERT a audit_runs del resto del plan (Task 8 en particular).
