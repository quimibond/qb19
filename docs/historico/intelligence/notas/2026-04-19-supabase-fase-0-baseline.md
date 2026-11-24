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

## Final (2026-04-19/20)

```json
{
  "odoo_crm_leads_rows": 20,
  "odoo_invoices_total": 23974,
  "invoices_unified_rows": 92737,
  "payments_unified_rows": 41251,
  "cfdi_uuid_groups_dupes": 0,
  "journal_flow_profile_rows": 16,
  "odoo_snapshots_max_created": "2026-04-20T04:08:05.678515+00:00",
  "reconciliation_issues_open": 44618,
  "cron_reconciliation_last_status": "succeeded",
  "odoo_invoices_archive_pre_dedup_rows": 5321,
  "reconciliation_issue_types_active_last_24h": ["complemento_missing_payment", "partner_blacklist_69b", "posted_but_sat_uncertified"]
}
```

## Delta baseline → final

| Métrica | Baseline | Final |
|---|---|---|
| cfdi_uuid_groups_dupes | 1,547 | 0 |
| odoo_invoices_total | 27,748 | 23,974 |
| reconciliation_types_last_24h | 2 | 3 |
| invoices_unified_rows | 96,511 | 92,737 |
| odoo_snapshots staleness | ~22h | ~6 min |
| journal_flow_profile_rows | 16 (no analizada en refresh) | 16 (incluida en refresh_all_matviews) |
| cron reconciliation (jobid=3) | failing 100% desde 2026-04-17 21:45 | succeeded |

**Fase 0 cerrada: 2026-04-19/20**

### Tareas skipped
- Task 6 (odoo_crm_leads): usuario no usa CRM en Odoo, skipped.

### Acciones pendientes por el usuario
- Push de `quimibond-intelligence/main` a Vercel para que la nueva `maxDuration=300` del snapshot cron tome efecto (Task 5).
