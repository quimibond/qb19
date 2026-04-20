# Fase 1 — Baseline (2026-04-20)

```json
{
  "reconciliation_issues_open_total": 44660,
  "reconciliation_issues_open_payment_missing_complemento": 730,
  "reconciliation_issues_open_complemento_missing_payment": 22754,
  "reconciliation_issues_open_cancelled_but_posted": 97,
  "reconciliation_issues_open_amount_mismatch": 21,
  "syntage_gap_cfdis": 3452,
  "odoo_invoices_with_cfdi": 15519,
  "syntage_match_pct_odoo_to_syntage": 77.76,
  "company_profile_columns": 0,
  "company_profile_kind": "matview"
}
```

## Archivos frontend a migrar (Step 0.4.A)

| Tabla raw | Archivo | Línea |
|---|---|---|
| odoo_invoices | src/__tests__/layer3/parity-fase5.test.ts | 15, 49 |
| odoo_invoices | src/app/api/agents/auto-fix/route.ts | 276 |
| odoo_invoices | src/app/api/syntage/health/route.ts | 138 |
| odoo_invoices | src/app/api/pipeline/health-scores/route.ts | 40 |
| odoo_sale_orders | src/app/api/pipeline/health-scores/route.ts | 44 |
| odoo_deliveries | src/app/api/pipeline/health-scores/route.ts | 48 |
| odoo_account_payments | src/app/api/pipeline/health-scores/route.ts | 57 |
| odoo_invoices | src/lib/queries/fiscal/syntage-health.ts | 124 |

## Ocurrencias USE_UNIFIED_LAYER (Step 0.4.B)

Archivos con flag activo en src/ (excluye docs/):
- `src/lib/queries/analytics/finance.ts` — líneas 9, 12, 14, 118, 132, 138
- `src/lib/queries/operational/purchases.ts` — líneas 12, 13, 646, 664, 667, 672
- `src/lib/queries/_shared/companies.ts` — líneas 7, 550, 636
- `src/lib/queries/unified/invoices.ts` — líneas 17, 25, 124, 289, 506, 555

## Helpers legacy* a borrar (Step 0.4.B)

| Función | Archivo |
|---|---|
| `legacyGetArZombies` | src/lib/queries/analytics/finance.ts:96 |
| `legacyGetSupplierInvoices` | src/lib/queries/operational/purchases.ts:648 |
| `legacyGetArAging` | src/lib/queries/unified/invoices.ts:52 |
| `legacyGetOverdueInvoices` | src/lib/queries/unified/invoices.ts:199 |
| `legacyGetOverdueInvoicesPage` | src/lib/queries/unified/invoices.ts:329 |
| `legacyGetOverdueSalespeopleOptions` | src/lib/queries/unified/invoices.ts:516 |
| `legacyGetCompanyInvoicesPage` | src/lib/queries/_shared/companies.ts:500 |
| `legacyGetCompanyInvoices` | src/lib/queries/_shared/companies.ts:615 |

## Expectativas post-fase

- reconciliation_issues_open_payment_missing_complemento: 730 → <73 (<10% baseline)
- reconciliation_issues_open_complemento_missing_payment: 22754 → <2276 (<10% baseline)
- reconciliation_issues_open_cancelled_but_posted: 97 → 0
- reconciliation_issues_open_amount_mismatch: 21 → 0
- syntage_gap_cfdis: 3452 → <173 (<5% baseline)
- company_profile columnas: 0 → +6 (revenue_ytd, sale_orders_ytd, purchases_ytd, purchase_orders_ytd, ar_aging_buckets, sat_compliance_score, last_activity_at, invoices_with_cfdi, invoices_with_syntage_match, sat_open_issues, otd_rate_90d)
- 0 supabase.from("odoo_invoices"|"odoo_payments"|"odoo_account_payments") fuera de /admin/debug

## Notas de ejecución

- INSERT original via SELECT subquery causó timeout en Supabase (subquery `NOT IN (SELECT uuid FROM syntage_invoices)` sobre 15k+ rows sin índice cross-table).
- Se resolvió calculando valores en queries separadas y haciendo VALUES literal.
- Valores de `syntage_gap_cfdis` y `syntage_match_pct` confirmados con NOT EXISTS (más eficiente).
- `company_profile_columns = 0`: la matview existe pero `information_schema.columns` no reporta columnas para matviews — comportamiento esperado de Postgres (matviews no aparecen en information_schema.columns).
