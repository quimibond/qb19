# Audit Invariants — Catálogo canónico

Spec: `docs/superpowers/specs/2026-04-19-sync-audit-design.md`

Cada invariante se identifica con `invariant_key` único y aparece como
filas en `audit_runs` con `source='odoo'` (cross-check) o `source='supabase'`
(interno SQL).

## Convenciones

- `severity = 'ok'`: diff dentro de tolerancia.
- `severity = 'warn'`: diff >10× tolerancia abs pero no crítico.
- `severity = 'error'`: diff grande; requiere investigación.
- `bucket_key`: agrupador (ej. `2026-04|sale|1`); `NULL` = snapshot.

## Invariantes cross-check (Odoo ↔ Supabase)

### `products.count_active`
**Mide:** count de productos activos.
**Violación:** conteos difieren.
**Acción:** revisar `_push_products` filtro de `active`.

### `products.count_with_default_code`
**Mide:** productos activos con `internal_ref`.
**Violación:** el mapeo de `default_code → internal_ref` pierde filas.

### `products.sum_standard_price`
**Mide:** suma simple de `standard_price`.
**Violación:** divergencia de valuación a nivel catálogo.

### `products.null_uom_count`
**Mide:** productos sin UoM.
**Violación:** falla upstream en Odoo o pérdida en push.

### `invoice_lines.count_per_bucket` / `.sum_subtotal_signed_mxn` / `.sum_qty_signed`
**Mide:** por (mes, move_type, company) — count, suma MXN firmada, suma qty firmada.
**Violación:** un bucket con diff indica push incompleto, FX mal aplicado, o signo roto en refunds.

### `order_lines.*` (análogo, sale + purchase separados)

### `deliveries.count_done_per_month`
**Mide:** stock.picking `state in ('done','cancel')` por mes/company.

### `manufacturing.count_per_state`, `.sum_qty_produced`

### `account_balances.inventory_accounts_balance` (1150.*)
`.cogs_accounts_balance` (5*)
`.revenue_accounts_balance` (4*)
**Mide:** balance agregado de grupo de cuentas por período/company.

### `bank_balances.count_per_journal`, `.native_balance_per_journal`

## Invariantes SQL internos (Supabase only)

### `invoice_lines.reversal_sign`
**Mide:** refunds con signo inconsistente entre quantity y price_subtotal.
**Acción:** bug en `_push_invoice_lines` → inspeccionar signo que emite.

### `invoice_lines.price_recompute`
**Mide:** `|price_unit × qty × (1 − discount) − price_subtotal| > 0.01`.
**Acción:** revisar cómo se calcula subtotal (con/sin descuento, con/sin impuesto).

### `invoice_lines.fx_present`
**Mide:** líneas en moneda ≠ MXN con FX faltante.
**Acción:** `_push_invoice_lines` no convirtió para esa moneda.

### `invoice_lines.fx_sanity`
**Mide:** consistencia `price × rate ≈ price_mxn` (1% tolerancia).
**Acción:** FX mal capturado en el momento del push.

### `order_lines.orphan_product`, `.orphan_order_sale`, `.orphan_order_purchase`
**Mide:** líneas con FK roto a producto/header.

### `products.null_standard_price_active` (warn)
**Mide:** productos activos con precio 0 o NULL.
**Acción:** puede ser legítimo; investigar si alimenta CMV.

### `products.null_uom` (error)
**Mide:** productos activos sin unidad de medida.

### `products.duplicate_default_code`
**Mide:** `internal_ref` duplicado entre productos activos.
**Acción:** identificar y limpiar duplicados en Odoo.

### `account_balances.trial_balance_zero_per_period`
**Mide:** suma de balances por período debe ser ~0.
**Violación:** asiento roto o pull parcial.

### `account_balances.orphan_account`
**Mide:** balances con código de cuenta que no existe en CoA.

### `invoice_lines.company_leak`, `order_lines.company_leak`
**Mide:** línea con `odoo_company_id` distinto al del header.

### `deliveries.orphan_partner`
**Mide:** delivery con partner_id que no existe en `contacts`.

### `deliveries.done_without_date`
**Mide:** state='done' sin date_done.

## Tolerancias

Configurables vía tabla `audit_tolerances`:
```sql
UPDATE audit_tolerances
SET pct_tolerance = 0.01
WHERE invariant_key = 'invoice_lines.sum_subtotal_signed_mxn';
```

## Correr una auditoría

Desde shell Odoo.sh:
```python
env['quimibond.sync.audit'].run_all(
    date_from='2025-04-01', date_to='2026-04-19',
)
```

Ver resultados:
```sql
SELECT invariant_key, bucket_key, severity, diff, details
FROM audit_runs
WHERE run_id = '<uuid>'
  AND severity <> 'ok'
ORDER BY abs(diff) DESC NULLS LAST;
```
