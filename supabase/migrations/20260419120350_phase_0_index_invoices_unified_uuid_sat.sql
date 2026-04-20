-- Fix B: índice para acelerar el JOIN iu.uuid_sat en refresh_payments_unified().
-- RCA: docs/superpowers/notes/2026-04-19-reconciliation-cron-rca.md
--
-- El INSERT de payment_missing_complemento hace:
--   NOT EXISTS (SELECT 1 FROM payment_allocations_unified pa WHERE pa.invoice_uuid_sat = iu.uuid_sat)
-- y el auto-resolve (post Fix A) hace:
--   JOIN invoices_unified iu ON iu.uuid_sat = mi.invoice_uuid_sat
-- Sin índice en invoices_unified.uuid_sat, ambos son seq-scans sobre ~96K rows.
-- Este índice parcial (WHERE uuid_sat IS NOT NULL) cubre los casos útiles.
--
-- Nota: CREATE INDEX CONCURRENTLY no puede correr dentro de una transacción.
-- Si el cliente lo envuelve automáticamente, se ejecuta sin CONCURRENTLY.
CREATE INDEX IF NOT EXISTS invoices_unified_uuid_sat_idx
  ON public.invoices_unified (uuid_sat)
  WHERE uuid_sat IS NOT NULL;
