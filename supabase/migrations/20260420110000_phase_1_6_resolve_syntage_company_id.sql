-- Phase 1.6 Fix A: Resolver syntage_invoices.company_id usando RFC match.
--
-- Diagnóstico pre-migración (A.1):
--   issued  / I / vigente:   7,756 null de 41,454 total
--   issued  / N / vigente:  28,399 null de 29,003 total
--   received/ I / vigente:  13,693 null de 37,569 total
--   received/ P / vigente:   1,989 null de  7,597 total
--   (otros tipos/estados también tienen nulls menores)
--
-- Regla de backfill:
-- - direction='issued' (PNT emitió CFDI): el receptor es el cliente.
--     → UPPER(companies.rfc) = UPPER(syntage_invoices.receptor_rfc)
-- - direction='received' (PNT recibió CFDI del proveedor): el emisor es el proveedor.
--     → UPPER(companies.rfc) = UPPER(syntage_invoices.emisor_rfc)
--
-- Los CFDIs que permanezcan NULL tras esta migración son:
--   - Contrapartes con RFC extranjero (XEXX010101000, etc.)
--   - Empresas que aún no existen en la tabla companies

BEGIN;

-- Backfill: direction='issued' — receptor es el cliente
UPDATE public.syntage_invoices si
SET company_id = c.id
FROM public.companies c
WHERE si.company_id IS NULL
  AND si.direction = 'issued'
  AND si.receptor_rfc IS NOT NULL
  AND UPPER(c.rfc) = UPPER(si.receptor_rfc);

-- Backfill: direction='received' — emisor es el proveedor
UPDATE public.syntage_invoices si
SET company_id = c.id
FROM public.companies c
WHERE si.company_id IS NULL
  AND si.direction = 'received'
  AND si.emisor_rfc IS NOT NULL
  AND UPPER(c.rfc) = UPPER(si.emisor_rfc);

-- Audit log — conteos post-backfill
INSERT INTO public.audit_runs (
  id, run_id, invariant_key, severity, source, model, details, run_at
)
SELECT
  gen_random_uuid(),
  gen_random_uuid(),
  'phase_1_6_resolve_syntage_company_id',
  'ok',
  'supabase',
  'migration',
  jsonb_build_object(
    'description', 'Backfill syntage_invoices.company_id via RFC match',
    'still_null_issued',
      (SELECT COUNT(*) FROM public.syntage_invoices
       WHERE company_id IS NULL AND direction = 'issued'),
    'still_null_received',
      (SELECT COUNT(*) FROM public.syntage_invoices
       WHERE company_id IS NULL AND direction = 'received'),
    'still_null_total',
      (SELECT COUNT(*) FROM public.syntage_invoices WHERE company_id IS NULL),
    'resolved_total',
      (SELECT COUNT(*) FROM public.syntage_invoices WHERE company_id IS NOT NULL)
  ),
  now();

COMMIT;
