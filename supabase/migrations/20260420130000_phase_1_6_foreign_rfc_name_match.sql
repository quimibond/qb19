-- Phase 1.6 fix: clientes/proveedores extranjeros con RFC genérico XEXX/XAXX.
--
-- Bug detectado 2026-04-20: la primera Fix A del Fase 1.6 hizo match por RFC.
-- Eso asignó incorrectamente a companies con el MISMO RFC genérico (todas las
-- empresas extranjeras comparten XEXX010101000, todos los "mostrador" comparten
-- XAXX010101000). Resultado: 363 CFDIs de SHAWMUT LLC ($111M) se asignaron a
-- HANGZHOU FENG HAI ELECTRONIC porque ambos tienen XEXX010101000.
--
-- Fix:
-- 1) Revertir todos los matches previos para receptor_rfc/emisor_rfc genéricos
-- 2) Re-match por nombre normalizado (ignora espacios, comas, puntos, case)
--
-- Safe idempotent: corre múltiples veces sin efecto adverso.

BEGIN;

-- Paso 1: revertir matches por RFC genérico (son ambiguos)
UPDATE public.syntage_invoices si
SET company_id = NULL
WHERE company_id IS NOT NULL
  AND (
    (si.direction='issued' AND si.receptor_rfc IN ('XEXX010101000','XAXX010101000'))
    OR (si.direction='received' AND si.emisor_rfc IN ('XEXX010101000','XAXX010101000'))
  );

-- Paso 2: match por nombre normalizado
WITH normalized_si AS (
  SELECT si.syntage_id, si.direction,
         LOWER(REGEXP_REPLACE(
           REGEXP_REPLACE(TRIM(
             CASE WHEN si.direction='issued' THEN si.receptor_nombre ELSE si.emisor_nombre END
           ), '[.,;:]+', '', 'g'),
           '\s+', ' ', 'g'
         )) AS norm_name
  FROM public.syntage_invoices si
  WHERE si.company_id IS NULL
    AND (
      (si.direction='issued' AND si.receptor_rfc IN ('XEXX010101000','XAXX010101000') AND si.receptor_nombre IS NOT NULL)
      OR (si.direction='received' AND si.emisor_rfc IN ('XEXX010101000','XAXX010101000') AND si.emisor_nombre IS NOT NULL)
    )
),
normalized_c AS (
  SELECT c.id,
         LOWER(REGEXP_REPLACE(
           REGEXP_REPLACE(TRIM(c.name), '[.,;:]+', '', 'g'),
           '\s+', ' ', 'g'
         )) AS norm_name
  FROM public.companies c
  WHERE c.is_customer = true OR c.is_supplier = true
),
best_match AS (
  SELECT DISTINCT ON (nsi.syntage_id)
    nsi.syntage_id, nc.id AS company_id
  FROM normalized_si nsi
  JOIN normalized_c nc ON nc.norm_name = nsi.norm_name
  ORDER BY nsi.syntage_id, nc.id
)
UPDATE public.syntage_invoices si
SET company_id = bm.company_id
FROM best_match bm
WHERE si.syntage_id = bm.syntage_id;

-- Audit log
INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
VALUES (
  gen_random_uuid(),
  'phase_1_6_foreign_rfc_name_match',
  'ok',
  'supabase',
  'migration',
  jsonb_build_object(
    'fix', 'foreign clients matched by normalized name (XEXX/XAXX have shared RFCs)',
    'remaining_null_xexx_xaxx', (
      SELECT COUNT(*) FROM public.syntage_invoices
      WHERE company_id IS NULL
        AND (receptor_rfc IN ('XEXX010101000','XAXX010101000')
          OR emisor_rfc IN ('XEXX010101000','XAXX010101000'))
    )
  ),
  now()
);

COMMIT;

REFRESH MATERIALIZED VIEW public.company_profile_sat;
