-- Phase 0 — Deduplicar odoo_invoices por cfdi_uuid.
-- 1,547 UUIDs con colisión / 3,774 filas extras.
-- Estrategia: archivar TODAS las filas con UUID duplicado; retener la de
-- write_date más reciente (desempate por id mayor); añadir UNIQUE INDEX parcial.

BEGIN;

-- 1) Crear tabla de archive con mismo schema que odoo_invoices
CREATE TABLE IF NOT EXISTS public.odoo_invoices_archive_pre_dedup (
  LIKE public.odoo_invoices INCLUDING ALL
);
-- Drop identity on id (copiada por LIKE INCLUDING ALL) — el archive no genera ids propios
ALTER TABLE public.odoo_invoices_archive_pre_dedup
  ALTER COLUMN id DROP IDENTITY IF EXISTS;
-- Añadir columnas de metadata de archivo
ALTER TABLE public.odoo_invoices_archive_pre_dedup
  ADD COLUMN IF NOT EXISTS archived_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS archive_reason text NOT NULL DEFAULT 'cfdi_uuid_dedup_phase_0';

-- 2) Drop UNIQUE constraints si existen en archive (copiadas por LIKE INCLUDING ALL)
-- para evitar conflicto al insertar las filas duplicadas
DO $$
DECLARE
  cn text;
BEGIN
  FOR cn IN
    SELECT conname FROM pg_constraint
    WHERE conrelid = 'public.odoo_invoices_archive_pre_dedup'::regclass
      AND contype = 'u'
  LOOP
    EXECUTE format('ALTER TABLE public.odoo_invoices_archive_pre_dedup DROP CONSTRAINT %I', cn);
  END LOOP;
END $$;

-- Drop indexes copiados que puedan causar conflicto de unicidad
DO $$
DECLARE
  idx text;
BEGIN
  FOR idx IN
    SELECT indexname FROM pg_indexes
    WHERE tablename = 'odoo_invoices_archive_pre_dedup'
      AND schemaname = 'public'
      AND indexname != 'odoo_invoices_archive_pre_dedup_pkey'
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS public.%I', idx);
  END LOOP;
END $$;

-- 3) Archivar TODAS las filas con cfdi_uuid duplicado (winners + losers)
INSERT INTO public.odoo_invoices_archive_pre_dedup
  (id, company_id, odoo_partner_id, name, move_type, amount_total, amount_residual,
   currency, invoice_date, due_date, payment_date, state, payment_state,
   days_overdue, days_to_pay, payment_status, ref, synced_at, amount_tax,
   amount_untaxed, amount_paid, payment_term, cfdi_uuid, cfdi_sat_state,
   cfdi_state, edi_state, amount_total_mxn, amount_untaxed_mxn,
   amount_residual_mxn, salesperson_name, salesperson_user_id, write_date,
   odoo_company_id, odoo_invoice_id,
   archived_at, archive_reason)
SELECT
  oi.id, oi.company_id, oi.odoo_partner_id, oi.name, oi.move_type,
  oi.amount_total, oi.amount_residual, oi.currency, oi.invoice_date,
  oi.due_date, oi.payment_date, oi.state, oi.payment_state,
  oi.days_overdue, oi.days_to_pay, oi.payment_status, oi.ref, oi.synced_at,
  oi.amount_tax, oi.amount_untaxed, oi.amount_paid, oi.payment_term,
  oi.cfdi_uuid, oi.cfdi_sat_state, oi.cfdi_state, oi.edi_state,
  oi.amount_total_mxn, oi.amount_untaxed_mxn, oi.amount_residual_mxn,
  oi.salesperson_name, oi.salesperson_user_id, oi.write_date,
  oi.odoo_company_id, oi.odoo_invoice_id,
  now(), 'cfdi_uuid_dedup_phase_0'
FROM public.odoo_invoices oi
WHERE oi.cfdi_uuid IN (
  SELECT cfdi_uuid FROM public.odoo_invoices
  WHERE cfdi_uuid IS NOT NULL
  GROUP BY cfdi_uuid HAVING COUNT(*) > 1
);

-- 4) Verificación: archive count ~= dup_groups + extra_rows (~5,321)
DO $$
DECLARE
  archive_count int;
  expected_min int := 4500;
  expected_max int := 6200;
BEGIN
  SELECT COUNT(*) INTO archive_count FROM public.odoo_invoices_archive_pre_dedup
  WHERE archive_reason = 'cfdi_uuid_dedup_phase_0';
  IF archive_count < expected_min OR archive_count > expected_max THEN
    RAISE EXCEPTION 'archive_count fuera de rango [%, %]: % — abortando', expected_min, expected_max, archive_count;
  END IF;
END $$;

-- 5) DELETE de losers (todas las filas duplicadas EXCEPTO la más reciente)
WITH ranked AS (
  SELECT id,
    ROW_NUMBER() OVER (
      PARTITION BY cfdi_uuid
      ORDER BY write_date DESC NULLS LAST, id DESC
    ) AS rn
  FROM public.odoo_invoices
  WHERE cfdi_uuid IS NOT NULL
    AND cfdi_uuid IN (
      SELECT cfdi_uuid FROM public.odoo_invoices
      WHERE cfdi_uuid IS NOT NULL
      GROUP BY cfdi_uuid HAVING COUNT(*) > 1
    )
)
DELETE FROM public.odoo_invoices
WHERE id IN (SELECT id FROM ranked WHERE rn > 1);

-- 6) Verificación: 0 duplicados
DO $$
DECLARE dup_count int;
BEGIN
  SELECT COUNT(*) INTO dup_count FROM (
    SELECT cfdi_uuid FROM public.odoo_invoices
    WHERE cfdi_uuid IS NOT NULL
    GROUP BY cfdi_uuid HAVING COUNT(*)>1
  ) x;
  IF dup_count > 0 THEN
    RAISE EXCEPTION 'dedup incompleto, quedan % grupos — abortando', dup_count;
  END IF;
END $$;

-- 7) UNIQUE INDEX parcial (idempotente) — previene recurrencia
CREATE UNIQUE INDEX IF NOT EXISTS odoo_invoices_cfdi_uuid_unique
  ON public.odoo_invoices (cfdi_uuid)
  WHERE cfdi_uuid IS NOT NULL;

-- 8) Audit log
INSERT INTO public.audit_runs (run_id, invariant_key, severity, source, model, details, run_at)
VALUES (
  gen_random_uuid(),
  'phase_0_dedup_cfdi_uuid',
  'ok',
  'supabase',
  'migration',
  jsonb_build_object(
    'action', 'dedup_odoo_invoices_cfdi_uuid',
    'archived_rows', (SELECT COUNT(*) FROM public.odoo_invoices_archive_pre_dedup WHERE archive_reason = 'cfdi_uuid_dedup_phase_0'),
    'remaining_rows', (SELECT COUNT(*) FROM public.odoo_invoices),
    'migration', '20260419120000_phase_0_dedup_cfdi_uuid.sql'
  ),
  now()
);

COMMIT;
