-- Phase 1.6 fix: company_profile_sat debe exponer subtotal (sin IVA en MXN)
-- además de total (con IVA). Syntage's "totalReceived" en el CSV insights es
-- subtotal × tipo_cambio, NO total. Cuadra al centavo para CONTITECH:
--   - total (con IVA): $569M
--   - subtotal × tipo_cambio (sin IVA MXN): $490.7M ← CSV totalReceived
--   - subtotal raw native: $264.5M (mix USD+MXN sin convertir, incorrecto)

DROP MATERIALIZED VIEW IF EXISTS public.company_profile_sat CASCADE;

CREATE MATERIALIZED VIEW public.company_profile_sat AS
SELECT
  c.id AS company_id,
  c.name,
  c.rfc,
  -- Métrica con IVA (bruto): útil para obligaciones fiscales totales
  COALESCE(sum(CASE WHEN si.direction='issued' AND si.tipo_comprobante='I' AND si.estado_sat='vigente'
                    THEN COALESCE(si.total_mxn, si.total) ELSE 0 END), 0) AS total_invoiced_sat_gross,
  -- Métrica SIN IVA (subtotal en MXN): cuadra con Syntage totalReceived
  COALESCE(sum(CASE WHEN si.direction='issued' AND si.tipo_comprobante='I' AND si.estado_sat='vigente'
                    THEN COALESCE(si.subtotal, 0) * COALESCE(si.tipo_cambio, 1) ELSE 0 END), 0) AS total_invoiced_sat,
  -- YTD (subtotal MXN)
  COALESCE(sum(CASE WHEN si.direction='issued' AND si.tipo_comprobante='I' AND si.estado_sat='vigente'
                    AND si.fecha_timbrado >= date_trunc('year', now())
                    THEN COALESCE(si.subtotal, 0) * COALESCE(si.tipo_cambio, 1) ELSE 0 END), 0) AS total_invoiced_sat_ytd,
  -- YTD (gross)
  COALESCE(sum(CASE WHEN si.direction='issued' AND si.tipo_comprobante='I' AND si.estado_sat='vigente'
                    AND si.fecha_timbrado >= date_trunc('year', now())
                    THEN COALESCE(si.total_mxn, si.total) ELSE 0 END), 0) AS total_invoiced_sat_ytd_gross,
  -- Proveedores (received)
  COALESCE(sum(CASE WHEN si.direction='received' AND si.tipo_comprobante='I' AND si.estado_sat='vigente'
                    THEN COALESCE(si.subtotal, 0) * COALESCE(si.tipo_cambio, 1) ELSE 0 END), 0) AS total_received_sat,
  COALESCE(sum(CASE WHEN si.direction='received' AND si.tipo_comprobante='I' AND si.estado_sat='vigente'
                    THEN COALESCE(si.total_mxn, si.total) ELSE 0 END), 0) AS total_received_sat_gross,
  COALESCE(sum(CASE WHEN si.direction='received' AND si.tipo_comprobante='I' AND si.estado_sat='vigente'
                    AND si.fecha_timbrado >= date_trunc('year', now())
                    THEN COALESCE(si.subtotal, 0) * COALESCE(si.tipo_cambio, 1) ELSE 0 END), 0) AS total_received_sat_ytd,
  -- Cancelados (para comparar contra CSV totalCancelledReceived)
  COALESCE(sum(CASE WHEN si.direction='issued' AND si.tipo_comprobante='I' AND si.estado_sat='cancelado'
                    THEN COALESCE(si.subtotal, 0) * COALESCE(si.tipo_cambio, 1) ELSE 0 END), 0) AS total_cancelled_invoiced,
  -- Notas de crédito emitidas (tipo E) — restan del bruto
  COALESCE(sum(CASE WHEN si.direction='issued' AND si.tipo_comprobante='E' AND si.estado_sat='vigente'
                    THEN COALESCE(si.subtotal, 0) * COALESCE(si.tipo_cambio, 1) ELSE 0 END), 0) AS total_credit_notes,
  max(si.fecha_timbrado) AS last_sat_invoice_date
FROM public.companies c
LEFT JOIN public.syntage_invoices si ON si.company_id = c.id
WHERE COALESCE(c.relationship_type, '') <> 'self'
GROUP BY c.id, c.name, c.rfc;

CREATE UNIQUE INDEX company_profile_sat_company_id_pk ON public.company_profile_sat (company_id);
REFRESH MATERIALIZED VIEW public.company_profile_sat;
ANALYZE public.company_profile_sat;
