-- Phase 1.6 Fix B: Nueva MV company_profile_sat con métricas de facturación fiscal SAT.
--
-- Contexto: company_profile.total_revenue mide odoo_sale_orders.amount_untaxed_mxn
-- (pedidos confirmados, subtotal). Es una métrica operativa distinta de la facturación
-- fiscal real. Esta MV añade las métricas fiscales sin tocar company_profile, evitando
-- recrear sus ~10 dependientes (otras MVs, funciones, etc.).
--
-- Columnas:
--   total_invoiced_sat     — lifetime de facturas tipo I emitidas al cliente (vigente)
--   total_invoiced_sat_ytd — idem pero año en curso
--   total_received_sat     — lifetime de facturas tipo I recibidas del proveedor (vigente)
--   total_received_sat_ytd — idem pero año en curso
--   last_sat_invoice_date  — fecha timbrado más reciente de cualquier dirección
--
-- Uso en frontend: JOIN con company_profile ON company_id para combinar ambas métricas.

CREATE MATERIALIZED VIEW IF NOT EXISTS public.company_profile_sat AS
SELECT
  c.id AS company_id,
  c.name,
  c.rfc,
  COALESCE(SUM(
    CASE
      WHEN si.direction = 'issued'
        AND si.tipo_comprobante = 'I'
        AND si.estado_sat = 'vigente'
      THEN COALESCE(si.total_mxn, si.total)
      ELSE 0
    END
  ), 0) AS total_invoiced_sat,
  COALESCE(SUM(
    CASE
      WHEN si.direction = 'issued'
        AND si.tipo_comprobante = 'I'
        AND si.estado_sat = 'vigente'
        AND si.fecha_timbrado >= date_trunc('year', now())
      THEN COALESCE(si.total_mxn, si.total)
      ELSE 0
    END
  ), 0) AS total_invoiced_sat_ytd,
  COALESCE(SUM(
    CASE
      WHEN si.direction = 'received'
        AND si.tipo_comprobante = 'I'
        AND si.estado_sat = 'vigente'
      THEN COALESCE(si.total_mxn, si.total)
      ELSE 0
    END
  ), 0) AS total_received_sat,
  COALESCE(SUM(
    CASE
      WHEN si.direction = 'received'
        AND si.tipo_comprobante = 'I'
        AND si.estado_sat = 'vigente'
        AND si.fecha_timbrado >= date_trunc('year', now())
      THEN COALESCE(si.total_mxn, si.total)
      ELSE 0
    END
  ), 0) AS total_received_sat_ytd,
  MAX(si.fecha_timbrado) AS last_sat_invoice_date
FROM public.companies c
LEFT JOIN public.syntage_invoices si ON si.company_id = c.id
WHERE COALESCE(c.relationship_type, '') <> 'self'
GROUP BY c.id, c.name, c.rfc;

CREATE UNIQUE INDEX IF NOT EXISTS company_profile_sat_company_id_pk
  ON public.company_profile_sat (company_id);

REFRESH MATERIALIZED VIEW public.company_profile_sat;
ANALYZE public.company_profile_sat;
