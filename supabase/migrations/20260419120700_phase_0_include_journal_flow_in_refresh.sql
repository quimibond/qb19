-- Phase 0: incluir journal_flow_profile en refresh_all_matviews
-- Root cause: la MV existía con datos (16 rows, ispopulated=true) pero nunca
-- fue añadida a refresh_all_matviews, de ahí que last_autoanalyze fuera NULL.
-- Fix: añadir REFRESH MATERIALIZED VIEW CONCURRENTLY journal_flow_profile al body.
-- La MV ya tiene UNIQUE INDEX idx_journal_flow_profile_pk(journal_name, payment_type)
-- por lo que CONCURRENTLY es válido.

CREATE OR REPLACE FUNCTION public.refresh_all_matviews()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW company_profile;
  REFRESH MATERIALIZED VIEW monthly_revenue_by_company;
  REFRESH MATERIALIZED VIEW portfolio_concentration;
  REFRESH MATERIALIZED VIEW ar_aging_detail;
  REFRESH MATERIALIZED VIEW accounting_anomalies;
  REFRESH MATERIALIZED VIEW customer_cohorts;
  REFRESH MATERIALIZED VIEW customer_margin_analysis;
  REFRESH MATERIALIZED VIEW customer_product_matrix;
  REFRESH MATERIALIZED VIEW supplier_product_matrix;
  REFRESH MATERIALIZED VIEW dead_stock_analysis;
  REFRESH MATERIALIZED VIEW inventory_velocity;
  REFRESH MATERIALIZED VIEW ops_delivery_health_weekly;
  -- Sprint 13: product_real_cost MUST be refreshed BEFORE product_margin_analysis
  -- because PMA LEFT JOINs prc to compute cost_source='bom' rows.
  REFRESH MATERIALIZED VIEW product_real_cost;
  REFRESH MATERIALIZED VIEW product_margin_analysis;
  REFRESH MATERIALIZED VIEW product_seasonality;
  REFRESH MATERIALIZED VIEW purchase_price_intelligence;
  REFRESH MATERIALIZED VIEW supplier_concentration_herfindahl;
  REFRESH MATERIALIZED VIEW company_email_intelligence;
  REFRESH MATERIALIZED VIEW company_handlers;
  REFRESH MATERIALIZED VIEW company_insight_history;
  REFRESH MATERIALIZED VIEW cross_director_signals;
  REFRESH MATERIALIZED VIEW cashflow_projection;
  REFRESH MATERIALIZED VIEW real_sale_price;
  REFRESH MATERIALIZED VIEW supplier_price_index;
  REFRESH MATERIALIZED VIEW company_narrative;
  REFRESH MATERIALIZED VIEW customer_ltv_health;
  REFRESH MATERIALIZED VIEW payment_predictions;
  REFRESH MATERIALIZED VIEW client_reorder_predictions;
  REFRESH MATERIALIZED VIEW rfm_segments;
  -- Phase 0 addition: journal_flow_profile was missing from this function
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.journal_flow_profile;
  RAISE NOTICE 'All 30 materialized views refreshed successfully';
END;
$function$;
