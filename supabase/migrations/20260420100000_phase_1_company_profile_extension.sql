-- Phase 1: extend company_profile to be one-stop para el frontend.
-- Adds: revenue_ytd, sale_orders_ytd, purchases_ytd, purchase_orders_ytd,
--       ar_aging_buckets (jsonb), sat_compliance_score, invoices_with_cfdi,
--       invoices_with_syntage_match, sat_open_issues, otd_rate_90d, last_activity_at.
--
-- NOTE: reconciliation_issues table does not exist yet; sat_open_issues = 0
-- and sat_compliance_score is based on syntage_invoices match rate.
--
-- DROP CASCADE drops 10 dependents (5 regular views + 5 materialized views);
-- all are recreated below in the same transaction.

BEGIN;

-- ============================================================
-- 1. DROP company_profile (CASCADE removes 10 dependents)
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS public.company_profile CASCADE;
DROP VIEW IF EXISTS public.company_profile CASCADE;

-- ============================================================
-- 2. CREATE new company_profile MATERIALIZED VIEW
-- ============================================================
CREATE MATERIALIZED VIEW public.company_profile AS
WITH sale_stats AS (
  SELECT so.company_id,
    sum(COALESCE(so.amount_untaxed_mxn, so.amount_untaxed)) AS total_revenue,
    count(*) AS total_orders,
    max(so.date_order) AS last_order_date,
    sum(CASE WHEN so.date_order >= date_trunc('year', now())
             THEN COALESCE(so.amount_untaxed_mxn, so.amount_untaxed) ELSE 0 END) AS revenue_ytd,
    count(*) FILTER (WHERE so.date_order >= date_trunc('year', now())) AS orders_ytd,
    sum(CASE WHEN so.date_order >= (now() - interval '90 days')
             THEN COALESCE(so.amount_untaxed_mxn, so.amount_untaxed) ELSE 0 END) AS revenue_90d,
    sum(CASE WHEN so.date_order >= (now() - interval '180 days')
               AND so.date_order < (now() - interval '90 days')
             THEN COALESCE(so.amount_untaxed_mxn, so.amount_untaxed) ELSE 0 END) AS revenue_prior_90d
  FROM public.odoo_sale_orders so
  WHERE so.state = ANY (ARRAY['sale','done'])
  GROUP BY so.company_id
),
purchase_stats AS (
  SELECT po.company_id,
    sum(COALESCE(po.amount_total_mxn, po.amount_total)) AS total_purchases,
    sum(CASE WHEN po.date_order >= date_trunc('year', now())
             THEN COALESCE(po.amount_total_mxn, po.amount_total) ELSE 0 END) AS purchases_ytd,
    count(*) FILTER (WHERE po.date_order >= date_trunc('year', now())) AS purchase_orders_ytd,
    max(po.date_order) AS last_purchase_date
  FROM public.odoo_purchase_orders po
  WHERE po.state = ANY (ARRAY['purchase','done'])
  GROUP BY po.company_id
),
invoice_stats AS (
  SELECT oi.company_id,
    count(*) AS total_invoices,
    sum(COALESCE(oi.amount_residual_mxn, oi.amount_residual)) AS total_pending,
    sum(CASE WHEN oi.days_overdue > 0
             THEN COALESCE(oi.amount_residual_mxn, oi.amount_residual) ELSE 0 END) AS overdue_amount,
    count(*) FILTER (WHERE oi.days_overdue > 0)  AS overdue_count,
    count(*) FILTER (WHERE oi.days_overdue > 30) AS overdue_30d_count,
    count(*) FILTER (WHERE oi.days_overdue > 60) AS overdue_60d_count,
    max(oi.days_overdue) AS max_days_overdue,
    jsonb_build_object(
      'bucket_0_30',   COALESCE(sum(CASE WHEN oi.days_overdue BETWEEN 0  AND 30  AND oi.amount_residual > 0
                                         THEN COALESCE(oi.amount_residual_mxn, oi.amount_residual) ELSE 0 END), 0),
      'bucket_31_60',  COALESCE(sum(CASE WHEN oi.days_overdue BETWEEN 31 AND 60
                                         THEN COALESCE(oi.amount_residual_mxn, oi.amount_residual) ELSE 0 END), 0),
      'bucket_61_90',  COALESCE(sum(CASE WHEN oi.days_overdue BETWEEN 61 AND 90
                                         THEN COALESCE(oi.amount_residual_mxn, oi.amount_residual) ELSE 0 END), 0),
      'bucket_90_plus',COALESCE(sum(CASE WHEN oi.days_overdue > 90
                                         THEN COALESCE(oi.amount_residual_mxn, oi.amount_residual) ELSE 0 END), 0)
    ) AS ar_aging_buckets,
    max(oi.invoice_date) AS last_invoice_date
  FROM public.odoo_invoices oi
  WHERE oi.move_type = 'out_invoice' AND oi.state = 'posted'
  GROUP BY oi.company_id
),
delivery_stats AS (
  SELECT od.company_id,
    count(*) AS total_deliveries,
    count(*) FILTER (WHERE od.is_late) AS late_deliveries,
    round(count(*) FILTER (WHERE NOT od.is_late)::numeric / NULLIF(count(*), 0) * 100, 1) AS otd_rate,
    round(
      count(*) FILTER (WHERE NOT od.is_late AND od.scheduled_date >= (now() - interval '90 days'))::numeric /
      NULLIF(count(*) FILTER (WHERE od.scheduled_date >= (now() - interval '90 days')), 0) * 100,
    1) AS otd_rate_90d,
    max(od.scheduled_date) AS last_delivery_date
  FROM public.odoo_deliveries od
  GROUP BY od.company_id
),
reconciliation_open_by_company AS (
  -- Pre-aggregate reconciliation_issues by company (avoids correlated subqueries).
  SELECT ri.company_id, count(*) AS open_issues
  FROM public.reconciliation_issues ri
  WHERE ri.resolved_at IS NULL AND ri.company_id IS NOT NULL
  GROUP BY ri.company_id
),
cfdi_by_company AS (
  -- Pre-aggregate CFDI match stats per company (avoids re-scanning odoo_invoices twice).
  SELECT
    oi.company_id,
    count(DISTINCT oi.id) AS invoices_with_cfdi,
    count(DISTINCT oi.id) FILTER (
      WHERE EXISTS (SELECT 1 FROM public.syntage_invoices si WHERE si.uuid = oi.cfdi_uuid)
    ) AS invoices_with_syntage_match
  FROM public.odoo_invoices oi
  WHERE oi.cfdi_uuid IS NOT NULL AND oi.company_id IS NOT NULL
  GROUP BY oi.company_id
),
sat_stats AS (
  -- SAT compliance: sat_open_issues from public.reconciliation_issues (44K+ open).
  -- sat_compliance_score = 100 - penalty; penalty = (open_issues / invoices_with_cfdi)*100 capped at 100.
  SELECT
    c.id AS company_id,
    COALESCE(cbc.invoices_with_cfdi, 0) AS invoices_with_cfdi,
    COALESCE(cbc.invoices_with_syntage_match, 0) AS invoices_with_syntage_match,
    COALESCE(robc.open_issues, 0) AS open_issues,
    GREATEST(0, 100 - LEAST(100,
      COALESCE(robc.open_issues, 0)::numeric * 100
      / NULLIF(cbc.invoices_with_cfdi, 0)
    )) AS sat_compliance_score
  FROM public.companies c
  LEFT JOIN cfdi_by_company             cbc  ON cbc.company_id  = c.id
  LEFT JOIN reconciliation_open_by_company robc ON robc.company_id = c.id
),
email_stats AS (
  SELECT ct.company_id,
    count(DISTINCT e.id) AS email_count,
    max(e.email_date) AS last_email_date
  FROM public.contacts ct
  JOIN public.emails e ON e.sender_contact_id = ct.id
  WHERE ct.company_id IS NOT NULL
  GROUP BY ct.company_id
),
contact_stats AS (
  SELECT contacts.company_id, count(*) AS contact_count
  FROM public.contacts
  WHERE contacts.company_id IS NOT NULL
  GROUP BY contacts.company_id
),
total_revenue_all AS (
  SELECT sum(COALESCE(amount_untaxed_mxn, amount_untaxed)) AS grand_total
  FROM public.odoo_sale_orders
  WHERE state = ANY (ARRAY['sale','done'])
)
SELECT
  c.id AS company_id,
  c.name,
  c.canonical_name,
  c.is_customer,
  c.is_supplier,
  c.industry,
  c.credit_limit,
  -- Core revenue/orders (preserved from original)
  COALESCE(s.total_revenue, 0)       AS total_revenue,
  COALESCE(s.total_orders, 0)        AS total_orders,
  s.last_order_date,
  COALESCE(s.revenue_90d, 0)         AS revenue_90d,
  COALESCE(s.revenue_prior_90d, 0)   AS revenue_prior_90d,
  -- Phase 1 NEW: YTD fields
  COALESCE(s.revenue_ytd, 0)         AS revenue_ytd,
  COALESCE(s.orders_ytd, 0)          AS sale_orders_ytd,
  COALESCE(p.purchases_ytd, 0)       AS purchases_ytd,
  COALESCE(p.purchase_orders_ytd, 0) AS purchase_orders_ytd,
  -- Phase 1 NEW: AR aging buckets
  COALESCE(i.ar_aging_buckets,
    '{"bucket_0_30":0,"bucket_31_60":0,"bucket_61_90":0,"bucket_90_plus":0}'::jsonb
  ) AS ar_aging_buckets,
  -- Phase 1 NEW: SAT compliance
  COALESCE(sat.sat_compliance_score, 100)          AS sat_compliance_score,
  COALESCE(sat.invoices_with_cfdi, 0)              AS invoices_with_cfdi,
  COALESCE(sat.invoices_with_syntage_match, 0)     AS invoices_with_syntage_match,
  COALESCE(sat.open_issues, 0)                     AS sat_open_issues,
  -- Phase 1 NEW: last_activity_at
  GREATEST(
    s.last_order_date,
    p.last_purchase_date,
    i.last_invoice_date,
    d.last_delivery_date,
    em.last_email_date
  ) AS last_activity_at,
  -- Preserved from original:
  CASE WHEN COALESCE(s.revenue_prior_90d, 0) > 0
    THEN round((COALESCE(s.revenue_90d, 0) - s.revenue_prior_90d) / s.revenue_prior_90d * 100, 1)
    ELSE NULL
  END AS trend_pct,
  COALESCE(p.total_purchases, 0) AS total_purchases,
  p.last_purchase_date,
  CASE WHEN COALESCE(s.total_revenue, 0) > 0
    THEN round(s.total_revenue / NULLIF((SELECT grand_total FROM total_revenue_all), 0) * 100, 2)
    ELSE 0
  END AS revenue_share_pct,
  COALESCE(i.total_pending, 0)      AS pending_amount,
  COALESCE(i.overdue_amount, 0)     AS overdue_amount,
  COALESCE(i.overdue_count, 0)      AS overdue_count,
  COALESCE(i.overdue_30d_count, 0)  AS overdue_30d_count,
  COALESCE(i.max_days_overdue, 0)   AS max_days_overdue,
  COALESCE(d.total_deliveries, 0)   AS total_deliveries,
  COALESCE(d.late_deliveries, 0)    AS late_deliveries,
  d.otd_rate,
  d.otd_rate_90d,
  COALESCE(em.email_count, 0)  AS email_count,
  em.last_email_date,
  COALESCE(cs.contact_count, 0) AS contact_count,
  CASE
    WHEN COALESCE(i.overdue_60d_count, 0) > 0 AND COALESCE(i.overdue_amount, 0) > 500000 THEN 'critical'
    WHEN COALESCE(i.overdue_30d_count, 0) > 0 AND COALESCE(i.overdue_amount, 0) > 100000 THEN 'high'
    WHEN COALESCE(i.overdue_count, 0) > 0 THEN 'medium'
    WHEN COALESCE(s.revenue_90d, 0) = 0 AND COALESCE(s.total_revenue, 0) > 100000 THEN 'medium'
    ELSE 'low'
  END AS risk_level,
  CASE
    WHEN COALESCE(s.total_revenue, 0) > 2000000 THEN 'strategic'
    WHEN COALESCE(s.total_revenue, 0) > 500000  THEN 'important'
    WHEN COALESCE(s.total_revenue, 0) > 100000  THEN 'regular'
    WHEN COALESCE(p.total_purchases, 0) > 500000 THEN 'key_supplier'
    ELSE 'minor'
  END AS tier
FROM public.companies c
LEFT JOIN sale_stats      s   ON s.company_id   = c.id
LEFT JOIN purchase_stats  p   ON p.company_id   = c.id
LEFT JOIN invoice_stats   i   ON i.company_id   = c.id
LEFT JOIN delivery_stats  d   ON d.company_id   = c.id
LEFT JOIN sat_stats       sat ON sat.company_id = c.id
LEFT JOIN email_stats     em  ON em.company_id  = c.id
LEFT JOIN contact_stats   cs  ON cs.company_id  = c.id
WHERE COALESCE(c.relationship_type, '') <> 'self';

-- ============================================================
-- 3. Indexes on company_profile
-- ============================================================
CREATE UNIQUE INDEX company_profile_company_id_pk
  ON public.company_profile (company_id);
CREATE INDEX company_profile_tier_idx
  ON public.company_profile (tier);
CREATE INDEX company_profile_risk_idx
  ON public.company_profile (risk_level);

-- ============================================================
-- 4. Recreate 5 regular VIEWs dropped by CASCADE
-- ============================================================

-- 4a. revenue_concentration
CREATE OR REPLACE VIEW public.revenue_concentration AS
WITH rev_12m AS (
  SELECT i.company_id,
    c.name AS company_name,
    COALESCE(cp.tier, 'minor') AS tier,
    sum(i.amount_total_mxn) AS rev_12m,
    sum(i.amount_total_mxn) FILTER (WHERE i.invoice_date >= (CURRENT_DATE - '30 days'::interval)) AS rev_30d,
    sum(i.amount_total_mxn) FILTER (WHERE i.invoice_date >= (CURRENT_DATE - '60 days'::interval) AND i.invoice_date < (CURRENT_DATE - '30 days'::interval)) AS rev_30d_prev,
    sum(i.amount_total_mxn) FILTER (WHERE i.invoice_date >= (CURRENT_DATE - '90 days'::interval)) AS rev_90d,
    max(i.invoice_date) AS last_invoice_date
  FROM public.odoo_invoices i
  JOIN public.companies c ON c.id = i.company_id
  LEFT JOIN public.company_profile cp ON cp.company_id = i.company_id
  WHERE i.move_type = 'out_invoice' AND i.state = 'posted'
    AND i.invoice_date >= (CURRENT_DATE - '365 days'::interval)
  GROUP BY i.company_id, c.name, cp.tier
  HAVING sum(i.amount_total_mxn) > 0
),
ranked AS (
  SELECT company_id, company_name, tier, rev_12m, rev_30d, rev_30d_prev, rev_90d, last_invoice_date,
    row_number() OVER (ORDER BY rev_12m DESC) AS rank_in_portfolio,
    rev_12m / NULLIF(sum(rev_12m) OVER (), 0) AS share_pct,
    sum(rev_12m) OVER (ORDER BY rev_12m DESC) / NULLIF(sum(rev_12m) OVER (), 0) AS cumulative_pct
  FROM rev_12m
)
SELECT
  company_id, company_name, tier, rank_in_portfolio,
  rev_12m::numeric(20,2) AS rev_12m,
  rev_90d::numeric(20,2) AS rev_90d,
  rev_30d::numeric(20,2) AS rev_30d,
  rev_30d_prev::numeric(20,2) AS rev_30d_prev,
  round(share_pct * 100, 2) AS share_pct,
  round(cumulative_pct * 100, 2) AS cumulative_pct,
  CASE
    WHEN cumulative_pct <= 0.80 THEN 'A'
    WHEN cumulative_pct <= 0.95 THEN 'B'
    ELSE 'C'
  END AS pareto_class,
  last_invoice_date,
  CURRENT_DATE - last_invoice_date AS days_since_last_invoice,
  round((rev_30d - COALESCE(rev_30d_prev, 0)) / NULLIF(rev_30d_prev, 0) * 100, 1) AS rev_30d_delta_pct,
  CASE
    WHEN rank_in_portfolio <= 5  AND rev_30d_prev > 0 AND (rev_30d - rev_30d_prev) / rev_30d_prev < -0.25 THEN 'TOP5_DECLINE_25PCT'
    WHEN rank_in_portfolio <= 10 AND rev_30d_prev > 0 AND (rev_30d - rev_30d_prev) / rev_30d_prev < -0.40 THEN 'TOP10_DECLINE_40PCT'
    WHEN rank_in_portfolio <= 5  AND (CURRENT_DATE - last_invoice_date) > 45 THEN 'TOP5_NO_ORDER_45D'
    ELSE NULL
  END AS tripwire
FROM ranked
ORDER BY rank_in_portfolio;

-- 4b. weekly_trends
CREATE OR REPLACE VIEW public.weekly_trends AS
WITH current_week AS (
  SELECT i.company_id,
    sum(COALESCE(i.amount_residual_mxn, i.amount_residual)) FILTER (WHERE i.days_overdue > 0) AS overdue_now,
    sum(COALESCE(i.amount_residual_mxn, i.amount_residual)) FILTER (WHERE i.payment_state = ANY (ARRAY['not_paid','partial'])) AS pending_now,
    count(*) FILTER (WHERE i.days_overdue > 0) AS overdue_count
  FROM public.odoo_invoices i
  WHERE i.move_type = 'out_invoice' AND i.state = 'posted'
    AND i.amount_residual > 0 AND i.company_id IS NOT NULL
  GROUP BY i.company_id
),
delivery_current AS (
  SELECT company_id,
    count(*) FILTER (WHERE is_late AND state <> ALL (ARRAY['done','cancel'])) AS late_now
  FROM public.odoo_deliveries
  WHERE company_id IS NOT NULL
  GROUP BY company_id
)
SELECT
  c.canonical_name AS company_name,
  cp.tier,
  COALESCE(cw.overdue_now, 0) AS overdue_now,
  COALESCE(cw.overdue_now, 0) AS overdue_delta,
  COALESCE(cw.pending_now, 0) AS pending_delta,
  COALESCE(dc.late_now, 0)    AS late_delta,
  CASE
    WHEN COALESCE(cw.overdue_now, 0) > 500000 AND COALESCE(dc.late_now, 0) > 2 THEN 'critical'
    WHEN COALESCE(cw.overdue_now, 0) > 200000 OR COALESCE(dc.late_now, 0) > 0  THEN 'warning'
    ELSE 'stable'
  END AS trend_signal
FROM public.companies c
JOIN public.company_profile cp ON cp.company_id = c.id
LEFT JOIN current_week cw ON cw.company_id = c.id
LEFT JOIN delivery_current dc ON dc.company_id = c.id
WHERE (cp.tier = ANY (ARRAY['strategic','important','regular','key_supplier']))
  AND (COALESCE(cw.overdue_now, 0) > 0 OR COALESCE(dc.late_now, 0) > 0);

-- 4c. cash_flow_aging
CREATE OR REPLACE VIEW public.cash_flow_aging AS
SELECT
  a.company_id,
  a.company_name,
  cp.tier,
  sum(a.amount_residual) FILTER (WHERE a.aging_bucket = 'current')                              AS current_amount,
  sum(a.amount_residual) FILTER (WHERE a.aging_bucket = '1-30')                                 AS overdue_1_30,
  sum(a.amount_residual) FILTER (WHERE a.aging_bucket = '31-60')                                AS overdue_31_60,
  sum(a.amount_residual) FILTER (WHERE a.aging_bucket = '61-90')                                AS overdue_61_90,
  sum(a.amount_residual) FILTER (WHERE a.aging_bucket = ANY (ARRAY['91-120','120+']))            AS overdue_90plus,
  sum(a.amount_residual)                                                                         AS total_receivable,
  cp.total_revenue,
  sum(a.amount_residual) FILTER (WHERE a.aging_bucket = '91-120')                               AS overdue_91_120,
  sum(a.amount_residual) FILTER (WHERE a.aging_bucket = '120+')                                 AS overdue_120plus
FROM public.ar_aging_detail a
LEFT JOIN public.company_profile cp ON cp.company_id = a.company_id
GROUP BY a.company_id, a.company_name, cp.tier, cp.total_revenue
ORDER BY sum(a.amount_residual) DESC;

-- 4d. analytics_customer_360
CREATE OR REPLACE VIEW public.analytics_customer_360 AS
SELECT
  cp.company_id, cp.name, cp.canonical_name, cp.is_customer, cp.is_supplier,
  cp.industry, cp.tier, cp.risk_level, cp.credit_limit,
  cp.total_revenue         AS revenue_lifetime_mxn,
  cp.total_orders,
  cp.last_order_date,
  cp.revenue_90d,
  cp.revenue_prior_90d,
  cp.trend_pct             AS revenue_trend_90d_pct,
  cp.revenue_share_pct,
  cp.pending_amount,
  cp.overdue_amount,
  cp.overdue_count,
  cp.overdue_30d_count,
  cp.max_days_overdue,
  cp.total_deliveries,
  cp.late_deliveries,
  cp.otd_rate,
  cp.total_purchases,
  cp.last_purchase_date,
  cp.email_count,
  cp.last_email_date,
  cp.contact_count,
  ltv.ltv_mxn,
  ltv.revenue_12m          AS revenue_12m_mxn,
  ltv.revenue_3m           AS revenue_3m_mxn,
  ltv.trend_pct_vs_prior_quarters AS trend_pct_yoy,
  ltv.first_purchase,
  ltv.last_purchase,
  ltv.churn_risk_score,
  ltv.overdue_risk_score,
  ltv.days_since_last_order,
  cn.salespeople,
  cn.top_products,
  cn.complaints            AS complaints_total,
  cn.recent_complaints,
  cn.commitments,
  cn.requests,
  cn.emails_30d,
  cn.risk_signal,
  fcl.lifetime_revenue_mxn AS fiscal_lifetime_revenue_mxn,
  fcl.revenue_12m_mxn      AS fiscal_revenue_12m_mxn,
  fcl.yoy_pct              AS fiscal_yoy_pct,
  fcl.cancellation_rate_pct AS fiscal_cancellation_rate_pct,
  fcl.days_since_last_cfdi AS fiscal_days_since_last_cfdi,
  fcl.first_cfdi           AS fiscal_first_cfdi,
  ccr.cancelados_24m       AS fiscal_cancelled_24m,
  ccr.cancelled_amount_mxn AS fiscal_cancelled_amount_mxn,
  (SELECT count(*) FROM public.reconciliation_issues ri
   WHERE ri.company_id = cp.company_id AND ri.resolved_at IS NULL) AS fiscal_issues_open,
  (SELECT count(*) FROM public.reconciliation_issues ri
   WHERE ri.company_id = cp.company_id AND ri.resolved_at IS NULL AND ri.severity = 'critical') AS fiscal_issues_critical
FROM public.company_profile cp
LEFT JOIN public.customer_ltv_health ltv ON ltv.company_id = cp.company_id
LEFT JOIN public.company_narrative   cn  ON cn.company_id  = cp.company_id
LEFT JOIN public.companies           c   ON c.id           = cp.company_id
LEFT JOIN public.analytics_customer_fiscal_lifetime  fcl ON lower(fcl.rfc) = lower(c.rfc)
LEFT JOIN public.analytics_customer_cancellation_rates ccr ON lower(ccr.rfc) = lower(c.rfc);

-- 4e. analytics_supplier_360
CREATE OR REPLACE VIEW public.analytics_supplier_360 AS
SELECT
  cp.company_id, cp.name, cp.canonical_name,
  cp.is_customer, cp.is_supplier, cp.industry,
  cp.tier, cp.risk_level,
  cp.total_purchases AS spend_lifetime_mxn,
  cp.last_purchase_date,
  (SELECT count(*)
   FROM public.odoo_invoices oi
   WHERE oi.company_id = cp.company_id AND oi.move_type = 'in_invoice'
     AND oi.payment_state = ANY (ARRAY['not_paid','partial'])) AS overdue_supplier_invoices,
  (SELECT sum(COALESCE(oi.amount_residual_mxn, oi.amount_residual))
   FROM public.odoo_invoices oi
   WHERE oi.company_id = cp.company_id AND oi.move_type = 'in_invoice'
     AND oi.payment_state = ANY (ARRAY['not_paid','partial'])) AS we_owe_mxn,
  sfl.lifetime_spend_mxn        AS fiscal_lifetime_spend_mxn,
  sfl.spend_12m_mxn             AS fiscal_spend_12m_mxn,
  sfl.yoy_pct                   AS fiscal_yoy_pct,
  sfl.retenciones_lifetime_mxn  AS fiscal_retenciones_mxn,
  sfl.first_cfdi                AS fiscal_first_cfdi,
  sfl.last_cfdi                 AS fiscal_last_cfdi,
  sfl.days_since_last_cfdi      AS fiscal_days_since_last_cfdi,
  (SELECT count(*) FROM public.reconciliation_issues ri
   WHERE ri.company_id = cp.company_id AND ri.resolved_at IS NULL) AS fiscal_issues_open,
  (SELECT count(*) FROM public.reconciliation_issues ri
   WHERE ri.company_id = cp.company_id AND ri.resolved_at IS NULL
     AND ri.issue_type = 'sat_only_cfdi_received') AS fiscal_gasto_no_capturado_count,
  (EXISTS (SELECT 1 FROM public.reconciliation_issues ri
           WHERE ri.company_id = cp.company_id AND ri.resolved_at IS NULL
             AND ri.issue_type = 'partner_blacklist_69b')) AS is_blacklist_69b
FROM public.company_profile cp
LEFT JOIN public.companies c ON c.id = cp.company_id
LEFT JOIN public.analytics_supplier_fiscal_lifetime sfl ON lower(sfl.rfc) = lower(c.rfc)
WHERE cp.is_supplier = true;

-- ============================================================
-- 5. Recreate 5 MATERIALIZED VIEWs dropped by CASCADE
-- ============================================================

-- 5a. company_narrative (no unique index — plain REFRESH)
CREATE MATERIALIZED VIEW public.company_narrative AS
WITH sale_stats AS (
  SELECT so.company_id,
    count(*) AS total_orders,
    sum(COALESCE(so.amount_total_mxn, so.amount_total)) AS total_revenue,
    sum(COALESCE(so.amount_total_mxn, so.amount_total)) FILTER (WHERE so.date_order >= (CURRENT_DATE - 90)) AS revenue_90d,
    sum(COALESCE(so.amount_total_mxn, so.amount_total)) FILTER (WHERE so.date_order >= (CURRENT_DATE - 180) AND so.date_order < (CURRENT_DATE - 90)) AS revenue_prior_90d,
    max(so.date_order) AS last_order_date,
    string_agg(DISTINCT so.salesperson_name, ', ') AS salespeople
  FROM public.odoo_sale_orders so
  WHERE so.company_id IS NOT NULL AND so.state = ANY (ARRAY['sale','done'])
  GROUP BY so.company_id
),
invoice_stats AS (
  SELECT oi.company_id,
    sum(COALESCE(oi.amount_residual_mxn, oi.amount_residual)) FILTER (WHERE oi.payment_state = ANY (ARRAY['not_paid','partial']) AND oi.move_type = 'out_invoice' AND oi.state = 'posted') AS pending_amount,
    sum(COALESCE(oi.amount_residual_mxn, oi.amount_residual)) FILTER (WHERE oi.days_overdue > 0 AND oi.move_type = 'out_invoice' AND oi.state = 'posted') AS overdue_amount,
    max(oi.days_overdue) FILTER (WHERE oi.move_type = 'out_invoice' AND oi.state = 'posted') AS max_days_overdue,
    count(*) FILTER (WHERE oi.days_overdue > 30 AND oi.move_type = 'out_invoice' AND oi.state = 'posted') AS invoices_overdue_30d
  FROM public.odoo_invoices oi
  WHERE oi.company_id IS NOT NULL
  GROUP BY oi.company_id
),
delivery_stats AS (
  SELECT od.company_id,
    count(*) FILTER (WHERE od.is_late = true AND od.state <> ALL (ARRAY['done','cancel'])) AS late_deliveries,
    count(*) FILTER (WHERE od.state <> ALL (ARRAY['done','cancel'])) AS pending_deliveries,
    round(count(*) FILTER (WHERE od.state = 'done' AND od.is_late = false)::numeric / NULLIF(count(*) FILTER (WHERE od.state = 'done'), 0) * 100, 0) AS otd_rate
  FROM public.odoo_deliveries od
  WHERE od.company_id IS NOT NULL
  GROUP BY od.company_id
),
email_stats AS (
  SELECT e.company_id,
    count(*) AS total_emails,
    count(*) FILTER (WHERE e.email_date > (now() - '30 days'::interval)) AS emails_30d,
    max(e.email_date)::date AS last_email_date
  FROM public.emails e
  WHERE e.company_id IS NOT NULL
  GROUP BY e.company_id
),
fact_stats AS (
  SELECT c_1.id AS company_id,
    count(*) FILTER (WHERE f.fact_type = 'complaint')   AS complaints,
    count(*) FILTER (WHERE f.fact_type = 'commitment')  AS commitments,
    count(*) FILTER (WHERE f.fact_type = 'request')     AS requests,
    string_agg(CASE WHEN f.fact_type = 'complaint' THEN f.fact_text ELSE NULL END, ' | ' ORDER BY f.created_at DESC) AS recent_complaints
  FROM public.companies c_1
  JOIN public.entities e ON e.id = c_1.entity_id
  JOIN public.facts f ON f.entity_id = e.id
  GROUP BY c_1.id
),
purchase_stats AS (
  SELECT po.company_id,
    sum(COALESCE(po.amount_total_mxn, po.amount_total)) AS total_purchases,
    max(po.date_order) AS last_purchase_date
  FROM public.odoo_purchase_orders po
  WHERE po.company_id IS NOT NULL AND po.state = ANY (ARRAY['purchase','done'])
  GROUP BY po.company_id
),
top_products AS (
  SELECT sub.company_id,
    string_agg(((sub.product_name || ' ($') || round(sub.subtotal_sum, 0)) || ')', ', ' ORDER BY sub.subtotal_sum DESC) AS products
  FROM (
    SELECT il.company_id, il.product_name,
      sum(COALESCE(il.price_subtotal_mxn, il.price_subtotal)) AS subtotal_sum,
      row_number() OVER (PARTITION BY il.company_id ORDER BY sum(COALESCE(il.price_subtotal_mxn, il.price_subtotal)) DESC) AS rn
    FROM public.odoo_invoice_lines il
    WHERE il.move_type = 'out_invoice' AND il.company_id IS NOT NULL AND il.quantity > 0
    GROUP BY il.company_id, il.product_name
  ) sub
  WHERE sub.rn <= 3
  GROUP BY sub.company_id
)
SELECT
  c.id AS company_id,
  c.canonical_name, c.is_customer, c.is_supplier, c.industry, c.rfc,
  cp.tier, cp.risk_level,
  COALESCE(ss.total_revenue, 0) AS total_revenue,
  COALESCE(ss.revenue_90d, 0)   AS revenue_90d,
  CASE
    WHEN ss.revenue_prior_90d > 0 THEN round((ss.revenue_90d - ss.revenue_prior_90d) / ss.revenue_prior_90d * 100, 0)
    WHEN ss.revenue_90d > 0      THEN 100
    ELSE 0
  END AS trend_pct,
  ss.last_order_date,
  CURRENT_DATE - ss.last_order_date AS days_since_last_order,
  ss.salespeople,
  tp.products AS top_products,
  COALESCE(is2.pending_amount, 0)    AS pending_amount,
  COALESCE(is2.overdue_amount, 0)    AS overdue_amount,
  COALESCE(is2.max_days_overdue, 0)  AS max_days_overdue,
  COALESCE(is2.invoices_overdue_30d, 0) AS invoices_overdue_30d,
  COALESCE(ds.late_deliveries, 0)    AS late_deliveries,
  COALESCE(ds.pending_deliveries, 0) AS pending_deliveries,
  ds.otd_rate,
  COALESCE(es.total_emails, 0) AS total_emails,
  COALESCE(es.emails_30d, 0)   AS emails_30d,
  es.last_email_date,
  COALESCE(fs.complaints, 0)   AS complaints,
  COALESCE(fs.commitments, 0)  AS commitments,
  COALESCE(fs.requests, 0)     AS requests,
  fs.recent_complaints,
  COALESCE(ps.total_purchases, 0) AS total_purchases,
  ps.last_purchase_date,
  CASE
    WHEN is2.overdue_amount > 500000 THEN 'CRITICO: cartera vencida >$500K'
    WHEN ss.revenue_90d = 0 AND ss.revenue_prior_90d > 100000 THEN 'CRITICO: churn silencioso (0 ventas en 90d)'
    WHEN ds.late_deliveries > 3 THEN ('ALTO: ' || ds.late_deliveries) || ' entregas atrasadas'
    WHEN fs.complaints > 0 THEN ('ALTO: ' || fs.complaints) || ' quejas detectadas en emails'
    WHEN is2.max_days_overdue > 60 THEN ('MEDIO: factura vencida ' || is2.max_days_overdue) || ' dias'
    WHEN es.emails_30d = 0 AND ss.total_revenue > 200000 THEN 'MEDIO: cliente grande sin comunicacion en 30d'
    ELSE NULL
  END AS risk_signal
FROM public.companies c
JOIN  public.company_profile cp ON cp.company_id = c.id
LEFT JOIN sale_stats     ss   ON ss.company_id  = c.id
LEFT JOIN invoice_stats  is2  ON is2.company_id = c.id
LEFT JOIN delivery_stats ds   ON ds.company_id  = c.id
LEFT JOIN email_stats    es   ON es.company_id  = c.id
LEFT JOIN fact_stats     fs   ON fs.company_id  = c.id
LEFT JOIN purchase_stats ps   ON ps.company_id  = c.id
LEFT JOIN top_products   tp   ON tp.company_id  = c.id
WHERE (cp.tier = ANY (ARRAY['strategic','important','key_supplier']))
   OR COALESCE(ss.total_revenue, 0) > 100000
   OR COALESCE(ps.total_purchases, 0) > 100000;

-- 5b. customer_ltv_health
CREATE MATERIALIZED VIEW public.customer_ltv_health AS
WITH sales_hist AS (
  SELECT il.company_id,
    count(DISTINCT il.move_name) AS total_invoices,
    min(il.invoice_date) AS first_purchase,
    max(il.invoice_date) AS last_purchase,
    sum(COALESCE(il.price_subtotal_mxn, il.price_subtotal)) AS ltv_revenue,
    sum(CASE WHEN il.invoice_date >= (CURRENT_DATE - '1 year'::interval) THEN COALESCE(il.price_subtotal_mxn, il.price_subtotal) ELSE 0 END) AS revenue_12m,
    sum(CASE WHEN il.invoice_date >= (CURRENT_DATE - '3 mons'::interval) THEN COALESCE(il.price_subtotal_mxn, il.price_subtotal) ELSE 0 END) AS revenue_3m,
    sum(CASE WHEN il.invoice_date >= (CURRENT_DATE - '1 year'::interval) AND il.invoice_date < (CURRENT_DATE - '3 mons'::interval) THEN COALESCE(il.price_subtotal_mxn, il.price_subtotal) ELSE 0 END) AS revenue_3m_to_12m
  FROM public.odoo_invoice_lines il
  WHERE il.move_type = 'out_invoice' AND il.company_id IS NOT NULL
    AND il.invoice_date IS NOT NULL AND il.quantity > 0
  GROUP BY il.company_id
),
overdue AS (
  SELECT oi.company_id,
    sum(COALESCE(oi.amount_residual_mxn, oi.amount_residual)) AS overdue_amount,
    max(oi.days_overdue) AS max_days_overdue,
    count(*) AS overdue_count
  FROM public.odoo_invoices oi
  WHERE oi.move_type = 'out_invoice' AND oi.state = 'posted'
    AND oi.payment_state = ANY (ARRAY['not_paid','partial'])
    AND oi.days_overdue > 0 AND oi.company_id IS NOT NULL
  GROUP BY oi.company_id
)
SELECT
  c.id AS company_id,
  c.canonical_name AS company_name,
  cp.tier,
  COALESCE(sh.total_invoices, 0) AS total_invoices,
  sh.first_purchase,
  sh.last_purchase,
  COALESCE(sh.ltv_revenue, 0) AS ltv_mxn,
  COALESCE(sh.revenue_12m, 0) AS revenue_12m,
  COALESCE(sh.revenue_3m, 0)  AS revenue_3m,
  CASE
    WHEN sh.revenue_3m_to_12m > 0 THEN round((COALESCE(sh.revenue_3m, 0) / (sh.revenue_3m_to_12m / 3.0) - 1) * 100, 1)
    ELSE NULL
  END AS trend_pct_vs_prior_quarters,
  COALESCE(o.overdue_amount, 0)     AS overdue_mxn,
  COALESCE(o.max_days_overdue, 0)   AS max_days_overdue,
  COALESCE(o.overdue_count, 0)      AS overdue_invoices,
  LEAST(100, GREATEST(0,
    CASE WHEN sh.last_purchase IS NULL THEN 100
         ELSE LEAST(80, CURRENT_DATE - sh.last_purchase)
    END +
    CASE
      WHEN COALESCE(o.max_days_overdue, 0) >= 60 THEN 20
      WHEN COALESCE(o.max_days_overdue, 0) >= 30 THEN 10
      ELSE 0
    END
  )) AS churn_risk_score,
  LEAST(100, GREATEST(0,
    LEAST(60, COALESCE(o.max_days_overdue, 0)) +
    CASE
      WHEN COALESCE(o.overdue_amount, 0) > 500000 THEN 40
      WHEN COALESCE(o.overdue_amount, 0) > 100000 THEN 20
      WHEN COALESCE(o.overdue_amount, 0) > 0      THEN 10
      ELSE 0
    END
  )) AS overdue_risk_score,
  CURRENT_DATE::timestamp without time zone - COALESCE(sh.last_purchase::timestamp without time zone, CURRENT_DATE - '1000 days'::interval) AS days_since_last_order,
  now() AS computed_at
FROM public.companies c
LEFT JOIN public.company_profile cp ON cp.company_id = c.id
LEFT JOIN sales_hist sh ON sh.company_id = c.id
LEFT JOIN overdue o     ON o.company_id  = c.id
WHERE c.is_customer = true;

CREATE INDEX idx_cltv_company_id ON public.customer_ltv_health (company_id);

-- 5c. payment_predictions
CREATE MATERIALIZED VIEW public.payment_predictions AS
WITH payment_history AS (
  SELECT i.company_id, i.days_to_pay, i.invoice_date, i.amount_total
  FROM public.odoo_invoices i
  WHERE i.move_type = 'out_invoice' AND i.payment_state = 'paid'
    AND i.days_to_pay > 0 AND i.company_id IS NOT NULL
),
company_patterns AS (
  SELECT ph.company_id,
    count(*) AS paid_invoices,
    round(avg(ph.days_to_pay), 0) AS avg_days_to_pay,
    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ph.days_to_pay::double precision)::numeric, 0) AS median_days_to_pay,
    round(stddev(ph.days_to_pay), 0) AS stddev_days,
    min(ph.days_to_pay) AS fastest_payment,
    max(ph.days_to_pay) AS slowest_payment,
    round(avg(ph.days_to_pay) FILTER (WHERE ph.invoice_date >= (CURRENT_DATE - 180)), 0) AS avg_recent_6m,
    round(avg(ph.days_to_pay) FILTER (WHERE ph.invoice_date < (CURRENT_DATE - 180)), 0) AS avg_older
  FROM payment_history ph
  GROUP BY ph.company_id
  HAVING count(*) >= 3
)
SELECT
  cp.company_id,
  c.canonical_name AS company_name,
  cp2.tier,
  cp.paid_invoices,
  cp.avg_days_to_pay,
  cp.median_days_to_pay,
  cp.stddev_days,
  cp.fastest_payment,
  cp.slowest_payment,
  CASE
    WHEN cp.avg_recent_6m IS NOT NULL AND cp.avg_older IS NOT NULL AND cp.avg_recent_6m > (cp.avg_older + 10) THEN 'deteriorando'
    WHEN cp.avg_recent_6m IS NOT NULL AND cp.avg_older IS NOT NULL AND cp.avg_recent_6m < (cp.avg_older - 10) THEN 'mejorando'
    ELSE 'estable'
  END AS payment_trend,
  cp.avg_recent_6m,
  cp.avg_older,
  pending.pending_count,
  pending.total_pending,
  pending.oldest_due_date,
  pending.max_days_overdue,
  pending.oldest_due_date + ((cp.median_days_to_pay || ' days')::interval) AS predicted_payment_date,
  CASE
    WHEN pending.max_days_overdue > cp.slowest_payment THEN 'CRITICO: excede maximo historico'
    WHEN pending.max_days_overdue::numeric > (cp.avg_days_to_pay + cp.stddev_days * 2) THEN 'ALTO: fuera de patron normal'
    WHEN pending.max_days_overdue::numeric > cp.avg_days_to_pay THEN 'MEDIO: pasado de promedio'
    ELSE 'NORMAL: dentro de patron'
  END AS payment_risk
FROM company_patterns cp
JOIN public.companies c ON c.id = cp.company_id
LEFT JOIN public.company_profile cp2 ON cp2.company_id = cp.company_id
LEFT JOIN LATERAL (
  SELECT count(*) AS pending_count,
    sum(COALESCE(i2.amount_residual_mxn, i2.amount_residual)) AS total_pending,
    min(i2.due_date) AS oldest_due_date,
    max(i2.days_overdue) AS max_days_overdue
  FROM public.odoo_invoices i2
  WHERE i2.company_id = cp.company_id AND i2.move_type = 'out_invoice'
    AND i2.payment_state = ANY (ARRAY['not_paid','partial'])
    AND i2.state = 'posted' AND i2.amount_residual > 0
) pending ON true
WHERE pending.pending_count > 0;

CREATE UNIQUE INDEX idx_payment_predictions_pk ON public.payment_predictions (company_id);

-- 5d. client_reorder_predictions
CREATE MATERIALIZED VIEW public.client_reorder_predictions AS
WITH order_gaps AS (
  SELECT so.company_id, so.date_order,
    lag(so.date_order) OVER (PARTITION BY so.company_id ORDER BY so.date_order) AS prev_order,
    so.date_order - lag(so.date_order) OVER (PARTITION BY so.company_id ORDER BY so.date_order) AS days_between,
    so.amount_total
  FROM public.odoo_sale_orders so
  WHERE so.company_id IS NOT NULL AND so.state = 'sale'
),
stats AS (
  SELECT og.company_id,
    count(*) AS order_count,
    round(avg(og.days_between), 0) AS avg_cycle_days,
    round(stddev(og.days_between), 0) AS stddev_days,
    max(og.date_order) AS last_order_date,
    round(avg(og.amount_total), 0) AS avg_order_value,
    CURRENT_DATE - max(og.date_order) AS days_since_last
  FROM order_gaps og
  WHERE og.days_between IS NOT NULL
  GROUP BY og.company_id
  HAVING count(*) >= 3
),
top_products AS (
  SELECT DISTINCT ON (il.company_id) il.company_id, il.product_ref
  FROM public.odoo_invoice_lines il
  WHERE il.move_type = 'out_invoice' AND il.product_ref IS NOT NULL
    AND il.product_ref <> '' AND il.quantity > 0
  GROUP BY il.company_id, il.product_ref
  ORDER BY il.company_id, sum(COALESCE(il.price_subtotal_mxn, il.price_subtotal)) DESC
),
latest_salesperson AS (
  SELECT DISTINCT ON (so.company_id) so.company_id, so.salesperson_name
  FROM public.odoo_sale_orders so
  WHERE so.salesperson_name IS NOT NULL
  ORDER BY so.company_id, so.date_order DESC
)
SELECT
  s.company_id,
  c.canonical_name AS company_name,
  s.order_count, s.avg_cycle_days, s.stddev_days,
  s.last_order_date, s.days_since_last, s.avg_order_value,
  s.last_order_date + s.avg_cycle_days::integer AS predicted_next_order,
  GREATEST(0, s.days_since_last::numeric - s.avg_cycle_days) AS days_overdue_reorder,
  CASE
    WHEN s.days_since_last::numeric > (s.avg_cycle_days * 3) THEN 'lost'
    WHEN s.days_since_last::numeric > (s.avg_cycle_days * 2) THEN 'critical'
    WHEN s.days_since_last::numeric > (s.avg_cycle_days * 1.5) THEN 'at_risk'
    WHEN s.days_since_last::numeric > s.avg_cycle_days THEN 'overdue'
    ELSE 'on_track'
  END AS reorder_status,
  cp.total_revenue, cp.tier,
  ls.salesperson_name,
  tp.product_ref AS top_product_ref
FROM stats s
JOIN public.companies c ON c.id = s.company_id
LEFT JOIN public.company_profile cp ON cp.company_id = s.company_id
LEFT JOIN latest_salesperson ls ON ls.company_id = s.company_id
LEFT JOIN top_products tp ON tp.company_id = s.company_id;

CREATE INDEX idx_reorder_predictions_id ON public.client_reorder_predictions (company_id);

-- 5e. rfm_segments
CREATE MATERIALIZED VIEW public.rfm_segments AS
WITH base AS (
  SELECT i.company_id,
    c.name AS company_name,
    COALESCE(cp.tier, 'minor') AS tier,
    max(i.invoice_date) AS last_purchase,
    min(i.invoice_date) AS first_purchase,
    CURRENT_DATE - max(i.invoice_date) AS recency_days,
    count(*) AS frequency,
    sum(i.amount_total_mxn)::numeric(20,2) AS monetary_2y,
    sum(i.amount_total_mxn) FILTER (WHERE i.invoice_date >= (CURRENT_DATE - '365 days'::interval))::numeric(20,2) AS monetary_12m,
    sum(i.amount_total_mxn) FILTER (WHERE i.invoice_date >= (CURRENT_DATE - '90 days'::interval))::numeric(20,2) AS monetary_90d,
    avg(i.amount_total_mxn)::numeric(20,2) AS avg_ticket,
    sum(CASE WHEN i.payment_state = ANY (ARRAY['not_paid','partial']) THEN i.amount_residual_mxn ELSE 0 END)::numeric(20,2) AS outstanding,
    max(i.days_overdue) AS max_days_overdue
  FROM public.odoo_invoices i
  JOIN public.companies c ON c.id = i.company_id
  LEFT JOIN public.company_profile cp ON cp.company_id = i.company_id
  WHERE i.move_type = 'out_invoice' AND i.state = 'posted'
    AND i.invoice_date >= (CURRENT_DATE - '730 days'::interval)
    AND c.is_customer = true
  GROUP BY i.company_id, c.name, cp.tier
),
scored AS (
  SELECT *,
    ntile(5) OVER (ORDER BY recency_days DESC) AS r_score,
    ntile(5) OVER (ORDER BY frequency) AS f_score,
    ntile(5) OVER (ORDER BY monetary_2y) AS m_score
  FROM base
)
SELECT
  company_id, company_name, tier,
  last_purchase, first_purchase, recency_days, frequency,
  monetary_2y, monetary_12m, monetary_90d, avg_ticket, outstanding, max_days_overdue,
  r_score, f_score, m_score,
  r_score * 100 + f_score * 10 + m_score AS rfm_code,
  CASE
    WHEN recency_days <= 60  AND frequency >= 12 AND monetary_12m >= 1000000 THEN 'CHAMPIONS'
    WHEN recency_days <= 90  AND frequency >= 6  AND r_score >= 4 THEN 'LOYAL'
    WHEN recency_days <= 90  AND frequency <= 3  AND first_purchase >= (CURRENT_DATE - '180 days'::interval) THEN 'NEW'
    WHEN recency_days >= 91  AND recency_days <= 180 AND frequency >= 6 THEN 'AT_RISK'
    WHEN recency_days >= 91  AND recency_days <= 180 THEN 'NEED_ATTENTION'
    WHEN recency_days >= 181 AND recency_days <= 365 THEN 'HIBERNATING'
    WHEN recency_days > 365 THEN 'LOST'
    ELSE 'OCCASIONAL'
  END AS segment,
  LEAST(100, GREATEST(0,
    CASE
      WHEN recency_days >= 91 AND recency_days <= 180 AND frequency >= 6 THEN 80 + LEAST(20, (monetary_12m / 500000)::integer)
      WHEN recency_days >= 91 AND recency_days <= 180 THEN 50 + LEAST(20, (monetary_12m / 500000)::integer)
      WHEN recency_days <= 60 AND frequency >= 12 THEN 30
      WHEN recency_days >= 181 AND recency_days <= 365 AND monetary_12m > 200000 THEN 60
      ELSE 10
    END
  )) AS contact_priority_score,
  now() AS computed_at
FROM scored;

CREATE UNIQUE INDEX idx_rfm_segments_pk       ON public.rfm_segments (company_id);
CREATE INDEX idx_rfm_segments_segment         ON public.rfm_segments (segment);
CREATE INDEX idx_rfm_segments_priority        ON public.rfm_segments (contact_priority_score DESC);

-- ============================================================
-- 6. Audit log
-- ============================================================
INSERT INTO public.schema_changes (ddl, success, error, applied_at)
VALUES (
  'Phase 1: extend company_profile with ytd, ar_aging_buckets, sat_compliance_score, last_activity_at, otd_rate_90d',
  true, NULL, now()
);

COMMIT;

-- ============================================================
-- 7. REFRESH + ANALYZE (outside transaction — CONCURRENTLY needs unique index to exist)
-- ============================================================
REFRESH MATERIALIZED VIEW public.company_profile;
ANALYZE public.company_profile;

REFRESH MATERIALIZED VIEW public.company_narrative;
REFRESH MATERIALIZED VIEW public.customer_ltv_health;
REFRESH MATERIALIZED VIEW public.payment_predictions;
REFRESH MATERIALIZED VIEW public.client_reorder_predictions;
REFRESH MATERIALIZED VIEW public.rfm_segments;
