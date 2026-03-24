-- ============================================================
-- Migration 005: Product Purchase, Inventory & Payment Intelligence
-- + Schema sync fixes (missing columns/tables from migration 002)
--
-- Supports qb19 quimibond_intelligence v19.0.22.0.0
-- Bloques: 1 (Product Purchase), 2 (Inventory), 3 (Payment Behavior)
--
-- ALREADY APPLIED to Supabase project tozqezmivpblmcubmnpi
-- on 2026-03-24.
--
-- Copy this file to:
--   quimibond-intelligence/supabase/migrations/005_product_inventory_payment.sql
-- ============================================================

-- ══════════════════════════════════════════════════════════════
-- 1. ALERT TYPE CATALOG — 6 new alert types
-- ══════════════════════════════════════════════════════════════

INSERT INTO alert_type_catalog (alert_type, display_name, description, default_severity, category, is_active)
VALUES
  ('volume_drop',        'Caída de volumen',         'Producto con >30% menos volumen vs periodo anterior',    'medium', 'comercial',  true),
  ('unusual_discount',   'Descuento inusual',        'Descuento aplicado fuera del rango histórico',           'medium', 'comercial',  true),
  ('cross_sell',         'Oportunidad cross-sell',    'Producto que clientes similares compran pero este no',   'low',    'comercial',  true),
  ('stockout_risk',      'Riesgo de desabasto',       'Producto con stock crítico o agotado',                   'high',   'operativo',  true),
  ('reorder_needed',     'Reorden necesario',         'Stock bajo o debajo del punto de reorden',               'medium', 'operativo',  true),
  ('payment_compliance', 'Deterioro en pago',         'Tendencia de pago empeorando o compliance <40%',         'medium', 'financiero', true)
ON CONFLICT (alert_type) DO NOTHING;

-- ══════════════════════════════════════════════════════════════
-- 2. CONTACTS — add payment compliance score
-- ══════════════════════════════════════════════════════════════

ALTER TABLE contacts
  ADD COLUMN IF NOT EXISTS payment_compliance_score integer;

COMMENT ON COLUMN contacts.payment_compliance_score IS
  'Payment compliance score 0-20 (5th scoring dimension). Derived from payment_behavior in odoo_context.';

-- ══════════════════════════════════════════════════════════════
-- 3. CUSTOMER HEALTH SCORES — add payment compliance dimension
-- ══════════════════════════════════════════════════════════════

ALTER TABLE customer_health_scores
  ADD COLUMN IF NOT EXISTS payment_compliance_score integer;

COMMENT ON COLUMN customer_health_scores.payment_compliance_score IS
  'Payment compliance component of health score (0-100 scale).';

-- ══════════════════════════════════════════════════════════════
-- 4. RPC: Get product intelligence for a contact
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION get_contact_product_intelligence(p_contact_email text)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  result json;
  v_ctx jsonb;
BEGIN
  SELECT odoo_context::jsonb INTO v_ctx
  FROM contacts
  WHERE email = p_contact_email;

  IF v_ctx IS NULL THEN
    RETURN '{}'::json;
  END IF;

  SELECT json_build_object(
    'purchase_patterns', v_ctx->'purchase_patterns',
    'inventory_intelligence', v_ctx->'inventory_intelligence',
    'payment_behavior', v_ctx->'payment_behavior',
    'products', v_ctx->'products',
    'lifetime', v_ctx->'lifetime',
    'delivery_performance', v_ctx->'delivery_performance'
  ) INTO result;

  RETURN result;
END;
$$;

GRANT EXECUTE ON FUNCTION get_contact_product_intelligence(text) TO anon;

-- ══════════════════════════════════════════════════════════════
-- 5. RPC: Get company product intelligence (aggregated)
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION get_company_product_intelligence(p_company_id bigint)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  result json;
  v_ctx jsonb;
BEGIN
  SELECT odoo_context::jsonb INTO v_ctx
  FROM companies
  WHERE id = p_company_id;

  IF v_ctx IS NULL THEN
    RETURN '{}'::json;
  END IF;

  SELECT json_build_object(
    'purchase_patterns', v_ctx->'purchase_patterns',
    'inventory_at_risk', v_ctx->'inventory_at_risk',
    'products', v_ctx->'products',
    'total_revenue_12m', v_ctx->'purchase_patterns'->'total_revenue_12m'
  ) INTO result;

  RETURN result;
END;
$$;

GRANT EXECUTE ON FUNCTION get_company_product_intelligence(bigint) TO anon;

-- ══════════════════════════════════════════════════════════════
-- 6. RPC: Dashboard KPIs for new alert types
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION get_product_intelligence_kpis()
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE result json;
BEGIN
  SELECT json_build_object(
    'stockout_alerts', (
      SELECT count(*)
      FROM alerts
      WHERE alert_type = 'stockout_risk' AND state = 'new'
    ),
    'volume_drop_alerts', (
      SELECT count(*)
      FROM alerts
      WHERE alert_type = 'volume_drop' AND state = 'new'
    ),
    'cross_sell_opportunities', (
      SELECT count(*)
      FROM alerts
      WHERE alert_type = 'cross_sell' AND state = 'new'
    ),
    'payment_compliance_alerts', (
      SELECT count(*)
      FROM alerts
      WHERE alert_type = 'payment_compliance' AND state = 'new'
    ),
    'low_compliance_contacts', (
      SELECT count(*)
      FROM contacts
      WHERE payment_compliance_score IS NOT NULL
        AND payment_compliance_score < 10
        AND contact_type = 'external'
    ),
    'reorder_alerts', (
      SELECT count(*)
      FROM alerts
      WHERE alert_type = 'reorder_needed' AND state = 'new'
    )
  ) INTO result;
  RETURN result;
END;
$$;

GRANT EXECUTE ON FUNCTION get_product_intelligence_kpis() TO anon;

-- ══════════════════════════════════════════════════════════════
-- 7. Update get_director_dashboard to include new KPIs
-- ══════════════════════════════════════════════════════════════

-- Add stockout_alerts and payment_compliance to the existing dashboard RPC
-- by recreating with extended KPI section:

CREATE OR REPLACE FUNCTION get_director_dashboard()
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE result json;
BEGIN
  SELECT json_build_object(
    'kpi', json_build_object(
      'open_alerts', (SELECT count(*) FROM alerts WHERE state = 'new'),
      'critical_alerts', (SELECT count(*) FROM alerts WHERE state = 'new' AND severity IN ('critical','high')),
      'pending_actions', (SELECT count(*) FROM action_items WHERE state = 'pending'),
      'overdue_actions', (SELECT count(*) FROM action_items WHERE state = 'pending' AND due_date < CURRENT_DATE),
      'at_risk_contacts', (SELECT count(*) FROM contacts WHERE risk_level = 'high'),
      'total_contacts', (SELECT count(*) FROM contacts WHERE contact_type = 'external'),
      'total_emails', (SELECT count(*) FROM emails),
      'completed_actions', (SELECT count(*) FROM action_items WHERE state = 'completed'),
      'resolved_alerts', (SELECT count(*) FROM alerts WHERE state = 'resolved'),
      -- New KPIs (Bloques 1-3)
      'stockout_alerts', (SELECT count(*) FROM alerts WHERE alert_type = 'stockout_risk' AND state = 'new'),
      'cross_sell_opportunities', (SELECT count(*) FROM alerts WHERE alert_type = 'cross_sell' AND state = 'new'),
      'low_compliance_contacts', (
        SELECT count(*) FROM contacts
        WHERE payment_compliance_score IS NOT NULL
          AND payment_compliance_score < 10
          AND contact_type = 'external'
      )
    ),
    'overdue_actions', (
      SELECT coalesce(json_agg(row_to_json(a)), '[]'::json)
      FROM (
        SELECT ai.id, ai.description, ai.contact_name, ai.contact_company,
               ai.assignee_email, ai.assignee_name, ai.due_date, ai.priority,
               ai.reason, ai.action_type,
               (CURRENT_DATE - ai.due_date) AS days_overdue
        FROM action_items ai
        WHERE ai.state = 'pending' AND ai.due_date < CURRENT_DATE
        ORDER BY ai.due_date ASC LIMIT 10
      ) a
    ),
    'critical_alerts', (
      SELECT coalesce(json_agg(row_to_json(al)), '[]'::json)
      FROM (
        SELECT al.id, al.title, al.severity, al.contact_name,
               al.description, al.business_impact, al.suggested_action,
               al.related_thread_id, al.created_at, al.alert_type, al.account
        FROM alerts al
        WHERE al.state = 'new' AND al.severity IN ('critical','high')
        ORDER BY
          CASE al.severity WHEN 'critical' THEN 0 ELSE 1 END,
          al.created_at DESC
        LIMIT 8
      ) al
    ),
    'accountability', (
      SELECT coalesce(json_agg(row_to_json(acc)), '[]'::json)
      FROM (
        SELECT
          coalesce(assignee_name, assignee_email, 'Sin asignar') AS name,
          assignee_email AS email,
          count(*) FILTER (WHERE state = 'pending') AS pending,
          count(*) FILTER (WHERE state = 'pending' AND due_date < CURRENT_DATE) AS overdue,
          count(*) FILTER (WHERE state = 'completed') AS completed
        FROM action_items
        WHERE assignee_email IS NOT NULL
        GROUP BY assignee_email, assignee_name
        HAVING count(*) FILTER (WHERE state = 'pending') > 0
        ORDER BY count(*) FILTER (WHERE state = 'pending' AND due_date < CURRENT_DATE) DESC
      ) acc
    ),
    'contacts_at_risk', (
      SELECT coalesce(json_agg(row_to_json(c)), '[]'::json)
      FROM (
        SELECT c.id, c.name, c.company, c.risk_level, c.sentiment_score,
               c.relationship_score, c.last_activity,
               c.score_breakdown, c.payment_compliance_score,
               (SELECT count(*) FROM alerts al WHERE al.contact_name = c.name AND al.state = 'new') AS open_alerts,
               (SELECT count(*) FROM action_items ai WHERE ai.contact_name = c.name AND ai.state = 'pending') AS pending_actions
        FROM contacts c
        WHERE c.risk_level = 'high' AND c.contact_type = 'external'
        ORDER BY c.relationship_score ASC NULLS FIRST LIMIT 8
      ) c
    ),
    'latest_briefing', (
      SELECT row_to_json(b)
      FROM (
        SELECT id, briefing_type, summary, html_content, created_at
        FROM briefings ORDER BY created_at DESC LIMIT 1
      ) b
    ),
    'pending_actions', (
      SELECT coalesce(json_agg(row_to_json(pa)), '[]'::json)
      FROM (
        SELECT ai.id, ai.description, ai.contact_name, ai.contact_company,
               ai.assignee_email, ai.assignee_name, ai.due_date, ai.priority,
               ai.reason, ai.action_type, ai.state
        FROM action_items ai
        WHERE ai.state = 'pending'
        ORDER BY ai.due_date ASC NULLS LAST LIMIT 10
      ) pa
    )
  ) INTO result;
  RETURN result;
END;
$$;
