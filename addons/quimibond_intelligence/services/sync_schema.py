"""
Quimibond Intelligence — Sync Schema Registry
Single source of truth for Supabase table schemas.

This file defines the expected columns for each Supabase table that the addon
writes to. It serves two purposes:
1. Documentation: what columns exist and which are auto-managed by Supabase
2. Validation: detect missing fields before they reach production

Schema hierarchy (general → particular):
  companies → contacts → emails → threads
  companies → company_odoo_snapshots
  contacts  → revenue_metrics, customer_health_scores
  entities  → facts, entity_relationships

When adding a new column to Supabase, update this file FIRST, then update
the code that builds the record. Tests will catch any mismatches.
"""


# ── Column sets per table ────────────────────────────────────────────────────
# Each table defines:
#   'writable': columns the code should populate when inserting/upserting
#   'auto':     columns managed by Supabase (defaults, sequences, triggers)
#   'upsert_key': on_conflict columns for upsert operations

SUPABASE_SCHEMAS = {

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 1: CORE BUSINESS ENTITIES
    # ═══════════════════════════════════════════════════════════════════════

    'companies': {
        'writable': {
            'name', 'canonical_name', 'odoo_partner_id', 'entity_id',
            'is_customer', 'is_supplier', 'industry',
            # Financial (from Odoo sync)
            'lifetime_value', 'total_credit_notes', 'delivery_otd_rate',
            'credit_limit', 'total_pending', 'monthly_avg', 'trend_pct',
            'odoo_context',
            # Enrichment (from Claude)
            'description', 'business_type', 'key_products',
            'relationship_summary', 'relationship_type',
            'country', 'city', 'risk_signals', 'opportunity_signals',
            'strategic_notes', 'enriched_at', 'enrichment_source',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('canonical_name',),
    },

    'contacts': {
        'writable': {
            'email', 'name', 'company', 'contact_type', 'department',
            'odoo_partner_id', 'is_customer', 'is_supplier', 'odoo_context',
            'company_id', 'entity_id',
            # Scores & sentiment (written by save_client_scores)
            'relationship_score', 'risk_level', 'sentiment_score',
            'payment_compliance_score',
            # Financial (written by sync_contact_odoo_data)
            'lifetime_value', 'total_credit_notes', 'delivery_otd_rate',
            # Profile fields (written by upsert_person_profile)
            'role', 'decision_power', 'communication_style',
            'language_preference', 'key_interests', 'personality_notes',
            'negotiation_style', 'response_pattern', 'influence_on_deals',
        },
        'auto': {
            'id', 'created_at', 'updated_at',
            # Computed by triggers/RPCs (refresh_contact_360)
            'total_sent', 'total_received', 'avg_response_time_hours',
            'last_activity', 'first_seen', 'interaction_count',
            'current_health_score', 'health_trend',
            'open_alerts_count', 'pending_actions_count',
        },
        'upsert_key': ('email',),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 2: COMMUNICATION (Gmail)
    # ═══════════════════════════════════════════════════════════════════════

    'emails': {
        'writable': {
            'account', 'sender', 'recipient', 'subject', 'body', 'snippet',
            'email_date', 'gmail_message_id', 'gmail_thread_id',
            'attachments', 'is_reply', 'sender_type', 'has_attachments',
            'kg_processed',
            # FK connections (populated by sync)
            'thread_id', 'sender_contact_id',
        },
        'auto': {'id', 'created_at', 'updated_at', 'embedding'},
        'upsert_key': ('gmail_message_id',),
    },

    'threads': {
        'writable': {
            'gmail_thread_id', 'subject', 'subject_normalized',
            'started_by', 'started_by_type', 'started_at', 'last_activity',
            'status', 'message_count', 'participant_emails',
            'has_internal_reply', 'has_external_reply',
            'last_sender', 'last_sender_type', 'hours_without_response',
            'account',
            # FK connections
            'started_by_contact_id', 'company_id',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('gmail_thread_id',),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 3: ODOO OPERATIONAL DATA
    # ═══════════════════════════════════════════════════════════════════════

    'company_odoo_snapshots': {
        'writable': {
            'company_id', 'snapshot_date',
            'total_invoiced', 'pending_amount', 'overdue_amount',
            'monthly_avg', 'open_orders_count',
            'pending_deliveries_count', 'late_deliveries_count',
            'crm_pipeline_value', 'crm_leads_count',
            'manufacturing_count', 'credit_notes_total',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': ('company_id', 'snapshot_date'),
    },

    'revenue_metrics': {
        'writable': {
            'contact_email', 'contact_id', 'company_id',
            'period_start', 'period_end', 'period_type',
            'total_invoiced', 'pending_amount', 'overdue_amount',
            'overdue_days_max', 'num_orders', 'avg_order_value',
            'odoo_partner_id', 'total_collected',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('contact_email', 'period_start', 'period_type'),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 4: INTELLIGENCE & ANALYTICS
    # ═══════════════════════════════════════════════════════════════════════

    'customer_health_scores': {
        'writable': {
            'contact_id', 'contact_email', 'company_id',
            'score_date', 'overall_score', 'trend',
            'communication_score', 'financial_score', 'sentiment_score',
            'responsiveness_score', 'engagement_score',
            'risk_signals', 'opportunity_signals',
            'payment_compliance_score', 'previous_score',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': ('contact_email', 'score_date'),
    },

    'alerts': {
        'writable': {
            'alert_type', 'severity', 'title', 'description',
            'contact_name', 'contact_id', 'company_id',
            'account', 'state', 'is_read', 'is_resolved',
            'prediction_id', 'prediction_confidence',
            'related_thread_id', 'thread_id',
            'business_impact', 'suggested_action',
            'resolved_at', 'resolution_notes',
            'time_to_resolve_hours',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': None,  # No upsert, always INSERT
    },

    'action_items': {
        'writable': {
            'assignee_entity_id', 'assignee_name', 'assignee_email',
            'related_entity_id', 'description', 'action_type',
            'priority', 'status', 'state', 'due_date',
            'completed_date', 'completed_at',
            'contact_name', 'contact_company', 'contact_id', 'company_id',
            'source_thread_id', 'thread_id',
            'prediction_id', 'prediction_confidence',
            'reason', 'action_category',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': None,
    },

    'topics': {
        'writable': {
            'topic', 'category', 'status', 'priority', 'summary',
            'related_accounts', 'first_seen', 'last_seen', 'times_seen',
            'company_id',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': None,  # Uses RPC upsert_topic
    },

    'account_summaries': {
        'writable': {
            'summary_date', 'account', 'department',
            'total_emails', 'external_emails', 'internal_emails',
            'key_items', 'waiting_response', 'urgent_items',
            'external_contacts', 'topics_detected',
            'summary_text', 'overall_sentiment', 'sentiment_detail',
            'risks_detected',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('summary_date', 'account'),
    },

    'response_metrics': {
        'writable': {
            'account', 'metric_date',
            'emails_received', 'emails_sent',
            'internal_received', 'external_received',
            'threads_started', 'threads_replied', 'threads_unanswered',
            'avg_response_hours', 'fastest_response_hours',
            'slowest_response_hours',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('metric_date', 'account'),
    },

    'daily_summaries': {
        'writable': {
            'summary_date', 'total_emails', 'summary_text', 'summary_html',
            'accounts_read', 'accounts_failed', 'topics_identified',
            'key_events',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': ('summary_date',),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 5: KNOWLEDGE GRAPH
    # ═══════════════════════════════════════════════════════════════════════

    'entities': {
        'writable': {
            'entity_type', 'name', 'canonical_name', 'email',
            'odoo_model', 'odoo_id', 'attributes',
            'first_seen', 'last_seen', 'mention_count',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('entity_type', 'canonical_name'),
    },

    'facts': {
        'writable': {
            'entity_id', 'fact_type', 'fact_text', 'verified',
            'verification_source', 'verification_date', 'confidence',
            'fact_date', 'is_future', 'expired', 'source_account',
            'extracted_at', 'source_type', 'fact_hash',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,
    },

    'entity_relationships': {
        'writable': {
            'entity_a_id', 'entity_b_id', 'relationship_type',
            'strength', 'context', 'first_seen', 'last_seen',
            'interaction_count',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('entity_a_id', 'entity_b_id', 'relationship_type'),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 6: SYSTEM
    # ═══════════════════════════════════════════════════════════════════════

    'sync_state': {
        'writable': {
            'account', 'last_history_id', 'emails_synced',
        },
        'auto': {'updated_at'},
        'upsert_key': ('account',),
    },

    'events': {
        'writable': {
            'event_type', 'entity_type', 'entity_id', 'entity_ref',
            'payload', 'source',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,
    },
}


def get_writable_columns(table: str) -> set:
    """Return the set of writable columns for a Supabase table."""
    schema = SUPABASE_SCHEMAS.get(table)
    if not schema:
        raise ValueError(f'Unknown table: {table}')
    return set(schema['writable'])
