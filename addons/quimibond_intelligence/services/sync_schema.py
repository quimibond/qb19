"""
Quimibond Intelligence — Sync Schema Registry
Single source of truth for Supabase table schemas.

This file defines the expected columns for each Supabase table that the addon
writes to. It serves two purposes:
1. Documentation: what columns exist and which are auto-managed by Supabase
2. Validation: detect missing fields before they reach production

Schema hierarchy (general → particular):
  companies → contacts → emails → threads → email_recipients
  companies → odoo_snapshots, odoo_invoices, odoo_deliveries, ...
  contacts  → health_scores, revenue_metrics, communication_edges
  entities  → facts, entity_relationships
  pipeline_runs → pipeline_logs

Consolidated schema (March 2026):
  - person_profiles merged into contacts
  - daily_summaries + account_summaries merged into briefings
  - response_metrics + communication_patterns merged into communication_metrics
  - company_odoo_snapshots renamed to odoo_snapshots
  - customer_health_scores renamed to health_scores
  - events table removed (use pipeline_logs)
  - alert_type_catalog, topic_category_catalog removed (CHECK constraints)

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
            'is_customer', 'is_supplier', 'industry', 'business_type',
            'country', 'city',
            # Financial (from Odoo sync)
            'lifetime_value', 'credit_limit', 'total_pending',
            'total_credit_notes', 'monthly_avg', 'trend_pct',
            'delivery_otd_rate',
            # Intelligence (from Claude enrichment)
            'description', 'key_products', 'relationship_summary',
            'relationship_type', 'risk_signals', 'opportunity_signals',
            'strategic_notes',
            # Odoo context
            'odoo_context',
            # Enrichment metadata
            'enriched_at', 'enrichment_source',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('canonical_name',),
    },

    'contacts': {
        'writable': {
            'email', 'name', 'company_id', 'odoo_partner_id', 'entity_id',
            # Classification
            'contact_type', 'department', 'is_customer', 'is_supplier',
            # Profile (consolidated from person_profiles)
            'role', 'decision_power', 'communication_style',
            'language_preference', 'key_interests', 'personality_notes',
            'negotiation_style', 'response_pattern', 'influence_on_deals',
            # Scores
            'relationship_score', 'sentiment_score', 'risk_level',
            'payment_compliance_score',
            # Financial
            'lifetime_value', 'total_credit_notes', 'delivery_otd_rate',
            # Odoo context
            'odoo_context',
        },
        'auto': {
            'id', 'created_at', 'updated_at',
            # Computed by triggers/RPCs
            'total_sent', 'total_received', 'avg_response_time_hours',
            'interaction_count', 'last_activity', 'first_seen',
            'current_health_score', 'health_trend',
            'open_alerts_count', 'pending_actions_count',
        },
        'upsert_key': ('email',),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 2: COMMUNICATION
    # ═══════════════════════════════════════════════════════════════════════

    'threads': {
        'writable': {
            'gmail_thread_id', 'subject', 'subject_normalized',
            'account', 'company_id',
            # Participants
            'started_by', 'started_by_type', 'started_by_contact_id',
            'last_sender', 'last_sender_type', 'participant_emails',
            # Status
            'status', 'message_count',
            'has_internal_reply', 'has_external_reply',
            'hours_without_response',
            # Timestamps
            'started_at', 'last_activity',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('gmail_thread_id',),
    },

    'emails': {
        'writable': {
            'gmail_message_id', 'gmail_thread_id', 'account',
            'thread_id', 'sender_contact_id', 'company_id',
            # Content
            'sender', 'recipient', 'subject', 'body', 'snippet',
            'email_date',
            # Classification
            'is_reply', 'sender_type', 'has_attachments', 'attachments',
            # Processing
            'kg_processed',
        },
        'auto': {'id', 'created_at', 'updated_at', 'embedding'},
        'upsert_key': ('gmail_message_id',),
    },

    'email_recipients': {
        'writable': {
            'email_id', 'contact_id', 'recipient_email', 'recipient_name',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': ('email_id', 'contact_id'),
    },

    'communication_edges': {
        'writable': {
            'from_contact_id', 'to_contact_id',
            'from_company_id', 'to_company_id',
            'email_count', 'first_email_at', 'last_email_at',
            'is_bidirectional', 'is_internal',
        },
        'auto': {'id', 'updated_at'},
        'upsert_key': ('from_contact_id', 'to_contact_id'),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 3: KNOWLEDGE GRAPH
    # ═══════════════════════════════════════════════════════════════════════

    'entities': {
        'writable': {
            'entity_type', 'canonical_name', 'name', 'email',
            'odoo_model', 'odoo_id', 'attributes',
            'mention_count', 'first_seen', 'last_seen',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('entity_type', 'canonical_name'),
    },

    'facts': {
        'writable': {
            'entity_id', 'fact_type', 'fact_text', 'fact_hash',
            'fact_date', 'confidence', 'verified',
            'verification_source', 'verification_date',
            'is_future', 'expired',
            'source_type', 'source_account', 'extracted_at',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,
    },

    'entity_relationships': {
        'writable': {
            'entity_a_id', 'entity_b_id', 'relationship_type',
            'strength', 'context', 'interaction_count',
            'first_seen', 'last_seen',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('entity_a_id', 'entity_b_id', 'relationship_type'),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 4: INTELLIGENCE OUTPUTS
    # ═══════════════════════════════════════════════════════════════════════

    'alerts': {
        'writable': {
            'alert_type', 'severity', 'title', 'description',
            'contact_id', 'contact_name', 'company_id',
            'thread_id', 'account',
            # State
            'state', 'is_read', 'resolved_at', 'resolution_notes',
            'time_to_resolve_hours',
            # AI context
            'business_impact', 'suggested_action', 'prediction_confidence',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': None,  # Always INSERT
    },

    'action_items': {
        'writable': {
            'action_type', 'action_category', 'description', 'reason',
            'priority',
            # Linked entities
            'contact_id', 'contact_name', 'contact_company',
            'company_id', 'thread_id',
            # Assignment
            'assignee_name', 'assignee_email',
            # State
            'state', 'due_date', 'completed_at',
            # AI context
            'prediction_confidence',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': None,  # Always INSERT
    },

    # Consolidated from daily_summaries + account_summaries
    'briefings': {
        'writable': {
            'scope', 'briefing_date', 'account', 'company_id',
            # Content
            'title', 'summary_text', 'summary_html',
            # Metrics
            'total_emails', 'key_events', 'topics_identified',
            'risks_detected', 'overall_sentiment', 'sentiment_detail',
            # Account-scope
            'department', 'external_emails', 'internal_emails',
            'waiting_response', 'urgent_items', 'external_contacts',
            # Daily-scope
            'accounts_processed', 'accounts_failed',
            # Metadata
            'metadata',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,  # Uses unique index on (scope, briefing_date, account)
    },

    'topics': {
        'writable': {
            'topic', 'category', 'status', 'priority', 'summary',
            'company_id', 'related_accounts',
            'times_seen', 'first_seen', 'last_seen',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': None,  # Uses RPC upsert_topic
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 5: METRICS & HISTORY
    # ═══════════════════════════════════════════════════════════════════════

    # Renamed from customer_health_scores
    'health_scores': {
        'writable': {
            'contact_id', 'contact_email', 'company_id', 'score_date',
            'overall_score', 'previous_score', 'trend',
            'communication_score', 'financial_score', 'sentiment_score',
            'responsiveness_score', 'engagement_score',
            'payment_compliance_score',
            'risk_signals', 'opportunity_signals',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': ('contact_email', 'score_date'),
    },

    'revenue_metrics': {
        'writable': {
            'contact_email', 'contact_id', 'company_id', 'odoo_partner_id',
            'period_type', 'period_start', 'period_end',
            'total_invoiced', 'total_collected', 'pending_amount',
            'overdue_amount', 'overdue_days_max',
            'num_orders', 'avg_order_value',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('contact_email', 'period_start', 'period_type'),
    },

    # Consolidated from response_metrics + communication_patterns
    'communication_metrics': {
        'writable': {
            'account', 'metric_date',
            # Volume
            'emails_received', 'emails_sent',
            'internal_received', 'external_received',
            # Threads
            'threads_started', 'threads_replied', 'threads_unanswered',
            # Response times
            'avg_response_hours', 'fastest_response_hours',
            'slowest_response_hours',
            # Weekly patterns
            'response_rate', 'top_external_contacts',
            'top_internal_contacts', 'busiest_hour',
            'common_subjects', 'sentiment_score',
        },
        'auto': {'id', 'created_at', 'updated_at'},
        'upsert_key': ('metric_date', 'account'),
    },

    # Renamed from company_odoo_snapshots
    'odoo_snapshots': {
        'writable': {
            'company_id', 'snapshot_date',
            'total_invoiced', 'pending_amount', 'overdue_amount',
            'monthly_avg', 'credit_notes_total',
            'open_orders_count', 'pending_deliveries_count',
            'late_deliveries_count',
            'crm_pipeline_value', 'crm_leads_count',
            'manufacturing_count',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': ('company_id', 'snapshot_date'),
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 6: ODOO INTEGRATION
    # ═══════════════════════════════════════════════════════════════════════

    'odoo_products': {
        'writable': {
            'odoo_product_id', 'name', 'internal_ref', 'category',
            'uom', 'product_type',
            'stock_qty', 'reserved_qty', 'available_qty',
            'reorder_min', 'reorder_max',
            'standard_price', 'list_price', 'active',
        },
        'auto': {'id', 'updated_at'},
        'upsert_key': ('odoo_product_id',),
    },

    'odoo_order_lines': {
        'writable': {
            'odoo_line_id', 'odoo_order_id', 'odoo_partner_id', 'company_id',
            'odoo_product_id',
            'order_name', 'order_date', 'order_type', 'order_state',
            'product_name', 'qty', 'price_unit', 'discount',
            'subtotal', 'currency',
        },
        'auto': {'id'},
        'upsert_key': ('odoo_line_id',),
    },

    'odoo_users': {
        'writable': {
            'odoo_user_id', 'name', 'email', 'department', 'job_title',
            'pending_activities_count', 'overdue_activities_count',
            'activities_json',
        },
        'auto': {'id', 'updated_at'},
        'upsert_key': ('odoo_user_id',),
    },

    'odoo_invoices': {
        'writable': {
            'company_id', 'odoo_partner_id',
            'name', 'move_type',
            'amount_total', 'amount_residual', 'currency',
            'invoice_date', 'due_date', 'payment_date',
            'state', 'payment_state', 'days_overdue',
            'days_to_pay', 'payment_status', 'ref',
        },
        'auto': {'id', 'synced_at'},
        'upsert_key': ('odoo_partner_id', 'name'),
    },

    'odoo_payments': {
        'writable': {
            'company_id', 'odoo_partner_id',
            'name', 'payment_type',
            'amount', 'currency',
            'payment_date', 'state',
        },
        'auto': {'id', 'synced_at'},
        'upsert_key': ('odoo_partner_id', 'name'),
    },

    'odoo_deliveries': {
        'writable': {
            'company_id', 'odoo_partner_id',
            'name', 'picking_type', 'origin',
            'scheduled_date', 'date_done', 'create_date',
            'state', 'is_late', 'lead_time_days',
        },
        'auto': {'id', 'synced_at'},
        'upsert_key': ('odoo_partner_id', 'name'),
    },

    'odoo_crm_leads': {
        'writable': {
            'company_id', 'odoo_partner_id', 'odoo_lead_id',
            'name', 'lead_type',
            'stage', 'expected_revenue', 'probability',
            'date_deadline', 'create_date', 'days_open',
            'assigned_user', 'active',
        },
        'auto': {'id', 'synced_at'},
        'upsert_key': ('odoo_lead_id',),
    },

    'odoo_activities': {
        'writable': {
            'company_id', 'odoo_partner_id',
            'activity_type', 'summary',
            'res_model', 'res_id',
            'date_deadline', 'assigned_to', 'is_overdue',
        },
        'auto': {'id', 'synced_at'},
        'upsert_key': None,  # Recreated on each sync
    },

    # ═══════════════════════════════════════════════════════════════════════
    # TIER 7: SYSTEM & OPERATIONS
    # ═══════════════════════════════════════════════════════════════════════

    'sync_state': {
        'writable': {
            'account', 'last_history_id', 'emails_synced', 'last_sync_at',
        },
        'auto': {'updated_at'},
        'upsert_key': ('account',),
    },

    'pipeline_runs': {
        'writable': {
            'run_type', 'status',
            'started_at', 'completed_at', 'duration_seconds',
            'emails_processed', 'alerts_generated', 'actions_generated',
            'errors', 'metadata',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,
    },

    'pipeline_logs': {
        'writable': {
            'run_id', 'level', 'phase', 'message', 'details',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,
    },

    'chat_memory': {
        'writable': {
            'question', 'answer', 'context_used',
            'rating', 'thumbs_up', 'times_retrieved',
        },
        'auto': {'id', 'saved_at'},
        'upsert_key': None,
    },

    'feedback_signals': {
        'writable': {
            'source_type', 'source_id', 'signal_type',
            'reward_score', 'context', 'account',
            'contact_email', 'reward_processed',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,
    },

    'sync_commands': {
        'writable': {
            'command', 'status', 'requested_by',
            'result', 'started_at', 'completed_at',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,
    },

    'token_usage': {
        'writable': {
            'endpoint', 'model', 'input_tokens', 'output_tokens',
        },
        'auto': {'id', 'created_at'},
        'upsert_key': None,
    },
}


# ── Mapping from old table names to new ones (for migration reference) ───────
TABLE_RENAMES = {
    'customer_health_scores': 'health_scores',
    'company_odoo_snapshots': 'odoo_snapshots',
    'response_metrics': 'communication_metrics',
    'daily_summaries': 'briefings',      # scope='daily'
    'account_summaries': 'briefings',    # scope='account'
}

# Tables removed (no longer exist)
REMOVED_TABLES = {
    'person_profiles',           # merged into contacts
    'events',                    # replaced by pipeline_logs
    'communication_patterns',    # merged into communication_metrics
    'entity_mentions',           # unused
    'prediction_outcomes',       # merged into feedback_signals
    'system_learnings',          # removed
    'alert_type_catalog',        # replaced by CHECK constraints
    'topic_category_catalog',    # replaced by category field on topics
}


def _resolve_table(table: str) -> str:
    """Resolve old table name to current name if renamed."""
    return TABLE_RENAMES.get(table, table)


def get_writable_columns(table: str) -> set:
    """Return the set of writable columns for a Supabase table."""
    schema = SUPABASE_SCHEMAS.get(_resolve_table(table))
    if not schema:
        raise ValueError(f'Unknown table: {table}')
    return set(schema['writable'])


def validate_record(table: str, record: dict,
                    required: set = None) -> list:
    """Validate a record against its Supabase schema.

    Returns a list of warning strings (empty = valid).
    Resolves old table names automatically.
    """
    schema = SUPABASE_SCHEMAS.get(_resolve_table(table))
    if not schema:
        return [f'Unknown table: {table}']

    warnings = []
    all_known = schema['writable'] | schema['auto']

    for key in record:
        if key not in all_known:
            warnings.append(f'{table}: unknown column "{key}"')

    if required:
        for col in required:
            if col not in record:
                warnings.append(f'{table}: missing required column "{col}"')

    return warnings


def check_coverage(table: str, record: dict) -> set:
    """Return writable columns NOT present in the record.

    Useful to detect schema drift: if a new column was added to the
    schema but the code doesn't populate it yet.
    Resolves old table names automatically.
    """
    schema = SUPABASE_SCHEMAS.get(_resolve_table(table))
    if not schema:
        return set()
    return schema['writable'] - set(record.keys())
