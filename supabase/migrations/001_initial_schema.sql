-- ================================================================
-- Quimibond Intelligence - Supabase Schema
-- Complete database setup for the intelligence system
-- ================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

-- ================================================================
-- 1. EMAILS
-- ================================================================
CREATE TABLE IF NOT EXISTS emails (
    id          BIGSERIAL PRIMARY KEY,
    account     TEXT NOT NULL,
    sender      TEXT,
    recipient   TEXT,
    subject     TEXT,
    body        TEXT,
    snippet     TEXT,
    email_date  TIMESTAMPTZ,
    gmail_message_id TEXT UNIQUE NOT NULL,
    gmail_thread_id  TEXT,
    attachments JSONB,
    is_reply    BOOLEAN DEFAULT FALSE,
    sender_type TEXT,               -- 'internal' | 'external'
    has_attachments BOOLEAN DEFAULT FALSE,
    kg_processed    BOOLEAN DEFAULT FALSE,
    embedding   vector(1024),       -- Voyage AI embeddings
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_emails_account ON emails(account);
CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(email_date DESC);
CREATE INDEX IF NOT EXISTS idx_emails_thread ON emails(gmail_thread_id);
CREATE INDEX IF NOT EXISTS idx_emails_kg ON emails(kg_processed) WHERE NOT kg_processed;

-- ================================================================
-- 2. THREADS
-- ================================================================
CREATE TABLE IF NOT EXISTS threads (
    id              BIGSERIAL PRIMARY KEY,
    gmail_thread_id TEXT UNIQUE NOT NULL,
    subject         TEXT,
    subject_normalized TEXT,
    started_by      TEXT,
    started_by_type TEXT,
    started_at      TIMESTAMPTZ,
    last_activity   TIMESTAMPTZ,
    status          TEXT DEFAULT 'active',
    message_count   INTEGER DEFAULT 1,
    participant_emails JSONB,
    has_internal_reply BOOLEAN DEFAULT FALSE,
    has_external_reply BOOLEAN DEFAULT FALSE,
    last_sender     TEXT,
    last_sender_type TEXT,
    hours_without_response REAL DEFAULT 0,
    account         TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_threads_account ON threads(account);
CREATE INDEX IF NOT EXISTS idx_threads_status ON threads(status);

-- ================================================================
-- 3. CONTACTS
-- ================================================================
CREATE TABLE IF NOT EXISTS contacts (
    id              BIGSERIAL PRIMARY KEY,
    email           TEXT UNIQUE NOT NULL,
    name            TEXT,
    company         TEXT,
    contact_type    TEXT,            -- 'client' | 'supplier' | 'internal' | 'other'
    department      TEXT,
    risk_level      TEXT DEFAULT 'low', -- 'high' | 'medium' | 'low'
    sentiment_score REAL,
    relationship_score REAL,
    last_interaction TIMESTAMPTZ,
    total_emails    INTEGER DEFAULT 0,
    tags            JSONB DEFAULT '[]'::jsonb,
    last_score_date DATE,
    score_breakdown JSONB,
    odoo_partner_id INTEGER,
    phone           TEXT,
    city            TEXT,
    country         TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contacts_type ON contacts(contact_type);
CREATE INDEX IF NOT EXISTS idx_contacts_risk ON contacts(risk_level);

-- ================================================================
-- 4. ALERTS
-- ================================================================
CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL PRIMARY KEY,
    alert_type  TEXT NOT NULL,      -- 'no_response' | 'sentiment' | 'opportunity' | 'risk' | 'accountability' | 'communication_gap'
    severity    TEXT NOT NULL DEFAULT 'medium', -- 'critical' | 'high' | 'medium' | 'low'
    title       TEXT NOT NULL,
    description TEXT,
    contact_name TEXT,
    contact_id  BIGINT REFERENCES contacts(id),
    account     TEXT,
    state       TEXT DEFAULT 'new', -- 'new' | 'acknowledged' | 'resolved'
    is_read     BOOLEAN DEFAULT FALSE,
    is_resolved BOOLEAN DEFAULT FALSE,
    alert_date  DATE,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_state ON alerts(state);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_date ON alerts(alert_date DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON alerts(is_resolved) WHERE NOT is_resolved;

-- ================================================================
-- 5. ACTION ITEMS
-- ================================================================
CREATE TABLE IF NOT EXISTS action_items (
    id              BIGSERIAL PRIMARY KEY,
    action_type     TEXT,
    description     TEXT NOT NULL,
    contact_name    TEXT,
    contact_id      BIGINT REFERENCES contacts(id),
    priority        TEXT DEFAULT 'medium', -- 'high' | 'medium' | 'low'
    due_date        DATE,
    state           TEXT DEFAULT 'pending',
    status          TEXT DEFAULT 'pending', -- 'pending' | 'completed'
    assignee_email  TEXT,
    completed_date  DATE,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_actions_status ON action_items(status);
CREATE INDEX IF NOT EXISTS idx_actions_assignee ON action_items(assignee_email);

-- ================================================================
-- 6. BRIEFINGS
-- ================================================================
CREATE TABLE IF NOT EXISTS briefings (
    id              BIGSERIAL PRIMARY KEY,
    briefing_type   TEXT DEFAULT 'daily', -- 'daily' | 'weekly' | 'account' | 'strategic'
    period_start    TIMESTAMPTZ,
    period_end      TIMESTAMPTZ,
    summary         TEXT,
    html_content    TEXT,
    account_email   TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_briefings_type ON briefings(briefing_type);
CREATE INDEX IF NOT EXISTS idx_briefings_date ON briefings(created_at DESC);

-- ================================================================
-- 7. RESPONSE METRICS
-- ================================================================
CREATE TABLE IF NOT EXISTS response_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    account             TEXT NOT NULL,
    metric_date         DATE NOT NULL,
    emails_received     INTEGER DEFAULT 0,
    emails_sent         INTEGER DEFAULT 0,
    internal_received   INTEGER DEFAULT 0,
    external_received   INTEGER DEFAULT 0,
    threads_started     INTEGER DEFAULT 0,
    threads_replied     INTEGER DEFAULT 0,
    threads_unanswered  INTEGER DEFAULT 0,
    avg_response_hours  REAL,
    fastest_response_hours REAL,
    slowest_response_hours REAL,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    UNIQUE(metric_date, account)
);

CREATE INDEX IF NOT EXISTS idx_metrics_date ON response_metrics(metric_date DESC);

-- ================================================================
-- 8. ACCOUNT SUMMARIES
-- ================================================================
CREATE TABLE IF NOT EXISTS account_summaries (
    id              BIGSERIAL PRIMARY KEY,
    summary_date    DATE NOT NULL,
    account         TEXT NOT NULL,
    department      TEXT,
    total_emails    INTEGER DEFAULT 0,
    external_emails INTEGER DEFAULT 0,
    internal_emails INTEGER DEFAULT 0,
    key_items       JSONB DEFAULT '[]'::jsonb,
    waiting_response JSONB DEFAULT '[]'::jsonb,
    urgent_items    JSONB DEFAULT '[]'::jsonb,
    external_contacts JSONB DEFAULT '[]'::jsonb,
    topics_detected JSONB DEFAULT '[]'::jsonb,
    summary_text    TEXT,
    overall_sentiment TEXT,
    sentiment_detail JSONB,
    risks_detected  JSONB,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(summary_date, account)
);

-- ================================================================
-- 9. DAILY SUMMARIES
-- ================================================================
CREATE TABLE IF NOT EXISTS daily_summaries (
    id              BIGSERIAL PRIMARY KEY,
    summary_date    DATE UNIQUE NOT NULL,
    summary_html    TEXT,
    summary_text    TEXT,
    total_emails    INTEGER DEFAULT 0,
    accounts_read   INTEGER DEFAULT 0,
    accounts_failed INTEGER DEFAULT 0,
    topics_identified INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ================================================================
-- 10. ENTITIES (Knowledge Graph)
-- ================================================================
CREATE TABLE IF NOT EXISTS entities (
    id              BIGSERIAL PRIMARY KEY,
    entity_type     TEXT NOT NULL DEFAULT 'person', -- 'person' | 'company' | 'product' | etc.
    name            TEXT NOT NULL,
    canonical_name  TEXT NOT NULL,
    email           TEXT,
    attributes      JSONB DEFAULT '{}'::jsonb,
    last_seen       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(entity_type, canonical_name)
);

CREATE INDEX IF NOT EXISTS idx_entities_email ON entities(email);

-- ================================================================
-- 11. ENTITY MENTIONS
-- ================================================================
CREATE TABLE IF NOT EXISTS entity_mentions (
    id          BIGSERIAL PRIMARY KEY,
    email_id    BIGINT REFERENCES emails(id) ON DELETE CASCADE,
    entity_id   BIGINT REFERENCES entities(id) ON DELETE CASCADE,
    context     TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mentions_entity ON entity_mentions(entity_id);
CREATE INDEX IF NOT EXISTS idx_mentions_email ON entity_mentions(email_id);

-- ================================================================
-- 12. ENTITY RELATIONSHIPS
-- ================================================================
CREATE TABLE IF NOT EXISTS entity_relationships (
    id                  BIGSERIAL PRIMARY KEY,
    entity_a_id         BIGINT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    entity_b_id         BIGINT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    relationship_type   TEXT NOT NULL,
    confidence          REAL DEFAULT 0.5,
    metadata            JSONB DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    UNIQUE(entity_a_id, entity_b_id, relationship_type)
);

-- ================================================================
-- 13. FACTS
-- ================================================================
CREATE TABLE IF NOT EXISTS facts (
    id          BIGSERIAL PRIMARY KEY,
    fact_text   TEXT NOT NULL,
    source_type TEXT,
    confidence  REAL DEFAULT 0.5,
    email_id    BIGINT REFERENCES emails(id) ON DELETE SET NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- ================================================================
-- 14. PERSON PROFILES (cumulative learning)
-- ================================================================
CREATE TABLE IF NOT EXISTS person_profiles (
    id                  BIGSERIAL PRIMARY KEY,
    canonical_key       TEXT UNIQUE NOT NULL,
    name                TEXT,
    email               TEXT,
    company             TEXT,
    role                TEXT,
    department          TEXT,
    decision_power      TEXT DEFAULT 'medium',   -- 'high' | 'medium' | 'low'
    communication_style TEXT DEFAULT 'formal',
    language_preference TEXT DEFAULT 'es',
    key_interests       JSONB DEFAULT '[]'::jsonb,
    personality_notes   TEXT,
    negotiation_style   TEXT,
    response_pattern    TEXT,
    influence_on_deals  TEXT,
    source_account      TEXT,
    last_seen_date      TIMESTAMPTZ,
    interaction_count   INTEGER DEFAULT 0,
    personality_traits  JSONB DEFAULT '[]'::jsonb,
    interests           JSONB DEFAULT '[]'::jsonb,
    decision_factors    JSONB DEFAULT '[]'::jsonb,
    summary             TEXT,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_profiles_email ON person_profiles(email);

-- ================================================================
-- 15. TOPICS
-- ================================================================
CREATE TABLE IF NOT EXISTS topics (
    id              BIGSERIAL PRIMARY KEY,
    topic           TEXT NOT NULL,
    category        TEXT,
    status          TEXT DEFAULT 'active',
    priority        TEXT DEFAULT 'medium',
    summary         TEXT,
    related_accounts JSONB DEFAULT '[]'::jsonb,
    embedding       vector(1024),
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ================================================================
-- 16. SYNC STATE (Gmail history tracking)
-- ================================================================
CREATE TABLE IF NOT EXISTS sync_state (
    id              BIGSERIAL PRIMARY KEY,
    account         TEXT UNIQUE NOT NULL,
    last_history_id TEXT,
    emails_synced   INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ================================================================
-- 17. COMMUNICATION PATTERNS
-- ================================================================
CREATE TABLE IF NOT EXISTS communication_patterns (
    id          BIGSERIAL PRIMARY KEY,
    week_start  DATE NOT NULL,
    account     TEXT NOT NULL,
    data        JSONB DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE(week_start, account)
);

-- ================================================================
-- 18. SYSTEM LEARNING
-- ================================================================
CREATE TABLE IF NOT EXISTS system_learning (
    id              BIGSERIAL PRIMARY KEY,
    learning_type   TEXT NOT NULL,
    description     TEXT,
    data            JSONB DEFAULT '{}'::jsonb,
    account         TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- ================================================================
-- RPC FUNCTIONS
-- ================================================================

-- Upsert contact
CREATE OR REPLACE FUNCTION upsert_contact(
    p_email TEXT,
    p_name TEXT DEFAULT '',
    p_contact_type TEXT DEFAULT 'other',
    p_department TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO contacts (email, name, contact_type, department)
    VALUES (p_email, p_name, p_contact_type, p_department)
    ON CONFLICT (email) DO UPDATE SET
        name = COALESCE(NULLIF(EXCLUDED.name, ''), contacts.name),
        contact_type = COALESCE(EXCLUDED.contact_type, contacts.contact_type),
        department = COALESCE(EXCLUDED.department, contacts.department),
        updated_at = now();
END;
$$ LANGUAGE plpgsql;

-- Account scorecard (last N days)
CREATE OR REPLACE FUNCTION get_account_scorecard(p_days INTEGER DEFAULT 7)
RETURNS TABLE (
    account TEXT,
    total_received BIGINT,
    total_sent BIGINT,
    avg_response_hrs REAL,
    unanswered BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        rm.account,
        SUM(rm.emails_received)::BIGINT AS total_received,
        SUM(rm.emails_sent)::BIGINT AS total_sent,
        AVG(rm.avg_response_hours)::REAL AS avg_response_hrs,
        SUM(rm.threads_unanswered)::BIGINT AS unanswered
    FROM response_metrics rm
    WHERE rm.metric_date >= CURRENT_DATE - p_days
    GROUP BY rm.account
    ORDER BY total_received DESC;
END;
$$ LANGUAGE plpgsql;

-- Semantic email search (RAG)
CREATE OR REPLACE FUNCTION search_similar_emails(
    query_embedding vector(1024),
    match_threshold REAL DEFAULT 0.3,
    match_count INTEGER DEFAULT 15
) RETURNS TABLE (
    id BIGINT,
    gmail_message_id TEXT,
    subject TEXT,
    from_email TEXT,
    body TEXT,
    snippet TEXT,
    date TEXT,
    similarity REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.id,
        e.gmail_message_id,
        e.subject,
        e.sender AS from_email,
        e.body,
        e.snippet,
        e.email_date::TEXT AS date,
        (1 - (e.embedding <=> query_embedding))::REAL AS similarity
    FROM emails e
    WHERE e.embedding IS NOT NULL
      AND (1 - (e.embedding <=> query_embedding)) > match_threshold
    ORDER BY e.embedding <=> query_embedding
    LIMIT match_count;
END;
$$ LANGUAGE plpgsql;

-- Entity intelligence lookup
CREATE OR REPLACE FUNCTION get_entity_intelligence(
    p_email TEXT DEFAULT NULL,
    p_name TEXT DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
    result JSONB;
    entity_rec RECORD;
    profile_rec RECORD;
BEGIN
    -- Find entity
    IF p_email IS NOT NULL THEN
        SELECT * INTO entity_rec FROM entities WHERE email = p_email LIMIT 1;
    ELSIF p_name IS NOT NULL THEN
        SELECT * INTO entity_rec FROM entities WHERE canonical_name = lower(trim(p_name)) LIMIT 1;
    END IF;

    -- Find person profile
    IF p_email IS NOT NULL THEN
        SELECT * INTO profile_rec FROM person_profiles WHERE canonical_key = lower(trim(p_email)) LIMIT 1;
    ELSIF p_name IS NOT NULL THEN
        SELECT * INTO profile_rec FROM person_profiles WHERE canonical_key = lower(trim(p_name)) LIMIT 1;
    END IF;

    result := jsonb_build_object(
        'entity', CASE WHEN entity_rec IS NOT NULL THEN row_to_json(entity_rec)::jsonb ELSE NULL END,
        'profile', CASE WHEN profile_rec IS NOT NULL THEN row_to_json(profile_rec)::jsonb ELSE NULL END,
        'recent_emails', (
            SELECT COALESCE(jsonb_agg(row_to_json(e)::jsonb), '[]'::jsonb)
            FROM (
                SELECT e2.subject, e2.sender, e2.email_date, e2.snippet
                FROM emails e2
                WHERE (p_email IS NOT NULL AND (e2.sender ILIKE '%' || p_email || '%' OR e2.recipient ILIKE '%' || p_email || '%'))
                   OR (p_name IS NOT NULL AND (e2.sender ILIKE '%' || p_name || '%'))
                ORDER BY e2.email_date DESC
                LIMIT 10
            ) e
        ),
        'contact', (
            SELECT row_to_json(c)::jsonb
            FROM contacts c
            WHERE (p_email IS NOT NULL AND c.email = p_email)
               OR (p_name IS NOT NULL AND c.name ILIKE '%' || p_name || '%')
            LIMIT 1
        )
    );

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Get pending actions for user
CREATE OR REPLACE FUNCTION get_my_pending_actions(p_assignee_email TEXT)
RETURNS TABLE (
    id BIGINT,
    action_type TEXT,
    description TEXT,
    contact_name TEXT,
    priority TEXT,
    due_date DATE,
    created_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ai.id,
        ai.action_type,
        ai.description,
        ai.contact_name,
        ai.priority,
        ai.due_date,
        ai.created_at
    FROM action_items ai
    WHERE ai.assignee_email = p_assignee_email
      AND ai.status = 'pending'
    ORDER BY
        CASE ai.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
        ai.due_date ASC NULLS LAST;
END;
$$ LANGUAGE plpgsql;

-- Upsert topic
CREATE OR REPLACE FUNCTION upsert_topic(
    p_topic TEXT,
    p_category TEXT DEFAULT '',
    p_status TEXT DEFAULT 'active',
    p_priority TEXT DEFAULT 'medium',
    p_summary TEXT DEFAULT '',
    p_related_accounts JSONB DEFAULT NULL,
    p_embedding vector(1024) DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO topics (topic, category, status, priority, summary, related_accounts, embedding)
    VALUES (p_topic, p_category, p_status, p_priority, p_summary,
            COALESCE(p_related_accounts, '[]'::jsonb), p_embedding)
    ON CONFLICT (topic) DO UPDATE SET  -- requires unique on topic
        category = COALESCE(EXCLUDED.category, topics.category),
        status = EXCLUDED.status,
        priority = EXCLUDED.priority,
        summary = EXCLUDED.summary,
        related_accounts = COALESCE(EXCLUDED.related_accounts, topics.related_accounts),
        embedding = COALESCE(EXCLUDED.embedding, topics.embedding),
        updated_at = now();
END;
$$ LANGUAGE plpgsql;

-- Add unique constraint for topics upsert
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'topics_topic_key') THEN
        ALTER TABLE topics ADD CONSTRAINT topics_topic_key UNIQUE (topic);
    END IF;
END $$;

-- ================================================================
-- AUTO-UPDATE updated_at TRIGGER
-- ================================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to all tables with updated_at
DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'emails', 'threads', 'contacts', 'alerts', 'action_items',
            'briefings', 'response_metrics', 'account_summaries',
            'daily_summaries', 'entities', 'entity_relationships',
            'person_profiles', 'topics', 'sync_state', 'communication_patterns'
        ])
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_updated_at ON %I; '
            'CREATE TRIGGER trg_updated_at BEFORE UPDATE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION update_updated_at();',
            tbl, tbl
        );
    END LOOP;
END $$;

-- ================================================================
-- ROW LEVEL SECURITY (RLS)
-- ================================================================
-- Enable RLS on all tables (policies use service_role key which bypasses RLS)
-- For the frontend with anon key, restrict to read-only on specific tables

ALTER TABLE emails ENABLE ROW LEVEL SECURITY;
ALTER TABLE threads ENABLE ROW LEVEL SECURITY;
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE alerts ENABLE ROW LEVEL SECURITY;
ALTER TABLE action_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE briefings ENABLE ROW LEVEL SECURITY;
ALTER TABLE response_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE account_summaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE daily_summaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE entities ENABLE ROW LEVEL SECURITY;
ALTER TABLE entity_mentions ENABLE ROW LEVEL SECURITY;
ALTER TABLE entity_relationships ENABLE ROW LEVEL SECURITY;
ALTER TABLE facts ENABLE ROW LEVEL SECURITY;
ALTER TABLE person_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE topics ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE communication_patterns ENABLE ROW LEVEL SECURITY;
ALTER TABLE system_learning ENABLE ROW LEVEL SECURITY;

-- Service role (backend) gets full access - these bypass RLS automatically
-- Anon key (frontend) gets read-only on dashboard-facing tables

CREATE POLICY "anon_read_alerts" ON alerts FOR SELECT TO anon USING (true);
CREATE POLICY "anon_update_alerts" ON alerts FOR UPDATE TO anon USING (true) WITH CHECK (true);
CREATE POLICY "anon_read_action_items" ON action_items FOR SELECT TO anon USING (true);
CREATE POLICY "anon_update_action_items" ON action_items FOR UPDATE TO anon USING (true) WITH CHECK (true);
CREATE POLICY "anon_read_briefings" ON briefings FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_contacts" ON contacts FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_daily_summaries" ON daily_summaries FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_response_metrics" ON response_metrics FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_topics" ON topics FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_person_profiles" ON person_profiles FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_entities" ON entities FOR SELECT TO anon USING (true);

-- ================================================================
-- EMBEDDING INDEX (for fast vector search)
-- ================================================================
CREATE INDEX IF NOT EXISTS idx_emails_embedding
    ON emails USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
