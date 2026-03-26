"""
Quimibond Intelligence — Supabase Metrics Mixin
Metrics, scores, alerts, and analytics for Supabase.
"""
import logging
import uuid
from datetime import datetime
from urllib.parse import quote as url_quote

from .enrichment_helpers import is_automated_sender
from .supabase_utils import _postgrest_in_list

_logger = logging.getLogger(__name__)


class SupabaseMetricsMixin:
    """Metrics, scores, alerts, and analytics for Supabase."""

    # ── Métricas ─────────────────────────────────────────────────────────────

    def save_metrics(self, metrics: list, today: str):
        batch = [{
            'account': m['account'],
            'metric_date': today,
            'emails_received': m['emails_received'],
            'emails_sent': m['emails_sent'],
            'internal_received': m['internal_received'],
            'external_received': m['external_received'],
            'threads_started': m.get('threads_started', 0),
            'threads_replied': m['threads_replied'],
            'threads_unanswered': m['threads_unanswered'],
            'avg_response_hours': m.get('avg_response_hours'),
            'fastest_response_hours': m.get('fastest_response_hours'),
            'slowest_response_hours': m.get('slowest_response_hours'),
        } for m in metrics]
        if batch:
            self._upsert_batch('/rest/v1/communication_metrics?on_conflict=metric_date,account',
                               batch, 'merge-duplicates')
            self._track(success=len(batch))
        _logger.info('✓ %d métricas guardadas', len(batch))

    # ── Alertas ──────────────────────────────────────────────────────────────

    def save_alerts(self, alerts: list, today: str):
        """Guarda alertas resolviendo contact_id desde contact_name.

        El frontend filtra alertas por contact_id en la ficha de contacto,
        así que es importante que este campo esté presente.

        También genera prediction_id para cada alerta y registra las predicciones.
        Deduplicación: agrupa por (contact_name, alert_type, title) y mantiene
        la de mayor severidad.
        """
        if not alerts:
            return

        # ── Dedup: keep highest severity per (contact, type, title) ──
        severity_rank = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
        seen = {}  # (contact_name, alert_type, title) → alert
        for a in alerts:
            key = (a.get('contact_name', ''), a.get('alert_type', ''), a.get('title', ''))
            existing = seen.get(key)
            if not existing:
                seen[key] = a
            else:
                # Keep higher severity
                if severity_rank.get(a.get('severity', ''), 0) > severity_rank.get(existing.get('severity', ''), 0):
                    seen[key] = a
        alerts = list(seen.values())
        _logger.debug('save_alerts: %d alertas after dedup (from %d)', len(alerts), len(seen))

        # Resolve contact_name → contact_id + company_id in batch (1 call)
        contact_names = list({
            a.get('contact_name', '') for a in alerts
            if a.get('contact_name')
        })
        name_to_contact = {}  # name → {id, company_id}
        if contact_names:
            try:
                enc = _postgrest_in_list(contact_names)
                if enc:
                    rows = self._request(
                        f'/rest/v1/contacts?name=in.({enc})'
                        '&select=id,name,company_id',
                    ) or []
                    for r in rows:
                        n = r.get('name')
                        if n:
                            name_to_contact[n] = r
            except Exception as exc:
                _logger.warning('batch resolve contact names: %s', exc)

        records = []
        predictions = []
        for a in alerts:
            prediction_id = str(uuid.uuid4())
            prediction_confidence = a.get('confidence', 0.5)

            record = {
                'alert_type': a.get('alert_type', 'risk'),
                'severity': a.get('severity', 'medium'),
                'title': a.get('title', ''),
                'description': a.get('description', ''),
                'contact_name': a.get('contact_name'),
                'account': a.get('account'),
                'state': 'new',
                'is_read': False,
                'prediction_confidence': prediction_confidence,
            }
            # Business context
            if a.get('business_impact'):
                record['business_impact'] = a['business_impact']
            if a.get('suggested_action'):
                record['suggested_action'] = a['suggested_action']
            # Include thread reference if available
            thread_id = a.get('related_thread_id') or a.get('thread_id')
            if thread_id:
                record['thread_id'] = thread_id
            # Resolve contact_id + company_id from contact_name
            contact_info = name_to_contact.get(a.get('contact_name'))
            if contact_info:
                record['contact_id'] = contact_info.get('id')
                if contact_info.get('company_id'):
                    record['company_id'] = contact_info['company_id']
            records.append(record)

            predictions.append({
                'alert': record,
                'prediction_id': prediction_id,
                'prediction_confidence': prediction_confidence,
                'today': today,
            })

        try:
            result = self._request(
                '/rest/v1/alerts', 'POST', records,
                extra_headers={'Prefer': 'return=representation'},
            )
            # Merge Supabase IDs back into original alert dicts
            if result and isinstance(result, list):
                for alert_dict, created in zip(alerts, result):
                    alert_dict['supabase_id'] = created.get('id')
            self._track(success=len(records))
            _logger.info('✓ %d alertas guardadas', len(records))
        except Exception as exc:
            self._track(failed=len(records))
            _logger.error('save_alerts POST: %s', exc)

    # ── Account Summaries ────────────────────────────────────────────────────

    def save_account_summaries(self, summaries: list, today: str):
        batch = [{
            'scope': 'account',
            'briefing_date': today,
            'account': s['account'],
            'department': s['department'],
            'total_emails': s.get('total_emails', 0),
            'external_emails': s.get('external_emails', 0),
            'internal_emails': s.get('internal_emails', 0),
            'waiting_response': s.get('waiting_response', []),
            'urgent_items': s.get('urgent_items', []),
            'external_contacts': s.get('external_contacts', []),
            'topics_identified': s.get('topics_detected', []),
            'summary_text': s.get('summary_text', ''),
            'overall_sentiment': s.get('overall_sentiment'),
            'sentiment_detail': s.get('sentiment_detail'),
            'risks_detected': s.get('risks_detected'),
        } for s in summaries]
        for record in batch:
            self._request(
                '/rest/v1/briefings', 'POST', record,
                extra_headers={'Prefer': 'resolution=merge-duplicates'},
            )
        _logger.info('✓ %d account briefings guardados', len(batch))

    # ── Client Scores ────────────────────────────────────────────────────────

    def save_client_scores(self, scores: list, today: str,
                           contact_sentiments: dict = None):
        """Actualiza contacts con relationship_score, risk_level Y sentiment_score.

        contact_sentiments: dict mapping email → sentiment_score (-1 to 1)
        from Claude analysis. This fills contacts.sentiment_score which the
        frontend uses for health bars.

        Uses per-email PATCH (PostgREST doesn't support bulk PATCH with
        different values per row), but tracks success/failure counts.
        """
        contact_sentiments = contact_sentiments or {}
        now_iso = datetime.now().isoformat()
        saved = 0
        failed = 0
        for s in scores:
            try:
                # Clean email: take first valid email if multi-email
                raw_email = s.get("email", "").strip().lower()
                clean = raw_email.split(';')[0].split(',')[0].split()[0].strip()
                if not clean or '@' not in clean:
                    continue
                encoded_email = url_quote(clean, safe='')
                patch = {
                    'relationship_score': s['total_score'],
                    'risk_level': s['risk_level'],
                    'updated_at': now_iso,
                }
                # Payment compliance score (0-20)
                if 'payment_compliance_score' in s:
                    patch['payment_compliance_score'] = s[
                        'payment_compliance_score']
                # Include Claude's sentiment_score (-1 to 1) if available
                raw_sentiment = contact_sentiments.get(clean)
                if raw_sentiment is not None:
                    try:
                        patch['sentiment_score'] = round(
                            float(raw_sentiment), 2,
                        )
                    except (ValueError, TypeError):
                        pass
                self._request(
                    f'/rest/v1/contacts?email=eq.{encoded_email}',
                    'PATCH', patch,
                )
                saved += 1
            except Exception as exc:
                failed += 1
                _logger.warning('save_client_score %s: %s', s.get('email'), exc)
        self._track(success=saved, failed=failed)
        _logger.info('✓ %d client scores guardados (%d fallidos)', saved, failed)

    # ── Daily Summary ────────────────────────────────────────────────────────

    def save_daily_summary(self, today: str, briefing_html: str,
                           total_emails: int, accounts_read: int,
                           accounts_failed: int, topics_count: int,
                           key_events: list = None):
        """Guarda resumen diario con key_events estructurados.

        Schema: briefings(scope, briefing_date, summary_text, summary_html,
                          total_emails, key_events jsonb, ...)
        """
        import re
        summary_text = re.sub(r'<[^>]+>', '', briefing_html)
        # Truncate summary_text to a reasonable size for the summary field
        summary_short = summary_text[:2000] if len(summary_text) > 2000 else summary_text

        self._request(
            '/rest/v1/briefings',
            'POST',
            {
                'scope': 'daily',
                'briefing_date': today,
                'total_emails': total_emails,
                'summary_text': summary_short,
                'summary_html': briefing_html[:50000] if briefing_html else '',
                'accounts_processed': accounts_read,
                'accounts_failed': accounts_failed,
                'topics_identified': topics_count,
                'key_events': key_events or [],
            },
            extra_headers={
                'Prefer': 'resolution=merge-duplicates',
            },
        )

    # ── Historical Context ───────────────────────────────────────────────────

    def get_historical_context(self) -> dict:
        """Obtiene contexto histórico para el briefing."""
        ctx = {
            'previousSummary': None,
            'openAlerts': [],
            'scorecard': [],
            'learnings': [],
            'volumeTrend': [],
        }
        try:
            summaries = self._request(
                '/rest/v1/briefings?scope=eq.daily&order=briefing_date.desc&limit=1'
                '&select=summary_text',
            )
            if summaries:
                ctx['previousSummary'] = summaries[0].get('summary_text', '')
        except Exception:
            pass
        try:
            ctx['openAlerts'] = self._request(
                '/rest/v1/alerts?state=neq.resolved&order=created_at.desc'
                '&limit=20&select=alert_type,severity,title,account',
            ) or []
        except Exception:
            pass
        try:
            ctx['scorecard'] = self._request(
                '/rest/v1/rpc/get_account_scorecard', 'POST', {'p_days': 7},
            ) or []
        except Exception:
            pass
        return ctx

    # ── Action Items ─────────────────────────────────────────────────────────

    def save_action_item(self, item):
        """Guarda un action item.

        Genera prediction_id para registrar la predicción de acción.
        Resuelve company_id desde contact_name si disponible.
        Returns created record list (for supabase_id capture).
        """
        # Resolve company_id from contact_name
        contact_name = item.get('contact_name')
        if contact_name and 'company_id' not in item:
            try:
                encoded = url_quote(contact_name, safe='')
                resp = self._request(
                    f'/rest/v1/contacts?name=eq.{encoded}'
                    '&select=company_id',
                )
                if resp and isinstance(resp, list) and resp:
                    cid = resp[0].get('company_id')
                    if cid:
                        item['company_id'] = cid
            except Exception:
                pass

        prediction_confidence = item.get('confidence', 0.5)

        # Only send columns that exist in the action_items table
        record = {
            'action_type': item.get('action_type', 'other'),
            'action_category': item.get('action_category'),
            'description': item.get('description', ''),
            'reason': item.get('reason'),
            'priority': item.get('priority', 'medium'),
            'contact_name': item.get('contact_name'),
            'contact_company': item.get('contact_company'),
            'contact_id': item.get('contact_id'),
            'company_id': item.get('company_id'),
            'thread_id': item.get('thread_id'),
            'assignee_name': item.get('assignee_name'),
            'assignee_email': item.get('assignee_email'),
            'state': item.get('state', 'pending'),
            'due_date': item.get('due_date'),
            'prediction_confidence': prediction_confidence,
        }
        # Remove None values
        record = {k: v for k, v in record.items() if v is not None}

        result = None
        try:
            result = self._request(
                '/rest/v1/action_items', 'POST', [record],
                extra_headers={'Prefer': 'return=representation'},
            )
        except Exception as exc:
            _logger.warning('save_action_item POST: %s', exc)

        return result

    def get_pending_actions(self, email):
        """Obtiene action items pendientes."""
        return self._request(
            '/rest/v1/rpc/get_my_pending_actions',
            'POST',
            {'p_assignee_email': email},
        )

    # ── Topics ────────────────────────────────────────────────────────────────

    def save_topics(self, topics: list, today: str):
        """Guarda temas detectados en Supabase via RPC upsert_topic."""
        for t in topics:
            try:
                params = {
                    'p_topic': t.get('topic', ''),
                    'p_category': t.get('category', ''),
                    'p_status': t.get('status', 'active'),
                    'p_priority': t.get('priority', 'medium'),
                    'p_summary': t.get('summary', ''),
                }
                # Only include array/vector params if they have values;
                # NULL doesn't cast cleanly to text[] / vector in PostgREST
                accounts = t.get('related_accounts')
                if accounts:
                    params['p_related_accounts'] = accounts
                self._request('/rest/v1/rpc/upsert_topic', 'POST', params)
            except Exception as exc:
                _logger.debug('save_topic: %s', exc)

    # ── Action Items (update status) ──────────────────────────────────────────

    def complete_action_item(self, action_id: int):
        """Marca un action item como completado en Supabase."""
        try:
            now = datetime.now()
            self._request(
                f'/rest/v1/action_items?id=eq.{action_id}',
                'PATCH', {
                    'state': 'completed',
                    'completed_at': now.isoformat(),
                },
            )
        except Exception as exc:
            _logger.debug('complete_action: %s', exc)

    def update_alert_state_by_id(self, alert_id: int, state: str,
                                resolution_notes: str = None):
        """Actualiza estado de una alerta en Supabase por ID."""
        try:
            patch = {
                'state': state,
            }
            if state == 'resolved':
                patch['resolved_at'] = datetime.now().isoformat()
            if resolution_notes:
                patch['resolution_notes'] = resolution_notes
            self._request(
                f'/rest/v1/alerts?id=eq.{alert_id}',
                'PATCH', patch,
            )
        except Exception as exc:
            _logger.debug('update_alert_state_by_id: %s', exc)

    def update_alert_state(self, alert_title: str, state: str,
                           resolution_notes: str = None):
        """Actualiza estado de una alerta en Supabase."""
        try:
            from urllib.parse import quote as _quote
            encoded = _quote(alert_title[:200], safe='')
            patch = {
                'state': state,
            }
            if state == 'resolved':
                patch['resolved_at'] = datetime.now().isoformat()
            if resolution_notes:
                patch['resolution_notes'] = resolution_notes
            self._request(
                f'/rest/v1/alerts?title=eq.{encoded}',
                'PATCH', patch,
            )
        except Exception as exc:
            _logger.debug('update_alert_state: %s', exc)

    # ── Revenue Metrics ──────────────────────────────────────────────────────

    def save_revenue_metrics(self, metrics: dict):
        """Guarda métricas de revenue para un contacto.

        Upsert por (contact_email, period_start, period_type).
        Resuelve contact_id y company_id desde contact_email para FK integrity.
        """
        try:
            # Resolve contact_id + company_id from contact_email
            email = metrics.get('contact_email', '')
            if email and 'contact_id' not in metrics:
                try:
                    encoded = url_quote(email, safe='')
                    resp = self._request(
                        f'/rest/v1/contacts?email=eq.{encoded}'
                        '&select=id,company_id',
                    )
                    if resp and isinstance(resp, list) and resp:
                        metrics['contact_id'] = resp[0]['id']
                        if resp[0].get('company_id'):
                            metrics['company_id'] = resp[0]['company_id']
                except Exception:
                    pass  # FK is optional, proceed without it
            self._request(
                '/rest/v1/revenue_metrics',
                'POST', metrics,
                extra_headers={
                    'Prefer': 'resolution=merge-duplicates',
                },
            )
        except Exception as exc:
            _logger.warning('save_revenue_metrics: %s', exc)

    def save_revenue_metrics_batch(self, metrics_list: list,
                                    _contact_cache: dict = None):
        """Batch-save revenue metrics (1 upsert instead of N).

        Pre-resolves contact_id/company_id from a shared cache or batch query.
        """
        if not metrics_list:
            return

        # Batch-resolve contact_id + company_id
        emails_needing_resolve = [
            m['contact_email'] for m in metrics_list
            if m.get('contact_email') and 'contact_id' not in m
        ]
        contact_map = _contact_cache or {}
        if emails_needing_resolve and not contact_map:
            try:
                enc = _postgrest_in_list(
                    list(set(e.lower() for e in emails_needing_resolve)))
                if enc:
                    rows = self._request(
                        f'/rest/v1/contacts?email=in.({enc})'
                        '&select=id,email,company_id',
                    ) or []
                    for r in rows:
                        em = (r.get('email') or '').lower()
                        if em:
                            contact_map[em] = r
            except Exception:
                pass

        # Enrich metrics with resolved FKs
        for m in metrics_list:
            email = (m.get('contact_email') or '').lower()
            info = contact_map.get(email)
            if info and 'contact_id' not in m:
                m['contact_id'] = info['id']
                if info.get('company_id'):
                    m['company_id'] = info['company_id']

        try:
            self._upsert_batch(
                '/rest/v1/revenue_metrics'
                '?on_conflict=contact_email,period_start,period_type',
                metrics_list, 'merge-duplicates',
            )
            self._track(success=len(metrics_list))
            _logger.info('✓ %d revenue metrics guardados (batch)',
                         len(metrics_list))
        except Exception as exc:
            self._track(failed=len(metrics_list))
            _logger.warning('save_revenue_metrics_batch: %s', exc)

    # ── Customer Health Scores ───────────────────────────────────────────────

    def save_customer_health_score(self, score: dict):
        """Guarda un health score para un contacto.

        Upsert por (contact_email, score_date).
        """
        try:
            self._request(
                '/rest/v1/health_scores',
                'POST', score,
                extra_headers={
                    'Prefer': 'resolution=merge-duplicates',
                },
            )
        except Exception as exc:
            _logger.warning('save_customer_health_score: %s', exc)

    def compute_and_save_health_scores(self, contacts: list,
                                        account_summaries: list,
                                        today: str):
        """Calcula y guarda health scores para todos los contactos externos.

        Score components (each 0-100):
        - communication: based on total_sent, total_received, recency
        - financial: from revenue_metrics (invoiced, overdue, trend)
        - sentiment: from Claude analysis of emails
        - responsiveness: avg_response_time_hours
        - engagement: KG facts, topics, entity mentions

        Batches reads and writes to minimize Supabase calls.
        """
        if not contacts:
            return

        # Filter external contacts with email (skip automated senders)
        external = [
            c for c in contacts
            if c.get('contact_type') == 'external' and c.get('email')
            and not is_automated_sender(c['email'])
        ]
        if not external:
            return

        # Clean emails: take first valid email from multi-email strings
        ext_emails = []
        for c in external:
            raw = (c.get('email') or '').strip().lower()
            clean = raw.split(';')[0].split(',')[0].split()[0].strip()
            if clean and '@' in clean:
                c['email'] = clean  # Mutate in place for downstream use
                ext_emails.append(clean)
            else:
                continue

        # ── Batch-fetch contact data (company_id, payment_compliance) ──
        contact_data_map = {}  # email → {company_id, payment_compliance_score}
        chunk = 50
        try:
            for i in range(0, len(ext_emails), chunk):
                part = ext_emails[i:i + chunk]
                enc = _postgrest_in_list(part)
                if not enc:
                    continue
                rows = self._request(
                    f'/rest/v1/contacts?email=in.({enc})'
                    '&select=email,company_id,payment_compliance_score',
                ) or []
                for r in rows:
                    em = (r.get('email') or '').lower()
                    if em:
                        contact_data_map[em] = r
        except Exception as exc:
            _logger.debug('batch contact data for health: %s', exc)

        # Build sentiment map from account summaries
        sentiment_map = {}
        for s in (account_summaries or []):
            for ec in s.get('external_contacts', []):
                email_addr = (ec.get('email') or '').lower()
                if email_addr:
                    try:
                        sentiment_map[email_addr] = float(
                            ec.get('sentiment_score', 0),
                        )
                    except (ValueError, TypeError):
                        pass

        # ── Batch-fetch revenue_metrics (chunked) ──
        revenue_map = {}  # email → latest revenue record
        chunk = 50
        try:
            for i in range(0, len(ext_emails), chunk):
                part = ext_emails[i:i + chunk]
                enc = _postgrest_in_list(part)
                if not enc:
                    continue
                rev_rows = self._request(
                    f'/rest/v1/revenue_metrics?contact_email=in.({enc})'
                    '&order=period_start.desc'
                    '&select=contact_email,total_invoiced,overdue_amount',
                ) or []
                for rm in rev_rows:
                    em = (rm.get('contact_email') or '').lower()
                    if em and em not in revenue_map:
                        revenue_map[em] = rm
        except Exception as exc:
            _logger.debug('batch revenue_metrics: %s', exc)

        # ── Batch-fetch previous health scores (chunked) ──
        prev_scores_map = {}  # email → previous overall_score
        try:
            for i in range(0, len(ext_emails), chunk):
                part = ext_emails[i:i + chunk]
                enc = _postgrest_in_list(part)
                if not enc:
                    continue
                prev_rows = self._request(
                    f'/rest/v1/health_scores?contact_email=in.({enc})'
                    '&order=score_date.desc'
                    '&select=contact_email,overall_score',
                ) or []
                for ps in prev_rows:
                    em = (ps.get('contact_email') or '').lower()
                    if em and em not in prev_scores_map:
                        prev_scores_map[em] = float(
                            ps.get('overall_score', 0),
                        )
        except Exception as exc:
            _logger.debug('batch prev health_scores: %s', exc)

        # ── Batch-fetch engagement data (facts + relationships, chunked) ──
        engagement_map = {}  # email → {'facts': int, 'rels': int}
        try:
            ent_id_map = {}  # entity_id → email (accumulated across chunks)
            for i in range(0, len(ext_emails), chunk):
                part = ext_emails[i:i + chunk]
                enc = _postgrest_in_list(part)
                if not enc:
                    continue
                ent_rows = self._request(
                    f'/rest/v1/entities?email=in.({enc})'
                    '&select=id,email',
                ) or []
                for er in ent_rows:
                    if er.get('email') and er.get('id'):
                        ent_id_map[er['id']] = er['email'].lower()
                        engagement_map[er['email'].lower()] = {
                            'facts': 0, 'rels': 0,
                        }
            # Count facts and relationships (also chunked by entity IDs)
            if ent_id_map:
                all_ent_ids = list(ent_id_map.keys())
                ent_chunk = 50
                for i in range(0, len(all_ent_ids), ent_chunk):
                    part_ids = all_ent_ids[i:i + ent_chunk]
                    ent_ids = ','.join(str(eid) for eid in part_ids)
                    fact_rows = self._request(
                        f'/rest/v1/facts?entity_id=in.({ent_ids})'
                        '&expired=eq.false'
                        '&select=entity_id',
                    ) or []
                    for fr in fact_rows:
                        eid = fr.get('entity_id')
                        em = ent_id_map.get(eid)
                        if em and em in engagement_map:
                            engagement_map[em]['facts'] += 1
                    rel_rows = self._request(
                        f'/rest/v1/entity_relationships'
                        f'?or=(entity_a_id.in.({ent_ids}),'
                        f'entity_b_id.in.({ent_ids}))'
                        '&select=entity_a_id,entity_b_id',
                    ) or []
                    for rr in rel_rows:
                        for side in ('entity_a_id', 'entity_b_id'):
                            eid = rr.get(side)
                            em = ent_id_map.get(eid)
                            if em and em in engagement_map:
                                engagement_map[em]['rels'] += 1
                                break  # count once per relationship
        except Exception as exc:
            _logger.debug('batch engagement data: %s', exc)

        # ── Compute scores per contact ──
        scores_to_save = []
        for c in external:
            email_addr = c['email'].lower()

            try:
                # ── Communication score ──
                total_msgs = (c.get('total_sent', 0) or 0) + (
                    c.get('total_received', 0) or 0
                )
                comm_score = min(100, total_msgs * 5)  # 20+ msgs = 100

                # ── Financial score (from batch) ──
                fin_score = 50  # default neutral
                rm = revenue_map.get(email_addr)
                if rm:
                    invoiced = float(rm.get('total_invoiced', 0) or 0)
                    overdue = float(rm.get('overdue_amount', 0) or 0)
                    if invoiced > 0:
                        fin_score = min(100, 50 + invoiced / 10000)
                    if overdue > 0:
                        fin_score = max(
                            0,
                            fin_score - min(50, overdue / 5000),
                        )

                # ── Sentiment score ──
                raw_sentiment = sentiment_map.get(email_addr, 0)
                sent_score = max(0, min(100, (raw_sentiment + 1) * 50))

                # ── Responsiveness score ──
                resp_time = c.get('avg_response_time_hours')
                if resp_time and float(resp_time) > 0:
                    hours = float(resp_time)
                    resp_score = max(0, min(100, 100 - (hours * 0.6)))
                else:
                    resp_score = 50

                # ── Engagement score (from batch data) ──
                eng = engagement_map.get(email_addr)
                if eng:
                    engagement_score = min(
                        100, 30 + eng['facts'] * 10 + eng['rels'] * 15,
                    )
                else:
                    engagement_score = 50

                # ── Weighted overall score ──
                overall = (
                    comm_score * 0.25
                    + fin_score * 0.30
                    + sent_score * 0.15
                    + resp_score * 0.15
                    + engagement_score * 0.15
                )

                # ── Determine trend (from batch) ──
                trend = 'stable'
                prev_score = prev_scores_map.get(email_addr)
                if prev_score is not None:
                    delta = overall - prev_score
                    if delta < -15:
                        trend = 'critical'
                    elif delta < -5:
                        trend = 'declining'
                    elif delta > 5:
                        trend = 'improving'

                # ── Risk and opportunity signals ──
                risk_signals = []
                opportunity_signals = []
                if comm_score < 20:
                    risk_signals.append('low_communication')
                if fin_score < 30:
                    risk_signals.append('financial_risk')
                if sent_score < 30:
                    risk_signals.append('negative_sentiment')
                if resp_score < 25:
                    risk_signals.append('slow_responder')
                if comm_score > 80 and sent_score > 70:
                    opportunity_signals.append('highly_engaged')
                if fin_score > 80:
                    opportunity_signals.append('strong_revenue')

                score_record = {
                    'contact_email': email_addr,
                    'score_date': today,
                    'overall_score': round(overall, 1),
                    'trend': trend,
                    'communication_score': round(comm_score, 1),
                    'financial_score': round(fin_score, 1),
                    'sentiment_score': round(sent_score, 1),
                    'responsiveness_score': round(resp_score, 1),
                    'engagement_score': round(engagement_score, 1),
                    'risk_signals': risk_signals,
                    'opportunity_signals': opportunity_signals,
                }

                # All records must have same keys for PostgREST batch
                cd = contact_data_map.get(email_addr)
                score_record['company_id'] = (
                    cd.get('company_id') if cd else None)
                score_record['payment_compliance_score'] = (
                    cd.get('payment_compliance_score') if cd else None)
                score_record['previous_score'] = (
                    round(prev_score, 1) if prev_score is not None else None)

                scores_to_save.append(score_record)

            except Exception as exc:
                _logger.debug('health_score skip %s: %s', email_addr, exc)

        # ── Batch-save all health scores (1 call instead of N) ──
        if scores_to_save:
            try:
                self._upsert_batch(
                    '/rest/v1/health_scores'
                    '?on_conflict=contact_email,score_date',
                    scores_to_save, 'merge-duplicates',
                )
                self._track(success=len(scores_to_save))
                _logger.info(
                    '✓ %d health scores calculados y guardados',
                    len(scores_to_save),
                )
            except Exception as exc:
                self._track(failed=len(scores_to_save))
                _logger.warning('batch save health_scores: %s', exc)
