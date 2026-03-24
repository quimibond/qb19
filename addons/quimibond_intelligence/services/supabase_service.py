"""
Quimibond Intelligence — Supabase Service
Persistencia en Supabase (emails, threads, contactos, alertas, métricas,
embeddings, knowledge graph, person profiles, learning).
"""
import json
import logging
import uuid
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import quote as url_quote

from .supabase_base import SupabaseBaseClient

_logger = logging.getLogger(__name__)


def _postgrest_in_list(values: list) -> str:
    """Construye la lista `in.(...)` para filtros PostgREST."""
    parts = []
    for s in values:
        if not s:
            continue
        esc = str(s).replace('\\', '\\\\').replace('"', '\\"')
        parts.append(f'"{esc}"')
    return ','.join(parts)


class SupabaseService(SupabaseBaseClient):
    """Cliente para Supabase REST API (PostgREST)."""

    # ── Events (event sourcing) ───────────────────────────────────────────

    def log_event(self, event_type: str, source: str = 'pipeline',
                  entity_type: str = None, entity_id: int = None,
                  entity_ref: str = None, payload: dict = None):
        """Log an event to the events table for timeline tracking."""
        try:
            self._request('/rest/v1/events', 'POST', {
                'event_type': event_type,
                'source': source,
                'entity_type': entity_type,
                'entity_id': entity_id,
                'entity_ref': entity_ref,
                'payload': payload or {},
            })
        except Exception as exc:
            _logger.debug('log_event: %s', exc)

    # ── Knowledge Graph — dedup, decay, traversal ─────────────────────────

    def find_duplicate_entities(self) -> list:
        """Return candidate duplicate entities for review/merge."""
        try:
            return self._request(
                '/rest/v1/rpc/find_duplicate_entities', 'POST', {},
            ) or []
        except Exception as exc:
            _logger.warning('find_duplicate_entities: %s', exc)
            return []

    def merge_entities(self, keep_id: int, merge_id: int) -> dict:
        """Merge two entities, keeping keep_id."""
        return self._request(
            '/rest/v1/rpc/merge_entities', 'POST',
            {'p_keep_id': keep_id, 'p_merge_id': merge_id},
        ) or {}

    def auto_deduplicate_entities(self) -> dict:
        """Auto-merge high-confidence duplicates (same_email only)."""
        dupes = self.find_duplicate_entities()
        merged = 0
        skipped = 0
        for d in dupes:
            if d.get('match_reason') != 'same_email':
                continue
            result = self.merge_entities(
                d['entity_a_id'], d['entity_b_id'],
            )
            if result.get('status') == 'ok':
                merged += 1
            else:
                skipped += 1
        if merged:
            self.log_event('auto_dedup', source='pipeline',
                           payload={'merged': merged, 'skipped': skipped})
        return {'merged': merged, 'skipped': skipped,
                'candidates': len(dupes)}

    def decay_fact_confidence(self) -> dict:
        """Run temporal decay on unverified facts."""
        try:
            return self._request(
                '/rest/v1/rpc/decay_fact_confidence', 'POST', {},
            ) or {}
        except Exception as exc:
            _logger.warning('decay_fact_confidence: %s', exc)
            return {}

    def verify_fact(self, fact_id: int, source: str = 'cross_reference') -> dict:
        """Mark a fact as verified, boosting confidence."""
        return self._request(
            '/rest/v1/rpc/verify_fact', 'POST',
            {'p_fact_id': fact_id, 'p_source': source},
        ) or {}

    def get_entity_network(self, entity_id: int, depth: int = 2,
                           min_strength: float = 0.0) -> dict:
        """BFS traversal of entity relationships graph."""
        try:
            return self._request(
                '/rest/v1/rpc/get_entity_network', 'POST',
                {'p_entity_id': entity_id, 'p_depth': depth,
                 'p_min_strength': min_strength},
            ) or {}
        except Exception as exc:
            _logger.warning('get_entity_network: %s', exc)
            return {}

    # ── Emails ───────────────────────────────────────────────────────────────

    def save_emails(self, emails: list):
        """Guarda emails en lotes de 50."""
        batch = []
        saved = 0
        for email in emails:
            try:
                raw_date = email['date']
                # Gmail returns RFC 2822 dates; fall back to ISO if needed
                try:
                    email_date = parsedate_to_datetime(raw_date).isoformat()
                except Exception:
                    email_date = datetime.fromisoformat(
                        raw_date.replace('Z', '+00:00')
                    ).isoformat()
            except Exception:
                email_date = datetime.now().isoformat()

            batch.append({
                'account': email['account'],
                'sender': email['from'],
                'recipient': email['to'],
                'subject': email['subject'],
                'body': email.get('body', ''),
                'snippet': email.get('snippet', ''),
                'email_date': email_date,
                'gmail_message_id': email['gmail_message_id'],
                'gmail_thread_id': email['gmail_thread_id'],
                'attachments': email['attachments'] if email['attachments'] else None,
                'is_reply': email['is_reply'],
                'sender_type': email['sender_type'],
                'has_attachments': email['has_attachments'],
                'kg_processed': email.get('kg_processed', False),
            })
            if len(batch) >= 50:
                self._upsert_batch('/rest/v1/emails?on_conflict=gmail_message_id',
                                   batch, 'ignore-duplicates')
                saved += len(batch)
                batch = []

        if batch:
            self._upsert_batch('/rest/v1/emails?on_conflict=gmail_message_id',
                               batch, 'ignore-duplicates')
            saved += len(batch)
        self._track(success=saved)
        _logger.info('✓ %d emails guardados', saved)

    # ── Contactos ────────────────────────────────────────────────────────────

    def save_contacts(self, contacts: list):
        """Guarda contactos en lotes via RPC, con fallback individual."""
        if not contacts:
            return
        # Batch RPC calls in chunks to reduce overhead
        chunk = 50
        saved = 0
        failed = 0
        for i in range(0, len(contacts), chunk):
            batch = contacts[i:i + chunk]
            for c in batch:
                try:
                    params = {
                        'p_email': c['email'],
                        'p_name': c.get('name', ''),
                        'p_contact_type': c['contact_type'],
                        'p_department': c.get('department'),
                    }
                    company = c.get('company')
                    if company:
                        params['p_company_name'] = company
                    self._request('/rest/v1/rpc/upsert_contact', 'POST', params)
                    saved += 1
                except Exception as exc:
                    failed += 1
                    _logger.warning('save_contact %s: %s', c.get('email'), exc)
        self._track(success=saved, failed=failed)
        _logger.info('✓ %d contactos guardados (%d fallidos)', saved, failed)

    # ── Threads ──────────────────────────────────────────────────────────────

    def save_threads(self, threads: list):
        batch = []
        for t in threads:
            batch.append({
                'gmail_thread_id': t.get('gmail_thread_id'),
                'subject': t['subject'],
                'subject_normalized': t['subject_normalized'],
                'started_by': t['started_by'],
                'started_by_type': t['started_by_type'],
                'started_at': t['started_at'],
                'last_activity': t['last_activity'],
                'status': t['status'],
                'message_count': t['message_count'],
                'participant_emails': t['participant_emails'],
                'has_internal_reply': t['has_internal_reply'],
                'has_external_reply': t['has_external_reply'],
                'last_sender': t['last_sender'],
                'last_sender_type': t['last_sender_type'],
                'hours_without_response': t.get('hours_without_response', 0),
                'account': t['account'],
            })
        if batch:
            self._upsert_batch('/rest/v1/threads?on_conflict=gmail_thread_id',
                               batch, 'merge-duplicates')
            self._track(success=len(batch))
        _logger.info('✓ %d threads guardados', len(batch))

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
            self._upsert_batch('/rest/v1/response_metrics?on_conflict=metric_date,account',
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
                'prediction_id': prediction_id,
                'prediction_confidence': prediction_confidence,
            }
            # Include thread reference if available
            thread_id = a.get('related_thread_id')
            if thread_id:
                record['related_thread_id'] = thread_id
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

        # Save predictions independently (don't lose them if alerts failed)
        if predictions:
            try:
                self.save_prediction_outcomes(predictions)
            except Exception as exc:
                _logger.warning('save_alerts predictions lost (%d): %s',
                                len(predictions), exc)

    # ── Account Summaries ────────────────────────────────────────────────────

    def save_account_summaries(self, summaries: list, today: str):
        batch = [{
            'summary_date': today,
            'account': s['account'],
            'department': s['department'],
            'total_emails': s.get('total_emails', 0),
            'external_emails': s.get('external_emails', 0),
            'internal_emails': s.get('internal_emails', 0),
            'key_items': s.get('key_items', []),
            'waiting_response': s.get('waiting_response', []),
            'urgent_items': s.get('urgent_items', []),
            'external_contacts': s.get('external_contacts', []),
            'topics_detected': s.get('topics_detected', []),
            'summary_text': s.get('summary_text', ''),
            'overall_sentiment': s.get('overall_sentiment'),
            'sentiment_detail': s.get('sentiment_detail'),
            'risks_detected': s.get('risks_detected'),
        } for s in summaries]
        self._upsert_batch(
            '/rest/v1/account_summaries?on_conflict=summary_date,account',
            batch, 'merge-duplicates',
        )
        _logger.info('✓ %d account summaries guardados', len(batch))

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
                encoded_email = url_quote(s["email"], safe='')
                patch = {
                    'relationship_score': s['total_score'],
                    'risk_level': s['risk_level'],
                    'updated_at': now_iso,
                }
                # Payment compliance score (0-20)
                if 'payment_compliance_score' in s:
                    patch['payment_compliance_score'] = s[
                        'payment_compliance_score']
                # Score breakdown for frontend radar charts
                patch['score_breakdown'] = {
                    'frequency': s.get('frequency_score', 0),
                    'responsiveness': s.get('responsiveness_score', 0),
                    'reciprocity': s.get('reciprocity_score', 0),
                    'sentiment': s.get('sentiment_score', 0),
                    'payment_compliance': s.get(
                        'payment_compliance_score', 0),
                }
                # Include Claude's sentiment_score (-1 to 1) if available
                raw_sentiment = contact_sentiments.get(s['email'].lower())
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

        Frontend schema: daily_summaries(summary_date, email_count, summary,
                                         key_events jsonb)
        """
        import re
        summary_text = re.sub(r'<[^>]+>', '', briefing_html)
        # Truncate summary_text to a reasonable size for the summary field
        summary_short = summary_text[:2000] if len(summary_text) > 2000 else summary_text

        self._upsert_batch(
            '/rest/v1/daily_summaries?on_conflict=summary_date',
            [{
                'summary_date': today,
                'total_emails': total_emails,
                'summary_text': summary_short,
                'summary_html': briefing_html[:50000] if briefing_html else '',
                'accounts_read': accounts_read,
                'accounts_failed': accounts_failed,
                'topics_identified': topics_count,
                'key_events': key_events or [],
            }],
            'merge-duplicates',
        )

    # ── Embeddings ───────────────────────────────────────────────────────────

    def update_email_embedding(self, gmail_message_id: str, embedding: list):
        try:
            self._request(
                f'/rest/v1/emails?gmail_message_id=eq.{gmail_message_id}',
                'PATCH', {'embedding': embedding},
            )
        except Exception as exc:
            _logger.debug('Embedding update fail: %s', exc)

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
                '/rest/v1/daily_summaries?order=summary_date.desc&limit=1'
                '&select=summary_text',
            )
            if summaries:
                ctx['previousSummary'] = summaries[0].get('summary_text', '')
        except Exception:
            pass
        try:
            ctx['openAlerts'] = self._request(
                '/rest/v1/alerts?is_resolved=eq.false&order=created_at.desc'
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

    # ── Knowledge Graph ──────────────────────────────────────────────────────

    def upsert_entity(self, entity):
        """Inserta o actualiza una entidad en el knowledge graph."""
        canonical = entity.get('name', '').lower().strip()
        data = {
            'entity_type': entity.get('type', 'person'),
            'name': entity.get('name', ''),
            'canonical_name': canonical,
            'email': entity.get('email'),
            'attributes': entity.get('attributes', {}),
            'last_seen': entity.get('date', None),
        }
        return self._request(
            '/rest/v1/entities?on_conflict=entity_type,canonical_name',
            'POST', data, {
                'Prefer': 'resolution=merge-duplicates,return=representation',
            })

    def save_fact(self, fact):
        """Guarda un hecho extraido (con dedup por entity_id + fact_type + hash)."""
        return self._request(
            '/rest/v1/facts', 'POST', fact,
            extra_headers={
                'Prefer': 'resolution=ignore-duplicates,return=representation',
            },
        )

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

        prediction_id = str(uuid.uuid4())
        prediction_confidence = item.get('confidence', 0.5)

        item_with_prediction = {
            **item,
            'prediction_id': prediction_id,
            'prediction_confidence': prediction_confidence,
        }

        result = None
        try:
            result = self._request(
                '/rest/v1/action_items', 'POST', [item_with_prediction],
                extra_headers={'Prefer': 'return=representation'},
            )
        except Exception as exc:
            _logger.warning('save_action_item POST: %s', exc)

        # Save prediction independently (don't lose it if POST failed)
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            self.save_prediction_outcomes([{
                'action': item_with_prediction,
                'prediction_id': prediction_id,
                'prediction_confidence': prediction_confidence,
                'today': today,
            }])
        except Exception as exc:
            _logger.warning('Action item prediction outcome lost: %s', exc)

        return result

    def save_relationship(self, rel):
        """Guarda o actualiza una relacion entre entidades."""
        return self._request(
            '/rest/v1/entity_relationships'
            '?on_conflict=entity_a_id,entity_b_id,relationship_type',
            'POST', rel, {
                'Prefer': 'resolution=merge-duplicates,return=representation',
            })

    def get_entity_by_name(self, name):
        """Busca una entidad por nombre."""
        canonical = name.lower().strip()
        result = self._request(
            '/rest/v1/entities?canonical_name=eq.' + url_quote(canonical, safe='') + '&limit=1',
        )
        return result[0] if result else None

    def get_entity_intelligence(self, name=None, email=None,
                                entity_type=None, odoo_id=None):
        """Llama al RPC get_entity_intelligence (4-param version).

        Uses the 4-param overload to avoid PostgREST HTTP 300
        'Multiple Choices' when both 2-param and 4-param versions exist.
        """
        return self._request(
            '/rest/v1/rpc/get_entity_intelligence',
            'POST',
            {
                'p_entity_type': entity_type,
                'p_name': name,
                'p_email': email,
                'p_odoo_id': odoo_id,
            },
            extra_headers={
                'Accept': 'application/json',
                'Content-Profile': 'public',
            },
        )

    def get_pending_actions(self, email):
        """Obtiene action items pendientes."""
        return self._request(
            '/rest/v1/rpc/get_my_pending_actions',
            'POST',
            {'p_assignee_email': email},
        )

    def get_gmail_message_ids_with_embedding(self, gmail_message_ids: list) -> set:
        """IDs que ya tienen embedding en Supabase (consulta por lotes)."""
        if not gmail_message_ids:
            return set()
        found = set()
        chunk = 80
        for i in range(0, len(gmail_message_ids), chunk):
            part = gmail_message_ids[i:i + chunk]
            enc = _postgrest_in_list(part)
            if not enc:
                continue
            try:
                rows = self._request(
                    '/rest/v1/emails?select=gmail_message_id'
                    f'&gmail_message_id=in.({enc})'
                    '&embedding=not.is.null',
                )
                if isinstance(rows, list):
                    for r in rows:
                        gid = r.get('gmail_message_id')
                        if gid:
                            found.add(gid)
            except Exception as exc:
                _logger.debug('get_gmail_message_ids_with_embedding: %s', exc)
        return found

    def get_gmail_message_ids_kg_processed(self, gmail_message_ids: list) -> set:
        """IDs ya marcados como kg_processed=true."""
        if not gmail_message_ids:
            return set()
        found = set()
        chunk = 80
        for i in range(0, len(gmail_message_ids), chunk):
            part = gmail_message_ids[i:i + chunk]
            enc = _postgrest_in_list(part)
            if not enc:
                continue
            try:
                rows = self._request(
                    '/rest/v1/emails?select=gmail_message_id'
                    f'&gmail_message_id=in.({enc})'
                    '&kg_processed=eq.true',
                )
                if isinstance(rows, list):
                    for r in rows:
                        gid = r.get('gmail_message_id')
                        if gid:
                            found.add(gid)
            except Exception as exc:
                _logger.debug('get_gmail_message_ids_kg_processed: %s', exc)
        return found

    def mark_emails_kg_processed(self, gmail_message_ids: list):
        """Marca emails como procesados por el knowledge graph."""
        if not gmail_message_ids:
            return
        chunk = 80
        for i in range(0, len(gmail_message_ids), chunk):
            part = [x for x in gmail_message_ids[i:i + chunk] if x]
            if not part:
                continue
            enc = _postgrest_in_list(part)
            try:
                self._request(
                    f'/rest/v1/emails?gmail_message_id=in.({enc})',
                    'PATCH',
                    {'kg_processed': True},
                )
            except Exception as exc:
                _logger.debug('mark_emails_kg_processed: %s', exc)

    # ── Person Profiles (aprendizaje acumulativo) ─────────────────────────

    def upsert_person_profile(self, profile: dict):
        """Actualiza campos de perfil/personalidad directamente en contacts.

        Cada vez que el sistema procesa emails, actualiza el perfil con
        nueva información. Los datos se acumulan — no se sobrescriben
        a menos que haya info más reciente.
        """
        email = profile.get('email')
        if not email:
            return None

        # Build patch with only non-None profile fields
        profile_fields = (
            'role', 'decision_power', 'communication_style',
            'language_preference', 'key_interests', 'personality_notes',
            'negotiation_style', 'response_pattern', 'influence_on_deals',
        )
        patch = {}
        for field in profile_fields:
            val = profile.get(field)
            if val is not None:
                patch[field] = val
        if profile.get('department'):
            patch['department'] = profile['department']

        if not patch:
            return None

        try:
            encoded = url_quote(email.lower().strip(), safe='')
            self._request(
                f'/rest/v1/contacts?email=eq.{encoded}',
                'PATCH', patch,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.debug('upsert_person_profile: %s', exc)
        return None

    def get_person_profile(self, email=None, name=None):
        """Obtiene el perfil acumulado de una persona desde contacts."""
        if email:
            key = url_quote(email.lower().strip(), safe='')
            filter_param = f'email=eq.{key}'
        elif name:
            key = url_quote(name.lower().strip(), safe='')
            filter_param = f'name=eq.{key}'
        else:
            return None
        try:
            result = self._request(
                f'/rest/v1/contacts?{filter_param}&limit=1'
                '&select=id,email,name,company,company_id,role,department,'
                'decision_power,communication_style,language_preference,'
                'key_interests,personality_notes,negotiation_style,'
                'response_pattern,influence_on_deals,interaction_count',
            )
            return result[0] if result else None
        except Exception:
            return None

    def get_person_profiles_for_contacts(self, emails: list) -> dict:
        """Obtiene perfiles de múltiples personas por email desde contacts.

        Retorna dict: {email → profile_data}
        """
        if not emails:
            return {}
        profiles = {}
        chunk = 50
        for i in range(0, len(emails), chunk):
            part = emails[i:i + chunk]
            enc = _postgrest_in_list([e.lower() for e in part if e])
            if not enc:
                continue
            try:
                rows = self._request(
                    '/rest/v1/contacts?'
                    'select=id,email,name,company,company_id,role,department,'
                    'decision_power,communication_style,language_preference,'
                    'key_interests,personality_notes,negotiation_style,'
                    'response_pattern,influence_on_deals,interaction_count'
                    f'&email=in.({enc})',
                )
                if isinstance(rows, list):
                    for r in rows:
                        key = (r.get('email') or '').lower()
                        if key:
                            profiles[key] = r
            except Exception as exc:
                _logger.debug('get_person_profiles batch: %s', exc)
        return profiles

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

    # ── Sync State (Gmail history) ────────────────────────────────────────────

    def save_sync_state(self, account: str, history_id: str):
        """Persiste el historyId de Gmail en Supabase sync_state."""
        try:
            self._request(
                '/rest/v1/sync_state?on_conflict=account',
                'POST', {
                    'account': account,
                    'last_history_id': history_id,
                    'emails_synced': 0,
                }, {
                    'Prefer': 'resolution=merge-duplicates,return=minimal',
                })
        except Exception as exc:
            _logger.debug('save_sync_state: %s', exc)

    def get_sync_state(self) -> dict:
        """Carga todos los sync states: {account → history_id}."""
        try:
            rows = self._request(
                '/rest/v1/sync_state?select=account,last_history_id',
            )
            if isinstance(rows, list):
                return {
                    r['account']: r['last_history_id']
                    for r in rows if r.get('last_history_id')
                }
        except Exception as exc:
            _logger.debug('get_sync_state: %s', exc)
        return {}

    # ── Contact Odoo Sync ─────────────────────────────────────────────────────

    def sync_contact_odoo_data(self, email: str, odoo_data: dict,
                               _company_cache: dict = None):
        """Actualiza un contacto en Supabase con datos de Odoo.

        Si company_name está en odoo_data, resuelve o crea la empresa
        en la tabla companies y asigna company_id al contacto.
        Pass _company_cache to avoid repeated company lookups.
        """
        try:
            # Resolve company_name → company_id
            company_name = odoo_data.get('company')
            if company_name and 'company_id' not in odoo_data:
                company_id = self._resolve_or_create_company(
                    company_name, odoo_data, _cache=_company_cache,
                )
                if company_id:
                    odoo_data['company_id'] = company_id

            encoded = url_quote(email, safe='')
            self._request(
                f'/rest/v1/contacts?email=eq.{encoded}',
                'PATCH', odoo_data,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.warning('sync_contact_odoo %s: %s', email, exc)

    # ── Companies ─────────────────────────────────────────────────────────

    def batch_resolve_companies(self, names: list) -> dict:
        """Batch-resolve company names to IDs (1 query instead of N).

        Returns dict: {canonical_name → company_id}
        """
        if not names:
            return {}
        canonicals = [n.lower().strip() for n in names if n and n.strip()]
        if not canonicals:
            return {}
        result = {}
        try:
            enc = _postgrest_in_list(canonicals)
            if enc:
                rows = self._request(
                    f'/rest/v1/companies?canonical_name=in.({enc})'
                    '&select=id,canonical_name',
                ) or []
                for r in rows:
                    cn = r.get('canonical_name', '')
                    if cn:
                        result[cn] = r['id']
        except Exception as exc:
            _logger.warning('batch_resolve_companies: %s', exc)
        return result

    def _resolve_or_create_company(self, name: str,
                                    odoo_data: dict = None,
                                    _cache: dict = None) -> int:
        """Busca o crea una empresa por nombre. Retorna company_id.

        Pass _cache (from batch_resolve_companies) to skip the lookup query.
        """
        if not name or not name.strip():
            return None
        canonical = name.lower().strip()

        # Check cache first (from batch_resolve_companies)
        if _cache and canonical in _cache:
            return _cache[canonical]

        try:
            encoded = url_quote(canonical, safe='')
            resp = self._request(
                f'/rest/v1/companies?canonical_name=eq.{encoded}&select=id',
            )
            if resp and isinstance(resp, list) and resp:
                cid = resp[0]['id']
                if _cache is not None:
                    _cache[canonical] = cid
                return cid
        except Exception:
            pass

        # Create new company
        company_data = {
            'name': name.strip(),
            'canonical_name': canonical,
        }
        if odoo_data:
            if odoo_data.get('odoo_partner_id'):
                company_data['odoo_partner_id'] = odoo_data['odoo_partner_id']
            if odoo_data.get('is_customer') is not None:
                company_data['is_customer'] = odoo_data['is_customer']
            if odoo_data.get('is_supplier') is not None:
                company_data['is_supplier'] = odoo_data['is_supplier']
        try:
            result = self._request(
                '/rest/v1/companies', 'POST', company_data,
                extra_headers={'Prefer': 'return=representation'},
            )
            if result and isinstance(result, list) and result:
                cid = result[0]['id']
                if _cache is not None:
                    _cache[canonical] = cid
                return cid
        except Exception as exc:
            _logger.debug('create_company %s: %s', name, exc)
        return None

    def sync_company_odoo_data(self, company_id: int, data: dict):
        """Actualiza datos agregados de una empresa desde Odoo.

        Acepta tanto métricas escalares (lifetime_value, etc.) como
        odoo_context JSONB con detalle operacional completo.
        """
        try:
            self._request(
                f'/rest/v1/companies?id=eq.{company_id}',
                'PATCH', data,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.warning('sync_company_odoo %s: %s', company_id, exc)

    def save_company_snapshots(self, snapshots: list):
        """Guarda snapshots diarios de métricas operacionales por empresa.

        Upsert por (company_id, snapshot_date) — si ya existe el snapshot
        del día, lo actualiza con datos frescos.
        """
        if not snapshots:
            return
        try:
            self._upsert_batch(
                '/rest/v1/company_odoo_snapshots'
                '?on_conflict=company_id,snapshot_date',
                snapshots, 'merge-duplicates',
            )
            self._track(success=len(snapshots))
            _logger.info('✓ %d company snapshots guardados', len(snapshots))
        except Exception as exc:
            self._track(failed=len(snapshots))
            _logger.warning('save_company_snapshots: %s', exc)

    def get_company_contacts(self, company_id: int) -> list:
        """Obtiene todos los contactos de una empresa."""
        try:
            return self._request(
                f'/rest/v1/contacts?company_id=eq.{company_id}'
                '&select=id,email,name,role,decision_power,'
                'relationship_score,risk_level'
                '&order=name',
            ) or []
        except Exception:
            return []

    def get_company_360(self, company_id: int) -> dict:
        """Vista 360 de una empresa (contactos, alertas, revenue, facts)."""
        try:
            return self._request(
                '/rest/v1/rpc/get_company_360', 'POST',
                {'p_company_id': company_id},
            ) or {}
        except Exception as exc:
            _logger.debug('get_company_360: %s', exc)
            return {}

    def get_companies_needing_enrichment(self, limit: int = 20) -> list:
        """Empresas sin perfil o con perfil desactualizado (>30 días)."""
        try:
            return self._request(
                '/rest/v1/companies?enriched_at=is.null'
                '&select=id,name,canonical_name,entity_id,'
                'is_customer,is_supplier,odoo_context'
                f'&order=updated_at.desc&limit={limit}',
            ) or []
        except Exception as exc:
            _logger.warning('get_companies_needing_enrichment: %s', exc)
            return []

    def save_company_profile(self, company_id: int, profile: dict):
        """Guarda perfil enriquecido por Claude en la empresa."""
        from datetime import datetime
        patch = {'enriched_at': datetime.now().isoformat(),
                 'enrichment_source': 'claude'}
        for field in ('description', 'business_type', 'relationship_type',
                      'relationship_summary', 'industry', 'country', 'city',
                      'key_products', 'risk_signals', 'opportunity_signals',
                      'strategic_notes'):
            val = profile.get(field)
            if val is not None:
                patch[field] = val
        try:
            self._request(
                f'/rest/v1/companies?id=eq.{company_id}',
                'PATCH', patch,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.warning('save_company_profile %s: %s', company_id, exc)


    # ── System Learning ───────────────────────────────────────────────────────

    def save_learning(self, learning_type: str, description: str,
                      data: dict = None, account: str = None):
        """Registra un aprendizaje del sistema."""
        try:
            self._request('/rest/v1/system_learning', 'POST', {
                'learning_type': learning_type,
                'description': description,
                'data': data or {},
                'account': account,
            })
        except Exception as exc:
            _logger.debug('save_learning: %s', exc)

    # ── Communication Patterns ────────────────────────────────────────────────

    def save_communication_patterns(self, patterns: list):
        """Guarda patrones de comunicación por cuenta/semana.

        Schema: communication_patterns(week_start, account, total_emails,
            response_rate, avg_response_hours, top_external_contacts,
            top_internal_contacts, busiest_hour, common_subjects,
            sentiment_score)
        Unique on (week_start, account).
        """
        if not patterns:
            return

        records = []
        for p in patterns:
            if not p.get('account') or not p.get('week_start'):
                continue
            records.append({
                'week_start': p['week_start'],
                'account': p['account'],
                'total_emails': p.get('total_emails', 0),
                'response_rate': p.get('response_rate'),
                'avg_response_hours': p.get('avg_response_hours'),
                'top_external_contacts': p.get('top_external_contacts', []),
                'top_internal_contacts': p.get('top_internal_contacts', []),
                'busiest_hour': p.get('busiest_hour'),
                'common_subjects': p.get('common_subjects', []),
                'sentiment_score': p.get('sentiment_score'),
            })

        if records:
            self._upsert_batch(
                '/rest/v1/communication_patterns'
                '?on_conflict=week_start,account',
                records, 'merge-duplicates',
            )
            self._track(success=len(records))
        _logger.info('✓ %d communication patterns guardados', len(records))

    # ── Action Items (update status) ──────────────────────────────────────────

    def complete_action_item(self, action_id: int):
        """Marca un action item como completado en Supabase."""
        try:
            now = datetime.now()
            self._request(
                f'/rest/v1/action_items?id=eq.{action_id}',
                'PATCH', {
                    'status': 'completed',
                    'state': 'completed',
                    'completed_date': now.strftime('%Y-%m-%d'),
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
                'is_resolved': state in ('resolved', 'dismissed'),
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
        """Actualiza estado de una alerta en Supabase.

        El frontend filtra alertas por is_resolved y state.
        """
        try:
            from urllib.parse import quote as _quote
            encoded = _quote(alert_title[:200], safe='')
            patch = {
                'state': state,
                'is_resolved': state in ('resolved', 'dismissed'),
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

    def save_prediction_outcomes(self, predictions: list):
        """Registra outcomes de predicciones (alertas y acciones).

        Cada predicción incluye:
        - alert/action: el registro completo de alerta o acción
        - prediction_id: UUID generado para esta predicción
        - prediction_confidence: confianza de la predicción (0-1)
        - today: fecha de la predicción

        Se inserta un registro en prediction_outcomes con:
        - prediction_id: UUID de la predicción
        - prediction_type: 'alert' o 'action'
        - prediction_date: fecha de predicción (hoy)
        - prediction_summary: título de alerta o descripción de acción
        - predicted_severity: severidad de alerta o prioridad de acción
        - confidence: confianza (0-1)
        - account: cuenta/departamento si aplica
        - contact_email: email del contacto si aplica
        - outcome_type: NULL hasta que el usuario proporcione feedback
        """
        if not predictions:
            return

        records = []
        for pred in predictions:
            alert = pred.get('alert')
            action = pred.get('action')
            prediction_id = pred.get('prediction_id')
            prediction_confidence = pred.get('prediction_confidence', 0.5)
            today = pred.get('today', datetime.now().strftime('%Y-%m-%d'))

            if alert:
                # Predicción de alerta
                record = {
                    'prediction_id': prediction_id,
                    'prediction_type': 'alert',
                    'prediction_date': today,
                    'prediction_summary': alert.get('title', ''),
                    'predicted_severity': alert.get('severity', 'medium'),
                    'confidence': prediction_confidence,
                    'account': alert.get('account'),
                    'contact_email': alert.get('contact_email'),
                    'outcome_type': None,
                }
                # Intentar resolver contact_email desde contact_name
                if not record.get('contact_email') and alert.get('contact_name'):
                    try:
                        encoded = url_quote(alert['contact_name'], safe='')
                        resp = self._request(
                            f'/rest/v1/contacts?name=eq.{encoded}&select=email'
                        )
                        if resp and isinstance(resp, list) and resp:
                            record['contact_email'] = resp[0].get('email')
                    except Exception:
                        pass
                records.append(record)

            elif action:
                # Predicción de acción
                record = {
                    'prediction_id': prediction_id,
                    'prediction_type': 'action',
                    'prediction_date': today,
                    'prediction_summary': action.get('description', ''),
                    'predicted_severity': action.get('priority', 'medium'),
                    'confidence': prediction_confidence,
                    'account': action.get('source_account'),
                    'contact_email': None,
                    'outcome_type': None,
                }
                records.append(record)

        if records:
            try:
                self._request('/rest/v1/prediction_outcomes', 'POST', records)
                self._track(success=len(records))
                _logger.info('✓ %d prediction outcomes guardados', len(records))
            except Exception as exc:
                self._track(failed=len(records))
                _logger.warning('save_prediction_outcomes lost %d records: %s',
                                len(records), exc)

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
                '/rest/v1/customer_health_scores',
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

        # Filter external contacts with email
        external = [
            c for c in contacts
            if c.get('contact_type') == 'external' and c.get('email')
        ]
        if not external:
            return

        ext_emails = [c['email'].lower() for c in external]

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

        # ── Batch-fetch revenue_metrics (1 call instead of N) ──
        revenue_map = {}  # email → latest revenue record
        try:
            enc = _postgrest_in_list(ext_emails)
            if enc:
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

        # ── Batch-fetch previous health scores (1 call instead of N) ──
        prev_scores_map = {}  # email → previous overall_score
        try:
            enc = _postgrest_in_list(ext_emails)
            if enc:
                prev_rows = self._request(
                    f'/rest/v1/customer_health_scores?contact_email=in.({enc})'
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

        # ── Batch-fetch engagement data (facts + relationships) ──
        # Instead of N RPC calls to get_entity_intelligence, do 2 batch queries
        engagement_map = {}  # email → {'facts': int, 'rels': int}
        try:
            enc = _postgrest_in_list(ext_emails)
            if enc:
                # Get entities by email
                ent_rows = self._request(
                    f'/rest/v1/entities?email=in.({enc})'
                    '&select=id,email',
                ) or []
                if ent_rows:
                    ent_id_map = {}  # entity_id → email
                    for er in ent_rows:
                        if er.get('email') and er.get('id'):
                            ent_id_map[er['id']] = er['email'].lower()
                            engagement_map[er['email'].lower()] = {
                                'facts': 0, 'rels': 0,
                            }
                    # Count facts per entity (batch)
                    ent_ids = ','.join(str(eid) for eid in ent_id_map)
                    if ent_ids:
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
                        # Count relationships per entity (batch)
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

                scores_to_save.append({
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
                })

            except Exception as exc:
                _logger.debug('health_score skip %s: %s', email_addr, exc)

        # ── Batch-save all health scores (1 call instead of N) ──
        if scores_to_save:
            try:
                self._upsert_batch(
                    '/rest/v1/customer_health_scores'
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

