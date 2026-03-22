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
        _logger.info('✓ %d emails guardados', saved)

    # ── Contactos ────────────────────────────────────────────────────────────

    def save_contacts(self, contacts: list):
        for c in contacts:
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
            except Exception as exc:
                _logger.warning('save_contact %s: %s', c.get('email'), exc)
        _logger.info('✓ %d contactos guardados', len(contacts))

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
        _logger.info('✓ %d métricas guardadas', len(batch))

    # ── Alertas ──────────────────────────────────────────────────────────────

    def save_alerts(self, alerts: list, today: str):
        """Guarda alertas resolviendo contact_id desde contact_name.

        El frontend filtra alertas por contact_id en la ficha de contacto,
        así que es importante que este campo esté presente.

        También genera prediction_id para cada alerta y registra las predicciones.
        """
        if not alerts:
            return

        # Resolve contact_name → contact_id + company_id for filtering
        contact_names = list({
            a.get('contact_name', '') for a in alerts
            if a.get('contact_name')
        })
        name_to_contact = {}  # name → {id, company_id}
        for name in contact_names:
            try:
                encoded = url_quote(name, safe='')
                resp = self._request(
                    f'/rest/v1/contacts?name=eq.{encoded}&select=id,company_id',
                )
                if resp and isinstance(resp, list) and resp:
                    name_to_contact[name] = resp[0]
            except Exception:
                pass

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
            _logger.info('✓ %d alertas guardadas', len(records))
        except Exception as exc:
            _logger.error('save_alerts POST: %s', exc)

        # Save predictions independently (don't lose them if alerts failed)
        if predictions:
            try:
                self.save_prediction_outcomes(predictions)
            except Exception as exc:
                _logger.debug('save_alerts predictions: %s', exc)

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
        """
        contact_sentiments = contact_sentiments or {}
        for s in scores:
            try:
                encoded_email = url_quote(s["email"], safe='')
                patch = {
                    'relationship_score': s['total_score'],
                    'risk_level': s['risk_level'],
                    'updated_at': datetime.now().isoformat(),
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
            except Exception as exc:
                _logger.warning('save_client_score %s: %s', s.get('email'), exc)
        _logger.info('✓ %d client scores guardados', len(scores))

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
        """Guarda un hecho extraido."""
        return self._request('/rest/v1/facts', 'POST', fact)

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
            _logger.debug('Action item prediction outcome: %s', exc)

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

    def get_entity_intelligence(self, name=None, email=None):
        """Llama al RPC get_entity_intelligence (2-param version)."""
        return self._request(
            '/rest/v1/rpc/get_entity_intelligence',
            'POST',
            {'p_email': email, 'p_name': name},
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

        Los campos de perfil ahora viven en la tabla contacts (migrados
        desde person_profiles). También mantiene person_profiles por
        compatibilidad temporal.
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

        # Update contacts table (primary destination)
        if patch:
            try:
                encoded = url_quote(email.lower().strip(), safe='')
                self._request(
                    f'/rest/v1/contacts?email=eq.{encoded}',
                    'PATCH', patch,
                    extra_headers={'Prefer': 'return=minimal'},
                )
            except Exception as exc:
                _logger.debug('upsert_person_profile contacts: %s', exc)

        # Also write to person_profiles for backward compatibility
        canonical = email.lower().strip()
        data = {
            'canonical_key': canonical,
            'name': profile.get('name', ''),
            'email': email,
            'company': profile.get('company'),
            'role': profile.get('role'),
            'department': profile.get('department'),
            'decision_power': profile.get('decision_power', 'medium'),
            'communication_style': profile.get('communication_style', 'formal'),
            'language_preference': profile.get('language_preference', 'es'),
            'key_interests': profile.get('key_interests', []),
            'personality_notes': profile.get('personality_notes', ''),
            'negotiation_style': profile.get('negotiation_style'),
            'response_pattern': profile.get('response_pattern'),
            'influence_on_deals': profile.get('influence_on_deals'),
            'source_account': profile.get('source_account'),
            'last_seen_date': profile.get('last_seen_date'),
        }
        try:
            result = self._request(
                '/rest/v1/person_profiles?on_conflict=canonical_key',
                'POST', data, {
                    'Prefer': 'resolution=merge-duplicates,return=representation',
                })
            if result and isinstance(result, list) and result:
                profile_id = result[0].get('id')
                current_count = result[0].get('interaction_count', 0) or 0
                if profile_id:
                    try:
                        self._request(
                            f'/rest/v1/person_profiles?id=eq.{profile_id}',
                            'PATCH',
                            {'interaction_count': current_count + 1},
                        )
                    except Exception:
                        pass
            return result
        except Exception as exc:
            _logger.debug('person_profile upsert: %s', exc)
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

    def sync_contact_odoo_data(self, email: str, odoo_data: dict):
        """Actualiza un contacto en Supabase con datos de Odoo.

        Si company_name está en odoo_data, resuelve o crea la empresa
        en la tabla companies y asigna company_id al contacto.
        """
        try:
            # Resolve company_name → company_id
            company_name = odoo_data.get('company')
            if company_name and 'company_id' not in odoo_data:
                company_id = self._resolve_or_create_company(
                    company_name, odoo_data,
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

    def _resolve_or_create_company(self, name: str,
                                    odoo_data: dict = None) -> int:
        """Busca o crea una empresa por nombre. Retorna company_id."""
        if not name or not name.strip():
            return None
        canonical = name.lower().strip()
        try:
            encoded = url_quote(canonical, safe='')
            resp = self._request(
                f'/rest/v1/companies?canonical_name=eq.{encoded}&select=id',
            )
            if resp and isinstance(resp, list) and resp:
                return resp[0]['id']
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
                return result[0]['id']
        except Exception as exc:
            _logger.debug('create_company %s: %s', name, exc)
        return None

    def sync_company_odoo_data(self, company_id: int, data: dict):
        """Actualiza datos agregados de una empresa desde Odoo."""
        try:
            self._request(
                f'/rest/v1/companies?id=eq.{company_id}',
                'PATCH', data,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.warning('sync_company_odoo %s: %s', company_id, exc)

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
                '&select=id,name,canonical_name,domain,entity_id,'
                'is_customer,is_supplier,odoo_context'
                f'&order=updated_at.desc&limit={limit}',
            ) or []
        except Exception:
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
                _logger.info('✓ %d prediction outcomes guardados', len(records))
            except Exception as exc:
                _logger.debug('save_prediction_outcomes: %s', exc)

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

                # ── Engagement score (individual RPC call) ──
                engagement_score = 50
                try:
                    entity_data = self.get_entity_intelligence(
                        email=email_addr,
                    )
                    if entity_data:
                        facts_count = len(entity_data.get('facts', []))
                        rels_count = len(
                            entity_data.get('relationships', []),
                        )
                        engagement_score = min(
                            100, 30 + facts_count * 10 + rels_count * 15,
                        )
                except Exception:
                    pass

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
                _logger.info(
                    '✓ %d health scores calculados y guardados',
                    len(scores_to_save),
                )
            except Exception as exc:
                _logger.warning('batch save health_scores: %s', exc)

