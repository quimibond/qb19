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

import httpx

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


class SupabaseService:
    """Cliente para Supabase REST API (PostgREST)."""

    def __init__(self, url: str, key: str):
        self._url = url.rstrip('/')
        self._key = key
        self._headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
        }

    def _request(self, path: str, method: str = 'GET',
                 payload=None, extra_headers: dict = None):
        headers = {**self._headers, **(extra_headers or {})}
        with httpx.Client(timeout=30) as client:
            resp = client.request(method, f'{self._url}{path}',
                                  headers=headers,
                                  json=payload if payload else None)
        if 200 <= resp.status_code < 300:
            text = resp.text
            try:
                return json.loads(text) if text else None
            except json.JSONDecodeError:
                return text
        raise RuntimeError(f'Supabase {resp.status_code}: {resp.text[:300]}')

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
                self._request('/rest/v1/rpc/upsert_contact', 'POST', {
                    'p_email': c['email'],
                    'p_name': c.get('name', ''),
                    'p_contact_type': c['contact_type'],
                    'p_department': c.get('department'),
                })
            except Exception:
                pass
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

        # Build contact_name → contact_id map for alerts that have a name
        contact_names = list({
            a.get('contact_name', '') for a in alerts
            if a.get('contact_name') and not a.get('contact_id')
        })
        name_to_id = {}
        for name in contact_names:
            try:
                encoded = url_quote(name, safe='')
                resp = self._request(
                    f'/rest/v1/contacts?name=eq.{encoded}&select=id'
                )
                if resp and isinstance(resp, list) and resp:
                    name_to_id[name] = resp[0]['id']
            except Exception:
                pass

        records = []
        predictions = []
        for a in alerts:
            prediction_id = str(uuid.uuid4())
            # Try to derive confidence from alert — if available in the alert dict
            prediction_confidence = a.get('confidence', 0.5)

            record = {
                'alert_type': a.get('alert_type', 'general'),
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
            # Resolve contact_id
            cid = a.get('contact_id')
            if not cid and a.get('contact_name'):
                cid = name_to_id.get(a['contact_name'])
            if cid:
                record['contact_id'] = cid
            records.append(record)

            # Prepare prediction outcome record
            predictions.append({
                'alert': record,
                'prediction_id': prediction_id,
                'prediction_confidence': prediction_confidence,
                'today': today,
            })

        self._request('/rest/v1/alerts', 'POST', records)
        _logger.info('✓ %d alertas guardadas', len(records))

        # Save prediction outcomes
        if predictions:
            self.save_prediction_outcomes(predictions)

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
            except Exception:
                pass
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
                'email_count': total_emails,
                'summary': summary_short,
                'key_events': json.dumps(key_events or []),
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

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _upsert_batch(self, path: str, batch: list, resolution: str):
        self._request(path, 'POST', batch, {
            'Prefer': f'resolution={resolution},return=minimal',
        })

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

    def save_entity_mention(self, mention):
        """Guarda una mencion de entidad en un email."""
        return self._request('/rest/v1/entity_mentions', 'POST', mention)

    def save_fact(self, fact):
        """Guarda un hecho extraido."""
        return self._request('/rest/v1/facts', 'POST', fact)

    def save_action_item(self, item):
        """Guarda un action item.

        Genera prediction_id para registrar la predicción de acción.
        """
        prediction_id = str(uuid.uuid4())
        prediction_confidence = item.get('confidence', 0.5)

        # Add prediction fields to the item
        item_with_prediction = {
            **item,
            'prediction_id': prediction_id,
            'prediction_confidence': prediction_confidence,
        }

        result = self._request('/rest/v1/action_items', 'POST', [item_with_prediction])

        # Prepare and save prediction outcome
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            self.save_prediction_outcomes([{
                'action': item_with_prediction,
                'prediction_id': prediction_id,
                'prediction_confidence': prediction_confidence,
                'today': today,
            }])
        except Exception as exc:
            _logger.debug('Action item prediction outcome save: %s', exc)

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
        """Llama al RPC get_entity_intelligence."""
        params = {}
        if email:
            params['p_email'] = email
        elif name:
            params['p_name'] = name
        return self._request(
            '/rest/v1/rpc/get_entity_intelligence',
            'POST',
            params,
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
        """Inserta o actualiza el perfil de una persona.

        Cada vez que el sistema procesa emails, actualiza el perfil con
        nueva información. Los datos se acumulan — no se sobrescriben
        a menos que haya info más reciente.

        Tabla: person_profiles (upsert por email o canonical_name)
        """
        canonical = (profile.get('email') or
                     profile.get('name', '')).lower().strip()
        if not canonical:
            return

        data = {
            'canonical_key': canonical,
            'name': profile.get('name', ''),
            'email': profile.get('email'),
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
            'interaction_count': 1,
        }

        # Intentar upsert — si la tabla no existe, fallar silenciosamente
        try:
            return self._request(
                '/rest/v1/person_profiles?on_conflict=canonical_key',
                'POST', data, {
                    'Prefer': 'resolution=merge-duplicates,return=representation',
                })
        except Exception as exc:
            _logger.debug('person_profile upsert: %s', exc)
            return None

    def get_person_profile(self, email=None, name=None):
        """Obtiene el perfil acumulado de una persona."""
        if email:
            key = email.lower().strip()
        elif name:
            key = name.lower().strip()
        else:
            return None
        try:
            result = self._request(
                f'/rest/v1/person_profiles?canonical_key=eq.{url_quote(key, safe="")}&limit=1',
            )
            return result[0] if result else None
        except Exception:
            return None

    def get_person_profiles_for_contacts(self, emails: list) -> dict:
        """Obtiene perfiles de múltiples personas por email.

        Retorna dict: {email → profile_data}
        """
        if not emails:
            return {}
        profiles = {}
        chunk = 50
        for i in range(0, len(emails), chunk):
            part = emails[i:i + chunk]
            enc = _postgrest_in_list(part)
            if not enc:
                continue
            try:
                rows = self._request(
                    '/rest/v1/person_profiles?select=*'
                    f'&canonical_key=in.({enc})',
                )
                if isinstance(rows, list):
                    for r in rows:
                        key = r.get('email') or r.get('canonical_key', '')
                        if key:
                            profiles[key.lower()] = r
            except Exception as exc:
                _logger.debug('get_person_profiles batch: %s', exc)
        return profiles

    # ── Topics ────────────────────────────────────────────────────────────────

    def save_topics(self, topics: list, today: str):
        """Guarda temas detectados en Supabase via RPC upsert_topic."""
        for t in topics:
            try:
                self._request('/rest/v1/rpc/upsert_topic', 'POST', {
                    'p_topic': t.get('topic', ''),
                    'p_category': t.get('category', ''),
                    'p_status': t.get('status', 'active'),
                    'p_priority': t.get('priority', 'medium'),
                    'p_summary': t.get('summary', ''),
                    'p_related_accounts': None,
                    'p_embedding': None,
                })
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
        """Actualiza un contacto en Supabase con datos de Odoo."""
        try:
            import urllib.parse
            encoded = urllib.parse.quote(email, safe='')
            self._request(
                f'/rest/v1/contacts?email=eq.{encoded}',
                'PATCH', odoo_data,
            )
        except Exception as exc:
            _logger.debug('sync_contact_odoo: %s', exc)

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
        """Guarda patrones de comunicación por contacto.

        Each pattern has contact_email; we resolve contact_id before saving.
        Schema: communication_patterns(contact_id, pattern_type, description,
                                       frequency, confidence)
        """
        if not patterns:
            return

        # Collect unique emails and resolve contact_ids
        emails = list({p['contact_email'] for p in patterns if p.get('contact_email')})
        contact_map = {}
        for email in emails:
            try:
                encoded = url_quote(email, safe='')
                resp = self._request(
                    f'/rest/v1/contacts?email=eq.{encoded}&select=id',
                )
                if resp and isinstance(resp, list) and resp:
                    contact_map[email] = resp[0]['id']
            except Exception:
                pass

        # Build records matching the frontend schema
        records = []
        for p in patterns:
            contact_id = contact_map.get(p.get('contact_email'))
            if not contact_id:
                continue
            records.append({
                'contact_id': contact_id,
                'pattern_type': p['pattern_type'],
                'description': p['description'],
                'frequency': p.get('frequency'),
                'confidence': p.get('confidence', 0.7),
            })

        if records:
            self._request('/rest/v1/communication_patterns', 'POST', records)
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

