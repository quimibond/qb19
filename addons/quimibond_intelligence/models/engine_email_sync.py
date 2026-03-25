"""
Engine — Email Sync (Gmail → Supabase)
"""
import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from odoo import api, models

from .intelligence_config import (
    TZ_CDMX,
    acquire_lock,
    get_account_departments,
    get_email_accounts,
    release_lock,
)

_logger = logging.getLogger(__name__)


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    # ══════════════════════════════════════════════════════════════════════════
    #   MICRO-PIPELINE: SYNC EMAILS
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_sync_emails(self):
        """Sync incremental de emails desde Gmail → Supabase. Corre cada 30 min."""
        lock = 'quimibond_intelligence.sync_emails_running'
        if not acquire_lock(self.env, lock):
            return
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            email_accounts = get_email_accounts(self.env)
            account_departments = get_account_departments(self.env)

            from ..services.gmail_service import GmailService
            from ..services.supabase_service import SupabaseService

            sa_info = json.loads(cfg['service_account_json'])
            gmail = GmailService(sa_info)

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                gmail_history = self._load_gmail_history_state()
                result = gmail.read_all_accounts(
                    email_accounts, history_state=gmail_history, max_workers=5,
                )
                self._save_gmail_history_state(result['gmail_history_state'])
                all_emails = result['emails']

                if not all_emails:
                    _logger.info('Sync: sin emails nuevos')
                    return

                emails = self._deduplicate(all_emails)
                for e in emails:
                    e['department'] = account_departments.get(
                        e['account'], 'Otro')

                supa.save_emails(emails)
                threads = self._build_threads(emails, cfg)
                supa.save_threads(threads)

                for acct, hid in result['gmail_history_state'].items():
                    supa.save_sync_state(acct, str(hid))

                supa.log_event('emails_synced', 'cron_sync_emails',
                               payload={
                                   'total': len(emails),
                                   'accounts_ok': result['success_count'],
                                   'accounts_failed': result['failed_count'],
                                   'threads': len(threads),
                               })

                _logger.info(
                    '✓ Sync: %d emails, %d threads (%.1fs)',
                    len(emails), len(threads),
                    time.time() - start,
                )
        except Exception as exc:
            _logger.error('run_sync_emails: %s', exc, exc_info=True)
        finally:
            release_lock(self.env, lock)

    # ── Gmail history state ──────────────────────────────────────────────────

    def _load_gmail_history_state(self) -> dict:
        raw = (
            self.env['ir.config_parameter'].sudo()
            .get_param('quimibond_intelligence.gmail_history_state', '{}')
        )
        try:
            data = json.loads(raw) if raw else {}
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}

    def _save_gmail_history_state(self, state: dict):
        if not state:
            return
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_intelligence.gmail_history_state',
            json.dumps(state),
        )

    # ── Deduplicación ─────────────────────────────────────────────────────────

    @staticmethod
    def _deduplicate(emails: list) -> list:
        """Elimina duplicados por fingerprint."""
        seen = set()
        unique = []
        for e in emails:
            try:
                date_str = e.get('date', '')
                date_minute = re.sub(r':\d{2}\s', ' ', date_str)[:16]
            except Exception:
                date_minute = ''

            fp = f"{e.get('from_email', '')}|{e.get('subject_normalized', '')}|{date_minute}"
            if fp not in seen:
                seen.add(fp)
                unique.append(e)
        return unique

    # ── Construcción de threads ───────────────────────────────────────────────

    @staticmethod
    def _build_threads(emails: list, cfg: dict) -> list:
        """Agrupa emails en threads por gmail_thread_id."""
        thread_map = defaultdict(list)
        for e in emails:
            tid = e.get('gmail_thread_id', e.get('subject_normalized', ''))
            thread_map[tid].append(e)

        now = datetime.now(timezone.utc)
        threads = []
        for tid, msgs in thread_map.items():
            msgs.sort(key=lambda m: m.get('date', ''))
            first = msgs[0]
            last = msgs[-1]

            participant_emails = list({
                m.get('from_email', '') for m in msgs if m.get('from_email')
            })
            has_internal = any(m['sender_type'] == 'internal' for m in msgs)
            has_external = any(m['sender_type'] == 'external' for m in msgs)

            def _parse_date(raw: str) -> str:
                if not raw:
                    return datetime.now(timezone.utc).isoformat()
                try:
                    return parsedate_to_datetime(raw).isoformat()
                except Exception:
                    try:
                        return datetime.fromisoformat(
                            raw.replace('Z', '+00:00')
                        ).isoformat()
                    except Exception:
                        return datetime.now(timezone.utc).isoformat()

            started_at_iso = _parse_date(first.get('date', ''))
            last_activity_iso = _parse_date(last.get('date', ''))

            hours_no_response = 0
            if last['sender_type'] == 'external':
                try:
                    last_date = datetime.fromisoformat(last_activity_iso)
                    hours_no_response = (now - last_date).total_seconds() / 3600
                except Exception:
                    pass

            no_resp_hours = cfg.get('no_response_hours', 24)
            stalled_hours = cfg.get('stalled_thread_hours', 48)
            if hours_no_response > stalled_hours:
                status = 'stalled'
            elif hours_no_response > no_resp_hours:
                status = 'needs_response'
            elif len(msgs) == 1:
                status = 'new'
            else:
                status = 'active'

            threads.append({
                'gmail_thread_id': tid,
                'subject': first.get('subject', ''),
                'subject_normalized': first.get('subject_normalized', ''),
                'started_by': first.get('from_email', ''),
                'started_by_type': first.get('sender_type', ''),
                'started_at': started_at_iso,
                'last_activity': last_activity_iso,
                'status': status,
                'message_count': len(msgs),
                'participant_emails': participant_emails,
                'has_internal_reply': has_internal,
                'has_external_reply': has_external,
                'last_sender': last.get('from_email', ''),
                'last_sender_type': last.get('sender_type', ''),
                'hours_without_response': round(hours_no_response, 1),
                'account': first.get('account', ''),
            })
        return threads
