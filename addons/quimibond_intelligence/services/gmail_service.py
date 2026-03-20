"""
Quimibond Intelligence — Gmail Service
Lectura de emails de 22 cuentas usando Service Account con Domain-Wide Delegation.
Usa google-auth + google-api-python-client en vez de OAuth2 de Apps Script.
Soporta lectura paralela con ThreadPoolExecutor.
"""
import base64
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

_logger = logging.getLogger(__name__)

# Etiquetas que excluimos al materializar mensajes desde history (equiv. a la query bootstrap)
_SKIP_LABEL_IDS = frozenset({
    'SPAM', 'CATEGORY_PROMOTIONS', 'CATEGORY_SOCIAL',
})

SCOPES_READ = ['https://www.googleapis.com/auth/gmail.readonly']
SCOPES_SEND = ['https://www.googleapis.com/auth/gmail.send']


class GmailService:
    """Lee emails de múltiples cuentas vía Gmail API con Service Account."""

    def __init__(self, service_account_info: dict):
        self._sa_info = service_account_info

    # ── Autenticación ────────────────────────────────────────────────────────

    def _get_service(self, user_email: str, scopes: list):
        """Crea un servicio Gmail autenticado para un usuario específico."""
        creds = service_account.Credentials.from_service_account_info(
            self._sa_info, scopes=scopes,
        )
        delegated = creds.with_subject(user_email)
        return build('gmail', 'v1', credentials=delegated, cache_discovery=False)

    # ── Lectura de emails ────────────────────────────────────────────────────

    def fetch_emails(
        self, account: str, max_results: int = 50,
        start_history_id: Optional[str] = None,
    ) -> Tuple[list, Optional[str]]:
        """Lee correos nuevos desde ``start_history_id`` (history.list) o bootstrap 24h.

        Retorna ``(emails, profile_history_id)`` para persistir en la siguiente corrida.
        """
        try:
            service = self._get_service(account, SCOPES_READ)
        except Exception as exc:
            _logger.error('Gmail error [%s]: %s', account, exc)
            raise

        use_bootstrap = not start_history_id
        new_ids = set()

        if start_history_id:
            try:
                new_ids = self._collect_message_ids_from_history(
                    service, start_history_id,
                )
            except HttpError as exc:
                status = getattr(getattr(exc, 'resp', None), 'status', None)
                if status == 404:
                    _logger.warning(
                        'Gmail historyId obsoleto [%s] — resync 24h', account,
                    )
                    use_bootstrap = True
                else:
                    raise

        emails = []
        if not use_bootstrap:
            _logger.info('%s: %d mensajes nuevos (history)', account, len(new_ids))
            for mid in new_ids:
                try:
                    detail = service.users().messages().get(
                        userId='me', id=mid, format='full',
                    ).execute()
                    if self._should_skip_message(detail):
                        continue
                    emails.append(self._parse_message(detail, account))
                except Exception as exc:
                    _logger.warning('  Skip msg %s: %s', mid, exc)
            return emails, self._profile_history_id(service)

        query = 'newer_than:1d -in:spam -category:promotions -category:social'
        try:
            result = service.users().messages().list(
                userId='me', q=query, maxResults=max_results,
            ).execute()
            stubs = result.get('messages', [])
        except Exception as exc:
            _logger.error('Gmail list [%s]: %s', account, exc)
            raise

        _logger.info('%s: bootstrap list → %d hilos', account, len(stubs))
        for msg_stub in stubs:
            try:
                detail = service.users().messages().get(
                    userId='me', id=msg_stub['id'], format='full',
                ).execute()
                emails.append(self._parse_message(detail, account))
            except Exception as exc:
                _logger.warning('  Skip msg %s: %s', msg_stub['id'], exc)

        return emails, self._profile_history_id(service)

    @staticmethod
    def _profile_history_id(service) -> Optional[str]:
        try:
            profile = service.users().getProfile(userId='me').execute()
            return profile.get('historyId')
        except Exception:
            return None

    @staticmethod
    def _should_skip_message(detail: dict) -> bool:
        labels = set(detail.get('labelIds') or [])
        return bool(labels & _SKIP_LABEL_IDS)

    @staticmethod
    def _collect_message_ids_from_history(service, start_history_id: str) -> set:
        """IDs añadidos al buzón desde ``start_history_id`` (paginado)."""
        found = set()
        page_token = None
        while True:
            kwargs = {
                'userId': 'me',
                'startHistoryId': str(start_history_id),
                'historyTypes': ['messageAdded'],
            }
            if page_token:
                kwargs['pageToken'] = page_token
            result = service.users().history().list(**kwargs).execute()
            for record in result.get('history', []):
                for added in record.get('messagesAdded', []):
                    mid = (added.get('message') or {}).get('id')
                    if mid:
                        found.add(mid)
            page_token = result.get('nextPageToken')
            if not page_token:
                break
        return found

    def read_all_accounts(
        self, accounts: list, history_state: Optional[Dict[str, str]] = None,
        max_workers: int = 5,
    ) -> dict:
        """Lee todas las cuentas en paralelo; ``history_state`` es account → historyId."""
        history_state = dict(history_state or {})
        all_emails = []
        success_count = 0
        failed_accounts = []
        new_history_state = dict(history_state)

        def _read_one(acct):
            start_hid = history_state.get(acct)
            return acct, self.fetch_emails(acct, start_history_id=start_hid)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_read_one, acct): acct for acct in accounts}
            for future in as_completed(futures):
                acct = futures[future]
                try:
                    _, (emails, new_hid) = future.result()
                    all_emails.extend(emails)
                    success_count += 1
                    if new_hid:
                        new_history_state[acct] = new_hid
                    _logger.info('[%d/%d] %s → %d emails',
                                 success_count + len(failed_accounts),
                                 len(accounts), acct, len(emails))
                except Exception as exc:
                    failed_accounts.append(acct)
                    _logger.error('[FAIL] %s: %s', acct, exc)

        return {
            'emails': all_emails,
            'success_count': success_count,
            'failed_count': len(failed_accounts),
            'failed_accounts': failed_accounts,
            'gmail_history_state': new_history_state,
        }

    # ── Envío de emails ──────────────────────────────────────────────────────

    def send_email(self, from_account: str, to_email: str,
                   subject: str, html_body: str):
        """Envía un email HTML vía Gmail API."""
        from email.mime.text import MIMEText

        service = self._get_service(from_account, SCOPES_SEND)
        msg = MIMEText(html_body, 'html')
        msg['to'] = to_email
        msg['subject'] = subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(
            userId='me', body={'raw': raw},
        ).execute()

    # ── Parsing interno ──────────────────────────────────────────────────────

    def _parse_message(self, detail: dict, account: str) -> dict:
        """Convierte un mensaje raw de Gmail API a nuestro formato."""
        headers = {}
        for h in detail.get('payload', {}).get('headers', []):
            headers[h['name'].lower()] = h['value']

        from_header = headers.get('from', '')
        sender_email = self._extract_email(from_header)
        sender_name = self._extract_name(from_header)
        subject = headers.get('subject', '(Sin asunto)')
        is_internal = sender_email.endswith('@quimibond.com') if sender_email else False

        in_reply_to = headers.get('in-reply-to', '')
        references = headers.get('references', '')
        is_reply = bool(in_reply_to or references or
                        re.match(r'^(re|fwd|rv):', subject, re.IGNORECASE))

        # Extraer cuerpo
        body = self._extract_body(detail.get('payload', {}))

        # Extraer adjuntos (metadata, no contenido)
        attachments = self._extract_attachments(detail.get('payload', {}))

        return {
            'gmail_message_id': detail.get('id', ''),
            'gmail_thread_id': detail.get('threadId', ''),
            'account': account,
            'department': None,  # se asigna después
            'from': from_header,
            'from_email': sender_email,
            'from_name': sender_name,
            'to': headers.get('to', ''),
            'cc': headers.get('cc', ''),
            'subject': subject,
            'subject_normalized': self._normalize_subject(subject),
            'date': headers.get('date', ''),
            'body': (body or '')[:3000],
            'snippet': detail.get('snippet', ''),
            'is_reply': is_reply,
            'sender_type': 'internal' if is_internal else 'external',
            'has_attachments': len(attachments) > 0,
            'attachments': attachments,
        }

    # ── Utilidades ───────────────────────────────────────────────────────────

    @staticmethod
    def _extract_email(from_header: str) -> str:
        match = re.search(r'<([^>]+)>', from_header)
        if match:
            return match.group(1).lower().strip()
        return from_header.lower().strip()

    @staticmethod
    def _extract_name(from_header: str) -> str:
        match = re.match(r'^"?([^"<]+)"?\s*<', from_header)
        if match:
            return match.group(1).strip()
        return from_header.split('@')[0]

    @staticmethod
    def _normalize_subject(subject: str) -> str:
        return re.sub(
            r'^(re|fwd|fw|rv|reenv)(\[\d+\])?:\s*', '', subject,
            flags=re.IGNORECASE,
        ).strip()

    @staticmethod
    def _extract_body(payload: dict) -> str:
        """Extrae el cuerpo de texto de un payload de Gmail."""
        mime = payload.get('mimeType', '')

        # Texto directo
        if mime in ('text/plain', 'text/html'):
            data = payload.get('body', {}).get('data', '')
            if data:
                return base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')

        # Multipart: buscar text/plain primero, luego text/html
        parts = payload.get('parts', [])
        plain = html = ''
        for part in parts:
            part_mime = part.get('mimeType', '')
            data = part.get('body', {}).get('data', '')
            if data:
                decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')
                if part_mime == 'text/plain' and not plain:
                    plain = decoded
                elif part_mime == 'text/html' and not html:
                    html = decoded
            # Recursivo para nested multipart
            if part.get('parts'):
                nested = GmailService._extract_body(part)
                if nested and not plain:
                    plain = nested

        # Limpiar HTML a texto si solo tenemos HTML
        if plain:
            return plain
        if html:
            return re.sub(r'<[^>]+>', ' ', html).strip()
        return ''

    @staticmethod
    def _extract_attachments(payload: dict) -> list:
        """Extrae metadata de adjuntos (no el contenido)."""
        attachments = []

        def _walk(part):
            filename = part.get('filename', '')
            if filename and part.get('body', {}).get('attachmentId'):
                attachments.append({
                    'filename': filename,
                    'mimeType': part.get('mimeType', ''),
                    'size': part.get('body', {}).get('size', 0),
                    'attachmentId': part['body']['attachmentId'],
                })
            for sub in part.get('parts', []):
                _walk(sub)

        _walk(payload)
        return attachments
