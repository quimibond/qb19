"""
Quimibond Intelligence — Gmail Service
Lectura de emails de 22 cuentas usando Service Account con Domain-Wide Delegation.
Usa google-auth + google-api-python-client en vez de OAuth2 de Apps Script.
Soporta lectura paralela con ThreadPoolExecutor.
"""
import base64
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from google.oauth2 import service_account
from googleapiclient.discovery import build

_logger = logging.getLogger(__name__)

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

    def fetch_emails(self, account: str, max_results: int = 50) -> list:
        """Lee emails de las últimas 24h de una cuenta."""
        try:
            service = self._get_service(account, SCOPES_READ)
            query = 'newer_than:1d -in:spam -category:promotions -category:social'
            result = service.users().messages().list(
                userId='me', q=query, maxResults=max_results,
            ).execute()
            messages = result.get('messages', [])
        except Exception as exc:
            _logger.error('Gmail error [%s]: %s', account, exc)
            raise

        emails = []
        for msg_stub in messages:
            try:
                detail = service.users().messages().get(
                    userId='me', id=msg_stub['id'], format='full',
                ).execute()
                emails.append(self._parse_message(detail, account))
            except Exception as exc:
                _logger.warning('  Skip msg %s: %s', msg_stub['id'], exc)
        return emails

    def read_all_accounts(self, accounts: list, max_workers: int = 5) -> dict:
        """Lee TODAS las cuentas en paralelo. Retorna dict con resultados."""
        all_emails = []
        success_count = 0
        failed_accounts = []

        def _read_one(acct):
            return acct, self.fetch_emails(acct)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_read_one, acct): acct for acct in accounts}
            for future in as_completed(futures):
                acct = futures[future]
                try:
                    _, emails = future.result()
                    all_emails.extend(emails)
                    success_count += 1
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
