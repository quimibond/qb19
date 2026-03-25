"""
Engine — Briefing Reply Detection

Detecta replies al email de briefing diario y genera respuestas
automáticas usando el RAG del sistema. El CEO puede responder
al briefing con preguntas y recibir follow-up por email.
"""
import json
import logging
import time

from odoo import api, models

_logger = logging.getLogger(__name__)

BRIEFING_SUBJECT_MARKER = 'Intelligence Briefing'


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    @api.model
    def run_check_briefing_replies(self):
        """Detecta replies al briefing y genera respuestas. Corre cada 30 min."""
        lock = 'quimibond_intelligence.briefing_reply_running'
        ICP = self.env['ir.config_parameter'].sudo()
        if ICP.get_param(lock, 'false') == 'true':
            return
        ICP.set_param(lock, 'true')

        try:
            cfg = self._load_config()
            if not cfg:
                return

            sender_email = (cfg.get('sender_email') or '').strip()
            recipient_email = (cfg.get('recipient_email') or '').strip()
            if not sender_email or not recipient_email:
                return

            from ..services.supabase_service import SupabaseService

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                replies = self._find_briefing_replies(
                    supa, sender_email, recipient_email)

                if not replies:
                    return

                _logger.info(
                    'Found %d briefing replies to process', len(replies))

                for reply in replies:
                    try:
                        self._process_briefing_reply(
                            reply, cfg, supa, sender_email, recipient_email)
                    except Exception as exc:
                        _logger.warning(
                            'Briefing reply processing: %s', exc)

        except Exception as exc:
            _logger.error('run_check_briefing_replies: %s', exc, exc_info=True)
        finally:
            ICP.set_param(lock, 'false')

    def _find_briefing_replies(self, supa, sender_email, recipient_email):
        """Encuentra emails que son respuestas al briefing."""
        try:
            # Buscar emails recientes del recipient al sender con subject
            # que contenga "Re:" y "Intelligence Briefing"
            from urllib.parse import quote as url_quote
            encoded_sender = url_quote(recipient_email, safe='')
            replies = supa._request(
                '/rest/v1/emails?order=email_date.desc'
                '&limit=10'
                '&select=gmail_message_id,sender,subject,body,snippet,'
                'email_date'
                f'&sender=eq.{encoded_sender}'
                f'&subject=ilike.*Re:*{BRIEFING_SUBJECT_MARKER}*',
            ) or []

            if not replies:
                return []

            # Filtrar los que ya fueron procesados
            processed_key = 'quimibond_intelligence.briefing_replies_processed'
            processed_raw = (
                self.env['ir.config_parameter'].sudo()
                .get_param(processed_key, '[]')
            )
            try:
                processed_ids = set(json.loads(processed_raw))
            except (json.JSONDecodeError, TypeError):
                processed_ids = set()

            new_replies = [
                r for r in replies
                if r.get('gmail_message_id')
                and r['gmail_message_id'] not in processed_ids
            ]
            return new_replies

        except Exception as exc:
            _logger.debug('find_briefing_replies: %s', exc)
            return []

    def _process_briefing_reply(self, reply, cfg, supa,
                                sender_email, recipient_email):
        """Procesa una reply al briefing: extrae pregunta, genera respuesta, envía."""
        question = (
            reply.get('body', '') or reply.get('snippet', '')
        ).strip()
        if not question or len(question) < 10:
            return

        # Guardar como query en Odoo (memoria conversacional)
        Query = self.env['intelligence.query'].sudo()
        query = Query.create({
            'question': question[:2000],
            'session_id': 'briefing-reply',
        })

        # Ejecutar RAG
        query.action_ask()

        if not query.answer:
            return

        # Enviar respuesta por email
        from ..services.analysis_service import AnalysisService
        from ..services.gmail_service import GmailService

        sa_info = json.loads(cfg['service_account_json'])
        gmail = GmailService(sa_info)
        analysis = AnalysisService()

        subject = reply.get('subject', 'Re: Intelligence Briefing')
        if not subject.startswith('Re:'):
            subject = f'Re: {subject}'

        from odoo.tools import html2plaintext
        body_html = (
            f'<div style="font-family: Arial, sans-serif;">'
            f'<p><strong>Respuesta del sistema de inteligencia:</strong></p>'
            f'{query.answer}'
            f'<hr/>'
            f'<p style="color: #666; font-size: 12px;">'
            f'Pregunta original: {html2plaintext(question)[:200]}</p>'
            f'</div>'
        )

        try:
            gmail.send_email(
                sender_email, recipient_email,
                subject, body_html,
            )
            _logger.info(
                'Briefing reply answered: %s', question[:80])
        except Exception as exc:
            _logger.warning('Send briefing reply: %s', exc)

        # Marcar como procesado
        processed_key = 'quimibond_intelligence.briefing_replies_processed'
        processed_raw = (
            self.env['ir.config_parameter'].sudo()
            .get_param(processed_key, '[]')
        )
        try:
            processed_ids = json.loads(processed_raw)
        except (json.JSONDecodeError, TypeError):
            processed_ids = []
        gid = reply.get('gmail_message_id', '')
        if gid:
            processed_ids.append(gid)
            # Keep only last 100 processed IDs
            self.env['ir.config_parameter'].sudo().set_param(
                processed_key,
                json.dumps(processed_ids[-100:]),
            )
