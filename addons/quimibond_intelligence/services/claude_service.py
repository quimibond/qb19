"""
Quimibond Intelligence — Claude & Voyage AI Service
Llamadas a Claude API para análisis y síntesis, y a Voyage AI para embeddings.
"""
import json
import logging
import time

import httpx

_logger = logging.getLogger(__name__)

CLAUDE_ENDPOINT = 'https://api.anthropic.com/v1/messages'
CLAUDE_MODEL = 'claude-sonnet-4-20250514'
CLAUDE_API_VERSION = '2023-06-01'
CLAUDE_MAX_TOKENS = 8000

VOYAGE_ENDPOINT = 'https://api.voyageai.com/v1/embeddings'
VOYAGE_MODEL = 'voyage-3'


class ClaudeService:
    """Interacción con Claude API para análisis de comunicaciones."""

    def __init__(self, api_key: str, delay_between_calls: float = 3.0):
        self._api_key = api_key
        self._delay = delay_between_calls

    def _call(self, system: str, user_content: str,
              max_tokens: int = 3000, retries: int = 3) -> str:
        """Llamada genérica a Claude con retry inteligente para rate limits."""
        headers = {
            'x-api-key': self._api_key,
            'anthropic-version': CLAUDE_API_VERSION,
            'content-type': 'application/json',
        }
        payload = {
            'model': CLAUDE_MODEL,
            'max_tokens': max_tokens,
            'system': system,
            'messages': [{'role': 'user', 'content': user_content}],
        }

        last_error = None
        for attempt in range(retries + 1):
            try:
                with httpx.Client(timeout=120) as client:
                    resp = client.post(CLAUDE_ENDPOINT, headers=headers,
                                       json=payload)
                if resp.status_code == 200:
                    return resp.json()['content'][0]['text']

                # Rate limit
                if resp.status_code == 429:
                    import re as _re
                    match = _re.search(r'try again in (\d+\.?\d*)', resp.text, _re.I)
                    wait = float(match.group(1)) + 2 if match else (attempt + 1) * 30
                    _logger.warning('Rate limit 429 — waiting %.0fs (retry %d/%d)',
                                    wait, attempt + 1, retries)
                    time.sleep(wait)
                    continue

                raise RuntimeError(f'Claude {resp.status_code}: {resp.text[:300]}')

            except Exception as exc:
                last_error = exc
                if attempt < retries:
                    wait = 2 ** attempt * 2
                    _logger.warning('Retry %d/%d: %s (wait %ds)',
                                    attempt + 1, retries, exc, wait)
                    time.sleep(wait)

        raise last_error

    # ── Fase 1: Resumen por cuenta ───────────────────────────────────────────

    def summarize_account(self, department: str, account: str,
                          email_text: str, ext_count: int, int_count: int) -> dict:
        """Analiza emails de una cuenta. Retorna JSON estructurado."""
        system = ('Eres un analista de comunicaciones empresariales para Quimibond '
                  '(no tejidos y textiles). Retorna SOLO JSON válido sin markdown.')

        prompt = (
            f'Analiza los {ext_count + int_count} emails de {department} ({account}).\n'
            f'{ext_count} son de externos, {int_count} internos.\n\n'
            'Retorna SOLO un JSON válido con esta estructura exacta:\n'
            '{\n'
            '  "summary_text": "Resumen narrativo de 2-3 oraciones",\n'
            '  "overall_sentiment": "positive|neutral|negative|mixed",\n'
            '  "sentiment_detail": "Breve explicación del tono general",\n'
            '  "key_items": [{"item": "desc", "priority": "high|medium|low", '
            '"from": "remitente", "action_needed": "qué hacer", "importance_score": 1}],\n'
            '  "waiting_response": [{"contact": "nombre <email>", "subject": "asunto", '
            '"hours_waiting": 0, "is_external": true, "urgency": "high|medium|low"}],\n'
            '  "urgent_items": [{"item": "desc", "reason": "por qué", '
            '"suggested_action": "qué debería hacer José"}],\n'
            '  "external_contacts": [{"name": "nombre", "email": "email", '
            '"company": "empresa", "topic": "de qué escriben", '
            '"sentiment": "positive|neutral|negative", '
            '"relationship_signal": "strengthening|stable|cooling|at_risk"}],\n'
            '  "topics_detected": [{"topic": "nombre", "status": "new|ongoing|resolved", '
            '"detail": "desc breve"}],\n'
            '  "attachment_insights": [{"filename": "nombre", "type": "pdf|excel|text|image", '
            '"summary": "resumen", "business_impact": "relevancia"}],\n'
            '  "risks_detected": [{"risk": "desc", "severity": "high|medium|low", '
            '"accounts_involved": ["cuentas"]}]\n'
            '}\n\n'
            'REGLAS:\n'
            '- Si no hay items urgentes, deja el array vacío\n'
            '- Sé específico con nombres y empresas\n'
            '- importance_score: 1=trivial, 5=normal, 8=importante, 10=crítico\n'
            '- CONTEXTO ODOO: Los emails con [ODOO: ...] tienen datos del ERP. '
            'Prioriza por impacto financiero.\n\n'
            f'EMAILS:\n{email_text}'
        )

        text = self._call(system, prompt, max_tokens=3000)

        # Extraer JSON
        import re
        match = re.search(r'\{[\s\S]*\}', text)
        if not match:
            raise ValueError('Claude no retornó JSON válido')
        parsed = json.loads(match.group(0))
        parsed['external_emails'] = ext_count
        parsed['internal_emails'] = int_count
        return parsed

    # ── Fase 2: Síntesis ejecutiva ───────────────────────────────────────────

    def synthesize_briefing(self, data_package: str) -> str:
        """Genera el HTML del briefing ejecutivo."""
        system = (
            'Eres el Chief Intelligence Officer de Quimibond, una productora de '
            'no tejidos y textiles en México. Produces un briefing diario para el '
            'Director General (José Mizrahi). Eres sus ojos y oídos.\n\n'
            'Tu briefing debe ser ACCIONABLE y DIRECTO. José necesita saber:\n'
            '1. Qué está pasando AHORA que requiere su atención\n'
            '2. Quién está haciendo bien su trabajo y quién no\n'
            '3. Qué clientes/proveedores esperan respuesta\n'
            '4. Qué temas nuevos aparecieron y cuáles siguen sin resolver\n'
            '5. Patrones y oportunidades\n\n'
            'FORMATO HTML con estas secciones:\n'
            '<h2>🚨 REQUIERE TU ATENCIÓN AHORA</h2>\n'
            '<h2>📊 SCORECARD DE HOY</h2>\n'
            '<h2>⏱️ TIEMPOS DE RESPUESTA</h2>\n'
            '<h2>🔍 ANÁLISIS POR ÁREA</h2>\n'
            '<h2>👥 ACCOUNTABILITY</h2>\n'
            '<h2>🤝 CLIENTES Y EXTERNOS</h2>\n'
            '<h2>⚠️ RIESGOS DETECTADOS</h2>\n'
            '<h2>💰 IMPACTO COMERCIAL</h2>\n'
            '<h2>📈 TENDENCIAS</h2>\n'
            '<h2>✅ ACCIONES SUGERIDAS</h2>\n\n'
            'Sé brutalmente honesto. Sin filtros. '
            'Usa <h2>, <h3>, <p>, <ul>, <li>, <strong>, <table>.'
        )
        return self._call(system, data_package, max_tokens=CLAUDE_MAX_TOKENS)

    # ── Extracción de temas ──────────────────────────────────────────────────

    def extract_topics(self, briefing_html: str) -> list:
        """Extrae temas clave del briefing."""
        try:
            text = self._call(
                'Extrae temas clave del briefing. Retorna JSON puro sin markdown.',
                f'Extrae temas. JSON: {{"topics":[{{"topic":"nombre","category":"área",'
                f'"status":"active|resolved|pending","priority":"high|medium|low",'
                f'"summary":"desc"}}]}}\n\n{briefing_html}',
                max_tokens=4000,
            )
            import re
            match = re.search(r'\{[\s\S]*\}', text)
            if match:
                return json.loads(match.group(0)).get('topics', [])
        except Exception as exc:
            _logger.warning('Error extrayendo temas: %s', exc)
        return []



    def extract_knowledge(self, emails_text, account):
        schema = (
            '{"entities": [{"name": "str", "type": "person|company|product|machine|raw_material",'
            ' "email": "str or null", "attributes": {}}],'
            ' "facts": [{"entity_name": "str",'
            ' "type": "commitment|statement|price|quantity|delivery_date|payment|complaint|request|'
            'approval|rejection|information|change",'
            ' "text": "str", "date": "YYYY-MM-DD or null", "is_future": false, "confidence": 0.8}],'
            ' "action_items": [{"assignee": "str", "related_to": "str", "description": "str",'
            ' "type": "call|email|meeting|follow_up|send_quote|send_invoice|review|approve|deliver|'
            'pay|investigate|other",'
            ' "priority": "low|medium|high|critical", "due_date": "YYYY-MM-DD or null"}],'
            ' "relationships": [{"entity_a": "str", "entity_b": "str",'
            ' "type": "works_at|buys_from|sells_to|manages|supplies|manufactures|'
            'negotiates_with|mentioned_with",'
            ' "context": "str"}]}'
        )
        prompt = (
            'Analiza emails de ' + account
            + ' de Quimibond (textiles no tejidos, Mexico).\n\n'
            + 'EMAILS:\n' + emails_text[:12000]
            + '\n\nExtrae en JSON schema:\n' + schema
            + '\n\nREGLAS: Solo info EXPLICITA. '
            + 'Facts = datos verificables. '
            + 'Confidence 0.8+ claros, 0.3-0.5 implicitos. '
            + 'Solo JSON valido.'
        )
        try:
            raw = self._call(prompt, max_tokens=3000)
            raw = raw.strip()
            if raw.startswith('`' * 3):
                lines = raw.split('\n')
                raw = '\n'.join(lines[1:])
                if raw.endswith('`' * 3):
                    raw = raw[:-3]
            import json
            return json.loads(raw)
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning('KG extract fail: %s', exc)
            return {
                'entities': [], 'facts': [],
                'action_items': [], 'relationships': [],
            }


class VoyageService:
    """Genera embeddings con Voyage AI para memoria semántica."""

    def __init__(self, api_key: str, model: str = VOYAGE_MODEL):
        self._api_key = api_key
        self._model = model

    def embed(self, texts: list[str], input_type: str = 'document') -> list:
        """Genera embeddings para una lista de textos."""
        if not texts:
            return []
        headers = {
            'Authorization': f'Bearer {self._api_key}',
            'Content-Type': 'application/json',
        }
        payload = {
            'model': self._model,
            'input': texts[:128],  # Voyage límite
            'input_type': input_type,
        }
        with httpx.Client(timeout=60) as client:
            resp = client.post(VOYAGE_ENDPOINT, headers=headers, json=payload)
        if resp.status_code != 200:
            raise RuntimeError(f'Voyage {resp.status_code}: {resp.text[:200]}')
        data = resp.json().get('data', [])
        return [item['embedding'] for item in data]

    def embed_query(self, text: str) -> list:
        """Genera embedding para una query de búsqueda."""
        results = self.embed([text], input_type='query')
        return results[0] if results else []
