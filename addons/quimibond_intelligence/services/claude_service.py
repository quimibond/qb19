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
CLAUDE_MODEL = 'claude-sonnet-4-6'
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

    @staticmethod
    def _extract_json(text: str) -> dict:
        """Extrae JSON de la respuesta de Claude, tolerando markdown fences."""
        import re
        # Strip markdown code fences
        cleaned = text.strip()
        if cleaned.startswith('```'):
            lines = cleaned.split('\n')
            cleaned = '\n'.join(lines[1:])
            if cleaned.endswith('```'):
                cleaned = cleaned[:-3]

        # Try direct parse first
        try:
            return json.loads(cleaned)
        except (json.JSONDecodeError, ValueError):
            pass

        # Find outermost JSON object
        match = re.search(r'\{[\s\S]*\}', cleaned)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        raise ValueError('Claude no retornó JSON válido')

    # ── Fase 1: Resumen por cuenta ───────────────────────────────────────────

    def summarize_account(self, department: str, account: str,
                          email_text: str, ext_count: int, int_count: int) -> dict:
        """Analiza emails de una cuenta con contexto profundo de Odoo."""
        system = (
            'Eres un analista de inteligencia empresarial para Quimibond '
            '(manufacturera de no tejidos y textiles en México). '
            'Retorna SOLO JSON válido sin markdown.\n\n'
            'IMPORTANTE: Los emails contienen etiquetas [ODOO: ...] con datos '
            'en vivo del ERP (ventas, facturas, entregas, CRM, pagos, '
            'actividades pendientes, producción). DEBES cruzar la información '
            'del email con los datos de Odoo para dar contexto real:\n'
            '- Si alguien pide cotización y tiene facturas vencidas → señalar\n'
            '- Si alguien pregunta por entrega y hay un picking retrasado → señalar\n'
            '- Si hay una oportunidad CRM abierta relacionada → mencionar\n'
            '- Si hay actividades vencidas del equipo con ese contacto → señalar\n'
            '- Si hay pagos recientes → contextualizar la relación\n'
            '- Si hay producción en curso para ese cliente → informar avance'
        )

        prompt = (
            f'Analiza los {ext_count + int_count} emails de {department} ({account}).\n'
            f'{ext_count} son de externos, {int_count} internos.\n\n'
            'Retorna SOLO un JSON válido con esta estructura exacta:\n'
            '{\n'
            '  "summary_text": "Resumen narrativo de 2-3 oraciones que CRUCE '
            'info del email con datos de Odoo",\n'
            '  "overall_sentiment": "positive|neutral|negative|mixed",\n'
            '  "sentiment_detail": "Breve explicación del tono general",\n'
            '  "key_items": [{"item": "desc", "priority": "high|medium|low", '
            '"from": "remitente", "action_needed": "qué hacer", '
            '"importance_score": 1, '
            '"odoo_context": "datos relevantes del ERP si aplica"}],\n'
            '  "waiting_response": [{"contact": "nombre <email>", '
            '"subject": "asunto", "hours_waiting": 0, "is_external": true, '
            '"urgency": "high|medium|low", '
            '"business_impact": "impacto si no se responde"}],\n'
            '  "urgent_items": [{"item": "desc", "reason": "por qué", '
            '"suggested_action": "qué debería hacer José", '
            '"financial_impact": "monto en riesgo si aplica"}],\n'
            '  "external_contacts": [{"name": "nombre", "email": "email", '
            '"company": "empresa", "topic": "de qué escriben", '
            '"sentiment": "positive|neutral|negative", '
            '"relationship_signal": "strengthening|stable|cooling|at_risk", '
            '"odoo_profile": "resumen de su situación en el ERP"}],\n'
            '  "topics_detected": [{"topic": "nombre", '
            '"status": "new|ongoing|resolved", "detail": "desc breve"}],\n'
            '  "attachment_insights": [{"filename": "nombre", '
            '"type": "pdf|excel|text|image", '
            '"summary": "resumen", "business_impact": "relevancia"}],\n'
            '  "risks_detected": [{"risk": "desc", '
            '"severity": "high|medium|low", '
            '"accounts_involved": ["cuentas"], '
            '"mitigation": "qué se puede hacer"}],\n'
            '  "person_insights": [{"name": "nombre completo", '
            '"email": "email", "company": "empresa", '
            '"role_detected": "rol inferido del email", '
            '"communication_style": "formal|informal|técnico|ejecutivo", '
            '"key_interests": ["temas que le importan"], '
            '"decision_power": "high|medium|low", '
            '"notes": "observaciones para recordar"}]\n'
            '}\n\n'
            'REGLAS:\n'
            '- Si no hay items urgentes, deja el array vacío\n'
            '- Sé específico con nombres y empresas\n'
            '- importance_score: 1=trivial, 5=normal, 8=importante, 10=crítico\n'
            '- CONTEXTO ODOO: Los emails con [ODOO: ...] tienen datos en vivo del '
            'ERP (ventas, facturas, pagos, entregas, CRM, actividades, producción). '
            'CRUZA esta información con el contenido del email.\n'
            '- person_insights: Identifica a CADA persona que escribe. Infiere '
            'su rol, estilo, nivel de decisión. Esto alimenta la memoria del sistema.\n\n'
            f'EMAILS:\n{email_text}'
        )

        text = self._call(system, prompt, max_tokens=4000)
        parsed = self._extract_json(text)
        parsed['external_emails'] = ext_count
        parsed['internal_emails'] = int_count
        return parsed

    # ── Fase 2: Síntesis ejecutiva ───────────────────────────────────────────

    def synthesize_briefing(self, data_package: str) -> str:
        """Genera el HTML del briefing ejecutivo con contexto profundo."""
        system = (
            'Eres el Chief Intelligence Officer de Quimibond, una productora de '
            'no tejidos y textiles en México. Produces un briefing diario para el '
            'Director General (José Mizrahi). Eres sus ojos y oídos.\n\n'
            'Tu briefing debe ser ACCIONABLE y DIRECTO. José necesita saber:\n'
            '1. Qué está pasando AHORA que requiere su atención\n'
            '2. Quién está haciendo bien su trabajo y quién no\n'
            '3. Qué clientes/proveedores esperan respuesta\n'
            '4. Qué temas nuevos aparecieron y cuáles siguen sin resolver\n'
            '5. Patrones y oportunidades\n'
            '6. Si las acciones sugeridas ayer se ejecutaron o no\n'
            '7. Estado de entregas, producción y pipeline comercial\n\n'
            'TIENES ACCESO A DATOS EN VIVO DE ODOO:\n'
            '- Pedidos de venta, facturas, pagos reales\n'
            '- Pipeline CRM (oportunidades, etapas, revenue esperado)\n'
            '- Entregas pendientes y retrasadas (stock.picking)\n'
            '- Producción en proceso (mrp.production)\n'
            '- Actividades del equipo (quién tiene qué pendiente)\n'
            '- Comunicación interna (chatter de Odoo)\n'
            '- Reuniones agendadas (calendar)\n'
            '- Verificación de acciones previas (¿se hicieron o no?)\n\n'
            'CRUZA la información de los emails con los datos de Odoo. '
            'Por ejemplo: si un cliente pregunta por su entrega en un email '
            'y en Odoo ves que el picking está retrasado, DILO CLARAMENTE.\n\n'
            'FORMATO HTML con estas secciones:\n'
            '<h2>🚨 REQUIERE TU ATENCIÓN AHORA</h2>\n'
            '(Solo lo verdaderamente urgente, cruzado con datos de Odoo)\n\n'
            '<h2>📊 SCORECARD DE HOY</h2>\n'
            '(Tabla: emails, threads, respuestas, alertas, pipeline CRM)\n\n'
            '<h2>⏱️ TIEMPOS DE RESPUESTA</h2>\n'
            '(Por cuenta, con contexto del impacto comercial)\n\n'
            '<h2>✅ SEGUIMIENTO DE ACCIONES</h2>\n'
            '(¿Se ejecutaron las acciones sugeridas ayer? Evidencia de Odoo. '
            'Tasa de completado. Quién cumplió y quién no.)\n\n'
            '<h2>🔍 ANÁLISIS POR ÁREA</h2>\n'
            '(Resumen por departamento con datos de Odoo integrados)\n\n'
            '<h2>📦 OPERACIONES</h2>\n'
            '(Entregas pendientes/retrasadas, producción en proceso, '
            'problemas de supply chain)\n\n'
            '<h2>💰 COMERCIAL Y PIPELINE</h2>\n'
            '(Oportunidades CRM, pedidos, facturación, cobranza, pagos)\n\n'
            '<h2>👥 ACCOUNTABILITY DEL EQUIPO</h2>\n'
            '(Actividades pendientes por persona, vencidas, cumplimiento)\n\n'
            '<h2>🤝 CLIENTES Y PROVEEDORES</h2>\n'
            '(Perfil cruzado: email + Odoo. Relación, riesgo, contexto)\n\n'
            '<h2>⚠️ RIESGOS DETECTADOS</h2>\n'
            '(Basados en datos reales: facturas vencidas + emails ignorados = peligro)\n\n'
            '<h2>📈 TENDENCIAS Y PATRONES</h2>\n'
            '(Comparativa con días anteriores, patrones recurrentes)\n\n'
            '<h2>🎯 ACCIONES PARA MAÑANA</h2>\n'
            '(Específicas: quién debe hacer qué, con quién, por qué)\n\n'
            'Sé brutalmente honesto. Sin filtros. Si alguien no hizo lo que debía, '
            'dilo. Si un cliente está en riesgo y nadie actuó, escálalo.\n'
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
            parsed = self._extract_json(text)
            return parsed.get('topics', [])
        except Exception as exc:
            _logger.warning('Error extrayendo temas: %s', exc)
        return []



    def extract_knowledge(self, emails_text, account):
        """Extrae knowledge graph con perfil profundo de personas."""
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
            'negotiates_with|mentioned_with|reports_to|collaborates_with",'
            ' "context": "str"}],'
            ' "person_profiles": [{"name": "str", "email": "str or null",'
            ' "company": "str or null",'
            ' "role": "str (cargo o rol inferido)",'
            ' "department": "str or null",'
            ' "decision_power": "high|medium|low",'
            ' "communication_style": "formal|informal|tecnico|ejecutivo",'
            ' "language_preference": "es|en|mixed",'
            ' "key_interests": ["temas que le importan"],'
            ' "personality_notes": "observaciones sobre cómo se comunica",'
            ' "negotiation_style": "aggressive|collaborative|passive|analytical or null",'
            ' "response_pattern": "fast|normal|slow or null",'
            ' "influence_on_deals": "str or null"}]}'
        )
        prompt = (
            'Analiza emails de ' + account
            + ' de Quimibond (textiles no tejidos, Mexico).\n\n'
            + 'EMAILS:\n' + emails_text[:12000]
            + '\n\nExtrae en JSON schema:\n' + schema
            + '\n\nREGLAS:\n'
            + '- Solo info EXPLICITA para facts. Confidence 0.8+ claros, 0.3-0.5 implicitos.\n'
            + '- person_profiles: Para CADA persona que aparece en los emails, '
            + 'construye un perfil. Infiere rol, estilo de comunicacion, nivel de decision, '
            + 'intereses clave. Esto alimenta la memoria a largo plazo del sistema.\n'
            + '- Identifica TODAS las relaciones entre personas y empresas.\n'
            + '- Solo JSON valido.'
        )
        try:
            system = (
                'Eres un analista de inteligencia de Quimibond (textiles no tejidos, Mexico). '
                'Extrae entidades, hechos, action items, relaciones Y perfiles '
                'detallados de personas. El objetivo es que el sistema APRENDA '
                'de cada persona con cada email que procesa. '
                'Retorna SOLO JSON valido sin markdown.'
            )
            raw = self._call(system, prompt, max_tokens=4000)
            return self._extract_json(raw)
        except Exception as exc:
            _logger.warning('KG extract fail: %s', exc)
            return {
                'entities': [], 'facts': [],
                'action_items': [], 'relationships': [],
                'person_profiles': [],
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
