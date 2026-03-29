"""
Quimibond Intelligence — Claude & Voyage AI Service
Llamadas a Claude API para análisis y síntesis, y a Voyage AI para embeddings.
Usa el SDK oficial de Anthropic (retries, rate limits, errores tipados built-in).
"""
import json
import logging
import re

import anthropic
import httpx

_logger = logging.getLogger(__name__)

CLAUDE_DEFAULT_MODEL = 'claude-sonnet-4-6'
CLAUDE_MAX_TOKENS = 8000

VOYAGE_ENDPOINT = 'https://api.voyageai.com/v1/embeddings'
VOYAGE_MODEL = 'voyage-3'


def _smart_truncate(text: str, max_chars: int = 6000) -> str:
    """Trunca texto en un límite de línea para preservar contexto completo."""
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars].rfind('\n')
    if cut > max_chars * 0.7:
        return text[:cut] + '\n[... truncado]'
    return text[:max_chars] + '\n[... truncado]'


_MODEL_CACHE = {}


def _resolve_model(client: anthropic.Anthropic, preferred: str) -> str:
    """Valida el modelo preferido contra la API. Cachea resultado en memoria."""
    if preferred in _MODEL_CACHE:
        return _MODEL_CACHE[preferred]

    try:
        client.models.retrieve(model_id=preferred)
        _MODEL_CACHE[preferred] = preferred
        return preferred
    except anthropic.NotFoundError:
        _logger.warning('Modelo %s no existe, buscando alternativa...', preferred)

    # Buscar el sonnet más reciente disponible
    try:
        available = client.models.list(limit=100)
        sonnet_models = [
            m for m in available.data
            if 'sonnet' in m.id and 'claude' in m.id
        ]
        if sonnet_models:
            sonnet_models.sort(key=lambda m: m.created_at, reverse=True)
            fallback = sonnet_models[0].id
            _logger.warning('Usando modelo alternativo: %s', fallback)
            _MODEL_CACHE[preferred] = fallback
            return fallback
    except Exception as exc:
        _logger.warning('No se pudo listar modelos: %s', exc)

    raise RuntimeError(
        f'Modelo {preferred} no disponible y no se encontró alternativa. '
        f'Configura un modelo válido en Ajustes > Intelligence System.'
    )


class ClaudeService:
    """Interacción con Claude API para análisis de comunicaciones."""

    def __init__(self, api_key: str, model: str = '', delay_between_calls: float = 3.0):
        self._client = anthropic.Anthropic(
            api_key=api_key,
            max_retries=3,
            timeout=120.0,
        )
        preferred = model or CLAUDE_DEFAULT_MODEL
        self._model = _resolve_model(self._client, preferred)
        self._delay = delay_between_calls

    def _call(self, system: str, user_content: str,
              max_tokens: int = 3000) -> str:
        """Llamada genérica a Claude. Retries y rate limits manejados por el SDK."""
        message = self._client.messages.create(
            model=self._model,
            max_tokens=max_tokens,
            system=system,
            messages=[{'role': 'user', 'content': user_content}],
        )

        if message.stop_reason == 'max_tokens':
            _logger.warning(
                'Claude response truncada (max_tokens=%d, usage=%d/%d)',
                max_tokens,
                message.usage.input_tokens,
                message.usage.output_tokens,
            )

        content = message.content
        if not content or not hasattr(content[0], 'text'):
            raise RuntimeError('Claude response missing content text')
        return content[0].text

    @staticmethod
    def _extract_json(text: str) -> dict:
        """Extrae JSON de la respuesta de Claude, tolerando markdown fences."""
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
            'en vivo del ERP. DEBES cruzar la información del email con los '
            'datos de Odoo para dar contexto de negocio real:\n'
            '- Si pide cotización y tiene facturas vencidas → señalar riesgo\n'
            '- Si pregunta por entrega y hay picking retrasado → señalar\n'
            '- Si hay oportunidad CRM abierta → mencionar revenue esperado\n'
            '- Si hay actividades vencidas del equipo → señalar accountability\n'
            '- Si hay pagos recientes → contextualizar relación\n'
            '- Si hay producción en curso → informar avance\n'
            '- LTV y TENDENCIA: Si compra menos que antes (📉) → alerta churn\n'
            '- CARTERA VENCIDA (aging 30/60/90+d): Riesgo financiero real\n'
            '- PRODUCTOS con stock → mencionar disponibilidad al cotizar\n'
            '- RED de contactos en misma empresa → contexto político\n'
            '- DEVOLUCIONES/NC → señal de problemas de calidad\n'
            '- ENTREGA OTD → si bajo, reconocer problema antes que reclamen'
        )

        email_text = _smart_truncate(email_text, 12000)

        prompt = (
            f'Analiza los {ext_count + int_count} emails de {department} ({account}).\n'
            f'{ext_count} son de externos, {int_count} internos.\n\n'
            'Retorna SOLO un JSON válido con esta estructura exacta:\n'
            '{\n'
            '  "summary_text": "Resumen narrativo de 2-3 oraciones que CRUCE '
            'info del email con datos de Odoo",\n'
            '  "overall_sentiment": "positive|neutral|negative|mixed",\n'
            '  "sentiment_score": 0.0,\n'
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
            '"sentiment_score": 0.0, '
            '"relationship_signal": "strengthening|stable|cooling|at_risk", '
            '"odoo_profile": "resumen de su situación en el ERP"}],\n'
            '  "competitors_mentioned": [{"name": "nombre del competidor", '
            '"context": "en qué contexto se mencionó", '
            '"threat_level": "high|medium|low", '
            '"mentioned_by": "nombre del contacto que lo mencionó", '
            '"detail": "qué dijo exactamente o se infiere"}],\n'
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
            '"personality_traits": ["rasgos observados: detallista, impaciente, '
            'negociador, leal, price-sensitive, etc."], '
            '"key_interests": ["temas que le importan"], '
            '"decision_power": "high|medium|low", '
            '"decision_factors": ["qué le importa al decidir: precio, plazo, '
            'calidad, relación, servicio"], '
            '"relationship_pattern": "gatekeeper|decision-maker|influencer|user", '
            '"notes": "observaciones para recordar"}]\n'
            '}\n\n'
            'REGLAS:\n'
            '- Si no hay items urgentes, deja el array vacío\n'
            '- Sé específico con nombres y empresas\n'
            '- importance_score: 1=trivial, 5=normal, 8=importante, 10=crítico\n'
            '- sentiment_score: número de -1.0 (muy negativo) a 1.0 (muy positivo).\n'
            '  Detecta matices: "cuando sería posible..." = urgente educado (-0.2),\n'
            '  "entiendo que están ocupados..." = frustrado pasivo (-0.4),\n'
            '  cortesía social vs entusiasmo real. NO te limites a positivo/negativo.\n'
            '- competitors_mentioned: Detecta CUALQUIER mención de competidores,\n'
            '  directa ("cotizamos con X") o indirecta ("otra opción", "alternativa").\n'
            '  threat_level: high=comparando precios, medium=mencionado, low=referencia casual.\n'
            '- personality_traits: Array de rasgos concretos observados en el email.\n'
            '  Ejemplos: ["detallista", "impaciente", "negociador agresivo", "leal",\n'
            '  "price-sensitive", "orientado a plazos", "técnico"].\n'
            '- decision_factors: Qué le importa a esta persona al tomar decisiones.\n'
            '- CONTEXTO ODOO: Los emails con [ODOO: ...] tienen datos en vivo del '
            'ERP (ventas, facturas, pagos, entregas, CRM, actividades, producción). '
            'CRUZA esta información con el contenido del email.\n'
            '- person_insights: Identifica a CADA persona que escribe. Infiere '
            'su rol, estilo, nivel de decisión. Esto alimenta la memoria del sistema.\n'
            '- role_detected OBLIGATORIO: Infiere el rol funcional de cada persona:\n'
            '  compras, ventas, dirección, logística, calidad, finanzas, '
            'producción, administración, legal, TI, RRHH, marketing.\n'
            '  Basa tu inferencia en: firma del email, tono, tipo de decisiones, '
            'temas que discute, cómo se refieren a esta persona otros.\n'
            '- decision_power OBLIGATORIO: Clasifica a cada persona:\n'
            '  high = toma decisiones de compra/venta directamente\n'
            '  medium = influye en decisiones, aprueba operaciones\n'
            '  low = ejecuta instrucciones, coordina operaciones\n'
            '- relationship_pattern: gatekeeper (controla acceso al decisor), '
            'decision-maker (decide), influencer (opina), user (usa el producto)\n\n'
            f'EMAILS:\n{email_text}'
        )

        text = self._call(system, prompt, max_tokens=8000)
        parsed = self._extract_json(text)
        parsed['external_emails'] = ext_count
        parsed['internal_emails'] = int_count
        return parsed

    # ── Análisis unificado (summary + KG en una sola llamada) ──────────────

    def analyze_account_full(self, department: str, account: str,
                             email_text: str, ext_count: int,
                             int_count: int) -> dict:
        """Análisis completo: resumen de cuenta + extracción de KG en una llamada.

        Retorna dict con dos claves top-level:
        - 'summary': mismo formato que summarize_account()
        - 'knowledge_graph': mismo formato que extract_knowledge()
        """
        system = (
            'Analista de inteligencia para Quimibond (textiles, México). '
            'Retorna SOLO JSON válido. Tags [ODOO:] son datos del ERP.'
        )

        email_text = _smart_truncate(email_text, 6000)

        prompt = (
            f'{department} ({account}): {ext_count} ext + {int_count} int emails.\n'
            f'{ext_count} externos, {int_count} internos.\n\n'
            'JSON:\n'
            '{"summary":{"summary_text":"resumen 2-3 oraciones",'
            '"overall_sentiment":"positive|neutral|negative|mixed",'
            '"sentiment_score":0.0,'
            '"topics_detected":[{"topic":"str","status":"new|ongoing|resolved"}],'
            '"risks_detected":[{"risk":"str","severity":"high|medium|low"}],'
            '"waiting_response":[{"contact":"nombre","subject":"str","hours_waiting":0}],'
            '"external_contacts":[{"name":"str","email":"str","company":"str",'
            '"sentiment":"positive|neutral|negative","sentiment_score":0.0}]},'
            '"knowledge_graph":{"entities":[{"name":"str",'
            '"type":"person|company|product|machine|raw_material","email":"str or null"}],'
            '"facts":[{"entity_name":"str",'
            '"type":"commitment|statement|price|complaint|request|information|change",'
            '"text":"str","date":"YYYY-MM-DD or null","confidence":0.8}],'
            '"action_items":[{"assignee":"quien","related_to":"contacto",'
            '"description":"accion","reason":"por que",'
            '"type":"call|email|meeting|follow_up|send_quote|review|other",'
            '"priority":"low|medium|high","due_date":"YYYY-MM-DD or null"}],'
            '"relationships":[{"entity_a":"str","entity_b":"str",'
            '"type":"works_at|buys_from|sells_to|supplies|mentioned_with","context":"str"}],'
            '"person_profiles":[{"name":"str","email":"str or null",'
            '"role":"str","decision_power":"high|medium|low",'
            '"communication_style":"formal|informal","personality_notes":"str"}]}}\n'
            'sentiment_score: -1 a 1. facts: solo explicito. Cruza [ODOO:] con emails.\n\n'
            f'EMAILS:\n{email_text}'
        )

        text = self._call(system, prompt, max_tokens=8000)
        parsed = self._extract_json(text)

        # Normalizar estructura
        summary = parsed.get('summary', parsed)
        kg = parsed.get('knowledge_graph', {})

        # Si Claude no separó en dos niveles, extraer KG del nivel superior
        if not kg and 'entities' in parsed:
            kg = {
                'entities': parsed.pop('entities', []),
                'facts': parsed.pop('facts', []),
                'action_items': parsed.pop('action_items', []),
                'relationships': parsed.pop('relationships', []),
                'person_profiles': parsed.pop('person_profiles', []),
            }
            summary = parsed

        summary['external_emails'] = ext_count
        summary['internal_emails'] = int_count

        return {
            'summary': summary,
            'knowledge_graph': kg or {
                'entities': [], 'facts': [],
                'action_items': [], 'relationships': [],
                'person_profiles': [],
            },
        }

    # ── Company Profiling ───────────────────────────────────────────────────

    def profile_company(self, company_name: str, context: str) -> dict:
        """Genera perfil de empresa basado en emails, KG facts y datos de Odoo."""
        system = (
            'Eres un analista de inteligencia empresarial para Quimibond '
            '(manufacturera de no tejidos y textiles en México). '
            'Analiza los datos disponibles sobre una empresa y genera un perfil. '
            'Retorna SOLO JSON válido sin markdown.'
        )

        prompt = (
            f'Genera un perfil de la empresa "{company_name}" basado en estos datos.\n\n'
            f'{context}\n\n'
            'Retorna SOLO un JSON con esta estructura:\n'
            '{\n'
            '  "description": "Descripción de 1-2 oraciones de qué hace la empresa '
            'y su relación con Quimibond",\n'
            '  "business_type": "cliente|proveedor|logistica|financiero|servicios|'
            'gobierno|tecnologia|otro",\n'
            '  "relationship_type": "buyer|supplier|logistics|financial|services|'
            'government|technology|other",\n'
            '  "key_products": ["productos o servicios que compra/vende/provee"],\n'
            '  "relationship_summary": "Resumen de la relación comercial con '
            'Quimibond: qué compra/vende, frecuencia, importancia",\n'
            '  "industry": "Sector o industria de la empresa",\n'
            '  "country": "País (si se puede inferir)",\n'
            '  "city": "Ciudad (si se puede inferir)",\n'
            '  "risk_signals": ["señales de riesgo detectadas"],\n'
            '  "opportunity_signals": ["oportunidades detectadas"],\n'
            '  "strategic_notes": "Notas estratégicas: qué debe saber el '
            'Director General sobre esta empresa"\n'
            '}\n\n'
            'REGLAS:\n'
            '- Sé específico y conciso\n'
            '- Si no hay datos suficientes para un campo, usa null\n'
            '- key_products: lista de productos/servicios concretos, no genéricos\n'
            '- risk_signals: solo si hay evidencia real (facturas vencidas, quejas, etc.)\n'
            '- opportunity_signals: solo si hay evidencia real (crecimiento, nuevos pedidos, etc.)\n'
            '- strategic_notes: el insight más importante para el Director General'
        )

        text = self._call(system, prompt, max_tokens=2000)
        return self._extract_json(text)

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
            '<h2>🏭 COMPETENCIA</h2>\n'
            '(Competidores mencionados hoy en emails, en qué contexto, '
            'quién los mencionó, nivel de amenaza, qué hacer al respecto)\n\n'
            '<h2>🎯 ACCIONES PARA MAÑANA</h2>\n'
            '(ESPECÍFICAS y EJECUTABLES. Para cada acción incluye:\n'
            '- QUIÉN debe hacerla (nombre de la persona del equipo)\n'
            '- QUÉ exactamente (no "dar seguimiento" sino "Enviar cotización '
            'actualizada de tela Oxford con 5% descuento por 800m")\n'
            '- CON QUIÉN (nombre y empresa del contacto)\n'
            '- POR QUÉ (contexto de negocio: riesgo, oportunidad, deadline)\n'
            '- CUÁNDO (fecha límite, idealmente ANTES del deadline del cliente)\n'
            '- PRIORIDAD (critical/high/medium/low)\n'
            'Si hay queja → primera acción = responder/disculparse en <24h.\n'
            'Si hay prospecto nuevo → seguimiento en <48h.\n'
            'Si hay competidor → contraoferta inmediata con diferenciadores.)\n\n'
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



    def verify_facts(self, facts_text: str, recent_context: str) -> list:
        """Verifica hechos del KG contra contexto reciente.

        Retorna lista de dicts: [{fact_id, verdict, reason}]
        verdict: 'confirmed' | 'contradicted' | 'uncertain'
        """
        try:
            system = (
                'Eres un verificador de hechos para Quimibond. '
                'Evalúa cada hecho contra el contexto reciente. '
                'Retorna SOLO JSON válido sin markdown.'
            )
            prompt = (
                'Verifica estos hechos del Knowledge Graph contra '
                'información reciente.\n\n'
                f'HECHOS A VERIFICAR:\n{facts_text}\n\n'
                f'CONTEXTO RECIENTE:\n{recent_context}\n\n'
                'Para cada hecho retorna:\n'
                '{"verifications": [{"fact_id": N, '
                '"verdict": "confirmed|contradicted|uncertain", '
                '"reason": "por qué"}]}\n\n'
                'REGLAS:\n'
                '- confirmed: hay evidencia que lo respalda\n'
                '- contradicted: hay evidencia contraria\n'
                '- uncertain: no hay suficiente información'
            )
            text = self._call(system, prompt, max_tokens=2000)
            parsed = self._extract_json(text)
            return parsed.get('verifications', [])
        except Exception as exc:
            _logger.warning('verify_facts: %s', exc)
            return []

    def extract_knowledge(self, emails_text, account, team_members=None):
        """Extrae knowledge graph con perfil profundo de personas.

        team_members: optional list of {"name": str, "email": str} for
        Claude to use when assigning action items to specific people.
        """
        schema = (
            '{"entities": [{"name": "str", "type": "person|company|product|machine|raw_material",'
            ' "email": "str or null", "attributes": {}}],'
            ' "facts": [{"entity_name": "str",'
            ' "type": "commitment|statement|price|quantity|delivery_date|payment|complaint|request|'
            'approval|rejection|information|change",'
            ' "text": "str", "date": "YYYY-MM-DD or null", "is_future": false, "confidence": 0.8}],'
            ' "action_items": [{"assignee": "str (nombre de quien debe ejecutar)",'
            ' "related_to": "str (nombre del contacto/cliente involucrado)",'
            ' "description": "str (accion especifica y ejecutable, NO generica)",'
            ' "reason": "str (1-2 oraciones explicando POR QUE es necesaria esta accion, '
            'referenciando el email o contexto que la origina)",'
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
            ' "personality_traits": ["rasgos concretos: detallista, impaciente, '
            'negociador, leal, price-sensitive, etc."],'
            ' "decision_factors": ["que le importa al decidir: precio, plazo, '
            'calidad, relacion, servicio"],'
            ' "personality_notes": "observaciones sobre cómo se comunica",'
            ' "negotiation_style": "aggressive|collaborative|passive|analytical or null",'
            ' "response_pattern": "fast|normal|slow or null",'
            ' "influence_on_deals": "str or null"}]}'
        )
        prompt = (
            'Analiza emails de ' + account
            + ' de Quimibond (textiles no tejidos, Mexico).\n\n'
            + 'EMAILS:\n' + _smart_truncate(emails_text, 12000)
            + '\n\nExtrae en JSON schema:\n' + schema
            + '\n\nREGLAS:\n'
            + '- Solo info EXPLICITA para facts. Confidence 0.8+ claros, 0.3-0.5 implicitos.\n'
            + '- person_profiles: Para CADA persona que aparece en los emails, '
            + 'construye un perfil. Infiere rol, estilo de comunicacion, nivel de decision, '
            + 'intereses clave. Esto alimenta la memoria a largo plazo del sistema.\n'
            + '- Identifica TODAS las relaciones entre personas y empresas.\n'
            + '- action_items.assignee: USA NOMBRES EXACTOS del equipo interno listado abajo.\n'
            + '- Solo JSON valido.'
            + (('\n\nEQUIPO INTERNO (asigna action items a estas personas):\n'
                + '\n'.join(
                    f'- {m["name"]} ({m.get("email", "")}'
                    f'{", " + m["department"] if m.get("department") else ""})'
                    for m in (team_members or []))
                ) if team_members else ''
               )
        )
        try:
            system = (
                'Eres un analista de inteligencia de Quimibond (textiles no tejidos, Mexico). '
                'Extrae entidades, hechos, action items, relaciones Y perfiles '
                'detallados de personas. El objetivo es que el sistema APRENDA '
                'de cada persona con cada email que procesa. '
                'Retorna SOLO JSON valido sin markdown.'
            )
            raw = self._call(system, prompt, max_tokens=8000)
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

    _RETRY_STATUSES = {429, 502, 503}
    _MAX_RETRIES = 3

    def __init__(self, api_key: str, model: str = VOYAGE_MODEL):
        self._api_key = api_key
        self._model = model
        self._client = httpx.Client(timeout=60)
        self._headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
        }

    def embed(self, texts: list[str], input_type: str = 'document') -> list:
        """Genera embeddings para una lista de textos con retry."""
        if not texts:
            return []
        import time
        payload = {
            'model': self._model,
            'input': texts[:128],  # Voyage límite
            'input_type': input_type,
        }
        last_exc = None
        for attempt in range(self._MAX_RETRIES):
            try:
                resp = self._client.post(
                    VOYAGE_ENDPOINT, headers=self._headers, json=payload,
                )
                if resp.status_code in self._RETRY_STATUSES and attempt < self._MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    _logger.warning(
                        'Voyage %d, retry in %ds', resp.status_code, wait,
                    )
                    time.sleep(wait)
                    continue
                if resp.status_code != 200:
                    raise RuntimeError(
                        f'Voyage {resp.status_code}: {resp.text[:200]}'
                    )
                data = resp.json().get('data', [])
                return [item['embedding'] for item in data]
            except httpx.TransportError as exc:
                last_exc = exc
                if attempt < self._MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError(
                    f'Voyage transport error after {self._MAX_RETRIES} '
                    f'attempts: {exc}'
                ) from exc
        raise RuntimeError(f'Voyage request failed: {last_exc}')

    def embed_query(self, text: str) -> list:
        """Genera embedding para una query de búsqueda."""
        results = self.embed([text], input_type='query')
        return results[0] if results else []
