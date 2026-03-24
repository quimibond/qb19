"""
Quimibond Intelligence — Analysis Service
Extraído de intelligence_engine.py: análisis, métricas, alertas, scoring, briefing.
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from ..models.intelligence_config import INTERNAL_DOMAIN

_logger = logging.getLogger(__name__)


class AnalysisService:
    """Funciones de análisis, métricas, alertas y scoring (sin estado Odoo)."""

    # ══════════════════════════════════════════════════════════════════════════
    #   FORMAT EMAILS FOR CLAUDE
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def format_emails_for_claude(emails: list, odoo_ctx: dict,
                                 person_profiles: dict = None) -> str:
        """Formatea emails con contexto profundo de Odoo + perfiles conocidos."""
        person_profiles = person_profiles or {}
        lines = []
        for i, e in enumerate(emails, 1):
            lines.append(f'--- EMAIL {i} ---')
            lines.append(f'De: {e.get("from", "")}')
            lines.append(f'Para: {e.get("to", "")}')
            if e.get('cc'):
                lines.append(f'CC: {e["cc"]}')
            lines.append(f'Asunto: {e["subject"]}')
            lines.append(f'Fecha: {e["date"]}')
            lines.append(f'Tipo: {e["sender_type"]}')
            if e['is_reply']:
                lines.append('(Es respuesta)')
            if e['has_attachments']:
                att_names = ', '.join(
                    a['filename'] for a in e.get('attachments', [])
                )
                lines.append(f'Adjuntos: {att_names}')

            # Contexto de negocio de Odoo (resumen consolidado)
            sender_email = e.get('from_email', '')
            biz = odoo_ctx.get('business_summary', {}).get(sender_email)
            if biz:
                lines.append(f'[ODOO: {biz}]')

            # Perfil conocido de la persona (memoria acumulativa)
            profile = person_profiles.get(sender_email.lower())
            if profile:
                profile_parts = []
                if profile.get('role'):
                    profile_parts.append(f"Rol: {profile['role']}")
                if profile.get('company'):
                    profile_parts.append(f"Empresa: {profile['company']}")
                if profile.get('decision_power'):
                    profile_parts.append(
                        f"Poder decisión: {profile['decision_power']}"
                    )
                if profile.get('communication_style'):
                    profile_parts.append(
                        f"Estilo: {profile['communication_style']}"
                    )
                if profile.get('key_interests'):
                    interests = profile['key_interests']
                    if isinstance(interests, list):
                        interests = ', '.join(interests[:5])
                    profile_parts.append(f"Intereses: {interests}")
                if profile.get('personality_traits'):
                    traits = profile['personality_traits']
                    if isinstance(traits, list):
                        traits = ', '.join(traits[:5])
                    profile_parts.append(f"Rasgos: {traits}")
                if profile.get('decision_factors'):
                    factors = profile['decision_factors']
                    if isinstance(factors, list):
                        factors = ', '.join(factors[:5])
                    profile_parts.append(f"Decide por: {factors}")
                if profile.get('negotiation_style'):
                    profile_parts.append(
                        f"Negociación: {profile['negotiation_style']}"
                    )
                if profile.get('personality_notes'):
                    profile_parts.append(
                        f"Notas: {profile['personality_notes'][:100]}"
                    )
                if profile_parts:
                    lines.append(
                        f'[PERSONA CONOCIDA: {" | ".join(profile_parts)}]'
                    )

            body = (e.get('body') or e.get('snippet', ''))[:1500]
            lines.append(f'Cuerpo:\n{body}')
            lines.append('')
        return '\n'.join(lines)

    # ══════════════════════════════════════════════════════════════════════════
    #   COMPUTE METRICS
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def compute_metrics(emails: list, threads: list, cfg: dict) -> list:
        """Calcula métricas de respuesta por cuenta."""
        by_account = defaultdict(lambda: {
            'received': 0, 'sent': 0, 'ext_received': 0, 'int_received': 0,
        })

        for e in emails:
            acct = e['account']
            if e.get('from_email', '').endswith(f'@{INTERNAL_DOMAIN}'):
                by_account[acct]['sent'] += 1
            else:
                by_account[acct]['received'] += 1
                if e['sender_type'] == 'external':
                    by_account[acct]['ext_received'] += 1
                else:
                    by_account[acct]['int_received'] += 1

        # Threads por cuenta
        acct_threads = defaultdict(list)
        for t in threads:
            acct_threads[t['account']].append(t)

        metrics = []
        for acct, counts in by_account.items():
            acct_t = acct_threads.get(acct, [])
            replied = [t for t in acct_t if t['has_internal_reply']]
            unanswered = [t for t in acct_t
                          if t['status'] in ('needs_response', 'stalled')]

            response_hours = [
                t['hours_without_response'] for t in acct_t
                if t['hours_without_response'] > 0
            ]

            metrics.append({
                'account': acct,
                'emails_received': counts['received'],
                'emails_sent': counts['sent'],
                'internal_received': counts['int_received'],
                'external_received': counts['ext_received'],
                'threads_started': len([t for t in acct_t if t['started_by_type'] == 'external']),
                'threads_replied': len(replied),
                'threads_unanswered': len(unanswered),
                'avg_response_hours': (
                    round(sum(response_hours) / len(response_hours), 1)
                    if response_hours else None
                ),
                'fastest_response_hours': (
                    round(min(response_hours), 1) if response_hours else None
                ),
                'slowest_response_hours': (
                    round(max(response_hours), 1) if response_hours else None
                ),
            })
        return metrics

    # ══════════════════════════════════════════════════════════════════════════
    #   EXTRACT CONTACTS (legacy, from emails)
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def extract_contacts(emails: list) -> list:
        """Extrae contactos únicos de los emails (legacy, para KG/alertas)."""
        contact_map = {}
        for e in emails:
            email_addr = e.get('from_email', '').lower()
            if not email_addr:
                continue
            if email_addr not in contact_map:
                contact_map[email_addr] = {
                    'email': email_addr,
                    'name': e.get('from_name', ''),
                    'contact_type': e.get('sender_type', 'external'),
                    'department': e.get('department'),
                }
        return list(contact_map.values())

    # ══════════════════════════════════════════════════════════════════════════
    #   GENERATE ALERTS
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def generate_alerts(threads: list, metrics: list, cfg: dict,
                        account_summaries: list = None,
                        odoo_ctx: dict = None) -> list:
        """Genera alertas basadas en umbrales configurables.

        Tipos: stalled_thread, no_response, high_volume, competitor,
        negative_sentiment, churn_risk, invoice_silence, delivery_risk,
        payment_delay, opportunity, quality_issue.
        """
        alerts = []
        account_summaries = account_summaries or []
        odoo_ctx = odoo_ctx or {}
        no_resp_hours = cfg.get('no_response_hours', 24)
        stalled_hours = cfg.get('stalled_thread_hours', 48)
        high_vol = cfg.get('high_volume_threshold', 50)

        # Alertas por threads sin respuesta
        for t in threads:
            if t['hours_without_response'] > stalled_hours and t['started_by_type'] == 'external':
                alerts.append({
                    'alert_type': 'stalled_thread',
                    'severity': 'high',
                    'title': f"Thread estancado: {t['subject'][:80]}",
                    'description': (
                        f"{t['hours_without_response']:.0f}h sin respuesta de "
                        f"{t['last_sender']} en {t['account']}"
                    ),
                    'contact_name': t.get('last_sender'),
                    'account': t['account'],
                    'related_thread_id': t['gmail_thread_id'],
                })
            elif t['hours_without_response'] > no_resp_hours and t['started_by_type'] == 'external':
                alerts.append({
                    'alert_type': 'no_response',
                    'severity': 'medium',
                    'title': f"Sin respuesta: {t['subject'][:80]}",
                    'description': (
                        f"{t['hours_without_response']:.0f}h esperando en {t['account']}"
                    ),
                    'contact_name': t.get('last_sender'),
                    'account': t['account'],
                    'related_thread_id': t['gmail_thread_id'],
                })

        # Alerta por volumen alto
        for m in metrics:
            total = m['emails_received'] + m['emails_sent']
            if total > high_vol:
                alerts.append({
                    'alert_type': 'high_volume',
                    'severity': 'low',
                    'title': f"Alto volumen: {m['account']}",
                    'description': f'{total} emails hoy (umbral: {high_vol})',
                    'account': m['account'],
                })

        # ── Alertas inteligentes desde análisis de Claude ────────────────────
        for s in account_summaries:
            account = s.get('account', '')

            # Competidores mencionados
            for comp in s.get('competitors_mentioned', []):
                threat = comp.get('threat_level', 'medium')
                severity = 'high' if threat == 'high' else 'medium'
                alerts.append({
                    'alert_type': 'competitor',
                    'severity': severity,
                    'title': (
                        f"Competidor: {comp.get('name', '?')} "
                        f"mencionado por {comp.get('mentioned_by', '?')}"
                    )[:120],
                    'description': comp.get('detail', comp.get('context', '')),
                    'contact_name': comp.get('mentioned_by'),
                    'account': account,
                })

            # Sentimiento negativo fuerte
            score = s.get('sentiment_score')
            if isinstance(score, (int, float)) and score < -0.3:
                severity = 'critical' if score < -0.6 else 'high'
                alerts.append({
                    'alert_type': 'negative_sentiment',
                    'severity': severity,
                    'title': (
                        f"Sentimiento negativo ({score:.1f}) en {account}"
                    ),
                    'description': s.get('sentiment_detail', ''),
                    'account': account,
                })

            # Contactos con señal de riesgo
            for contact in s.get('external_contacts', []):
                c_score = contact.get('sentiment_score')
                signal = contact.get('relationship_signal', '')
                if signal == 'at_risk' or (
                    isinstance(c_score, (int, float)) and c_score < -0.4
                ):
                    alerts.append({
                        'alert_type': 'churn_risk',
                        'severity': 'high',
                        'title': (
                            f"Relación en riesgo: "
                            f"{contact.get('name', '?')} "
                            f"({contact.get('company', '?')})"
                        )[:120],
                        'description': (
                            f"Señal: {signal}. "
                            f"Sentimiento: {c_score}. "
                            f"Tema: {contact.get('topic', '?')}"
                        ),
                        'contact_name': contact.get('name'),
                        'account': account,
                    })

        # ── Factura vencida + sin respuesta a emails ─────────────────────────
        partners = odoo_ctx.get('partners', {})
        stalled_emails = set()
        for t in threads:
            if t['status'] in ('stalled', 'needs_response'):
                stalled_emails.update(t.get('participant_emails', []))

        for email_addr, p in partners.items():
            overdue_invoices = [
                inv for inv in p.get('pending_invoices', [])
                if inv.get('days_overdue', 0) > 0
            ]
            if overdue_invoices and email_addr in stalled_emails:
                total_overdue = sum(
                    inv.get('amount_residual', 0) for inv in overdue_invoices
                )
                max_days = max(
                    inv.get('days_overdue', 0) for inv in overdue_invoices
                )
                alerts.append({
                    'alert_type': 'invoice_silence',
                    'severity': 'critical',
                    'title': (
                        f"Factura vencida + sin respuesta: "
                        f"{p.get('name', email_addr)}"
                    )[:120],
                    'description': (
                        f"${total_overdue:,.0f} en facturas vencidas "
                        f"(máx {max_days}d) Y tiene emails sin responder. "
                        f"Riesgo de cobranza. Requiere acción inmediata."
                    ),
                    'contact_name': p.get('name', email_addr),
                    'account': '',
                })

        # ── Riesgo de entrega (stock.picking retrasado) ──────────────────────
        for email_addr, p in partners.items():
            for delivery in p.get('pending_deliveries', []):
                scheduled = delivery.get('scheduled_date', '')
                if scheduled:
                    try:
                        sched_dt = datetime.strptime(
                            scheduled[:10], '%Y-%m-%d',
                        ).date()
                        from datetime import date as _date
                        if sched_dt < _date.today():
                            days_late = (_date.today() - sched_dt).days
                            severity = 'critical' if days_late > 3 else 'high'
                            alerts.append({
                                'alert_type': 'delivery_risk',
                                'severity': severity,
                                'title': (
                                    f"Entrega retrasada ({days_late}d): "
                                    f"{delivery.get('name', '?')} → "
                                    f"{p.get('name', email_addr)}"
                                )[:120],
                                'description': (
                                    f"Programada: {scheduled[:10]}. "
                                    f"Días de retraso: {days_late}. "
                                    f"Estado: {delivery.get('state', '?')}"
                                ),
                                'contact_name': p.get('name', email_addr),
                                'account': '',
                            })
                    except Exception:
                        pass

        # ── Pago vencido ─────────────────────────────────────────────────────
        for email_addr, p in partners.items():
            for inv in p.get('pending_invoices', []):
                days_overdue = inv.get('days_overdue', 0)
                if days_overdue > 15:
                    severity = (
                        'critical' if days_overdue > 45
                        else 'high' if days_overdue > 30
                        else 'medium'
                    )
                    alerts.append({
                        'alert_type': 'payment_delay',
                        'severity': severity,
                        'title': (
                            f"Pago vencido ({days_overdue}d): "
                            f"{inv.get('name', '?')} — "
                            f"{p.get('name', email_addr)}"
                        )[:120],
                        'description': (
                            f"Factura: {inv.get('name', '?')}. "
                            f"Monto: ${inv.get('amount_residual', 0):,.0f}. "
                            f"Vencida hace {days_overdue} días."
                        ),
                        'contact_name': p.get('name', email_addr),
                        'account': '',
                    })

        # ── Oportunidad detectada (CRM leads) ───────────────────────────────
        for email_addr, p in partners.items():
            for lead in p.get('crm_leads', []):
                if lead.get('type') == 'opportunity' and lead.get(
                    'probability', 0
                ) >= 50:
                    alerts.append({
                        'alert_type': 'opportunity',
                        'severity': 'low',
                        'title': (
                            f"Oportunidad: {lead.get('name', '?')} "
                            f"({lead.get('probability', 0):.0f}%)"
                        )[:120],
                        'description': (
                            f"Cliente: {p.get('name', email_addr)}. "
                            f"Valor: ${lead.get('expected_revenue', 0):,.0f}. "
                            f"Etapa: {lead.get('stage', '?')}"
                        ),
                        'contact_name': p.get('name', email_addr),
                        'account': '',
                    })

        # ── Calidad (detectada por Claude en risks_detected) ─────────────────
        for s in account_summaries:
            account = s.get('account', '')
            for risk in s.get('risks_detected', []):
                risk_text = risk.get('risk', '').lower()
                if any(w in risk_text for w in (
                    'calidad', 'quality', 'reclamo', 'defecto', 'rechazo',
                    'queja', 'devolución', 'devolucion',
                )):
                    alerts.append({
                        'alert_type': 'quality_issue',
                        'severity': risk.get('severity', 'high'),
                        'title': f"Calidad: {risk.get('risk', '?')}"[:120],
                        'description': (
                            f"Mitigación sugerida: "
                            f"{risk.get('mitigation', 'N/A')}. "
                            f"Cuentas: {', '.join(risk.get('accounts_involved', [account]))}"
                        ),
                        'account': account,
                    })

        # ── Product Purchase Intelligence alerts ──────────────────────────────
        for email_addr, p in partners.items():
            patterns = p.get('purchase_patterns', {})
            partner_name = p.get('name', email_addr)

            # Volume drop alerts
            for vd in patterns.get('volume_drops', []):
                trend = vd.get('trend_pct', 0)
                severity = 'high' if trend <= -50 else 'medium'
                alerts.append({
                    'alert_type': 'volume_drop',
                    'severity': severity,
                    'title': (
                        f"Caida de volumen: {vd['product']} "
                        f"({trend:+d}%) — {partner_name}"
                    )[:120],
                    'description': (
                        f"Producto: {vd['product']}. "
                        f"Ultimos 6m: {vd['recent_qty']:.0f} "
                        f"vs anteriores 6m: {vd['previous_qty']:.0f} "
                        f"({trend:+d}%). Investigar causa."
                    ),
                    'contact_name': partner_name,
                    'account': '',
                })

            # Unusual discount alerts
            for da in patterns.get('discount_anomalies', []):
                severity = 'high' if da['delta'] > 10 else 'medium'
                alerts.append({
                    'alert_type': 'unusual_discount',
                    'severity': severity,
                    'title': (
                        f"Descuento inusual: {da['product']} "
                        f"({da['last_discount']:.1f}% vs "
                        f"prom {da['avg_discount']:.1f}%) — {partner_name}"
                    )[:120],
                    'description': (
                        f"Producto: {da['product']}. "
                        f"Ultimo descuento: {da['last_discount']:.1f}%. "
                        f"Promedio historico: {da['avg_discount']:.1f}%. "
                        f"Diferencia: {da['delta']:+.1f} pts."
                    ),
                    'contact_name': partner_name,
                    'account': '',
                })

            # Cross-sell opportunity alerts
            for cs in patterns.get('cross_sell', []):
                alerts.append({
                    'alert_type': 'cross_sell',
                    'severity': 'low',
                    'title': (
                        f"Cross-sell: {cs['product']} "
                        f"para {partner_name}"
                    )[:120],
                    'description': (
                        f"{cs['similar_clients_buying']} de "
                        f"{cs['total_similar_clients']} clientes similares "
                        f"compran {cs['product']} pero {partner_name} no. "
                        f"Oportunidad de venta cruzada."
                    ),
                    'contact_name': partner_name,
                    'account': '',
                })

        # ── Inventory Intelligence alerts ─────────────────────────────────────
        for email_addr, p in partners.items():
            inv_intel = p.get('inventory_intelligence', {})
            partner_name = p.get('name', email_addr)

            for item in inv_intel.get('at_risk', []):
                status = item.get('status', '')
                product = item.get('product', '?')
                days_inv = item.get('days_of_inventory')
                current_qty = item.get('current_qty', 0)
                can_fulfill = item.get('can_fulfill_next_order')
                next_order = item.get('client_next_order_days')

                if status == 'stockout':
                    severity = 'critical'
                    alert_type = 'stockout_risk'
                    title = (
                        f"Sin stock: {product} "
                        f"(cliente {partner_name} lo compra)"
                    )[:120]
                    desc = (
                        f"Producto {product} tiene stock 0. "
                        f"{partner_name} lo compra regularmente."
                    )
                    if next_order is not None and next_order <= 7:
                        desc += (
                            f" Proximo pedido estimado en ~{next_order}d."
                        )
                elif status == 'critical':
                    severity = 'high'
                    alert_type = 'stockout_risk'
                    title = (
                        f"Stock critico: {product} "
                        f"({days_inv}d restantes) — {partner_name}"
                    )[:120]
                    desc = (
                        f"Producto {product}: {current_qty:.0f} unidades, "
                        f"~{days_inv} dias de inventario."
                    )
                    if can_fulfill is False:
                        desc += (
                            " NO alcanza para cubrir el proximo pedido "
                            f"de {partner_name}."
                        )
                elif status in ('low', 'below_reorder'):
                    severity = 'medium'
                    alert_type = 'reorder_needed'
                    title = (
                        f"Reorden sugerido: {product} "
                        f"({current_qty:.0f} uds) — {partner_name}"
                    )[:120]
                    desc_parts = [
                        f"Producto {product}: {current_qty:.0f} unidades",
                    ]
                    if days_inv is not None:
                        desc_parts.append(
                            f"~{days_inv} dias de inventario")
                    if status == 'below_reorder':
                        desc_parts.append("por debajo del punto de reorden")
                    desc = '. '.join(desc_parts) + '.'
                else:
                    continue

                alerts.append({
                    'alert_type': alert_type,
                    'severity': severity,
                    'title': title,
                    'description': desc,
                    'contact_name': partner_name,
                    'account': '',
                })

        return alerts

    # ══════════════════════════════════════════════════════════════════════════
    #   COMPUTE CLIENT SCORES
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def compute_client_scores(contacts: list, emails: list, threads: list,
                              cfg: dict,
                              account_summaries: list = None) -> list:
        """Calcula score de relación 0-100 para contactos externos."""
        external = [c for c in contacts if c['contact_type'] == 'external']
        if not external:
            return []

        email_counts = defaultdict(int)
        for e in emails:
            email_counts[e.get('from_email', '')] += 1

        thread_participation = defaultdict(int)
        for t in threads:
            for p in t.get('participant_emails', []):
                thread_participation[p] += 1

        contact_sentiments = {}
        for s in (account_summaries or []):
            for ec in s.get('external_contacts', []):
                email_addr = (ec.get('email') or '').lower()
                if email_addr and ec.get('sentiment_score') is not None:
                    try:
                        contact_sentiments[email_addr] = float(
                            ec['sentiment_score'],
                        )
                    except (ValueError, TypeError):
                        pass

        scores = []
        for c in external:
            addr = c['email']
            msg_count = email_counts.get(addr, 0)
            thread_count = thread_participation.get(addr, 0)

            freq_score = min(25, 5 + msg_count * 4)
            resp_score = min(25, 5 + thread_count * 4)

            related_threads = [
                t for t in threads
                if addr in t.get('participant_emails', [])
            ]
            replied_count = sum(1 for t in related_threads if t['has_internal_reply'])
            recip_score = (
                round(replied_count / len(related_threads) * 25)
                if related_threads else 12
            )

            claude_sentiment = contact_sentiments.get(addr.lower())
            if claude_sentiment is not None:
                sent_score = round((claude_sentiment + 1) * 12.5)
                sent_score = max(0, min(25, sent_score))
            else:
                sent_score = 15

            total = freq_score + resp_score + recip_score + sent_score

            if total >= 60:
                risk = 'low'
            elif total >= 35:
                risk = 'medium'
            else:
                risk = 'high'

            scores.append({
                'email': addr,
                'total_score': total,
                'frequency_score': freq_score,
                'responsiveness_score': resp_score,
                'reciprocity_score': recip_score,
                'sentiment_score': sent_score,
                'risk_level': risk,
            })

        return scores

    # ══════════════════════════════════════════════════════════════════════════
    #   BUILD KEY EVENTS
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def build_key_events(alerts: list, account_summaries: list) -> list:
        """Construye key_events JSON para daily_summaries (frontend urgency panel)."""
        events = []

        severity_urgency = {
            'critical': 'critical', 'high': 'high',
            'medium': 'medium', 'low': 'low',
        }
        for a in (alerts or []):
            sev = a.get('severity', 'low')
            if sev in ('critical', 'high'):
                events.append({
                    'type': a.get('alert_type', 'alert'),
                    'description': a.get('title', ''),
                    'urgency': severity_urgency.get(sev, 'medium'),
                })

        for s in (account_summaries or []):
            for item in s.get('urgent_items', []):
                events.append({
                    'type': 'urgent_item',
                    'description': item.get('item', ''),
                    'urgency': 'high',
                })

            for comp in s.get('competitors_mentioned', []):
                threat = comp.get('threat_level', 'medium')
                events.append({
                    'type': 'competitor',
                    'description': (
                        f"Competidor {comp.get('name', '?')} mencionado "
                        f"por {comp.get('mentioned_by', '?')}"
                    ),
                    'urgency': 'high' if threat == 'high' else 'medium',
                })

            for contact in s.get('external_contacts', []):
                signal = contact.get('relationship_signal', '')
                if signal == 'at_risk':
                    events.append({
                        'type': 'churn_risk',
                        'description': (
                            f"Relación en riesgo: "
                            f"{contact.get('name', '?')} "
                            f"({contact.get('company', '?')})"
                        ),
                        'urgency': 'high',
                    })

        seen = set()
        unique = []
        for e in events:
            key = e['description'][:80]
            if key not in seen:
                seen.add(key)
                unique.append(e)
        return unique[:20]

    # ══════════════════════════════════════════════════════════════════════════
    #   BUILD DATA PACKAGE
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def build_data_package(today: str, summaries: list, metrics: list,
                           alerts: list, threads: list, client_scores: list,
                           odoo_ctx: dict, historical: dict) -> str:
        """Construye el paquete de datos completo para Claude fase 2."""
        sections = [
            f'FECHA: {today}',
            f'TOTAL CUENTAS ANALIZADAS: {len(summaries)}',
        ]

        # ── Contexto histórico ──────────────────────────────────────────────
        if historical.get('previousSummary'):
            sections.append(
                f"\nRESUMEN DEL DÍA ANTERIOR:\n"
                f"{historical['previousSummary'][:1000]}"
            )
        if historical.get('openAlerts'):
            sections.append(
                f"\nALERTAS ABIERTAS PREVIAS:\n"
                + json.dumps(historical['openAlerts'][:10], default=str)
            )

        # ── Resúmenes por cuenta ────────────────────────────────────────────
        sections.append('\n═══ ANÁLISIS POR CUENTA ═══')
        for s in summaries:
            sections.append(
                f"\n── {s['department']} ({s['account']}) ──\n"
                f"Emails: {s.get('total_emails', 0)} "
                f"(ext:{s.get('external_emails', 0)}, "
                f"int:{s.get('internal_emails', 0)})\n"
                f"Resumen: {s.get('summary_text', '')}\n"
                f"Sentimiento: {s.get('overall_sentiment', 'N/A')}\n"
                f"Items clave: {json.dumps(s.get('key_items', []), default=str, ensure_ascii=False)}\n"
                f"Esperando respuesta: {json.dumps(s.get('waiting_response', []), default=str, ensure_ascii=False)}\n"
                f"Urgentes: {json.dumps(s.get('urgent_items', []), default=str, ensure_ascii=False)}\n"
                f"Contactos: {json.dumps(s.get('external_contacts', []), default=str, ensure_ascii=False)}\n"
                f"Temas: {json.dumps(s.get('topics_detected', []), default=str, ensure_ascii=False)}\n"
                f"Riesgos: {json.dumps(s.get('risks_detected', []), default=str, ensure_ascii=False)}\n"
                f"Sentimiento numérico: {s.get('sentiment_score', 'N/A')}\n"
                f"Competidores: {json.dumps(s.get('competitors_mentioned', []), default=str, ensure_ascii=False)}"
            )

        # ── Métricas ────────────────────────────────────────────────────────
        sections.append('\n═══ MÉTRICAS DE RESPUESTA ═══')
        for m in metrics:
            sections.append(
                f"{m['account']}: recv={m['emails_received']} "
                f"sent={m['emails_sent']} "
                f"replied={m['threads_replied']} "
                f"unanswered={m['threads_unanswered']} "
                f"avg_hrs={m.get('avg_response_hours', 'N/A')}"
            )

        # ── Alertas ─────────────────────────────────────────────────────────
        if alerts:
            sections.append(f'\n═══ ALERTAS ({len(alerts)}) ═══')
            for a in alerts[:20]:
                sections.append(
                    f"[{a['severity'].upper()}] {a['alert_type']}: "
                    f"{a['title']}"
                )

        # ── Perfiles detallados de contactos ────────────────────────────────
        partners = odoo_ctx.get('partners', {})
        if partners:
            sections.append('\n═══ PERFILES DE CONTACTOS (Odoo ERP — datos en vivo) ═══')
            for email_addr, p in partners.items():
                summary = p.get('_summary', '')
                if not summary:
                    continue
                parts = [f"\n── {p.get('name', email_addr)} ({email_addr}) ──"]
                parts.append(f"RESUMEN: {summary}")

                # CRM Pipeline
                leads = p.get('crm_leads', [])
                if leads:
                    for l in leads[:3]:
                        parts.append(
                            f"  CRM: {l['name']} | Etapa: {l['stage']} | "
                            f"Revenue: ${l['expected_revenue']:,.0f} | "
                            f"Prob: {l['probability']}% | "
                            f"Responsable: {l['user']} | "
                            f"{l['days_open']}d abierto"
                        )

                # Actividades pendientes
                acts = p.get('pending_activities', [])
                if acts:
                    overdue = [a for a in acts if a['is_overdue']]
                    if overdue:
                        parts.append(
                            f"  ⚠ ACTIVIDADES VENCIDAS ({len(overdue)}):"
                        )
                        for a in overdue[:3]:
                            parts.append(
                                f"    - {a['type']}: {a['summary'][:80]} "
                                f"(vencida {a['deadline']}, "
                                f"asignada a {a['assigned_to']})"
                            )
                    pending = [a for a in acts if not a['is_overdue']]
                    if pending:
                        parts.append(
                            f"  Actividades programadas ({len(pending)}):"
                        )
                        for a in pending[:3]:
                            parts.append(
                                f"    - {a['type']}: {a['summary'][:80]} "
                                f"(para {a['deadline']}, {a['assigned_to']})"
                            )

                # Entregas pendientes
                deliveries = p.get('pending_deliveries', [])
                if deliveries:
                    late = [d for d in deliveries if d['is_late']]
                    if late:
                        parts.append(
                            f"  ⚠ ENTREGAS RETRASADAS ({len(late)}):"
                        )
                        for d in late[:3]:
                            parts.append(
                                f"    - {d['name']}: programada {d['scheduled']}"
                                f" ({d['type']}) origen: {d['origin']}"
                            )
                    on_time = [d for d in deliveries if not d['is_late']]
                    if on_time:
                        for d in on_time[:3]:
                            parts.append(
                                f"  Entrega: {d['name']} programada "
                                f"{d['scheduled']} ({d['type']})"
                            )

                # Manufactura
                mfg = p.get('manufacturing', [])
                if mfg:
                    parts.append(f"  Producción en proceso ({len(mfg)}):")
                    for m in mfg[:3]:
                        parts.append(
                            f"    - {m['name']}: {m['product']} "
                            f"x{m['qty']} ({m['state']}) "
                            f"origen: {m['origin']}"
                        )

                # Reuniones próximas
                meetings = p.get('upcoming_meetings', [])
                if meetings:
                    parts.append(f"  Reuniones próximas ({len(meetings)}):")
                    for ev in meetings[:3]:
                        parts.append(
                            f"    - {ev['name']} ({ev['start']}) "
                            f"con: {', '.join(ev['attendees'][:3])}"
                        )

                # Pagos recientes
                payments = p.get('recent_payments', [])
                if payments:
                    for pay in payments[:3]:
                        direction = (
                            'Cobro recibido' if pay['payment_type'] == 'inbound'
                            else 'Pago emitido'
                        )
                        parts.append(
                            f"  {direction}: {pay['name']} ${pay['amount']:,.0f}"
                            f" {pay['currency']} ({pay['date']})"
                        )

                # Comunicación reciente en Odoo (chatter)
                chatter = p.get('recent_chatter', [])
                related = p.get('related_chatter', [])
                all_msgs = chatter + related
                if all_msgs:
                    parts.append(
                        f"  Comunicación interna Odoo ({len(all_msgs)} msgs "
                        f"en 7d):"
                    )
                    for msg in all_msgs[:5]:
                        parts.append(
                            f"    - [{msg.get('date', '')}] "
                            f"{msg.get('author', '')}: "
                            f"{msg.get('preview', '')[:100]}"
                        )

                sections.append('\n'.join(parts))

        # ── Verificación de acciones (accountability) ───────────────────────
        followup = odoo_ctx.get('action_followup', {})
        if followup.get('items'):
            sections.append(
                f"\n═══ VERIFICACIÓN DE ACCIONES SUGERIDAS ═══\n"
                f"Tasa de completado (7 días): {followup.get('completion_rate', 0)}%\n"
                f"Completadas hoy: {followup.get('completed_today', 0)}\n"
                f"Vencidas sin hacer: {followup.get('overdue_count', 0)}"
            )
            for item in followup['items'][:15]:
                status = '⚠ VENCIDA' if item['is_overdue'] else 'pendiente'
                line = (
                    f"\n  [{item['priority'].upper()}] {item['description'][:100]}"
                    f" ({status})"
                )
                if item.get('assigned_to'):
                    line += f" → {item['assigned_to']}"
                if item.get('partner'):
                    line += f" | Contacto: {item['partner']}"
                if item.get('due_date'):
                    line += f" | Vence: {item['due_date']}"
                line += f" | {item['days_open']}d abierto"

                evidence = item.get('evidence_of_action', [])
                if evidence:
                    line += '\n    EVIDENCIA ENCONTRADA:'
                    for ev in evidence[:3]:
                        if ev['type'] == 'chatter_message':
                            line += (
                                f"\n      ✓ Mensaje en Odoo de "
                                f"{ev['author']} ({ev['date']}): "
                                f"{ev['preview'][:80]}"
                            )
                        elif ev['type'] == 'scheduled_activity':
                            line += (
                                f"\n      ✓ Actividad programada: "
                                f"{ev['activity']} para {ev['deadline']} "
                                f"({ev['assigned_to']})"
                            )
                else:
                    line += '\n    ✗ SIN EVIDENCIA DE ACCIÓN'

                sections.append(line)

        # ── Pipeline comercial global ───────────────────────────────────────
        pipeline = odoo_ctx.get('global_pipeline', {})
        if pipeline:
            sections.append(
                f"\n═══ PIPELINE COMERCIAL (CRM) ═══\n"
                f"Total oportunidades: {pipeline.get('total_opportunities', 0)}\n"
                f"Revenue esperado total: "
                f"${pipeline.get('total_expected_revenue', 0):,.0f}"
            )
            for stage, data in pipeline.get('by_stage', {}).items():
                sections.append(
                    f"  {stage}: {data['count']} opps "
                    f"(${data['revenue']:,.0f})"
                )

        # ── Actividades del equipo ──────────────────────────────────────────
        team = odoo_ctx.get('team_activities', {})
        if team:
            sections.append('\n═══ ACTIVIDADES DEL EQUIPO (próximos 3 días) ═══')
            for user_name, data in team.items():
                overdue_str = (
                    f' ⚠ {data["overdue"]} VENCIDAS'
                    if data['overdue'] else ''
                )
                sections.append(
                    f"\n  {user_name}: {data['pending']} pendientes"
                    f"{overdue_str}"
                )
                for act in data['items'][:3]:
                    marker = '⚠' if act['overdue'] else '·'
                    sections.append(
                        f"    {marker} {act['type']}: {act['summary'][:60]} "
                        f"(vence {act['deadline']})"
                    )

        # ── Client scores ───────────────────────────────────────────────────
        if client_scores:
            at_risk = [s for s in client_scores if s['risk_level'] == 'high']
            if at_risk:
                sections.append('\n═══ CLIENTES EN RIESGO ═══')
                for s in at_risk:
                    sections.append(
                        f"{s['email']}: score={s['total_score']}/100 "
                        f"(freq={s['frequency_score']}, "
                        f"resp={s['responsiveness_score']}, "
                        f"recip={s['reciprocity_score']}, "
                        f"sent={s['sentiment_score']})"
                    )

        return '\n'.join(sections)

    # ══════════════════════════════════════════════════════════════════════════
    #   COMPUTE COMMUNICATION PATTERNS
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def compute_communication_patterns(
        emails: list, threads: list, today: str,
    ) -> list:
        """Calcula patrones de comunicación POR CUENTA por semana."""
        from email.utils import parsedate_to_datetime as _pdt

        try:
            d = datetime.strptime(today, '%Y-%m-%d')
        except (ValueError, TypeError):
            d = datetime.now()
        week_start = (d - timedelta(days=d.weekday())).strftime('%Y-%m-%d')

        by_account = defaultdict(lambda: {
            'total': 0, 'hours': defaultdict(int),
            'subjects': defaultdict(int),
            'external_contacts': defaultdict(int),
            'internal_contacts': defaultdict(int),
            'replied_threads': 0, 'total_threads': 0,
        })

        for e in emails:
            account = e.get('account', '')
            if not account:
                continue
            data = by_account[account]
            data['total'] += 1
            try:
                dt = _pdt(e.get('date', ''))
                data['hours'][dt.hour] += 1
            except Exception:
                pass
            subj = e.get('subject_normalized', e.get('subject', ''))
            if subj:
                data['subjects'][subj] += 1
            sender = e.get('from_email', '')
            if sender and e.get('sender_type') == 'external':
                data['external_contacts'][sender] += 1
            elif sender:
                data['internal_contacts'][sender] += 1

        for t in threads:
            account = t.get('account', '')
            if account and account in by_account:
                by_account[account]['total_threads'] += 1
                if t.get('has_internal_reply'):
                    by_account[account]['replied_threads'] += 1

        patterns = []
        for account, data in by_account.items():
            if data['total'] < 1:
                continue
            resp_rate = None
            if data['total_threads'] > 0:
                resp_rate = round(
                    data['replied_threads'] / data['total_threads'], 2)
            busiest = None
            if data['hours']:
                busiest = max(data['hours'].items(), key=lambda x: x[1])[0]
            top_ext = sorted(
                data['external_contacts'].items(), key=lambda x: -x[1])[:5]
            top_int = sorted(
                data['internal_contacts'].items(), key=lambda x: -x[1])[:5]
            top_subj = sorted(
                data['subjects'].items(), key=lambda x: -x[1])[:5]

            patterns.append({
                'week_start': week_start,
                'account': account,
                'total_emails': data['total'],
                'response_rate': resp_rate,
                'busiest_hour': busiest,
                'top_external_contacts': [c for c, _ in top_ext],
                'top_internal_contacts': [c for c, _ in top_int],
                'common_subjects': [s for s, _ in top_subj],
            })

        return patterns

    # ══════════════════════════════════════════════════════════════════════════
    #   DETECT LEARNINGS
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def detect_learnings(metrics, alerts, client_scores, odoo_ctx, supa):
        """Detecta patrones y los registra como aprendizajes del sistema."""
        # 1. Accounts with degraded response times
        for m in metrics:
            if m.get('avg_response_hours') and m['avg_response_hours'] > 24:
                supa.save_learning(
                    'response_degradation',
                    f"Cuenta {m['account']}: tiempo promedio de respuesta "
                    f"{m['avg_response_hours']}h (>24h)",
                    {'account': m['account'],
                     'avg_hours': m['avg_response_hours'],
                     'unanswered': m.get('threads_unanswered', 0)},
                    account=m['account'],
                )

        # 2. At-risk clients
        at_risk = [
            s for s in client_scores if s.get('risk_level') == 'high'
        ]
        if at_risk:
            supa.save_learning(
                'trend_identified',
                f"{len(at_risk)} clientes en riesgo alto detectados",
                {'clients': [s['email'] for s in at_risk[:10]]},
            )

        # 3. High alert volume
        if len(alerts) > 15:
            supa.save_learning(
                'pattern_detected',
                f"Alto volumen de alertas: {len(alerts)} alertas en un día",
                {'alert_count': len(alerts),
                 'types': list({a.get('alert_type') for a in alerts})},
            )

        # 4. Action completion rate insight
        followup = odoo_ctx.get('action_followup', {})
        rate = followup.get('completion_rate', 0)
        if rate < 30 and followup.get('items'):
            supa.save_learning(
                'trend_identified',
                f"Tasa de completado de acciones muy baja: {rate}%",
                {'completion_rate': rate,
                 'overdue': followup.get('overdue_count', 0),
                 'total_pending': len(followup.get('items', []))},
            )
        elif rate > 80 and followup.get('items'):
            supa.save_learning(
                'response_improvement',
                f"Excelente tasa de completado de acciones: {rate}%",
                {'completion_rate': rate},
            )

    # ══════════════════════════════════════════════════════════════════════════
    #   WRAP BRIEFING HTML
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def wrap_briefing_html(body_html: str, today: str, weekly: bool = False) -> str:
        """Envuelve el briefing en un template HTML completo para email."""
        title = 'Weekly Intelligence Report' if weekly else 'Daily Intelligence Briefing'
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 800px; margin: 0 auto; padding: 20px; color: #1a1a1a;
         line-height: 1.6; }}
  h1 {{ color: #1e3a5f; border-bottom: 3px solid #2563eb; padding-bottom: 10px; }}
  h2 {{ color: #1e3a5f; margin-top: 25px; }}
  h3 {{ color: #374151; }}
  table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
  th, td {{ border: 1px solid #d1d5db; padding: 8px 12px; text-align: left; }}
  th {{ background: #f3f4f6; font-weight: 600; }}
  .header {{ background: linear-gradient(135deg, #1e3a5f, #2563eb);
             color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
  .header h1 {{ color: white; border: none; margin: 0; }}
  .footer {{ margin-top: 30px; padding-top: 15px; border-top: 1px solid #e5e7eb;
             font-size: 0.85em; color: #6b7280; }}
  strong {{ color: #1e3a5f; }}
  ul {{ padding-left: 20px; }}
  li {{ margin-bottom: 5px; }}
</style>
</head>
<body>
<div class="header">
  <h1>Quimibond {title}</h1>
  <p style="margin:5px 0 0;opacity:0.9">{today} — Generado por Intelligence System v19</p>
</div>
{body_html}
<div class="footer">
  <p>Generado automáticamente por <strong>Quimibond Intelligence System</strong> (Odoo 19).<br>
  Powered by Claude AI + Voyage AI + Supabase + Google Workspace.</p>
</div>
</body>
</html>"""
