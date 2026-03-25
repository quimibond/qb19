"""
Quimibond Intelligence — Social Mixin
Activities, pipeline, and team tracking for OdooEnrichmentService.
"""
import logging
from collections import defaultdict
from datetime import timedelta

_logger = logging.getLogger(__name__)


class SocialMixin:
    """Verify pending actions, global pipeline, team activities."""

    def verify_pending_actions(self, today) -> dict:
        """Verifica si las acciones sugeridas previamente se ejecutaron."""
        result = {
            'items': [],
            'completion_rate': 0,
            'overdue_count': 0,
            'completed_today': 0,
        }
        try:
            ActionItem = self.env['intelligence.action.item'].sudo()
            pending = ActionItem.search([
                ('state', 'in', ['open', 'in_progress']),
            ], order='priority_seq asc, due_date asc', limit=30)

            if not pending:
                return result

            completed = ActionItem.search_count([
                ('state', '=', 'done'),
                ('write_date', '>=', today.strftime('%Y-%m-%d')),
            ])
            result['completed_today'] = completed

            for action in pending:
                item = {
                    'id': action.id,
                    'description': action.name,
                    'type': action.action_type,
                    'priority': action.priority,
                    'due_date': (action.due_date.strftime('%Y-%m-%d')
                                 if action.due_date else ''),
                    'assigned_to': (action.assignee_id.name
                                    if action.assignee_id else ''),
                    'partner': (action.partner_id.name
                                if action.partner_id else ''),
                    'days_open': (
                        (today - action.create_date.date()).days
                        if action.create_date else 0
                    ),
                    'is_overdue': action.is_overdue,
                    'evidence_of_action': [],
                }

                # Buscar evidencia de que alguien actuó
                if action.partner_id:
                    try:
                        MailMsg = self.env['mail.message'].sudo()
                        recent_msgs = MailMsg.search([
                            ('res_id', '=', action.partner_id.id),
                            ('model', '=', 'res.partner'),
                            ('date', '>=', action.create_date),
                            ('message_type', 'in', ['comment', 'email']),
                        ], limit=3, order='date desc')

                        for msg in recent_msgs:
                            item['evidence_of_action'].append({
                                'type': 'chatter_message',
                                'date': msg.date.strftime('%Y-%m-%d %H:%M'),
                                'author': (msg.author_id.name
                                           if msg.author_id else ''),
                                'preview': (msg.body or '')[:100],
                            })

                        MailActivity = self.env['mail.activity'].sudo()
                        activity_type_map = {
                            'call': 'Llamada',
                            'email': 'Correo',
                            'meeting': 'Reunión',
                        }
                        act_type_name = activity_type_map.get(
                            action.action_type, '',
                        )
                        if act_type_name:
                            scheduled = MailActivity.search([
                                ('res_id', '=', action.partner_id.id),
                                ('res_model', '=', 'res.partner'),
                            ], limit=3)
                            for act in scheduled:
                                item['evidence_of_action'].append({
                                    'type': 'scheduled_activity',
                                    'activity': (act.activity_type_id.name
                                                 if act.activity_type_id
                                                 else ''),
                                    'deadline': (
                                        act.date_deadline.strftime('%Y-%m-%d')
                                        if act.date_deadline else ''
                                    ),
                                    'assigned_to': (act.user_id.name
                                                    if act.user_id else ''),
                                })
                    except Exception:
                        pass

                if action.is_overdue:
                    result['overdue_count'] += 1

                result['items'].append(item)

            # Tasa de completado de los últimos 7 días
            week_ago = (
                today - timedelta(days=7)
            ).strftime('%Y-%m-%d')
            total_week = ActionItem.search_count([
                ('create_date', '>=', week_ago),
            ])
            done_week = ActionItem.search_count([
                ('create_date', '>=', week_ago),
                ('state', '=', 'done'),
            ])
            result['completion_rate'] = (
                round(done_week / total_week * 100)
                if total_week > 0 else 0
            )

        except Exception as exc:
            _logger.warning('Action verification error: %s', exc)

        return result

    def get_global_pipeline(self, CrmLead) -> dict:
        """Resumen global del pipeline comercial."""
        try:
            all_opps = CrmLead.search([
                ('type', '=', 'opportunity'),
                ('active', '=', True),
            ])
            if not all_opps:
                return {}

            by_stage = defaultdict(lambda: {'count': 0, 'revenue': 0})
            for opp in all_opps:
                stage_name = opp.stage_id.name if opp.stage_id else 'Sin etapa'
                by_stage[stage_name]['count'] += 1
                by_stage[stage_name]['revenue'] += opp.expected_revenue or 0

            total_revenue = sum(s['revenue'] for s in by_stage.values())
            return {
                'total_opportunities': len(all_opps),
                'total_expected_revenue': total_revenue,
                'by_stage': dict(by_stage),
            }
        except Exception as exc:
            _logger.debug('Pipeline error: %s', exc)
            return {}

    def get_team_activities(self, MailActivity, today) -> dict:
        """Actividades pendientes del equipo agrupadas por usuario."""
        try:
            all_activities = MailActivity.search([
                ('date_deadline', '<=',
                 (today + timedelta(days=3)).strftime('%Y-%m-%d')),
            ], order='date_deadline asc', limit=50)

            by_user = defaultdict(lambda: {
                'pending': 0, 'overdue': 0, 'items': [],
            })
            for act in all_activities:
                user_name = act.user_id.name if act.user_id else 'Sin asignar'
                is_overdue = (
                    act.date_deadline < today if act.date_deadline else False
                )
                by_user[user_name]['pending'] += 1
                if is_overdue:
                    by_user[user_name]['overdue'] += 1
                if len(by_user[user_name]['items']) < 5:
                    by_user[user_name]['items'].append({
                        'type': (act.activity_type_id.name
                                 if act.activity_type_id else 'Tarea'),
                        'summary': act.summary or '',
                        'deadline': (act.date_deadline.strftime('%Y-%m-%d')
                                     if act.date_deadline else ''),
                        'model': act.res_model or '',
                        'overdue': is_overdue,
                    })
            return dict(by_user)
        except Exception as exc:
            _logger.debug('Team activities error: %s', exc)
            return {}
