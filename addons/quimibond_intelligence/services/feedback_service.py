"""
Quimibond Intelligence — Feedback Service (Phase 2)
Procesa señales de retroalimentación del usuario, calibra alertas y apoya mejora continua.
"""
import logging
from datetime import datetime, timedelta

from .supabase_base import SupabaseBaseClient

_logger = logging.getLogger(__name__)


class FeedbackService(SupabaseBaseClient):
    """Cliente para procesar feedback, calibrar alertas y optimizar acciones."""

    def process_feedback_rewards(self) -> tuple:
        """Procesa señales de retroalimentación no procesadas y calcula reward scores.

        Busca feedback_signals con reward_processed=false, calcula un reward_score
        basado en el tipo de señal, marca como procesado y retorna (count, total_reward).

        Scoring:
        - 'resolved' y < 24h: +1.0
        - 'acknowledged': +0.5
        - 'ignored': -0.5
        - 'completed' o 'done': +1.0
        - 'dismissed' o 'discarded': -0.8
        - 'positive' o 'thumbs_up': +0.5
        - 'negative' o 'thumbs_down': -1.0
        - 'briefing_action': +1.5
        - default: 0
        """
        count_processed = 0
        total_reward = 0.0

        try:
            # Fetch unprocessed feedback signals
            feedback_signals = self._request(
                '/rest/v1/feedback_signals?reward_processed=eq.false&limit=500'
            ) or []
        except Exception as exc:
            _logger.warning('Error fetching feedback_signals: %s', exc)
            return (0, 0.0)

        for signal in feedback_signals:
            signal_type = (signal.get('signal_type') or '').lower()
            reward_score = 0.0

            # Calculate reward based on signal type
            if 'resolved' in signal_type:
                created_at = signal.get('created_at', '')
                try:
                    created = datetime.fromisoformat(
                        created_at.replace('Z', '+00:00')
                    )
                    hours_ago = (datetime.now(created.tzinfo) - created).total_seconds() / 3600
                    if hours_ago < 24:
                        reward_score = 1.0
                except Exception:
                    reward_score = 0.5
            elif 'acknowledged' in signal_type:
                reward_score = 0.5
            elif 'ignored' in signal_type:
                reward_score = -0.5
            elif 'completed' in signal_type or 'done' in signal_type:
                reward_score = 1.0
            elif 'dismissed' in signal_type or 'discarded' in signal_type:
                reward_score = -0.8
            elif 'positive' in signal_type or 'thumbs_up' in signal_type:
                reward_score = 0.5
            elif 'negative' in signal_type or 'thumbs_down' in signal_type:
                reward_score = -1.0
            elif 'briefing_action' in signal_type:
                reward_score = 1.5
            else:
                reward_score = 0.0

            # Mark as processed
            signal_id = signal.get('id')
            if signal_id:
                try:
                    self._request(
                        f'/rest/v1/feedback_signals?id=eq.{signal_id}',
                        'PATCH',
                        {'reward_processed': True, 'reward_score': reward_score}
                    )
                    count_processed += 1
                    total_reward += reward_score
                except Exception as exc:
                    _logger.debug('Error marking feedback as processed: %s', exc)

        _logger.info('Feedback: %d signals processed, total reward: %.2f',
                     count_processed, total_reward)
        return (count_processed, total_reward)

    def calibrate_alerts(self) -> dict:
        """Llama RPC get_feedback_rewards y aplica calibraciones a alertas.

        Retorna dict con calibraciones aplicadas:
        - types con false_positive_rate > 0.6 → 'lower_severity'
        - types con resolution rate > 0.8 → 'raise_importance'
        """
        calibrations = {}

        try:
            result = self._request('/rest/v1/rpc/get_feedback_rewards', 'POST', {})
            if not result or not isinstance(result, list):
                return calibrations

            for feedback_data in result:
                alert_type = feedback_data.get('alert_type', '')
                false_positive_rate = float(feedback_data.get('false_positive_rate', 0))
                resolution_rate = float(feedback_data.get('resolution_rate', 0))

                actions = []

                if false_positive_rate > 0.6:
                    actions.append('lower_severity')
                    try:
                        self._request('/rest/v1/alert_calibration_log', 'POST', {
                            'alert_type': alert_type,
                            'calibration_action': 'lower_severity',
                            'false_positive_rate': false_positive_rate,
                            'applied_at': datetime.now().isoformat(),
                        })
                    except Exception as exc:
                        _logger.debug('Error saving calibration: %s', exc)

                if resolution_rate > 0.8:
                    actions.append('raise_importance')
                    try:
                        self._request('/rest/v1/alert_calibration_log', 'POST', {
                            'alert_type': alert_type,
                            'calibration_action': 'raise_importance',
                            'resolution_rate': resolution_rate,
                            'applied_at': datetime.now().isoformat(),
                        })
                    except Exception as exc:
                        _logger.debug('Error saving calibration: %s', exc)

                if actions:
                    calibrations[alert_type] = actions

        except Exception as exc:
            _logger.warning('Error in calibrate_alerts: %s', exc)

        _logger.info('Calibrations applied: %s', calibrations)
        return calibrations

    def get_action_priorities(self) -> dict:
        """Obtiene prioridades de acciones desde RPC get_action_effectiveness.

        Retorna dict: {action_type → priority_modifier (float)}
        """
        priorities = {}

        try:
            result = self._request('/rest/v1/rpc/get_action_effectiveness', 'POST', {})
            if not result or not isinstance(result, list):
                return priorities

            for action_data in result:
                action_type = action_data.get('action_type', '')
                effectiveness_score = float(action_data.get('effectiveness_score', 0.5))
                completion_rate = float(action_data.get('completion_rate', 0.5))

                # Calculate priority modifier: higher effectiveness and completion = higher priority
                priority_modifier = (effectiveness_score * 0.6) + (completion_rate * 0.4) - 0.5
                priorities[action_type] = priority_modifier

        except Exception as exc:
            _logger.warning('Error in get_action_priorities: %s', exc)

        _logger.info('Action priorities: %s', priorities)
        return priorities

    def save_learning(self, learning_type: str, description: str,
                      metric_name: str, metric_before: float, metric_after: float):
        """Calcula mejora y guarda en learning_effectiveness.

        Calcula improvement_pct = ((metric_after - metric_before) / abs(metric_before)) * 100
        si metric_before != 0, else 100 si metric_after > 0.
        """
        try:
            if metric_before != 0:
                improvement_pct = ((metric_after - metric_before) / abs(metric_before)) * 100
            elif metric_after > 0:
                improvement_pct = 100.0
            else:
                improvement_pct = 0.0

            self._request('/rest/v1/learning_effectiveness', 'POST', {
                'learning_type': learning_type,
                'description': description,
                'metric_name': metric_name,
                'metric_before': metric_before,
                'metric_after': metric_after,
                'improvement_pct': improvement_pct,
                'recorded_at': datetime.now().isoformat(),
            })
            _logger.info('Learning saved: %s (improvement: %.2f%%)',
                         learning_type, improvement_pct)
        except Exception as exc:
            _logger.debug('Error saving learning: %s', exc)
