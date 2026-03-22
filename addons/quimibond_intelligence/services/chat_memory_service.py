"""
Quimibond Intelligence — Chat Memory Service (Phase 2)
Gestiona un repositorio de preguntas y respuestas exitosas para mejora continua del RAG.
"""
import logging
from datetime import datetime

from .supabase_base import SupabaseBaseClient

_logger = logging.getLogger(__name__)


class ChatMemoryService(SupabaseBaseClient):
    """Servicio para guardar, recuperar y usar memoria de chat para few-shot learning."""

    def save_successful_qa(self, question: str, answer: str, context_used: str,
                           rating: str = 'positive'):
        """Guarda una Q&A exitosa en chat_memory.

        Args:
            question: Pregunta del usuario
            answer: Respuesta de Claude
            context_used: Contexto que se utilizó (fuentes, tablas, etc.)
            rating: 'positive', 'neutral', o 'negative'
        """
        try:
            record = {
                'question': question,
                'answer': answer,
                'context_used': context_used,
                'rating': rating,
                'thumbs_up': rating == 'positive',
                'times_retrieved': 0,
                'saved_at': datetime.now().isoformat(),
            }
            self._request('/rest/v1/chat_memory', 'POST', [record])
            _logger.info('Chat memory: Q&A saved (rating: %s)', rating)
        except Exception as exc:
            _logger.debug('Error saving chat_memory: %s', exc)

    def retrieve_similar_memories(self, question: str, limit: int = 3) -> list:
        """Recupera memorias similares ordenadas por relevancia.

        Consulta chat_memory filtrando por thumbs_up=true, ordenado por times_retrieved desc.
        Incrementa times_retrieved para los registros recuperados.

        Args:
            question: Pregunta del usuario (usado como referencia)
            limit: Número máximo de memorias a recuperar

        Returns:
            Lista de registros de chat_memory
        """
        memories = []

        try:
            # Fetch successful memories (thumbs_up=true), ordered by frequency of use
            records = self._request(
                f'/rest/v1/chat_memory?thumbs_up=eq.true&order=times_retrieved.desc'
                f'&limit={limit}'
            ) or []

            if isinstance(records, list):
                # Update times_retrieved for each retrieved record
                for memory in records:
                    memory_id = memory.get('id')
                    if memory_id:
                        try:
                            new_count = (memory.get('times_retrieved', 0) or 0) + 1
                            self._request(
                                f'/rest/v1/chat_memory?id=eq.{memory_id}',
                                'PATCH',
                                {'times_retrieved': new_count}
                            )
                        except Exception as exc:
                            _logger.debug('Error incrementing times_retrieved: %s', exc)
                memories = records

        except Exception as exc:
            _logger.debug('Error retrieving chat memories: %s', exc)

        _logger.info('Retrieved %d chat memories', len(memories))
        return memories

    def process_chat_feedback(self, chat_feedback_id: int, rating: str):
        """Procesa feedback de usuario y guarda Q&A en memoria si es positivo.

        Si el rating es positivo, busca el chat_feedback y extrae question/answer
        para guardar en chat_memory.

        Args:
            chat_feedback_id: ID del feedback
            rating: 'positive', 'neutral', o 'negative'
        """
        if rating != 'positive':
            _logger.debug('Skipping non-positive feedback')
            return

        try:
            # Fetch the feedback record
            feedback_records = self._request(
                f'/rest/v1/chat_feedback?id=eq.{chat_feedback_id}&limit=1'
            ) or []

            if feedback_records and isinstance(feedback_records, list):
                feedback = feedback_records[0]
                question = feedback.get('question', '')
                answer = feedback.get('answer', '')
                context = feedback.get('context_used', '')

                if question and answer:
                    self.save_successful_qa(
                        question=question,
                        answer=answer,
                        context_used=context,
                        rating='positive'
                    )
        except Exception as exc:
            _logger.debug('Error processing chat feedback: %s', exc)

    def get_few_shot_examples(self, question: str, limit: int = 2) -> str:
        """Obtiene ejemplos de few-shot para usar en prompts.

        Recupera memories similares y las formatea como ejemplos para el LLM.

        Args:
            question: Pregunta actual del usuario
            limit: Número de ejemplos

        Returns:
            String formateado con ejemplos (ej. "Q: ...\nA: ...\n\nQ: ...\nA: ...")
        """
        memories = self.retrieve_similar_memories(question, limit=limit)

        if not memories:
            return ''

        examples = []
        for memory in memories:
            q = memory.get('question', '')
            a = memory.get('answer', '')
            if q and a:
                examples.append(f'Q: {q}\nA: {a}')

        formatted = '\n\n'.join(examples)
        _logger.info('Few-shot examples: %d examples formatted', len(examples))
        return formatted
