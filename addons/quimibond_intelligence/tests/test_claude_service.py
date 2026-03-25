"""
Tests for ClaudeService — JSON extraction and response parsing.
Runs WITHOUT Claude API — tests only the parsing logic.

Run with: python -m pytest addons/quimibond_intelligence/tests/test_claude_service.py -v
"""
import unittest

import json
import re

# We only test the static _extract_json method, which doesn't need the
# anthropic SDK. Import the class if available, otherwise define a minimal
# stub with just the parsing logic.
_ClaudeService = None
try:
    from odoo.addons.quimibond_intelligence.services.claude_service import (
        ClaudeService as _ClaudeService,
    )
except ImportError:
    try:
        import os
        import sys
        _services_dir = os.path.join(
            os.path.dirname(__file__), '..', 'services',
        )
        if _services_dir not in sys.path:
            sys.path.insert(0, os.path.abspath(_services_dir))
        from claude_service import ClaudeService as _ClaudeService
    except ImportError:
        pass


if _ClaudeService is None:
    # Minimal stub with just the method under test
    class _ClaudeService:
        @staticmethod
        def _extract_json(text: str) -> dict:
            cleaned = text.strip()
            if cleaned.startswith('```'):
                lines = cleaned.split('\n')
                cleaned = '\n'.join(lines[1:])
                if cleaned.endswith('```'):
                    cleaned = cleaned[:-3]
            try:
                return json.loads(cleaned)
            except (json.JSONDecodeError, ValueError):
                pass
            match = re.search(r'\{[\s\S]*\}', cleaned)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    pass
            raise ValueError('Claude no retornó JSON válido')


ClaudeService = _ClaudeService


class TestExtractJson(unittest.TestCase):
    """Test ClaudeService._extract_json static method."""

    def test_plain_json(self):
        text = '{"key": "value", "num": 42}'
        result = ClaudeService._extract_json(text)
        self.assertEqual(result['key'], 'value')
        self.assertEqual(result['num'], 42)

    def test_json_with_markdown_fences(self):
        text = '```json\n{"key": "value"}\n```'
        result = ClaudeService._extract_json(text)
        self.assertEqual(result['key'], 'value')

    def test_json_with_plain_fences(self):
        text = '```\n{"items": [1, 2, 3]}\n```'
        result = ClaudeService._extract_json(text)
        self.assertEqual(result['items'], [1, 2, 3])

    def test_json_with_surrounding_text(self):
        text = 'Here is the result:\n{"answer": true}\nEnd of response.'
        result = ClaudeService._extract_json(text)
        self.assertTrue(result['answer'])

    def test_nested_json(self):
        text = '{"summary": {"text": "ok"}, "knowledge_graph": {"entities": []}}'
        result = ClaudeService._extract_json(text)
        self.assertIn('summary', result)
        self.assertIn('knowledge_graph', result)

    def test_invalid_json_raises(self):
        with self.assertRaises(ValueError):
            ClaudeService._extract_json('This is not JSON at all')

    def test_empty_string_raises(self):
        with self.assertRaises(ValueError):
            ClaudeService._extract_json('')

    def test_json_with_whitespace(self):
        text = '  \n  {"key": "value"}  \n  '
        result = ClaudeService._extract_json(text)
        self.assertEqual(result['key'], 'value')

    def test_json_with_trailing_comma_in_text(self):
        """Claude sometimes adds preamble text before JSON."""
        text = 'Aquí está el análisis:\n\n{"summary_text": "Todo bien", "sentiment": "positive"}'
        result = ClaudeService._extract_json(text)
        self.assertEqual(result['summary_text'], 'Todo bien')

    def test_analyze_account_full_response_structure(self):
        """Verify the expected structure from analyze_account_full."""
        text = '''{
            "summary": {
                "summary_text": "Resumen",
                "overall_sentiment": "neutral",
                "sentiment_score": 0.1,
                "key_items": [],
                "external_contacts": [],
                "person_insights": []
            },
            "knowledge_graph": {
                "entities": [{"name": "Acme", "type": "company"}],
                "facts": [],
                "action_items": [],
                "relationships": [],
                "person_profiles": []
            }
        }'''
        result = ClaudeService._extract_json(text)
        self.assertIn('summary', result)
        self.assertIn('knowledge_graph', result)
        self.assertEqual(len(result['knowledge_graph']['entities']), 1)


if __name__ == '__main__':
    unittest.main()
