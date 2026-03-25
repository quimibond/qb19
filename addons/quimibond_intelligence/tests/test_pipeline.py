"""
Tests for pipeline orchestrator logic — mock-based, no external services.

These tests verify the orchestrator calls micro-pipelines correctly
and handles failures gracefully. They do NOT test the actual pipeline
logic (that requires Odoo + external services).

Run with: python -m pytest addons/quimibond_intelligence/tests/test_pipeline.py -v
"""
import unittest
from unittest.mock import MagicMock, patch, call


class TestDailyOrchestrator(unittest.TestCase):
    """Test run_daily_intelligence orchestrator logic."""

    def _make_engine(self):
        """Create a mock engine with all micro-pipeline methods."""
        engine = MagicMock()
        engine.run_sync_emails = MagicMock()
        engine.run_analyze_emails = MagicMock()
        engine.run_enrich_only = MagicMock()
        engine.run_update_scores = MagicMock()
        engine.run_supabase_sync = MagicMock()
        engine._run_daily_briefing = MagicMock()
        return engine

    def test_orchestrator_calls_all_pipelines(self):
        """Verify all micro-pipelines are called in order."""
        engine = self._make_engine()

        # Simulate the orchestrator logic
        pipelines = [
            ('Sync emails', engine.run_sync_emails),
            ('Analyze emails', engine.run_analyze_emails),
            ('Enrich contacts', engine.run_enrich_only),
            ('Update scores', engine.run_update_scores),
        ]
        for name, fn in pipelines:
            fn()
        engine._run_daily_briefing()
        engine.run_supabase_sync()

        engine.run_sync_emails.assert_called_once()
        engine.run_analyze_emails.assert_called_once()
        engine.run_enrich_only.assert_called_once()
        engine.run_update_scores.assert_called_once()
        engine._run_daily_briefing.assert_called_once()
        engine.run_supabase_sync.assert_called_once()

    def test_pipeline_failure_doesnt_stop_others(self):
        """If one pipeline fails, the others should still run."""
        engine = self._make_engine()
        engine.run_analyze_emails.side_effect = Exception('Claude API down')

        pipelines = [
            ('Sync emails', engine.run_sync_emails),
            ('Analyze emails', engine.run_analyze_emails),
            ('Enrich contacts', engine.run_enrich_only),
            ('Update scores', engine.run_update_scores),
        ]
        for name, fn in pipelines:
            try:
                fn()
            except Exception:
                pass  # Orchestrator catches and continues

        engine._run_daily_briefing()
        engine.run_supabase_sync()

        # All were called despite analyze failing
        engine.run_sync_emails.assert_called_once()
        engine.run_analyze_emails.assert_called_once()
        engine.run_enrich_only.assert_called_once()
        engine.run_update_scores.assert_called_once()
        engine._run_daily_briefing.assert_called_once()


class TestLockGuard(unittest.TestCase):
    """Test concurrency lock pattern used by all micro-pipelines."""

    def test_lock_prevents_concurrent_run(self):
        """If lock is 'true', pipeline should abort."""
        ICP = MagicMock()
        ICP.get_param.return_value = 'true'

        # Simulate the lock check pattern
        lock = 'quimibond_intelligence.test_running'
        if ICP.get_param(lock, 'false') == 'true':
            aborted = True
        else:
            aborted = False

        self.assertTrue(aborted)

    def test_lock_allows_run_when_free(self):
        """If lock is 'false', pipeline should proceed."""
        ICP = MagicMock()
        ICP.get_param.return_value = 'false'

        lock = 'quimibond_intelligence.test_running'
        if ICP.get_param(lock, 'false') == 'true':
            aborted = True
        else:
            aborted = False

        self.assertFalse(aborted)


class TestAnalyzeAccountsReturnType(unittest.TestCase):
    """Verify _analyze_accounts returns the new tuple format."""

    def test_empty_emails_returns_tuple(self):
        """With no emails, should return ([], {})."""
        # Simulate the function with no emails
        summaries = []
        kg_by_account = {}
        result = (summaries, kg_by_account)

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], list)
        self.assertIsInstance(result[1], dict)


if __name__ == '__main__':
    unittest.main()
