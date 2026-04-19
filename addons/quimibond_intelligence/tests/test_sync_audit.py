"""Tests for quimibond.sync.audit — integrity invariants Odoo↔Supabase."""
from unittest.mock import patch, MagicMock
from odoo.tests.common import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestSyncAuditBase(TransactionCase):
    """Smoke test: model exists and run_all returns summary dict."""

    def setUp(self):
        super().setUp()
        self.Audit = self.env['quimibond.sync.audit']

    def test_model_exists(self):
        self.assertTrue(self.Audit)

    def test_run_all_returns_summary(self):
        with patch.object(self.Audit, '_get_client') as m_client:
            m_client.return_value = MagicMock()
            result = self.Audit.run_all(
                date_from='2026-01-01',
                date_to='2026-04-19',
                scope=[],  # empty scope → no invariants run
                dry_run=True,
            )
        self.assertIn('run_id', result)
        self.assertIn('summary', result)
        self.assertEqual(result['summary'], {'ok': 0, 'warn': 0, 'error': 0})
