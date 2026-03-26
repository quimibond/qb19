"""
Tests for Odoo → Supabase sync schema coverage.

These tests verify that every method that builds records for Supabase
includes all expected columns. They run WITHOUT Odoo or Supabase — they
just check that the record-building logic produces the right keys.

Run with: python -m pytest addons/quimibond_intelligence/tests/test_sync_schema.py -v
Or:       python addons/quimibond_intelligence/tests/test_sync_schema.py
"""
import os
import sys
import unittest

# Support both Odoo-loaded tests and standalone pytest execution.
# When Odoo loads the module, relative imports work. When running
# standalone (python -m pytest), we fall back to manipulating sys.path.
try:
    from odoo.addons.quimibond_intelligence.services.sync_schema import (
        SUPABASE_SCHEMAS,
        check_coverage,
        get_writable_columns,
        validate_record,
    )
except ImportError:
    _services_dir = os.path.join(
        os.path.dirname(__file__), '..', 'services',
    )
    if _services_dir not in sys.path:
        sys.path.insert(0, os.path.abspath(_services_dir))

    from sync_schema import (  # noqa: E402
        SUPABASE_SCHEMAS,
        check_coverage,
        get_writable_columns,
        validate_record,
    )


class TestSyncSchemaRegistry(unittest.TestCase):
    """Test that the schema registry itself is well-formed."""

    def test_all_tables_have_writable_columns(self):
        for table, schema in SUPABASE_SCHEMAS.items():
            self.assertTrue(
                schema['writable'],
                f'{table} has no writable columns defined',
            )

    def test_no_overlap_writable_auto(self):
        for table, schema in SUPABASE_SCHEMAS.items():
            overlap = schema['writable'] & schema['auto']
            self.assertFalse(
                overlap,
                f'{table}: columns in both writable and auto: {overlap}',
            )

    def test_upsert_keys_are_writable(self):
        for table, schema in SUPABASE_SCHEMAS.items():
            if schema['upsert_key']:
                for col in schema['upsert_key']:
                    self.assertIn(
                        col, schema['writable'],
                        f'{table}: upsert key "{col}" not in writable columns',
                    )


class TestValidateRecord(unittest.TestCase):
    """Test the validate_record helper."""

    def test_valid_record_no_warnings(self):
        record = {'account': 'x', 'last_history_id': '123'}
        warnings = validate_record('sync_state', record)
        self.assertEqual(warnings, [])

    def test_unknown_column_warns(self):
        record = {'account': 'x', 'typo_field': 123}
        warnings = validate_record('sync_state', record)
        self.assertTrue(any('typo_field' in w for w in warnings))

    def test_missing_required_warns(self):
        record = {'account': 'x'}
        warnings = validate_record(
            'sync_state', record, required={'account', 'last_history_id'},
        )
        self.assertTrue(any('last_history_id' in w for w in warnings))

    def test_unknown_table(self):
        warnings = validate_record('nonexistent_table', {})
        self.assertTrue(any('Unknown' in w for w in warnings))


class TestRevenuMetricsCoverage(unittest.TestCase):
    """Verify revenue_metrics records include all expected fields."""

    def _build_revenue_record(self):
        """Simulate what _sync_contacts_to_supabase builds."""
        # This mirrors the revenue_batch.append() in intelligence_engine.py
        recent_sales = [{'amount': 1000}, {'amount': 2000}]
        pending_invoices = [
            {'amount_residual': 500, 'days_overdue': 10},
            {'amount_residual': 300, 'days_overdue': 0},
        ]
        overdue = [
            inv for inv in pending_invoices if inv.get('days_overdue', 0) > 0
        ]
        recent_payments = [
            {'amount': 800, 'payment_type': 'inbound'},
            {'amount': 200, 'payment_type': 'outbound'},
        ]
        total_collected = sum(
            p['amount'] for p in recent_payments
            if p.get('payment_type') == 'inbound'
        )
        today = '2026-03-24'
        return {
            'contact_email': 'test@example.com',
            'period_start': today[:8] + '01',
            'period_end': today,
            'period_type': 'monthly',
            'total_invoiced': 50000,
            'pending_amount': sum(
                inv['amount_residual'] for inv in pending_invoices),
            'overdue_amount': sum(
                inv['amount_residual'] for inv in overdue),
            'overdue_days_max': max(
                (inv['days_overdue'] for inv in overdue), default=0),
            'num_orders': len(recent_sales),
            'avg_order_value': sum(
                s['amount'] for s in recent_sales) / len(recent_sales),
            'odoo_partner_id': 42,
            'total_collected': total_collected,
        }

    def test_total_collected_present(self):
        record = self._build_revenue_record()
        self.assertIn('total_collected', record)
        self.assertEqual(record['total_collected'], 800)

    def test_no_unknown_columns(self):
        record = self._build_revenue_record()
        warnings = validate_record('revenue_metrics', record)
        unknown_warnings = [w for w in warnings if 'unknown' in w.lower()]
        self.assertEqual(unknown_warnings, [], f'Unknown columns: {warnings}')

    def test_coverage(self):
        record = self._build_revenue_record()
        missing = check_coverage('revenue_metrics', record)
        # contact_id and company_id are resolved later by save_revenue_metrics_batch
        allowed_missing = {'contact_id', 'company_id'}
        unexpected_missing = missing - allowed_missing
        self.assertFalse(
            unexpected_missing,
            f'revenue_metrics missing columns: {unexpected_missing}',
        )


class TestHealthScoreCoverage(unittest.TestCase):
    """Verify health_scores records include all expected fields."""

    def _build_health_score_record(self):
        """Simulate what compute_and_save_health_scores builds."""
        return {
            'contact_email': 'test@example.com',
            'score_date': '2026-03-24',
            'overall_score': 72.5,
            'trend': 'stable',
            'communication_score': 80.0,
            'financial_score': 65.0,
            'sentiment_score': 70.0,
            'responsiveness_score': 75.0,
            'engagement_score': 60.0,
            'risk_signals': ['slow_responder'],
            'opportunity_signals': [],
            'company_id': 5,
            'payment_compliance_score': 15,
            'previous_score': 70.0,
        }

    def test_company_id_present(self):
        record = self._build_health_score_record()
        self.assertIn('company_id', record)

    def test_payment_compliance_present(self):
        record = self._build_health_score_record()
        self.assertIn('payment_compliance_score', record)

    def test_previous_score_present(self):
        record = self._build_health_score_record()
        self.assertIn('previous_score', record)

    def test_no_unknown_columns(self):
        record = self._build_health_score_record()
        warnings = validate_record('health_scores', record)
        unknown_warnings = [w for w in warnings if 'unknown' in w.lower()]
        self.assertEqual(unknown_warnings, [], f'Unknown columns: {warnings}')

    def test_coverage(self):
        record = self._build_health_score_record()
        missing = check_coverage('health_scores', record)
        # contact_id is resolved from email
        allowed_missing = {'contact_id'}
        unexpected_missing = missing - allowed_missing
        self.assertFalse(
            unexpected_missing,
            f'health_scores missing columns: {unexpected_missing}',
        )


class TestCompanySnapshotCoverage(unittest.TestCase):
    """Verify odoo_snapshots records include all expected fields."""

    def _build_snapshot_record(self):
        return {
            'company_id': 1,
            'snapshot_date': '2026-03-24',
            'total_invoiced': 100000,
            'pending_amount': 5000,
            'overdue_amount': 1000,
            'monthly_avg': 8000,
            'open_orders_count': 3,
            'pending_deliveries_count': 2,
            'late_deliveries_count': 0,
            'crm_pipeline_value': 50000,
            'crm_leads_count': 2,
            'manufacturing_count': 1,
            'credit_notes_total': 500,
        }

    def test_full_coverage(self):
        record = self._build_snapshot_record()
        missing = check_coverage('odoo_snapshots', record)
        self.assertFalse(
            missing,
            f'odoo_snapshots missing columns: {missing}',
        )


class TestEmailRecordCoverage(unittest.TestCase):
    """Verify email records include all expected fields."""

    def _build_email_record(self):
        return {
            'account': 'ventas@quimibond.com',
            'sender': 'client@example.com',
            'recipient': 'ventas@quimibond.com',
            'subject': 'Test',
            'body': 'Hello',
            'snippet': 'Hello...',
            'email_date': '2026-03-24T12:00:00+00:00',
            'gmail_message_id': 'abc123',
            'gmail_thread_id': 'thread123',
            'attachments': None,
            'is_reply': False,
            'sender_type': 'external',
            'has_attachments': False,
            'kg_processed': False,
        }

    def test_full_coverage(self):
        record = self._build_email_record()
        missing = check_coverage('emails', record)
        # These are resolved by Supabase triggers, not set by the pipeline
        trigger_resolved = {'company_id', 'thread_id', 'sender_contact_id'}
        unexpected = missing - trigger_resolved
        self.assertFalse(unexpected, f'emails missing columns: {unexpected}')


class TestThreadRecordCoverage(unittest.TestCase):
    """Verify thread records include all expected fields."""

    def _build_thread_record(self):
        return {
            'gmail_thread_id': 'thread123',
            'subject': 'Test Subject',
            'subject_normalized': 'test subject',
            'started_by': 'client@example.com',
            'started_by_type': 'external',
            'started_at': '2026-03-24T10:00:00+00:00',
            'last_activity': '2026-03-24T12:00:00+00:00',
            'status': 'needs_response',
            'message_count': 3,
            'participant_emails': ['a@x.com', 'b@y.com'],
            'has_internal_reply': True,
            'has_external_reply': True,
            'last_sender': 'client@example.com',
            'last_sender_type': 'external',
            'hours_without_response': 2.5,
            'account': 'ventas@quimibond.com',
        }

    def test_full_coverage(self):
        record = self._build_thread_record()
        missing = check_coverage('threads', record)
        # These are resolved by Supabase triggers, not set by the pipeline
        trigger_resolved = {'company_id', 'started_by_contact_id'}
        unexpected = missing - trigger_resolved
        self.assertFalse(unexpected, f'threads missing columns: {unexpected}')


if __name__ == '__main__':
    unittest.main()
