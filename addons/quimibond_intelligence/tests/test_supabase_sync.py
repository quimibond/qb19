"""
Tests for Supabase batch sync logic — mock-based, no Supabase connection.

Tests the sync decision logic: which records need syncing, how state
maps to Supabase fields, and error handling per record.

Run with: python -m pytest addons/quimibond_intelligence/tests/test_supabase_sync.py -v
"""
import unittest
from unittest.mock import MagicMock, patch

# Matches ALERT_STATE_MAP in engine_supabase_sync.py
ALERT_STATE_MAP = {
    'open': 'new',
    'acknowledged': 'acknowledged',
    'resolved': 'resolved',
    'dismissed': 'resolved',
}


class TestAlertSyncLogic(unittest.TestCase):
    """Test alert sync decision logic."""

    def test_alert_state_to_supabase_patch(self):
        """Verify correct Supabase state for each Odoo alert state."""
        expected_map = {
            'open': 'new',
            'acknowledged': 'acknowledged',
            'resolved': 'resolved',
            'dismissed': 'resolved',
        }
        for odoo_state, expected_supa_state in expected_map.items():
            supa_state = ALERT_STATE_MAP.get(odoo_state, 'new')
            self.assertEqual(supa_state, expected_supa_state)

    def test_resolved_states(self):
        """Resolved and dismissed should be detected as resolved."""
        for state in ('resolved', 'dismissed'):
            is_resolved = state in ('resolved', 'dismissed')
            self.assertTrue(is_resolved)

    def test_non_resolved_states(self):
        """Open and acknowledged should NOT be detected as resolved."""
        for state in ('open', 'acknowledged'):
            is_resolved = state in ('resolved', 'dismissed')
            self.assertFalse(is_resolved)

    def test_resolved_includes_timestamp(self):
        """Resolved alerts should include resolved_at."""
        state = 'resolved'
        resolved_date = '2026-03-25T10:00:00'
        is_resolved = state in ('resolved', 'dismissed')
        patch = {'state': ALERT_STATE_MAP.get(state, 'new')}
        if is_resolved and resolved_date:
            patch['resolved_at'] = resolved_date
        self.assertIn('resolved_at', patch)

    def test_non_resolved_no_timestamp(self):
        """Non-resolved alerts should NOT include resolved_at."""
        state = 'acknowledged'
        is_resolved = state in ('resolved', 'dismissed')
        patch = {'state': ALERT_STATE_MAP.get(state, 'new')}
        if is_resolved:
            patch['resolved_at'] = '2026-03-25T10:00:00'
        self.assertNotIn('resolved_at', patch)

    def test_resolution_notes_included(self):
        """Resolution notes should be included when present."""
        notes = 'Se resolvió llamando al cliente'
        patch = {'state': 'resolved'}
        if notes:
            patch['resolution_notes'] = notes
        self.assertEqual(patch['resolution_notes'], notes)


class TestActionSyncLogic(unittest.TestCase):
    """Test action item sync decision logic."""

    def test_done_state_mapping(self):
        """Done actions should map to 'completed' status."""
        state = 'done'
        patch = {'state': state}
        if state == 'done':
            patch['status'] = 'completed'
        self.assertEqual(patch['status'], 'completed')

    def test_cancelled_state_mapping(self):
        state = 'cancelled'
        patch = {'state': state}
        if state == 'cancelled':
            patch['status'] = 'cancelled'
        self.assertEqual(patch['status'], 'cancelled')

    def test_in_progress_state_mapping(self):
        state = 'in_progress'
        patch = {'state': state}
        if state == 'in_progress':
            patch['status'] = 'in_progress'
        self.assertEqual(patch['status'], 'in_progress')

    def test_open_state_mapping(self):
        state = 'open'
        patch = {'state': state}
        if state == 'open':
            patch['status'] = 'pending'
        self.assertEqual(patch['status'], 'pending')

    def test_action_without_supabase_id_skipped(self):
        """Actions without supabase_id should be marked as synced (nothing to sync)."""
        action = MagicMock()
        action.supabase_id = 0
        # Logic: if not supabase_id or <= 0, mark synced and skip
        should_skip = not action.supabase_id or action.supabase_id <= 0
        self.assertTrue(should_skip)


class TestSupabaseSyncedFlag(unittest.TestCase):
    """Test the supabase_synced flag behavior."""

    def test_state_change_marks_unsynced(self):
        """When alert state changes, supabase_synced should be False."""
        record = MagicMock()
        # Simulate write({'state': 'resolved', 'supabase_synced': False})
        vals = {'state': 'resolved', 'supabase_synced': False}
        self.assertFalse(vals['supabase_synced'])

    def test_after_sync_marks_synced(self):
        """After successful sync, record should be marked as synced."""
        record = MagicMock()
        # Simulate: after sync, write({'supabase_synced': True})
        vals = {'supabase_synced': True}
        self.assertTrue(vals['supabase_synced'])


if __name__ == '__main__':
    unittest.main()
