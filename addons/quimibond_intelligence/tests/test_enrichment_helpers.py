"""
Tests for enrichment helpers — pure functions, no Odoo dependency.

Run with: python -m pytest addons/quimibond_intelligence/tests/test_enrichment_helpers.py -v
"""
import unittest

try:
    from odoo.addons.quimibond_intelligence.services.enrichment_helpers import (
        is_automated_sender,
        is_generic_domain,
        GENERIC_DOMAINS,
        SERVICE_DOMAINS,
    )
except ImportError:
    import os
    import sys
    _services_dir = os.path.join(
        os.path.dirname(__file__), '..', 'services',
    )
    if _services_dir not in sys.path:
        sys.path.insert(0, os.path.abspath(_services_dir))
    from enrichment_helpers import (
        is_automated_sender,
        is_generic_domain,
        GENERIC_DOMAINS,
        SERVICE_DOMAINS,
    )


class TestIsAutomatedSender(unittest.TestCase):

    def test_noreply_prefix(self):
        self.assertTrue(is_automated_sender('noreply@example.com'))

    def test_no_reply_prefix(self):
        self.assertTrue(is_automated_sender('no-reply@example.com'))

    def test_notifications_prefix(self):
        self.assertTrue(is_automated_sender('notifications@example.com'))

    def test_noreply_with_suffix(self):
        self.assertTrue(is_automated_sender('noreply-sales@example.com'))

    def test_normal_email(self):
        self.assertFalse(is_automated_sender('juan.perez@quimibond.com'))

    def test_service_domain_exact(self):
        self.assertTrue(is_automated_sender('hello@github.com'))

    def test_service_domain_subdomain(self):
        self.assertTrue(is_automated_sender('hello@status.incident.io'))

    def test_service_domain_deep_subdomain(self):
        self.assertTrue(is_automated_sender('x@transactional.n8n.io'))

    def test_normal_business_email(self):
        self.assertFalse(is_automated_sender('ventas@acme.com.mx'))

    def test_empty_string(self):
        self.assertFalse(is_automated_sender(''))

    def test_none(self):
        self.assertFalse(is_automated_sender(None))

    def test_case_insensitive(self):
        self.assertTrue(is_automated_sender('NoReply@Example.COM'))

    def test_banking_domain(self):
        self.assertTrue(is_automated_sender('avisos@bbva.mx'))

    def test_sat_domain(self):
        self.assertTrue(is_automated_sender('buzontributario@sat.gob.mx'))


class TestIsGenericDomain(unittest.TestCase):

    def test_gmail(self):
        self.assertTrue(is_generic_domain('gmail.com'))

    def test_outlook(self):
        self.assertTrue(is_generic_domain('outlook.com'))

    def test_yahoo_mexico(self):
        self.assertTrue(is_generic_domain('yahoo.com.mx'))

    def test_business_domain(self):
        self.assertFalse(is_generic_domain('quimibond.com'))

    def test_case_insensitive(self):
        self.assertTrue(is_generic_domain('Gmail.COM'))

    def test_unknown_domain(self):
        self.assertFalse(is_generic_domain('acme-textiles.com.mx'))


class TestConstantsIntegrity(unittest.TestCase):
    """Verify constants are properly defined."""

    def test_generic_domains_has_gmail(self):
        self.assertIn('gmail.com', GENERIC_DOMAINS)

    def test_service_domains_has_github(self):
        self.assertIn('github.com', SERVICE_DOMAINS)

    def test_no_overlap_generic_service(self):
        """Generic domains that are also service domains is OK (github.com),
        but verify the sets are properly defined."""
        self.assertIsInstance(GENERIC_DOMAINS, frozenset)
        self.assertIsInstance(SERVICE_DOMAINS, frozenset)


if __name__ == '__main__':
    unittest.main()
