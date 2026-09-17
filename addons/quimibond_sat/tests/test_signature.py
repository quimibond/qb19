# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged

from ..syntage_signature import compute_signature, parse_signature_header, verify_signature


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSyntageSignature(TransactionCase):

    def test_valid_signature(self):
        body = b'{"id":"evt_1","type":"invoice.created"}'
        t = 1_700_000_000
        sig = compute_signature(body, 'secreto', t)
        header = 't=%d,s=%s' % (t, sig)
        self.assertTrue(verify_signature(body, header, 'secreto', now=lambda: t + 10))
        # Mayúsculas en la firma también pasan
        self.assertTrue(verify_signature(body, 't=%d,s=%s' % (t, sig.upper()), 'secreto', now=lambda: t))

    def test_wrong_secret_or_body(self):
        body = b'{"id":"evt_1"}'
        t = 1_700_000_000
        header = 't=%d,s=%s' % (t, compute_signature(body, 'secreto', t))
        self.assertFalse(verify_signature(body, header, 'otro', now=lambda: t))
        self.assertFalse(verify_signature(b'{"id":"evt_2"}', header, 'secreto', now=lambda: t))

    def test_timestamp_window(self):
        body = b'{}'
        t = 1_700_000_000
        header = 't=%d,s=%s' % (t, compute_signature(body, 'secreto', t))
        self.assertFalse(verify_signature(body, header, 'secreto', now=lambda: t + 301))
        self.assertTrue(verify_signature(body, header, 'secreto', tolerance=0, now=lambda: t + 99999))

    def test_malformed_header(self):
        self.assertIsNone(parse_signature_header(''))
        self.assertIsNone(parse_signature_header('t=abc,s=00'))
        self.assertIsNone(parse_signature_header('s=00'))
        self.assertEqual(parse_signature_header(' t=5 , s=ab '), (5, 'ab'))
        self.assertFalse(verify_signature(b'{}', 'garbage', 'secreto'))
        self.assertFalse(verify_signature(b'{}', 't=1,s=ab', ''))
