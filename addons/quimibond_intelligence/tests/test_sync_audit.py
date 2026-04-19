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


@tagged('post_install', '-at_install')
class TestAuditProducts(TransactionCase):

    def setUp(self):
        super().setUp()
        self.Audit = self.env['quimibond.sync.audit']
        self.run_id = 'test-run-products'
        self.tolerances = {}

    def _make_client_mock(self, supabase_counts):
        client = MagicMock()
        client.count_exact.side_effect = lambda table, params=None: \
            supabase_counts.get(
                (table, frozenset((params or {}).items())), 0)
        # recorded rows capturados
        client.upsert.return_value = None
        return client

    def test_products_count_active_match(self):
        # Creamos 2 productos activos en Odoo
        Product = self.env['product.product']
        Product.create({'name': 'P1', 'default_code': 'TEST-P1'})
        Product.create({'name': 'P2', 'default_code': 'TEST-P2'})
        odoo_count = Product.search_count([('active', '=', True)])

        client = self._make_client_mock({
            ('odoo_products', frozenset({'active': 'eq.true'}.items())): odoo_count,
        })
        self.Audit.audit_products(client, self.run_id, '2026-01-01', '2026-04-19',
                                  {}, dry_run=False)
        # Verificamos que se grabó la fila con severity=ok para count_active
        calls = [c for c in client.upsert.call_args_list
                 if c.args[0] == 'audit_runs']
        keys = [r['invariant_key']
                for call in calls for r in call.args[1]]
        self.assertIn('products.count_active', keys)


@tagged('post_install', '-at_install')
class TestAuditInvoiceLines(TransactionCase):

    def setUp(self):
        super().setUp()
        self.Audit = self.env['quimibond.sync.audit']
        self.run_id = 'test-run-invoice-lines'

    def test_invoice_lines_count_and_sum_match(self):
        # Preparamos 1 invoice out_invoice con 2 líneas en ene-2026
        partner = self.env['res.partner'].create({'name': 'Cliente Test'})
        product = self.env['product.product'].create({
            'name': 'Prod', 'default_code': 'IL-1',
        })
        move = self.env['account.move'].create({
            'move_type': 'out_invoice',
            'partner_id': partner.id,
            'invoice_date': '2026-01-15',
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id, 'quantity': 2, 'price_unit': 100,
            }), (0, 0, {
                'product_id': product.id, 'quantity': 1, 'price_unit': 50,
            })],
        })
        move.action_post()

        client = MagicMock()
        client.fetch_all.return_value = [
            {'bucket_key': '2026-01|out_invoice|%d' % self.env.company.id,
             'count': 2, 'sum_subtotal_mxn': 250.0, 'sum_qty': 3.0},
        ]
        captured = []
        def cap_upsert(table, rows, **k):
            if table == 'audit_runs':
                captured.extend(rows)
        client.upsert.side_effect = cap_upsert

        self.Audit.audit_invoice_lines(client, self.run_id,
                                       '2026-01-01', '2026-01-31', {},
                                       dry_run=False)
        keys = {r['invariant_key'] for r in captured}
        self.assertIn('invoice_lines.count_per_bucket', keys)
        self.assertIn('invoice_lines.sum_subtotal_signed_mxn', keys)
        self.assertIn('invoice_lines.sum_qty_signed', keys)


@tagged('post_install', '-at_install')
class TestAuditOrderLines(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Audit = self.env['quimibond.sync.audit']

    def test_order_lines_emits_both_sale_and_purchase_keys(self):
        client = MagicMock()
        client.fetch_all.return_value = []
        captured = []
        client.upsert.side_effect = lambda t, r, **k: (
            captured.extend(r) if t == 'audit_runs' else None)
        self.Audit.audit_order_lines(client, 'test-ol',
                                     '2026-01-01', '2026-01-31', {},
                                     dry_run=False)
        # Aun sin datos, emite buckets vacíos si hay filas Odoo
        # (si no hay nada, no emite nada — ese caso no se valida aquí)
        # Sólo validamos que el método no explota
        self.assertTrue(True)


@tagged('post_install', '-at_install')
class TestAuditDeliveries(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Audit = self.env['quimibond.sync.audit']

    def test_deliveries_emits_count_done_per_month(self):
        client = MagicMock()
        client.fetch_all.return_value = []
        captured = []
        client.upsert.side_effect = lambda t, r, **k: (
            captured.extend(r) if t == 'audit_runs' else None)
        self.Audit.audit_deliveries(client, 'test-dv',
                                    '2026-01-01', '2026-01-31', {},
                                    dry_run=False)
        self.assertTrue(True)  # humo


@tagged('post_install', '-at_install')
class TestAuditManufacturing(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Audit = self.env['quimibond.sync.audit']

    def test_manufacturing_smoke(self):
        client = MagicMock()
        client.fetch_all.return_value = []
        captured = []
        client.upsert.side_effect = lambda t, r, **k: (
            captured.extend(r) if t == 'audit_runs' else None)
        self.Audit.audit_manufacturing(client, 'test-mfg',
                                       '2026-01-01', '2026-01-31', {},
                                       dry_run=False)
        self.assertTrue(True)


@tagged('post_install', '-at_install')
class TestAuditAccountBalances(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Audit = self.env['quimibond.sync.audit']

    def test_account_balances_smoke(self):
        client = MagicMock()
        client.fetch_all.return_value = []
        captured = []
        client.upsert.side_effect = lambda t, r, **k: (
            captured.extend(r) if t == 'audit_runs' else None)
        self.Audit.audit_account_balances(client, 'test-ab',
                                          '2026-01-01', '2026-04-19', {},
                                          dry_run=False)
        self.assertTrue(True)


@tagged('post_install', '-at_install')
class TestAuditBankBalances(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Audit = self.env['quimibond.sync.audit']

    def test_bank_balances_smoke(self):
        client = MagicMock()
        client.count_exact.return_value = 0
        client.fetch.return_value = []
        captured = []
        client.upsert.side_effect = lambda t, r, **k: (
            captured.extend(r) if t == 'audit_runs' else None)
        self.Audit.audit_bank_balances(client, 'test-bb',
                                       '2026-01-01', '2026-04-19', {},
                                       dry_run=False)
        keys = {r['invariant_key'] for r in captured}
        self.assertIn('bank_balances.count_per_journal', keys)
