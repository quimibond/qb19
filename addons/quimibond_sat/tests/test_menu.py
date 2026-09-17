# -*- coding: utf-8 -*-
"""El menú SAT cuelga de Contabilidad (Enterprise) cuando existe; si no, de
Facturación. En el CI (community) solo existe Facturación."""
from odoo.tests import TransactionCase, tagged

from odoo.addons.quimibond_sat.hooks import reparent_sat_menu


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatMenu(TransactionCase):

    def test_menu_parent(self):
        root = self.env.ref('quimibond_sat.menu_sat_root')
        accounting = self.env.ref('account_accountant.menu_accounting', raise_if_not_found=False)
        moved = reparent_sat_menu(self.env)
        if accounting:
            self.assertTrue(moved)
            self.assertEqual(root.parent_id, accounting)
        else:
            self.assertFalse(moved)
            self.assertEqual(root.parent_id, self.env.ref('account.menu_finance'))
        # Idempotente
        reparent_sat_menu(self.env)
