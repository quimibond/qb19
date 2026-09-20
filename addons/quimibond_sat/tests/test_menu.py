# -*- coding: utf-8 -*-
"""El menú SAT cuelga de Contabilidad (Enterprise) cuando existe; si no, de
Facturación. En el CI (community) solo existe Facturación."""
from odoo.tests import TransactionCase, tagged

from odoo.addons.quimibond_sat.hooks import accounting_root_menu, mark_root_noupdate, reparent_sat_menu


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatMenu(TransactionCase):

    def test_menu_parent(self):
        root = self.env.ref('quimibond_sat.menu_sat_root')
        accounting = accounting_root_menu(self.env)
        moved = reparent_sat_menu(self.env)
        if accounting:
            self.assertTrue(moved)
            self.assertEqual(root.parent_id, accounting)
        else:
            self.assertFalse(moved)
            self.assertEqual(root.parent_id, self.env.ref('account.menu_finance'))
        # Idempotente
        reparent_sat_menu(self.env)

    def test_root_is_noupdate(self):
        data = self.env['ir.model.data'].search([('module', '=', 'quimibond_sat'), ('name', '=', 'menu_sat_root')])
        self.assertTrue(data.noupdate, 'la raíz del menú debe ser noupdate para no regresar a Facturación al actualizar')
        # Base vieja (bandera en False): la migración la fija una sola vez
        data.write({'noupdate': False})
        self.assertTrue(mark_root_noupdate(self.env))
        self.assertTrue(data.noupdate)
        self.assertFalse(mark_root_noupdate(self.env))
