# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestEscalateNc(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Alert = cls.env['quality.alert']
        cls.team_main = cls.env['quality.alert.team'].search(
            [('name', '=', 'Main Quality Team')], limit=1)
        if not cls.team_main:
            cls.team_main = cls.env['quality.alert.team'].create({'name': 'Equipo Piso Test'})
        cls.team_nc = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.product = cls.env['product.product'].create({'name': 'Prod escala test'})

    def test_01_escalate_assigns_folio_and_keeps_links(self):
        alert = self.Alert.create({
            'title': 'Alerta operativa de piso',
            'team_id': self.team_main.id,
            'product_id': self.product.id,
        })
        self.assertFalse(alert.sgi_folio)
        alert.action_sgi_escalate_to_nc()
        self.assertTrue(alert.sgi_folio, "Debe asignarse folio de NC.")
        self.assertEqual(alert.team_id, self.team_nc, "Debe moverse a NC Internas.")
        self.assertEqual(alert.sgi_origin_type, 'proceso')
        self.assertEqual(alert.product_id, self.product,
                         "Debe conservar el producto ligado.")

    def test_02_escalate_blocked_if_already_nc(self):
        alert = self.Alert.create({
            'title': 'NC ya existente',
            'team_id': self.team_nc.id,
        })
        # Al crearse en NC Internas ya trae folio; re-escalar debe fallar.
        self.assertTrue(alert.sgi_folio)
        with self.assertRaises(UserError):
            alert.action_sgi_escalate_to_nc()
