# -*- coding: utf-8 -*-
"""57.115.0: la pantalla que produce la evidencia no es «Pantalla que no va con su medición»."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestPantallaProduce(TransactionCase):

    def test_01_modelos_ligados(self):
        Activity = self.env['sgi.process.activity']
        self.assertTrue(Activity._sgi_screen_feeds_evidence('quality.alert', 'sgi.action.line'),
                        "La NC abre sus acciones (alert_id).")
        self.assertTrue(Activity._sgi_screen_feeds_evidence('sgi.management.review',
                                                            'sgi.management.review.agreement'))
        self.assertTrue(Activity._sgi_screen_feeds_evidence('stock.picking.type', 'stock.picking'))

    def test_02_pares_sin_campo(self):
        Activity = self.env['sgi.process.activity']
        self.assertTrue(Activity._sgi_screen_feeds_evidence('stock.quant', 'stock.move'),
                        "Aplicar el inventario físico crea los movimientos.")
        self.assertFalse(Activity._sgi_screen_feeds_evidence('quality.check', 'project.task'),
                         "Un control de calidad no produce una tarea: el aviso sigue.")
