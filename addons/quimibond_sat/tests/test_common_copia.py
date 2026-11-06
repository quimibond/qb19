# -*- coding: utf-8 -*-
"""La base de las pruebas tiene que poder armarse sobre datos que ya existen.

Odoo.sh corre estas pruebas en una copia de producción. Ahí el USD del
2026-01-01 ya tiene tipo de cambio y los RFC de prueba ya son de contactos
reales. En una base nueva no hay ni lo uno ni lo otro: los dos caminos
pasan por los mismos helpers.
"""
from odoo.tests import tagged

from .common import RFC_PROVEEDOR, SatCommon


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatCommonCopia(SatCommon):

    def test_repetir_la_base_no_choca_con_lo_que_ya_existe(self):
        self._tipo_de_cambio_de_prueba()
        rates = self.env['res.currency.rate'].search([
            ('currency_id', '=', self.usd.id),
            ('company_id', '=', self.company.id),
            ('name', '=', '2026-01-01'),
        ])
        self.assertEqual(len(rates), 1)
        self.assertAlmostEqual(rates.rate, 1 / 20.0, places=6)

        previo = self.proveedor
        nuevo = self._partner_con_rfc('PAPELERA SANDOVAL', RFC_PROVEEDOR)
        self.assertEqual(nuevo.vat, RFC_PROVEEDOR)
        self.assertFalse(previo.vat)
        self.assertEqual(
            self.env['res.partner'].search_count([('vat', '=ilike', RFC_PROVEEDOR)]), 1)
