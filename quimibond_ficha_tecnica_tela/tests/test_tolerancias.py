# -*- coding: utf-8 -*-
"""2.29.0: ancho, espesor, encogimientos y elongaciones con tolerancia hacia arriba y hacia abajo, como el peso."""

from odoo.tests import TransactionCase, tagged

from ..wizard.ficha_tecnica_acabado_import_wizard import ASYM_TOL_FIELDS as ASYM_ACABADO
from ..wizard.ficha_tecnica_tejido_import_wizard import ASYM_TOL_FIELDS as ASYM_TEJIDO

ACABADO = ('peso_acabado_tol', 'ancho_acabado_tol', 'encogimiento_largo_tol', 'encogimiento_ancho_tol',
           'espesor_acabado_tol', 'elongacion_largo_acabado_tol', 'elongacion_ancho_acabado_tol')
TEJIDO = ('peso_acondicionado_tol', 'ancho_acondicionado_tol', 'espesor_acondicionado_tol',
          'elongacion_carga_largo_tol', 'elongacion_carga_ancho_tol')


@tagged('post_install', '-at_install')
class TestTolerancias(TransactionCase):

    def test_01_campos(self):
        for model, names in (('ficha.tecnica.acabado', ACABADO), ('ficha.tecnica.tejido', TEJIDO)):
            fields_ = self.env[model]._fields
            for name in names:
                self.assertEqual(fields_[name + '_minus'].type, 'float', name)
                self.assertNotEqual(fields_[name].string, fields_[name + '_minus'].string, name)
        espesor = self.env['ficha.tecnica.acabado']._fields['espesor_acabado_tol_minus']
        self.assertEqual(espesor.get_digits(self.env), (16, 3))
        self.assertEqual(set(ASYM_ACABADO), set(ACABADO))
        self.assertEqual(set(ASYM_TEJIDO), set(TEJIDO))

    def test_02_asimetrica(self):
        tejido = self.env['ficha.tecnica.tejido'].create({
            'articulo': 'WJ999TOL229', 'ancho_acondicionado': 150, 'ancho_acondicionado_tol': 5,
            'ancho_acondicionado_tol_minus': 0})
        self.assertEqual((tejido.ancho_acondicionado_tol, tejido.ancho_acondicionado_tol_minus), (5, 0))
