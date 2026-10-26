# -*- coding: utf-8 -*-
"""2.16.1: la longitud de malla y su tolerancia se muestran con 3 decimales (NN053Q66HNT098: 0.205 / 0.353 ± 0.002 cm).
Sin digits la ficha mostraba 0.21 / 0.35 / +/- 0.00."""
from odoo.tests import Form, TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestLongitudMalla(TransactionCase):

    def test_tres_decimales(self):
        Ficha = self.env['ficha.tecnica.tejido']
        for name in ('longitud_malla_polea1', 'longitud_malla_polea2', 'longitud_malla_tol'):
            self.assertEqual(Ficha._fields[name].get_digits(self.env), (16, 3), name)
        ficha = Ficha.create({'articulo': 'ZZ053Q66HNT098', 'longitud_malla_polea1': 0.205,
                              'longitud_malla_polea2': 0.353, 'longitud_malla_tol': 0.002})
        self.assertEqual((ficha.longitud_malla_polea1, ficha.longitud_malla_polea2, ficha.longitud_malla_tol),
                         (0.205, 0.353, 0.002))
        with Form(ficha) as form:  # el formulario no los redondea al guardar
            form.notas = 'revisión'
        self.assertEqual(ficha.longitud_malla_tol, 0.002)
