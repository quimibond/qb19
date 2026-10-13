# -*- coding: utf-8 -*-
"""2.2.0: la máquina de la ficha de tejido es un centro de trabajo y las tolerancias son números."""
from odoo.tests import TransactionCase, tagged

from ..wizard._tabular_import_utils import parse_tolerance


@tagged('post_install', '-at_install')
class TestCampos220(TransactionCase):

    def test_01_tipos(self):
        Tejido = self.env['ficha.tecnica.tejido']
        self.assertEqual(Tejido._fields['maquina_tejido'].type, 'many2one')
        self.assertEqual(Tejido._fields['maquina_tejido'].comodel_name, 'mrp.workcenter')
        self.assertEqual(Tejido._fields['maquina_tejido_texto'].type, 'char')
        for name in ('tension_tol', 'punto_cilindro_tol', 'estiraje_tol', 'peso_acondicionado_tol',
                     'peso_acondicionado_tol_minus', 'columnas_tol', 'elongacion_carga_ancho_tol'):
            self.assertEqual(Tejido._fields[name].type, 'float', name)
        Acabado = self.env['ficha.tecnica.acabado']
        for name in ('peso_acabado_tol', 'peso_acabado_tol_minus', 'ancho_acabado_tol', 'espesor_acabado_tol'):
            self.assertEqual(Acabado._fields[name].type, 'float', name)

    def test_02_tolerancia_del_excel(self):
        self.assertEqual(parse_tolerance('0.5'), (0.5, 0.0))
        self.assertEqual(parse_tolerance('+12 / -6'), (12.0, 6.0))
        self.assertEqual(parse_tolerance(3), (3.0, 0.0))
        self.assertEqual(parse_tolerance(None), (0.0, 0.0))
        self.assertEqual(parse_tolerance('1,5'), (1.5, 0.0))

    def test_03_maquina_del_excel(self):
        wc = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 77 FT prueba'})
        Wizard = self.env['ficha.tecnica.tejido.import.wizard']
        self.assertEqual(Wizard._machine_values('circular 77 ft prueba'), {'maquina_tejido': wc.id})
        self.assertEqual(Wizard._machine_values('Máquina que no existe ZZ'),
                         {'maquina_tejido': False, 'maquina_tejido_texto': 'Máquina que no existe ZZ'})
        ficha = self.env['ficha.tecnica.tejido'].create({'articulo': 'WJ999FT220', 'maquina_tejido': wc.id,
                                                         'peso_acondicionado_tol': 12,
                                                         'peso_acondicionado_tol_minus': 6})
        self.assertEqual(ficha.maquina_tejido, wc)
