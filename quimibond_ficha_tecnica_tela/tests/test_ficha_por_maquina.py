# -*- coding: utf-8 -*-
"""2.7.0: una ficha de tejido por artículo, revisión y máquina (los formatos de la planta llevan una hoja por
máquina); el importador empata por máquina y liga el número de la máquina con su código."""
import psycopg2

from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger


@tagged('post_install', '-at_install')
class TestFichaPorMaquina(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        circular = cls.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tejido_circular')
        tintoreria = cls.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tintoreria')
        Workcenter = cls.env['mrp.workcenter']
        cls.c91 = Workcenter.create({'name': 'CIRCULAR 91 prueba', 'code': 'P91', 'ft_tipo_id': circular.id})
        cls.c92 = Workcenter.create({'name': 'CIRCULAR 92 prueba', 'code': 'P92', 'ft_tipo_id': circular.id})
        cls.jet = Workcenter.create({'name': 'JET 91 prueba', 'code': 'P91', 'ft_tipo_id': tintoreria.id})
        cls.Ficha = cls.env['ficha.tecnica.tejido']

    def test_01_una_por_maquina(self):
        f91 = self.Ficha.create({'articulo': 'WJ091PRUEBA', 'maquina_tejido': self.c91.id})
        f92 = self.Ficha.create({'articulo': 'WJ091PRUEBA', 'maquina_tejido': self.c92.id})
        self.assertEqual(f91.display_name, 'WJ091PRUEBA · CIRCULAR 91 prueba')
        self.assertNotEqual(f91, f92)
        with mute_logger('odoo.sql_db'), self.assertRaises(psycopg2.IntegrityError), self.cr.savepoint():
            self.Ficha.create({'articulo': 'WJ091PRUEBA', 'maquina_tejido': self.c91.id})

    def test_02_importador_por_maquina(self):
        wizard = self.env['ficha.tecnica.tejido.import.wizard'].new({})
        self.assertEqual(wizard._machine_values('P92'), {'maquina_tejido': self.c92.id})
        self.assertEqual(wizard._machine_values('P91'), {'maquina_tejido': self.c91.id},
                         "El mismo código en un jet: se queda la circular")
        self.assertEqual(wizard._machine_values('NO EXISTE 91'),
                         {'maquina_tejido': False, 'maquina_tejido_texto': 'NO EXISTE 91'})
