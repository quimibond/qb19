# -*- coding: utf-8 -*-
"""2.3.0: ficha de la máquina con configuraciones por fecha; la ficha de tejido toma la vigente a su fecha."""
from datetime import date

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestMaquinaConfig(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.wc = cls.env['mrp.workcenter'].create({'name': 'CIRCULAR 88 configuración prueba',
                                                   'ft_marca': 'Mayer', 'ft_modelo': 'Relanit'})
        Config = cls.env['ficha.tecnica.maquina.config']
        cls.c28 = Config.create({'workcenter_id': cls.wc.id, 'date_from': date(2025, 1, 1), 'galga': 28,
                                 'diametro': 30, 'no_agujas': 2640, 'no_alimentadores': 90, 'velocidad_max': 30,
                                 'motivo': 'Alta'})
        cls.c24 = Config.create({'workcenter_id': cls.wc.id, 'date_from': date(2026, 6, 1), 'galga': 24,
                                 'diametro': 34, 'no_agujas': 2556, 'no_alimentadores': 102, 'velocidad_max': 28,
                                 'motivo': 'Cambio de cilindro'})

    def _ficha(self, **vals):
        return self.env['ficha.tecnica.tejido'].create(dict({'articulo': 'WJ88CFG', 'maquina_tejido': self.wc.id},
                                                           **vals))

    def test_01_vigente_por_fecha(self):
        self.assertEqual(self.wc._ft_config_at(date(2025, 12, 31)), self.c28)
        self.assertEqual(self.wc._ft_config_at(date(2026, 6, 1)), self.c24)
        self.assertFalse(self.wc._ft_config_at(date(2024, 12, 31)), "Antes de la primera no hay configuración")
        self.assertEqual(self.c28.date_to, date(2026, 5, 31))
        self.assertFalse(self.c24.date_to)
        self.assertEqual(self.wc.ft_config_id, self.c24)
        self.assertEqual(self.wc.ft_galga, 24)

    def test_02_la_ficha_toma_la_configuracion_de_su_fecha(self):
        vieja = self._ficha(articulo='WJ88CFG-A', fecha_elaboracion=date(2025, 3, 1), galga=99)
        self.assertEqual(vieja.maquina_config_id, self.c28)
        self.assertEqual((vieja.galga, vieja.diametro, vieja.no_agujas, vieja.no_alimentadores),
                         (28, 30, 2640, 90), "Los datos de máquina salen de la configuración, no de la captura")
        self.assertEqual(vieja.marca_maquina, 'Mayer')
        nueva = self._ficha(articulo='WJ88CFG-B', fecha_elaboracion=date(2026, 7, 1))
        self.assertEqual(nueva.maquina_config_id, self.c24)
        self.assertEqual(nueva.galga, 24)
        self.env['ficha.tecnica.maquina.config'].create({
            'workcenter_id': self.wc.id, 'date_from': date(2026, 9, 1), 'galga': 32, 'diametro': 30})
        self.assertEqual(vieja.galga, 28, "Un cambio de cilindro posterior no altera la ficha")
        self.assertEqual(nueva.galga, 24)

    def test_03_configuracion_de_otra_maquina_y_borrado(self):
        otra = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 89 configuración prueba'})
        ficha = self._ficha(articulo='WJ88CFG-C', fecha_elaboracion=date(2026, 7, 1))
        with self.assertRaises(ValidationError):
            ficha.write({'maquina_tejido': otra.id, 'maquina_config_id': self.c24.id})
        with self.assertRaises(UserError, msg="Una configuración usada no se borra"):
            self.c24.unlink()

    def test_04_maquinas_compatibles_y_velocidad(self):
        self.assertIn(self.wc, self.env['mrp.workcenter'].search([('ft_galga', '=', 24)]))
        self.assertNotIn(self.wc, self.env['mrp.workcenter'].search([('ft_galga', '=', 28)]),
                         "Hoy corre galga 24")
        ficha = self._ficha(articulo='WJ88CFG-D', fecha_elaboracion=date(2026, 7, 1), velocidad=29)
        self.assertIn('supera la máxima', ficha.velocidad_aviso)
        ficha.velocidad = 25
        self.assertFalse(ficha.velocidad_aviso)
