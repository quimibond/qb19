# -*- coding: utf-8 -*-
"""2.4.0: la ficha de la máquina sirve para cualquier tipo de máquina (parámetros por tipo)."""
from datetime import date

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestMaquinaTipos(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.circular = cls.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tejido_circular')
        cls.devanadora = cls.env['ficha.tecnica.maquina.tipo'].create({
            'name': 'Devanadora prueba', 'code': 'devanadora_prueba',
            'parametro_ids': [(0, 0, {'sequence': 10, 'code': 'husos', 'name': 'Husos', 'kind': 'num'}),
                              (0, 0, {'sequence': 20, 'code': 'velocidad', 'name': 'Velocidad', 'unit': 'm/min',
                                      'kind': 'num'}),
                              (0, 0, {'sequence': 30, 'code': 'tipo_cono', 'name': 'Tipo de cono',
                                      'kind': 'text'})]})
        cls.wc = cls.env['mrp.workcenter'].create({'name': 'DEVANADORA 98 prueba', 'ft_tipo_id': cls.devanadora.id})
        cls.Config = cls.env['ficha.tecnica.maquina.config']

    def test_01_un_renglon_por_parametro_del_tipo(self):
        config = self.Config.create({'workcenter_id': self.wc.id, 'date_from': date(2026, 1, 1)})
        self.assertEqual(config.tipo_id, self.devanadora, "El tipo sale de la máquina")
        self.assertEqual(config.valor_ids.parametro_id.mapped('code'), ['husos', 'velocidad', 'tipo_cono'])
        config.valor_ids.filtered(lambda v: v.code == 'husos').valor = 12
        config.valor_ids.filtered(lambda v: v.code == 'velocidad').valor = 800
        config.valor_ids.filtered(lambda v: v.code == 'tipo_cono').valor_texto = 'Cónico 9°15'
        self.assertEqual(config._ft_value('husos'), 12)
        self.assertEqual(config._ft_value('tipo_cono'), 'Cónico 9°15')
        self.assertEqual(config.resumen, 'Husos: 12 · Velocidad: 800 m/min · Tipo de cono: Cónico 9°15')
        self.assertEqual(self.wc.ft_config_id, config)
        self.assertEqual(self.wc.ft_valor_ids, config.valor_ids)
        self.assertFalse(config.galga, "Una devanadora no tiene galga")

    def test_02_parametro_de_otro_tipo_y_columnas_de_circular(self):
        config = self.Config.create({'workcenter_id': self.wc.id, 'date_from': date(2026, 2, 1)})
        galga = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_param_circular_galga')
        with self.assertRaises(ValidationError):
            self.env['ficha.tecnica.maquina.valor'].create({'config_id': config.id, 'parametro_id': galga.id})
        with self.assertRaises(UserError, msg="El tipo devanadora no tiene galga"):
            config.galga = 24

    def test_03_circular_por_columnas_o_por_valores(self):
        wc = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 97 prueba'})
        config = self.Config.create({'workcenter_id': wc.id, 'tipo_id': self.circular.id,
                                     'date_from': date(2026, 3, 1), 'galga': 18, 'diametro': 32})
        self.assertEqual(wc.ft_tipo_id, self.circular, "La máquina sin tipo toma el de su primera configuración")
        self.assertEqual(len(config.valor_ids), 6, "Los seis parámetros de tejido circular (2.14.0: salida de la tela)")
        self.assertEqual(config._ft_value('galga'), 18)
        config.valor_ids.filtered(lambda v: v.code == 'no_alimentadores').valor = 96
        self.assertEqual(config.no_alimentadores, 96, "La columna sale del valor")
        self.assertEqual(wc.ft_no_alimentadores, 96)
        self.assertIn(wc, self.env['mrp.workcenter'].search([('ft_galga', '=', 18)]))

    def test_04_parametro_nuevo_y_borrado(self):
        config = self.Config.create({'workcenter_id': self.wc.id, 'date_from': date(2026, 4, 1)})
        config.valor_ids.filtered(lambda v: v.code == 'husos').valor = 10
        husos = self.devanadora.parametro_ids.filtered(lambda p: p.code == 'husos')
        with self.assertRaises(UserError, msg="Un parámetro con valores no se borra"):
            husos.unlink()
        nuevo = self.env['ficha.tecnica.maquina.parametro'].create({
            'tipo_id': self.devanadora.id, 'code': 'tension', 'name': 'Tensión', 'unit': 'cN'})
        otra = self.Config.create({'workcenter_id': self.wc.id, 'date_from': date(2026, 5, 1)})
        self.assertIn(nuevo, otra.valor_ids.parametro_id, "Las configuraciones nuevas traen el parámetro nuevo")
        vacia = self.devanadora.parametro_ids.filtered(lambda p: p.code == 'tipo_cono')
        vacia.unlink()
        self.assertNotIn('tipo_cono', otra.valor_ids.mapped('code'), "Sin valores, el parámetro se borra")
