# -*- coding: utf-8 -*-
"""57.158.0 (C1): el checklist de factibilidad con la ficha de la máquina (circulares por galga, jets por
rendimiento, ramas por ancho, producción real) y lo que contesta una persona no se vuelve a tocar."""
from datetime import date
from unittest.mock import patch

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevMachineSheet(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.circular = cls.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tejido_circular')
        cls.tintoreria = cls.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tintoreria')
        Config = cls.env['ficha.tecnica.maquina.config']
        cls.wc = cls.env['mrp.workcenter'].create({'name': 'CIRCULAR 95 factibilidad prueba',
                                                   'ft_tipo_id': cls.circular.id})
        Config.create({'workcenter_id': cls.wc.id, 'date_from': date(2020, 1, 1), 'galga': 17,
                       'diametro': 32, 'no_alimentadores': 96})
        Param = cls.env['ficha.tecnica.maquina.parametro']
        params = {}
        for seq, code in ((10, 'capacidad'), (40, 'capacidad_grupo_c')):
            params[code] = Param.search([('tipo_id', '=', cls.tintoreria.id), ('code', '=', code)], limit=1) or \
                Param.create({'tipo_id': cls.tintoreria.id, 'sequence': seq, 'code': code, 'name': code,
                              'unit': 'kg'})
        cls.jet = cls.env['mrp.workcenter'].create({'name': 'JET 95 factibilidad prueba',
                                                    'ft_tipo_id': cls.tintoreria.id})
        Config.create({'workcenter_id': cls.jet.id, 'date_from': date(2020, 1, 1), 'valor_ids': [
            (0, 0, {'parametro_id': params['capacidad'].id, 'valor': 1000}),
            (0, 0, {'parametro_id': params['capacidad_grupo_c'].id, 'valor': 450})]})
        partner = cls.env['res.partner'].create({'name': 'CLIENTE FACTIBILIDAD PRUEBA', 'is_company': True})
        cls.project = cls.env['project.project'].create({
            'name': 'x', 'sgi_is_ft': True, 'partner_id': partner.id, 'sgi_dev_product_name': 'Jersey factibilidad'})
        cls.target = {'peso': 42, 'ancho': 160, 'galga': 17, 'comp': '', 'dib': '',
                      'peso_lim': (None, None), 'ancho_lim': (None, None)}

    def _patch(self, **changes):
        return patch.object(type(self.project), '_sgi_dev_target', return_value=dict(self.target, **changes))

    def test_01_circulares_por_galga(self):
        with self._patch():
            answer, obs = self.project._sgi_dev_auto_maquina_circular()
        self.assertEqual(answer, 'si')
        self.assertIn('CIRCULAR 95 factibilidad prueba (32", 96 alim.)', obs)
        with self._patch(galga=99):
            answer, obs = self.project._sgi_dev_auto_maquina_circular()
        self.assertEqual(answer, 'no', "Ninguna circular corre galga 99")
        with self._patch(galga=0):
            self.assertEqual(self.project._sgi_dev_auto_maquina_circular()[0], 'pendiente')

    def test_02_jets_por_rendimiento(self):
        with self._patch():
            answer, obs = self.project._sgi_dev_auto_tintoreria()
        self.assertEqual(answer, 'pendiente', "Receta y colorantes los confirma Tintorería")
        self.assertIn('Rendimiento 14.88 m/kg', obs)
        self.assertIn('JET 95 factibilidad prueba 450 kg', obs, "Grupo C: 1000 / (42 × 1.6) = 14.88")

    def test_03_lo_que_contesta_una_persona_no_se_toca(self):
        item = self.env['sgi.dev.feasibility.item'].create({
            'line': 'tejido_circular', 'name': 'Máquina circular prueba', 'auto': 'maquina_circular'})
        self.project.sgi_dev_line_key = 'tejido_circular'
        with self._patch():
            self.project.action_sgi_dev_load_feasibility()
            line = self.project.sgi_dev_feasibility_ids.filtered(lambda l: l.item_id == item)
            self.assertEqual(line.answer, 'si')
            self.assertFalse(line.answered_by_id, "La contestó Odoo")
            line.answer = 'no'
            self.assertTrue(line.answered_by_id)
            self.project.action_sgi_dev_load_feasibility()
        self.assertEqual(line.answer, 'no', "Lo que contestó una persona no se vuelve a tocar")

    def test_04_capacidad_con_produccion_real(self):
        with self._patch():
            answer, obs = self.project._sgi_dev_auto_capacidad()
            galga, circulares = self.project._sgi_dev_circulares()
            rows = self.project._sgi_dev_capacity_machines(self.project._sgi_dev_start_approval_labels())
        self.assertEqual(answer, 'pendiente', "Lo disponible lo confirma Planeación")
        self.assertIn('galga 17', obs)
        self.assertIn(self.wc, circulares)
        real = sum(self.project._sgi_dev_produccion_real(circulares).values())
        self.assertEqual(bool(rows), bool(real), "La fila de circulares sale solo con producción real en kg")

    def test_05_circulares_de_las_fichas_del_crudo(self):
        """57.196.0: con fichas de tejido del crudo, solo sus circulares; otra de la misma galga sin ficha
        (una tubular con un crudo que pide tela abierta) no entra."""
        Config = self.env['ficha.tecnica.maquina.config']
        tubular = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 96 tubular prueba',
                                                     'ft_tipo_id': self.circular.id})
        Config.create({'workcenter_id': tubular.id, 'date_from': date(2020, 1, 1), 'galga': 17, 'diametro': 32})
        crudo = self.env['product.product'].create({'name': 'Crudo factibilidad', 'default_code': 'WJ035Q17HNT299'})
        self.project.sgi_dev_tejido_id = crudo
        with self._patch():
            galga, circulares = self.project._sgi_dev_circulares()
        self.assertIn(tubular, circulares, "Sin fichas del crudo: todas las de la galga")
        self.env['ficha.tecnica.tejido'].create({'articulo': crudo.default_code, 'maquina_tejido': self.wc.id})
        with self._patch():
            galga, circulares = self.project._sgi_dev_circulares()
            answer, obs = self.project._sgi_dev_auto_maquina_circular()
        self.assertEqual(circulares, self.wc, "Con fichas del crudo: solo sus circulares")
        self.assertEqual(answer, 'si')
        self.assertIn('ficha de tejido del crudo WJ035Q17HNT299', obs)
        self.assertNotIn('CIRCULAR 96', obs)
        with self._patch(galga=99):
            self.assertEqual(self.project._sgi_dev_auto_maquina_circular()[0], 'no',
                             "Ninguna circular de las fichas ni de la planta corre galga 99")
