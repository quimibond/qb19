# -*- coding: utf-8 -*-
"""2.5.0: aviso de galga al programar tejido y cargas por jet en tintorería."""
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestAvisosMaquina(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.circular = cls.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tejido_circular')
        cls.tintoreria = cls.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tintoreria')
        Config = cls.env['ficha.tecnica.maquina.config']
        cls.wc = cls.env['mrp.workcenter'].create({'name': 'CIRCULAR 96 aviso prueba', 'ft_tipo_id': cls.circular.id})
        Config.create({'workcenter_id': cls.wc.id, 'date_from': date(2020, 1, 1), 'galga': 24, 'diametro': 30})
        Param = cls.env['ficha.tecnica.maquina.parametro']
        params = {}
        for seq, code, name, kind in ((10, 'capacidad', 'Capacidad', 'num'),
                                      (20, 'capacidad_grupo_a', 'Grupo A', 'num'),
                                      (30, 'capacidad_grupo_b', 'Grupo B', 'num'),
                                      (40, 'capacidad_grupo_c', 'Grupo C', 'num')):
            params[code] = Param.search([('tipo_id', '=', cls.tintoreria.id), ('code', '=', code)], limit=1) or \
                Param.create({'tipo_id': cls.tintoreria.id, 'sequence': seq, 'code': code, 'name': name,
                              'unit': 'kg', 'kind': kind})
        cls.jet = cls.env['mrp.workcenter'].create({'name': 'JET 96 prueba', 'ft_tipo_id': cls.tintoreria.id})
        values = {'capacidad': 1000, 'capacidad_grupo_a': 900, 'capacidad_grupo_b': 600, 'capacidad_grupo_c': 450}
        Config.create({'workcenter_id': cls.jet.id, 'date_from': date(2020, 1, 1), 'valor_ids': [
            (0, 0, {'parametro_id': params[code].id, 'valor': value}) for code, value in values.items()]})
        cls.component = cls.env['product.product'].create({'name': 'Hilo prueba aviso', 'uom_id': cls.kg.id})
        cls.crudo = cls.env['product.product'].create({'name': 'Crudo prueba aviso', 'default_code': 'XJ130Q21HNT096',
                                                      'is_storable': True, 'uom_id': cls.kg.id})
        cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.component.id, 'product_qty': 1})],
            'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': cls.wc.id, 'time_cycle_manual': 1})]})
        cls.en_proceso = cls.env['product.product'].create({
            'name': 'En proceso prueba aviso', 'default_code': 'WJ035Q22INT200', 'is_storable': True,
            'uom_id': cls.kg.id})
        cls.picking_type = cls.env['stock.picking.type'].search(
            [('code', '=', 'mrp_operation'), ('company_id', '=', cls.env.company.id)], limit=1)
        cls.picking_type.ft_cargas_jet = True

    def _tejido(self):
        production = self.env['mrp.production'].create({'product_id': self.crudo.id, 'product_qty': 100,
                                                        'product_uom_id': self.kg.id})
        production.action_confirm()
        return production

    def test_01_galga_por_codigo_del_articulo(self):
        production = self._tejido()
        workorder = production.workorder_ids
        self.assertEqual(workorder.workcenter_id, self.wc)
        self.assertIn('galga 24', workorder.ft_aviso_maquina)
        self.assertIn('pide galga 18', workorder.ft_aviso_maquina, "XJ130Q21: posiciones 7 y 8 = 21 → galga 18")
        self.assertIn(workorder, self.env['mrp.workorder'].search([('ft_aviso_maquina', '!=', False)]))
        self.assertIn('galga 18', production.ft_aviso_maquina, "La orden de producción repite el aviso")

    def test_02_la_ficha_manda_sobre_el_codigo(self):
        self.env['ficha.tecnica.tejido'].create({'articulo': 'XJ130Q21HNT096', 'galga_requerida': 24})
        workorder = self._tejido().workorder_ids
        self.assertFalse(workorder.ft_aviso_maquina, "La ficha pide galga 24, la que corre la máquina")
        self.assertNotIn(workorder, self.env['mrp.workorder'].search([('ft_aviso_maquina', '!=', False)]))

    def test_03_cargas_por_jet_con_capacidad_general(self):
        production = self.env['mrp.production'].create({
            'product_id': self.en_proceso.id, 'product_qty': 2000, 'product_uom_id': self.kg.id,
            'picking_type_id': self.picking_type.id})
        self.assertTrue(production.ft_es_tintoreria)
        self.assertFalse(production.ft_rendimiento)
        self.assertIn('JET 96 prueba', production.ft_cargas_html)
        self.assertIn('capacidad general', production.ft_cargas_html)
        self.assertFalse(production.ft_aviso_maquina, "Sin jet elegido no hay aviso")
        production.ft_jet_id = self.jet
        self.assertIn('son 2 cargas', production.ft_aviso_maquina)

    def test_04_cargas_por_grupo_de_rendimiento(self):
        self.env['ficha.tecnica.tejido'].create({'articulo': 'WJ035Q22INT200', 'peso_acondicionado': 42,
                                                 'ancho_acondicionado': 160, 'ancho_acondicionado_tol_unit': 'cm'})
        production = self.env['mrp.production'].create({
            'product_id': self.en_proceso.id, 'product_qty': 2000, 'product_uom_id': self.kg.id,
            'picking_type_id': self.picking_type.id, 'ft_jet_id': self.jet.id})
        self.assertAlmostEqual(production.ft_rendimiento, 14.88, places=2)
        self.assertIn('grupo C', production.ft_aviso_maquina)
        self.assertIn('son 5 cargas', production.ft_aviso_maquina, "2000 kg entre 450 kg del grupo C")
        production.product_qty = 400
        self.assertFalse(production.ft_aviso_maquina, "400 kg caben en una carga")

    # 2.6.0: producto único de acabado en kg con el jet como orden de trabajo de su ruta.
    def _acabado(self, code, qty):
        acabado = self.env['product.product'].create({'name': 'Acabado prueba %s' % code, 'default_code': code,
                                                      'is_storable': True, 'uom_id': self.kg.id})
        self.env['mrp.bom'].create({
            'product_tmpl_id': acabado.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': self.kg.id,
            'bom_line_ids': [(0, 0, {'product_id': self.component.id, 'product_qty': 1})],
            'operation_ids': [(0, 0, {'name': 'Teñir', 'workcenter_id': self.jet.id, 'time_cycle_manual': 1})]})
        production = self.env['mrp.production'].create({'product_id': acabado.id, 'product_qty': qty,
                                                        'product_uom_id': self.kg.id})
        production.action_confirm()
        return acabado, production

    def test_05_aviso_de_carga_en_la_orden_de_trabajo_del_jet(self):
        _acabado, production = self._acabado('WJ042Q22JNT160', 2000)
        workorder = production.workorder_ids
        self.assertEqual(workorder.workcenter_id, self.jet)
        self.assertIn('son 2 cargas', workorder.ft_aviso_maquina, "2000 kg y 1000 kg de capacidad general")
        self.assertIn(workorder, self.env['mrp.workorder'].search([('ft_aviso_maquina', '!=', False)]))
        self.assertTrue(production.ft_es_tintoreria, "Una orden con orden de trabajo en jet trae las cargas")
        self.assertEqual(production.ft_jet_wo_id, self.jet)
        self.assertIn('son 2 cargas', production.ft_aviso_maquina)
        _chico, cabe = self._acabado('WJ042Q22JNT162', 800)
        self.assertFalse(cabe.workorder_ids.ft_aviso_maquina, "800 kg caben en una carga")

    def test_06_rendimiento_por_la_ficha_de_acabado(self):
        tejido = self.env['ficha.tecnica.tejido'].create({
            'articulo': 'WJ035Q22HNT200', 'peso_acondicionado': 42, 'ancho_acondicionado': 160,
            'ancho_acondicionado_tol_unit': 'cm'})
        acabado, production = self._acabado('WJ042Q22JNT161', 2000)
        self.env['ficha.tecnica.acabado'].create({'articulo': 'WJ042Q22JNT161', 'tejido_id': tejido.id,
                                                  'product_acabado_id': acabado.id})
        rendimiento, origen = acabado._ft_rendimiento()
        self.assertAlmostEqual(rendimiento, 14.88, places=2)
        self.assertIn('base de la ficha de acabado', origen)
        self.assertIn('grupo C', production.workorder_ids.ft_aviso_maquina)
        self.assertIn('son 5 cargas', production.workorder_ids.ft_aviso_maquina)
