# -*- coding: utf-8 -*-
"""2.20.0: alternativas nativas armadas por tipo y estado, filtradas al planear con los procesos de la ficha."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestAlternativas(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.tipo = cls.env['ficha.tecnica.maquina.tipo'].create({
            'name': 'Rama alternativas prueba', 'code': 'rama_alt_prueba',
            'parametro_ids': [(0, 0, {'code': 'velocidad', 'name': 'Velocidad', 'unit': 'm/min', 'kind': 'num',
                                      'ambito': 'articulo'})]})
        Workcenter = cls.env['mrp.workcenter']
        cls.r1 = Workcenter.create({'name': 'RAMA 81 prueba', 'code': 'R81', 'ft_tipo_id': cls.tipo.id})
        cls.r2 = Workcenter.create({'name': 'RAMA 82 prueba', 'code': 'R82', 'ft_tipo_id': cls.tipo.id})
        cls.r3 = Workcenter.create({'name': 'RAMA 83 prueba', 'code': 'R83', 'ft_tipo_id': cls.tipo.id})
        cls.tela = cls.env['product.product'].create({'name': 'Tela alternativas prueba',
                                                      'default_code': 'IZZ081Q21JNT165', 'is_storable': True,
                                                      'uom_id': cls.kg.id})
        cls.crudo = cls.env['product.product'].create({'name': 'Crudo alternativas prueba', 'uom_id': cls.kg.id})
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 1.1})],
            'operation_ids': [(0, 0, {'name': 'Rama', 'workcenter_id': cls.r1.id, 'time_mode': 'manual',
                                      'time_cycle_manual': 0.3})]})
        cls.Proceso = cls.env['ficha.tecnica.proceso']

    def _orden(self):
        production = self.env['mrp.production'].create({'product_id': self.tela.id, 'product_qty': 100,
                                                        'product_uom_id': self.kg.id, 'bom_id': self.bom.id})
        production.action_confirm()
        return production

    def test_01_sin_marca_no_toca_las_alternativas(self):
        self.assertFalse(self.r1.alternative_workcenter_ids, "Un tipo sin «Alternativas entre sí» no las arma")
        self.r1.alternative_workcenter_ids = self.r3
        self.r2.ft_estado = 'montaje'
        self.assertEqual(self.r1.alternative_workcenter_ids, self.r3, "Las de a mano se quedan")

    def test_02_alternativas_por_tipo_y_estado(self):
        self.tipo.alternativas_por_tipo = True
        self.assertEqual(self.r1.alternative_workcenter_ids, self.r2 | self.r3)
        self.assertTrue(self.r1.ft_alternativas_auto)
        self.r3.ft_estado = 'montaje'
        self.assertEqual(self.r1.alternative_workcenter_ids, self.r2, "En montaje deja de ser alternativa")
        self.assertEqual(self.r3.alternative_workcenter_ids, self.r1 | self.r2,
                         "La máquina en montaje conserva las suyas (no se planea en ella por su cuenta)")
        self.r3.ft_estado = 'operando'
        self.assertEqual(self.r1.alternative_workcenter_ids, self.r2 | self.r3, "Vuelve al operar")
        r4 = self.env['mrp.workcenter'].create({'name': 'RAMA 84 prueba', 'code': 'R84', 'ft_tipo_id': self.tipo.id})
        self.assertIn(r4, self.r1.alternative_workcenter_ids, "Una máquina nueva del tipo entra sola")
        r4.active = False
        self.assertNotIn(r4, self.r1.alternative_workcenter_ids, "Archivada sale")

    def test_03_maquinas_posibles_en_la_operacion(self):
        self.tipo.alternativas_por_tipo = True
        self.r3.ft_estado = 'pruebas'
        self.Proceso.create({'articulo': 'IZZ081Q21JNT165', 'workcenter_id': self.r1.id})
        texto = self.bom.operation_ids.ft_maquinas_posibles
        self.assertIn('✓ RAMA 81 prueba', texto)
        self.assertIn('✗ RAMA 82 prueba (sin proceso del artículo)', texto)
        self.assertNotIn('RAMA 83', texto, "En pruebas ya no es alternativa de la 81")
        self.Proceso.create({'articulo': 'IZZ081Q21JNT165', 'workcenter_id': self.r3.id})
        self.bom.operation_ids.invalidate_recordset(['ft_maquinas_posibles'])
        self.assertIn('○ RAMA 83 prueba (corre ahí; en pruebas)', self.bom.operation_ids.ft_maquinas_posibles)
        self.r2.ft_estado = 'montaje'
        r4 = self.env['mrp.workcenter'].create({'name': 'RAMA 84 prueba', 'code': 'R84', 'ft_tipo_id': self.tipo.id})
        self.r1.with_context(ft_sync_alternativas=True).alternative_workcenter_ids = self.r2
        self.Proceso.create({'articulo': 'IZZ081Q21JNT165', 'workcenter_id': r4.id})
        self.bom.operation_ids.invalidate_recordset(['ft_maquinas_posibles'])
        texto = self.bom.operation_ids.ft_maquinas_posibles
        self.assertIn('⚠ RAMA 82 prueba (en montaje)', texto)
        self.assertIn('○ RAMA 84 prueba (corre ahí y no es alternativa)', texto)

    def test_04_planea_solo_donde_hay_proceso(self):
        self.tipo.alternativas_por_tipo = True
        self.Proceso.create({'articulo': 'IZZ081Q21JNT165', 'workcenter_id': self.r2.id})
        production = self._orden()
        workorder = production.workorder_ids
        self.assertEqual(workorder.workcenter_id, self.r1)
        self.assertIn('RAMA 81 prueba no tiene proceso', workorder.ft_aviso_maquina)
        production.button_plan()
        self.assertEqual(workorder.workcenter_id, self.r2,
                         "La 81 (la de la operación) y la 83 no tienen proceso: Odoo la planea en la 82")
        self.assertFalse(workorder.ft_aviso_maquina)

    def test_05_sin_ningun_proceso_planea_como_siempre(self):
        self.tipo.alternativas_por_tipo = True
        production = self._orden()
        production.button_plan()
        self.assertEqual(production.workorder_ids.workcenter_id, self.r1,
                         "Ninguna tiene proceso: se planea como Odoo (faltar un dato no impide planear)")
        self.assertIn('no tiene proceso', production.workorder_ids.ft_aviso_maquina)
        self.assertIn(production.workorder_ids,
                      self.env['mrp.workorder'].search([('ft_aviso_maquina', '!=', False)]))

    def test_06_maquina_que_no_opera(self):
        self.tipo.alternativas_por_tipo = True
        for wc in self.r1 | self.r2 | self.r3:
            self.Proceso.create({'articulo': 'IZZ081Q21JNT165', 'workcenter_id': wc.id})
        self.r1.ft_estado = 'fuera'
        production = self._orden()
        self.assertIn('RAMA 81 prueba está fuera de servicio', production.workorder_ids.ft_aviso_maquina)
        production.button_plan()
        self.assertIn(production.workorder_ids.workcenter_id, self.r2 | self.r3,
                      "La máquina de la operación está fuera de servicio: se planea en una que opera")

    def test_07_maquina_fija_no_se_mueve(self):
        """2.21.0: con la máquina fijada a mano el planificador no la cambia, aunque no tenga proceso ni opere."""
        self.tipo.alternativas_por_tipo = True
        self.Proceso.create({'articulo': 'IZZ081Q21JNT165', 'workcenter_id': self.r2.id})
        self.r1.ft_estado = 'montaje'
        production = self._orden()
        workorder = production.workorder_ids
        production.with_context(ft_maquina_fija=True).button_plan()
        self.assertEqual(workorder.workcenter_id, self.r1, "Sin la clave se iría a la 82, la única con proceso")
        self.assertIn('RAMA 81 prueba está en montaje', workorder.ft_aviso_maquina, "Queda el aviso")
