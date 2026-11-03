# -*- coding: utf-8 -*-
"""2.23.0: tiempos de inspección, devanado, abridora y tintorería desde la ficha; capacidad de los jets y conversión
kg → m vigiladas contra la ficha. Códigos ZZ931 que no existen en producción (las pruebas corren también sobre una
copia de producción)."""
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.tools import float_round


@tagged('post_install', '-at_install')
class TestTiempoFicha(TransactionCase):

    @classmethod
    def _param(cls, tipo, code, unit, ambito, kind='num'):
        Param = cls.env['ficha.tecnica.maquina.parametro']
        return Param.search([('tipo_id', '=', tipo.id), ('code', '=', code)], limit=1) or Param.create({
            'tipo_id': tipo.id, 'code': code, 'name': '%s (prueba 2.23.0)' % code, 'unit': unit, 'kind': kind,
            'ambito': ambito})

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.meter = cls.env.ref('uom.product_uom_meter')
        ref = lambda xmlid: cls.env.ref('quimibond_ficha_tecnica_tela.%s' % xmlid)  # noqa: E731
        inspeccion, abridora = ref('ft_maquina_tipo_inspeccion'), ref('ft_maquina_tipo_abridora')
        devanadora, tintoreria = ref('ft_maquina_tipo_devanadora'), ref('ft_maquina_tipo_tintoreria')
        cls._param(inspeccion, 'velocidad_inspeccion', 'm/h', 'articulo')
        for code in ('velocidad', 'velocidad_tubular', 'velocidad_abierta'):
            cls._param(abridora, code, 'm/min', 'articulo')
        cls._param(devanadora, 'velocidad', 'm/min', 'articulo')
        cls._param(tintoreria, 'tiempo_ciclo', 'h', 'articulo')
        cls._param(tintoreria, 'kg_carga', 'kg', 'articulo')
        maquina = {code: cls._param(tintoreria, code, 'kg', 'maquina')
                   for code in ('capacidad', 'capacidad_grupo_a', 'capacidad_grupo_b', 'capacidad_grupo_c')}

        Workcenter = cls.env['mrp.workcenter']
        cls.insp = Workcenter.create({'name': 'INSP 931 prueba', 'code': 'I931', 'ft_tipo_id': inspeccion.id})
        cls.abr = Workcenter.create({'name': 'ABR 931 prueba', 'code': 'A931', 'ft_tipo_id': abridora.id})
        cls.dev = Workcenter.create({'name': 'DEV 931 prueba', 'code': 'D931', 'ft_tipo_id': devanadora.id})
        cls.jet = Workcenter.create({'name': 'JET 931 prueba', 'code': 'J931', 'ft_tipo_id': tintoreria.id})
        cls.jet2 = Workcenter.create({'name': 'JET 932 prueba', 'code': 'J932', 'ft_tipo_id': tintoreria.id})
        cls.jet.alternative_workcenter_ids = cls.jet2
        Config = cls.env['ficha.tecnica.maquina.config']
        valores = {'capacidad': 1000, 'capacidad_grupo_a': 900, 'capacidad_grupo_b': 600, 'capacidad_grupo_c': 450}
        for jet in (cls.jet, cls.jet2):
            Config.create({'workcenter_id': jet.id, 'date_from': date(2020, 1, 1), 'valor_ids': [
                (0, 0, {'parametro_id': maquina[code].id, 'valor': valor}) for code, valor in valores.items()]})

        Product = cls.env['product.product']
        cls.tela = Product.create({'name': 'Tela 931 prueba (kg)', 'default_code': 'IZZ931Q22JNT165',
                                   'is_storable': True, 'uom_id': cls.kg.id})
        cls.tela_m = Product.create({'name': 'Tela 931 prueba (m)', 'default_code': 'ZZ931Q22JNT165',
                                     'is_storable': True, 'uom_id': cls.meter.id})
        cls.crudo = Product.create({'name': 'Crudo 931 prueba', 'default_code': 'ZZ931Q22HNT200',
                                    'is_storable': True, 'uom_id': cls.kg.id})
        cls.tejido = cls.env['ficha.tecnica.tejido'].create({
            'articulo': 'ZZ931Q22HNT200', 'product_proceso_id': cls.crudo.id, 'salida_tela_ficha': 'Tubular'})
        cls.tejido.rendimiento_tela_tejida = 10.0  # banda B (7 a 11 m/kg)
        cls.acabado = cls.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ931Q22JNT165', 'tejido_id': cls.tejido.id, 'product_acabado_id': cls.tela_m.id,
            'product_proceso_id': cls.tela.id, 'peso_acabado': 53, 'rendimiento_tela_acabada': 8.0})

        def op(name, seq, wc):
            return (0, 0, {'name': name, 'sequence': seq, 'workcenter_id': wc.id, 'time_mode': 'manual',
                           'time_cycle_manual': 0.1})
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 1.1})],
            'operation_ids': [op('Devanado', 10, cls.dev), op('Tintorería', 20, cls.jet), op('Abridora', 30, cls.abr),
                              op('Inspección', 40, cls.insp)]})
        por_wc = {o.workcenter_id: o for o in cls.bom.operation_ids}
        cls.op_dev, cls.op_tint, cls.op_abr, cls.op_insp = (por_wc[w] for w in (cls.dev, cls.jet, cls.abr, cls.insp))
        cls.bom_m = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.tela_m.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.meter.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.tela.id, 'product_qty': 0.2})]})

        Proceso = cls.env['ficha.tecnica.proceso']
        cls.p_insp = Proceso.create({'articulo': 'IZZ931Q22JNT165', 'workcenter_id': cls.insp.id})
        cls.p_insp._ft_set_value('velocidad_inspeccion', 600)
        cls.p_dev = Proceso.create({'articulo': 'IZZ931Q22JNT165', 'workcenter_id': cls.dev.id})
        cls.p_dev._ft_set_value('velocidad', 50)
        cls.p_abr = Proceso.create({'articulo': 'IZZ931Q22JNT165', 'workcenter_id': cls.abr.id})
        cls.p_abr._ft_set_value('velocidad', 40)
        cls.p_abr._ft_set_value('velocidad_tubular', 25)
        cls.p_jet = Proceso.create({'articulo': 'IZZ931Q22JNT165', 'workcenter_id': cls.jet.id})
        cls.p_jet._ft_set_value('tiempo_ciclo', 4)
        Proceso.create({'articulo': 'IZZ931Q22JNT165', 'workcenter_id': cls.jet2.id})._ft_set_value('tiempo_ciclo', 3)

    def test_01_inspeccion(self):
        self.assertAlmostEqual(self.op_insp.ft_tiempo_ficha, 0.8, places=4, msg="8 m/kg × 60 ÷ 600 m/h")
        self.assertIn('600 m/h', self.op_insp.ft_tiempo_calculo)
        self.assertTrue(self.op_insp.ft_tiempo_difiere)
        Op = self.env['mrp.routing.workcenter']
        self.assertIn(self.op_insp, Op.search([('ft_tiempo_difiere', '=', True)]), "El filtro de Operaciones la encuentra")
        self.op_insp.action_ft_usar_tiempo_ficha()
        self.env.invalidate_all()
        self.assertNotIn(self.op_insp, Op.search([('ft_tiempo_difiere', '=', True)]))
        self.p_insp._ft_set_value('velocidad_inspeccion', 0)
        self.env.invalidate_all()  # el tiempo de la ficha no se guarda: se vuelve a calcular al leer
        self.assertFalse(self.op_insp.ft_tiempo_ficha)
        self.assertIn('velocidad de inspección', self.op_insp.ft_tiempo_calculo)

    def test_02_devanado_y_abridora(self):
        self.assertAlmostEqual(self.op_dev.ft_tiempo_ficha, 0.22, places=4, msg="1.1 kg × 10 m/kg ÷ 50 m/min")
        self.assertIn('1.1 kg de crudo', self.op_dev.ft_tiempo_calculo)
        self.assertAlmostEqual(self.op_abr.ft_tiempo_ficha, 0.44, places=4, msg="Tela tubular: 11 m ÷ 25 m/min")
        self.assertIn('tela tubular', self.op_abr.ft_tiempo_calculo)
        self.p_abr._ft_set_value('velocidad_tubular', 0)
        self.env.invalidate_all()
        self.assertAlmostEqual(self.op_abr.ft_tiempo_ficha, 0.275, places=4, msg="Sin la tubular, la general")
        self.tejido.rendimiento_tela_tejida = 0.0
        self.env.invalidate_all()
        self.assertFalse(self.op_dev.ft_tiempo_ficha)
        self.assertIn('rendimiento', self.op_dev.ft_tiempo_calculo)

    def test_03_tintoreria_por_bano_pide_capacidad(self):
        self.assertFalse(self.op_tint.ft_tiempo_ficha, "Sin capacidad del jet para el producto un ciclo no es un baño")
        self.assertIn('falta la capacidad', self.op_tint.ft_tiempo_calculo)
        capacidad, motivo = self.jet._ft_capacidad_jet(self.tela)
        self.assertEqual(capacidad, 600.0, "Banda B (rendimiento 10 m/kg) del jet")
        self.assertIn('grupo B', motivo)
        self.op_tint._ft_sincronizar_capacidad()
        self.env.invalidate_all()
        line = self.jet.capacity_ids.filtered(lambda c: c.product_id == self.tela)
        self.assertEqual(line.capacity, 600.0)
        self.assertEqual(self.jet2.capacity_ids.filtered(lambda c: c.product_id == self.tela).capacity, 600.0,
                         "La alternativa con proceso del artículo también")
        self.assertEqual(self.op_tint.ft_tiempo_ficha, 240.0, "4 h de ciclo = 240 min por baño, sin multiplicar")
        self.op_tint.action_ft_usar_tiempo_ficha()
        self.assertEqual(self.op_tint.time_cycle_manual, 240.0)

    def test_04_capacidad_del_jet_vigilada(self):
        line = self.env['mrp.workcenter.capacity'].create({
            'workcenter_id': self.jet.id, 'product_id': self.tela.id, 'product_uom_id': self.kg.id, 'capacity': 700})
        self.assertEqual(line.ft_capacidad_ficha, 600.0)
        self.assertTrue(line.ft_capacidad_difiere)
        self.jet.action_ft_capacidades_ficha()
        self.env.invalidate_all()
        self.assertEqual(line.capacity, 600.0)
        self.assertFalse(line.ft_capacidad_difiere)
        self.p_jet._ft_set_value('kg_carga', 500)
        self.env.invalidate_all()
        self.assertEqual(line.ft_capacidad_ficha, 500.0, "Los kg por carga del artículo mandan")
        self.assertIn('kg por carga', line.ft_capacidad_motivo)
        self.tejido.rendimiento_tela_tejida = 20.0
        self.assertEqual(self.jet2._ft_capacidad_jet(self.tela)[0], 0.0, "Fuera de las bandas: no se inventa")
        self.assertEqual(self.jet2._ft_capacidad_carga(20.0, product=self.tela)[0], 1000.0,
                         "Para estimar (avisos, costeo) la misma regla tolera: capacidad general")

    def test_07_regla_unica_topada_y_sin_bandas(self):
        config = self.jet2._ft_config_at()
        config.valor_ids.filtered(lambda v: v.parametro_id.code == 'capacidad').valor = 500
        self.env.invalidate_all()
        regla = self.jet2._ft_regla_capacidad(10.0, self.tela)
        self.assertEqual((regla['capacidad'], regla['banda'], regla['origen']), (500.0, 'B', 'banda'),
                         "La banda B (600) se topa a la capacidad general (500)")
        self.assertIn('topado', regla['motivo'])
        config.valor_ids.filtered(lambda v: v.parametro_id.code.startswith('capacidad_grupo')).write({'valor': 0})
        self.env.invalidate_all()
        regla = self.jet2._ft_regla_capacidad(10.0, self.tela)
        self.assertEqual((regla['capacidad'], regla['origen']), (500.0, 'general'), "Sin bandas: la general")
        self.assertEqual(self.jet2._ft_regla_capacidad(10.0, self.tela, day='2019-01-01')['falla'], 'sin_config')

    def _kg_guardables(self, kg):
        """Los kg como los guarda la línea: con la precisión de su cantidad (2 decimales en una base nueva)."""
        digits = self.env['mrp.bom.line']._fields['product_qty'].get_digits(self.env)[1]
        return float_round(kg, precision_digits=digits)

    def test_05_conversion_a_metros(self):
        self.assertAlmostEqual(self.bom_m.ft_conversion_kg, 0.125, places=4, msg="1 m ÷ 8 m/kg")
        self.assertTrue(self.bom_m.ft_conversion_difiere)
        self.assertIn(self.bom_m, self.env['mrp.bom'].search([('ft_conversion_difiere', '=', True)]))
        self.assertFalse(self.bom.ft_conversion_kg, "La lista en kg no es de conversión")
        self.bom_m.action_ft_usar_conversion_ficha()
        self.env.invalidate_all()
        self.assertAlmostEqual(self.bom_m.bom_line_ids.product_qty, self._kg_guardables(0.125), places=6)
        self.assertFalse(self.bom_m.ft_conversion_difiere, "Lo más cerca que la línea puede guardar ya no avisa")
        self.assertNotIn(self.bom_m, self.env['mrp.bom'].search([('ft_conversion_difiere', '=', True)]))

    def test_06_usar_los_datos_de_la_ficha_en_listas_archivadas(self):
        (self.bom | self.bom_m).action_archive()
        (self.bom | self.bom_m).action_ft_datos_ficha()
        self.assertAlmostEqual(self.op_dev.time_cycle_manual, 0.22, places=4)
        self.assertAlmostEqual(self.op_abr.time_cycle_manual, 0.44, places=4)
        self.assertAlmostEqual(self.op_insp.time_cycle_manual, 0.8, places=4)
        self.assertEqual(self.op_tint.time_cycle_manual, 240.0, "La capacidad va antes que el tiempo por baño")
        self.assertAlmostEqual(self.bom_m.bom_line_ids.product_qty, self._kg_guardables(0.125), places=6)
