# -*- coding: utf-8 -*-
"""1.7.0: «Receta del desarrollo». Reproduce el proyecto 630 de producción (COT-2026-0015, WJ038Q22JVH165 teñido
verde, hermano WJ038Q22JNT160 natural) con los números del período 2026-10 que se leyeron por MCP el 2026-10-26."""
from odoo.tests import tagged

from .common import CotizadorCase


@tagged('post_install', '-at_install')
class TestReceta(CotizadorCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        # Producción guarda las cantidades de la lista de materiales con 4 decimales o más (0.0654, 0.0047);
        # una base nueva las redondea a 2.
        env['decimal.precision'].search([('name', 'in', ('Product Unit of Measure', 'Product Unit'))]).write(
            {'digits': 6})
        kg, m = env.ref('uom.product_uom_kgm'), env.ref('uom.product_uom_meter')
        litro = env.ref('uom.product_uom_litre')
        Centro = env['qb.centro']
        Centro.search([('company_id', '=', cls.company.id)]).write({'active': False})
        cls.jet = env['mrp.workcenter'].create({'name': 'TINTORERIA 4 prueba receta'})
        cls.c_tej = Centro.create({'code': 'TEJ630', 'name': 'Tejido 630', 'nature': 'directo',
                                   'driver': 'workorder'})
        cls.c_tin = Centro.create({'code': 'TIN630', 'name': 'Tintorería 630', 'nature': 'directo',
                                   'driver': 'kg_ciclo', 'workcenter_ids': [(6, 0, cls.jet.ids)]})
        cls.c_aca = Centro.create({'code': 'ACA630', 'name': 'Acabado 630', 'nature': 'directo',
                                   'driver': 'm_velocidad'})
        Prod = env['product.product']

        def prod(code, uom, std=0.0):
            return Prod.create({'name': code, 'default_code': code, 'type': 'consu', 'uom_id': uom.id,
                                'standard_price': std})

        def bom(product, uom, lines, ops=()):
            return env['mrp.bom'].create({
                'product_tmpl_id': product.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': uom.id,
                'bom_line_ids': [(0, 0, {'product_id': p.id, 'product_qty': q, 'product_uom_id': u.id})
                                 for p, q, u in lines],
                'operation_ids': [(0, 0, o) for o in ops]})

        cls.crudo = prod('WJ035Q22HNT200', kg)
        cls.engomado = prod('ENGOMADO002', kg, std=25.18)
        cls.agua = prod('AGUA', litro)
        quim_a, quim_b = prod('QUIMICO A 630', kg, std=60.0), prod('QUIMICO B 630', kg, std=33.304)
        cls.verde = prod('VERDE HOJA 001', kg)  # fórmula: standard_price 0 y lista de materiales
        bom(cls.verde, kg, [(quim_a, 0.5, kg), (quim_b, 0.5, kg)])
        cls.tenido = prod('WJ035Q22IVH200', kg)
        cls.bom_tenido = bom(cls.tenido, kg, [(cls.crudo, 1.0, kg), (cls.verde, 0.042, kg), (cls.agua, 24, litro)])
        cls.nuevo = prod('WJ038Q22JVH165', m)
        bom(cls.nuevo, m, [(cls.engomado, 0.0047, kg), (cls.tenido, 0.0654, kg)])
        cls.int200 = prod('WJ035Q22INT200', kg)
        cls.hermano = prod('WJ038Q22JNT160', m)
        bom(cls.hermano, m, [(cls.engomado, 0.0047, kg), (cls.int200, 0.0718, kg)])

        Costo = env['qb.costo.unitario']
        base = {'periodo_id': cls.periodo.id, 'company_id': cls.company.id, 'op_pct': 0.10, 'calidad': 'alta'}

        def costo(product, mp, fab, energia, **extra):
            return Costo.create(dict(base, product_id=product.id, mp_unit=mp, fabricacion_unit=fab,
                                     energia_unit=energia, costo_variable=mp + energia, costo_produccion=mp + fab,
                                     **extra))

        def linea(centro, horas, tarifa, variable, unit):
            return (0, 0, {'centro_id': centro.id, 'horas_unidad': horas, 'tarifa': tarifa,
                           'tarifa_variable': variable, 'costo_unit': unit})

        costo(cls.crudo, 41.5407, 18.2924, 2.8788, uom_name='kg', kg_por_unidad=1.0, rendimiento=0.9009,
              linea_ids=[linea(cls.c_tej, 0.124571, 146.8434, 23.1095, 18.2924)])
        costo(cls.engomado, 38.2694, 0.0, 0.0, uom_name='kg', kg_por_unidad=1.0)
        cls.costo_hermano = costo(
            cls.hermano, 3.2286, 1.6872, 0.2067, uom_name='m', kg_por_unidad=0.06345, rendimiento=0.9119,
            linea_ids=[linea(cls.c_tej, 0.008944, 146.8434, 23.1095, 1.3134),
                       linea(cls.c_tin, 0.000816, 264.9777, 0.0, 0.2162),
                       linea(cls.c_aca, 0.000607, 259.6732, 0.0, 0.1576)])

    def _cot_receta(self, **vals):
        base = {'product_id': self.nuevo.id, 'hermano_product_id': self.hermano.id, 'costo_fuente': 'receta',
                'spec_gramaje': 38, 'spec_ancho': 1.65, 'volumen': 1341, 'volumen_uom': 'm', 'precio_objetivo': 12.0}
        base.update(vals)
        return self._cot(**base)

    def test_01_proyecto_630(self):
        cot = self._cot_receta()
        cot.action_calcular()
        # Teñido por kg: crudo con su costo (41.5407 MP) + 0.042 kg de VERDE HOJA 001 a su lista (46.652 $/kg).
        mp = 0.0047 * 38.2694 + 0.0654 * (41.5407 + 0.042 * 46.652)
        fab = 0.0654 * 18.2924 + (0.2162 + 0.1576) / 0.0718 * 0.0654
        self.assertAlmostEqual(cot.mp_unit, mp, places=4)
        self.assertAlmostEqual(cot.fabricacion_unit, fab, places=4)
        self.assertAlmostEqual(cot.energia_unit, 0.0654 * 2.8788, places=4)
        self.assertAlmostEqual(cot.costo_produccion, mp + fab, places=4)
        self.assertAlmostEqual(cot.rendimiento, 0.9119, places=4, msg='Rendimiento del hermano')
        self.assertAlmostEqual(cot.op_pct, 0.10, places=6)
        self.assertAlmostEqual(cot.piso_lleno, (mp + fab) / 0.9119 / 0.9, places=3)
        self.assertFalse(cot.costo_id, 'La foto no es el costo del hermano')
        self.assertEqual(cot.calidad, 'media')
        self.assertIn('Tintorería 630', cot.calidad_detalle)
        self.assertIn('Acabado 630', cot.calidad_detalle)
        self.assertIn('rendimiento', cot.calidad_detalle)
        self.assertIn('AGUA', cot.receta_detalle, 'Sin compras ni costo estándar: dicho')
        self.assertIn('WJ035Q22HNT200', cot.receta_detalle)
        self.assertIn('Receta', cot.supuestos)

    def test_02_formula_con_su_lista(self):
        # VERDE HOJA 001: standard_price 0 y lista de materiales → 46.652 $/kg (unos 47).
        cot = self._cot_receta(product_id=self.verde.id, hermano_product_id=False, volumen_uom='kg')
        cot.action_calcular()
        self.assertAlmostEqual(cot.mp_unit, 46.652, places=3)
        self.assertIn('QUIMICO A 630', cot.receta_detalle)
        self.assertEqual(cot.calidad, 'baja', 'Sin hermano: rendimiento de planta, calidad baja')

    def test_03_consumo_contra_hermano(self):
        cot = self._cot_receta()
        cot.action_calcular()
        self.assertAlmostEqual(cot.consumo_kg, 0.0654, places=4)
        self.assertAlmostEqual(cot.consumo_hermano_kg, 0.0718 * 165 / 160, places=4)
        self.assertTrue(cot.consumo_alerta, '0.0654 contra 0.0740 kg/m: −11.7 %')
        self.assertIn('-11.7', cot.consumo_alerta)
        # Dentro del 10 %: sin aviso.
        cot.write({'spec_ancho': 1.50})  # hermano ajustado: 0.0718 × 150/160 = 0.0673
        cot.action_calcular()
        self.assertFalse(cot.consumo_alerta)

    def test_04_horas_de_la_ruta(self):
        # Con la operación del jet en la ruta, la tintorería sale de las horas × la tarifa y no se hereda.
        self.env['qb.tarifa'].create({'periodo_id': self.periodo.id, 'centro_id': self.c_tin.id,
                                      'tarifa_fija': 200.0, 'tarifa_variable': 20.0})
        self.bom_tenido.write({'operation_ids': [(0, 0, {'name': 'Teñir', 'workcenter_id': self.jet.id,
                                                         'time_cycle_manual': 6.0})]})
        cot = self._cot_receta()
        cot.action_calcular()
        fab = 0.0654 * (18.2924 + 0.1 * 220.0) + 0.1576 / 0.0718 * 0.0654
        self.assertAlmostEqual(cot.fabricacion_unit, fab, places=4)
        self.assertAlmostEqual(cot.energia_unit, 0.0654 * (2.8788 + 0.1 * 20.0), places=4)
        self.assertNotIn('Tintorería 630', cot.calidad_detalle)
        self.assertIn('Tintorería 630 0.1 h', cot.receta_detalle)

    def test_05_manual_en_blanco(self):
        cot = self._cot_receta()
        cot.action_calcular()
        self.assertTrue(cot.mp_unit)
        cot.write({'costo_fuente': 'manual'})
        self.assertFalse(cot.mp_unit or cot.fabricacion_unit or cot.energia_unit or cot.costo_produccion)
        self.assertFalse(cot.costo_id)
        self.assertEqual(cot.calidad, 'ninguna')
        cot.write({'mp_unit': 3.0, 'fabricacion_unit': 1.0})
        cot.action_calcular()
        self.assertAlmostEqual(cot.costo_produccion, 4.0)
        # Capturado en el mismo cambio: se respeta.
        her = self._cot(costo_fuente='hermano', hermano_product_id=self.hermano.id, product_id=False,
                        spec_descripcion='x')
        her.action_calcular()
        her.write({'costo_fuente': 'manual', 'mp_unit': 2.5})
        self.assertAlmostEqual(her.mp_unit, 2.5)
        self.assertFalse(her.fabricacion_unit)
