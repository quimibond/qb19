# -*- coding: utf-8 -*-
from odoo import Command
from odoo.exceptions import UserError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase

from odoo.addons.quimibond_tintoreria_programacion.models.mrp_production import banda_rendimiento


class _Config:
    """Configuración de jet simulada: devuelve el valor por código de parámetro."""

    def __init__(self, **valores):
        self.valores = valores

    def _ft_value(self, codigo):
        return self.valores.get(codigo, 0.0)


CONFIG_HTJ1 = dict(capacidad=1000.0, capacidad_grupo_a=900.0, capacidad_grupo_b=600.0, capacidad_grupo_c=450.0)
CONFIG_HTJ3 = dict(capacidad=600.0, capacidad_grupo_a=600.0)   # bandas B y C: N/A
CONFIG_HTJ5 = dict(capacidad=1200.0)                           # sin bandas: solo capacidad general


@tagged('post_install', '-at_install', 'quimibond_tintoreria_programacion')
class TestProgramacionTintoreria(TransactionCase):
    """Caso del cliente: rendimiento de tejido 9 (banda B), HTJ con 600 kg en banda B, MO de 1000 kg
    -> 2 MOs de 500 kg en el mismo HTJ."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        Workcenter = cls.env['mrp.workcenter']
        cls.htj1 = Workcenter.create({'name': 'Tintoreria TEST 1', 'code': 'HTJT1'})
        cls.htj3 = Workcenter.create({'name': 'Tintoreria TEST 3', 'code': 'HTJT3'})
        cls.htj5 = Workcenter.create({'name': 'Tintoreria TEST 5', 'code': 'HTJT5'})
        cls.configs = {
            cls.htj1.id: CONFIG_HTJ1,
            cls.htj3.id: CONFIG_HTJ3,
            cls.htj5.id: CONFIG_HTJ5,
        }
        cls.producto_kg = cls._producto('IWT105Q56JNT155AF')
        cls.componente = cls._producto('COMP-TEST', storable=False)
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.producto_kg.product_tmpl_id.id,
            'product_qty': 1,
            'product_uom_id': cls.kg.id,
            'operation_ids': [Command.create({'name': 'Tintorería', 'workcenter_id': cls.htj1.id})],
            'bom_line_ids': [Command.create({'product_id': cls.componente.id, 'product_qty': 0.1})],
        })

    @classmethod
    def _producto(cls, codigo, storable=True):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu',
            'is_storable': storable, 'uom_id': cls.kg.id,
        })

    def setUp(self):
        super().setUp()
        self.rendimiento = 9.0
        self.es_kg = True
        configs = self.configs
        # Las fichas técnicas se simulan: configuración vigente del jet, capacidad del artículo,
        # rendimiento de tejido y vínculo kg <-> metros.
        self.patch(self.env.registry['mrp.workcenter'], '_ft_config_at',
                   lambda wc, day=None: _Config(**configs[wc.id]) if wc.id in configs else False)
        self.patch(self.env.registry['mrp.workcenter'], '_ft_capacidad_carga',
                   lambda wc, rendimiento, day=None, product=None: (0.0, 'sin capacidad capturada'))
        self.patch(self.env.registry['product.product'], '_ft_rendimiento',
                   lambda producto: (self.rendimiento, 'prueba'))
        self.patch(self.env.registry['mrp.production'], '_es_producto_acabado_kg',
                   lambda orden: self.es_kg)

    def _crear_mo(self, kg=1000.0):
        mo = self.env['mrp.production'].create({
            'product_id': self.producto_kg.id,
            'product_qty': kg,
            'product_uom_id': self.kg.id,
            'bom_id': self.bom.id,
        })
        mo.action_confirm()
        return mo

    # ------------------------------------------------------------------
    def test_banda_continua(self):
        self.assertFalse(banda_rendimiento(2.9))
        self.assertEqual(banda_rendimiento(3), 'A')
        self.assertEqual(banda_rendimiento(6.5), 'A')
        self.assertEqual(banda_rendimiento(7), 'B')
        self.assertEqual(banda_rendimiento(10.5), 'B')
        self.assertEqual(banda_rendimiento(11), 'C')
        self.assertEqual(banda_rendimiento(15.5), 'C')
        self.assertFalse(banda_rendimiento(16))
        self.assertFalse(banda_rendimiento(0))

    def test_requiere_programacion_por_ficha_no_por_codigo(self):
        mo = self._crear_mo()
        self.assertTrue(mo.requiere_programacion_tintoreria)
        self.assertTrue(mo.tintoreria_pendiente)
        self.es_kg = False
        mo2 = self._crear_mo()
        self.assertFalse(mo2.requiere_programacion_tintoreria)
        with self.assertRaises(UserError):
            mo2._check_programable()

    def test_banda_b_divide_en_dos(self):
        mo = self._crear_mo(1000.0)
        programacion = mo._programar_tintoreria(self.htj1)
        ordenes = programacion.production_ids
        self.assertEqual(programacion.banda, 'B')
        self.assertEqual(programacion.capacidad_kg, 600.0)
        self.assertEqual(len(ordenes), 2)
        self.assertEqual(sorted(ordenes.mapped('product_qty')), [500.0, 500.0])
        self.assertTrue(all(ordenes.mapped('tintoreria_programada')))

    def test_fuera_de_bandas_bloquea(self):
        self.rendimiento = 20.0
        mo = self._crear_mo()
        with self.assertRaises(UserError):
            mo._programar_tintoreria(self.htj1)

    def test_banda_na_en_jet_con_bandas_bloquea(self):
        mo = self._crear_mo()
        with self.assertRaises(UserError):
            mo._programar_tintoreria(self.htj3)

    def test_jet_sin_bandas_usa_capacidad_general_con_aviso(self):
        mo = self._crear_mo(1000.0)
        plan = mo._calcular_plan_tintoreria(self.htj5)
        self.assertEqual(plan['capacidad_kg'], 1200.0)
        self.assertEqual(plan['n_banos'], 1)
        self.assertTrue(plan['aviso'])

    def test_kg_por_carga_del_articulo_manda(self):
        self.patch(self.env.registry['mrp.workcenter'], '_ft_capacidad_carga',
                   lambda wc, rendimiento, day=None, product=None: (
                       400.0, 'kg por carga del artículo en este jet'))
        mo = self._crear_mo(1000.0)
        plan = mo._calcular_plan_tintoreria(self.htj1)
        self.assertEqual(plan['capacidad_kg'], 400.0)
        self.assertEqual(plan['n_banos'], 3)

    def test_arranque_sin_programar_bloquea_y_acepta_argumentos_de_enterprise(self):
        mo = self._crear_mo(100.0)
        orden_trabajo = mo.workorder_ids[:1]
        with self.assertRaises(UserError):
            orden_trabajo.button_start()
        # Regresión: Enterprise llama button_start con argumentos extra (datos demo y Shop Floor).
        with self.assertRaises(UserError):
            orden_trabajo.button_start(True, True)

    def test_tiempos_por_jet_en_la_formula(self):
        """1.4.0: los minutos por jet se capturan en la propia fórmula."""
        formula = self.env['tintoreria.formula'].create({
            'name': 'FORMULA TIEMPOS TEST',
            'tiempo_ids': [Command.create({'workcenter_id': self.htj1.id, 'minutos': 240.0})]})
        tiempo = self.env['tintoreria.formula.tiempo'].search([('formula_id', '=', formula.id)])
        self.assertEqual((tiempo.workcenter_id, tiempo.minutos), (self.htj1, 240.0))
        self.assertEqual(formula.tiempo_ids, tiempo)
        self.assertEqual(tiempo.horas, 4.0)
