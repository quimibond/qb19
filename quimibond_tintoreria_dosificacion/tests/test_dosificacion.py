# -*- coding: utf-8 -*-
from odoo.exceptions import ValidationError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase


@tagged('post_install', '-at_install')
class TestDosificacionTintoreria(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Categ = cls.env['product.category']
        raiz = Categ.create({'name': 'Materia Prima'})
        cls.cat_quimico = Categ.create({'name': 'Químico', 'parent_id': raiz.id})
        cls.cat_colorante = Categ.create({'name': 'Colorante', 'parent_id': raiz.id})
        cls.kg = cls.env.ref('uom.product_uom_kgm')

        def producto(nombre, categ=None, codigo=None):
            return cls.env['product.product'].create({
                'name': nombre, 'default_code': codigo, 'type': 'consu', 'is_storable': True,
                'uom_id': cls.kg.id, 'categ_id': (categ or cls.env.ref('product.product_category_all')).id,
            })

        cls.detergente = producto('Detergente', cls.cat_quimico, 'T-DET')
        cls.secuestrante = producto('Secuestrante', cls.cat_quimico, 'T-SEC')
        cls.amarillo = producto('Amarillo disperso', cls.cat_colorante, 'T-AMA')
        cls.tela = producto('Tela acabada kg', None, 'T-ITELA')
        cls.otro = producto('Hilo', None, 'T-HILO')

        cls.wc1 = cls.env['mrp.workcenter'].create({'name': 'TINTORERIA T1'})
        cls.wc2 = cls.env['mrp.workcenter'].create({'name': 'TINTORERIA T2'})
        Rend = cls.env['tintoreria.capacidad.rendimiento']
        Rend.create({'workcenter_id': cls.wc1.id, 'capacidad_kg': 1000, 'relacion_bano': 7})
        Rend.create({'workcenter_id': cls.wc2.id, 'capacidad_kg': 600, 'relacion_bano': 6})

        cls.formula = cls.env['tintoreria.formula'].create({
            'name': 'NATURAL T05',
            'line_ids': [
                (0, 0, {'product_id': cls.detergente.id, 'gramos_por_litro': 1.5}),
                (0, 0, {'product_id': cls.secuestrante.id, 'gramos_por_litro': 1.0}),
                (0, 0, {'product_id': cls.amarillo.id, 'porcentaje_colorante': 0.034}),
            ],
        })
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id,
            'product_qty': 1.0,
            'product_uom_id': cls.kg.id,
            'operation_ids': [(0, 0, {'name': 'TINTORERIA', 'workcenter_id': cls.wc1.id})],
            'bom_line_ids': [(0, 0, {'product_id': cls.otro.id, 'product_qty': 1.0})],
            'formula_tintoreria_id': cls.formula.id,
        })
        cls.bom.action_cargar_formula()

    def _mov(self, mo, producto):
        return mo.move_raw_ids.filtered(lambda m: m.product_id == producto)

    def test_tipo_desde_categoria(self):
        lineas = self.bom.bom_line_ids
        self.assertEqual(lineas.filtered(lambda l: l.product_id == self.detergente).tipo_componente, 'quimico')
        self.assertEqual(lineas.filtered(lambda l: l.product_id == self.amarillo).tipo_componente, 'colorante')
        self.assertFalse(lineas.filtered(lambda l: l.product_id == self.otro).dosificado)

    def test_carga_formula_y_estado(self):
        self.assertEqual(len(self.bom.bom_line_ids.filtered('dosificado')), 3)
        self.assertEqual(self.bom.formula_estado, 'sincronizada')
        self.bom.bom_line_ids.filtered(lambda l: l.product_id == self.detergente).gramos_por_litro = 2.0
        self.assertEqual(self.bom.formula_estado, 'modificada')
        self.assertTrue(self.bom._diferencias_formula())
        self.bom.action_cargar_formula()
        self.assertEqual(self.bom.formula_estado, 'sincronizada')

    def test_qty_teorica_bom(self):
        # BoM de 1 kg, RB del work center de la operación = 7 L/kg: 1.5 g/L x 7 / 1000
        linea = self.bom.bom_line_ids.filtered(lambda l: l.product_id == self.detergente)
        self.assertAlmostEqual(linea.product_qty, 0.0105, places=6)

    def test_calculo_en_mo_y_recalculo_por_cambio_de_maquina(self):
        mo = self.env['mrp.production'].create({
            'product_id': self.tela.id, 'product_qty': 100.0,
            'product_uom_id': self.kg.id, 'bom_id': self.bom.id,
        })
        # Químico: g/L x (RB x kg) / 1000 ; RB = 7
        self.assertAlmostEqual(self._mov(mo, self.detergente).product_uom_qty, 1.5 * 7 * 100 / 1000, places=6)
        self.assertAlmostEqual(self._mov(mo, self.secuestrante).product_uom_qty, 1.0 * 7 * 100 / 1000, places=6)
        # Colorante: % / 100 x kg (no depende de la RB)
        self.assertAlmostEqual(self._mov(mo, self.amarillo).product_uom_qty, 0.034 / 100 * 100, places=6)
        # Cambio de máquina: RB = 6
        mo.workorder_ids.filtered(lambda w: w.operation_id.name == 'TINTORERIA').write({'workcenter_id': self.wc2.id})
        self.assertAlmostEqual(self._mov(mo, self.detergente).product_uom_qty, 1.5 * 6 * 100 / 1000, places=6)
        self.assertAlmostEqual(self._mov(mo, self.amarillo).product_uom_qty, 0.034, places=6)

    def test_validaciones_catalogo(self):
        with self.assertRaises(ValidationError):  # químico sin g/L
            self.env['tintoreria.formula.line'].create({
                'formula_id': self.formula.id, 'product_id': self.detergente.id})
        with self.assertRaises(ValidationError):  # colorante con g/L
            self.env['tintoreria.formula.line'].create({
                'formula_id': self.formula.id, 'product_id': self.amarillo.id, 'gramos_por_litro': 1.0})
        with self.assertRaises(ValidationError):  # producto que no es químico ni colorante
            self.env['tintoreria.formula.line'].create({
                'formula_id': self.formula.id, 'product_id': self.otro.id, 'gramos_por_litro': 1.0})
