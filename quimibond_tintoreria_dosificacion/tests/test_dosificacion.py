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
        # Categoría propia: en bases reales puede no existir el xmlid product.product_category_all.
        cls.cat_general = Categ.create({'name': 'General (prueba)'})
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        # Precisión de producción (kg a 0.0001 y «Product Unit» a 4 decimales): una base nueva redondea el kg a
        # 0.01 y las dosis chicas (34 g de colorante) salían redondeadas hacia arriba.
        cls.kg.rounding = 0.0001
        cls.env['decimal.precision'].search([('name', 'in', ('Product Unit', 'Product Unit of Measure'))]).digits = 4

        def producto(nombre, categ=None, codigo=None):
            return cls.env['product.product'].create({
                'name': nombre, 'default_code': codigo, 'type': 'consu', 'is_storable': True,
                'uom_id': cls.kg.id, 'categ_id': (categ or cls.cat_general).id,
            })

        cls.detergente = producto('Detergente', cls.cat_quimico, 'T-DET')
        cls.secuestrante = producto('Secuestrante', cls.cat_quimico, 'T-SEC')
        cls.amarillo = producto('Amarillo disperso', cls.cat_colorante, 'T-AMA')
        cls.tela = producto('Tela acabada kg', None, 'T-ITELA')
        cls.otro = producto('Hilo', None, 'T-HILO')

        cls.wc1 = cls.env['mrp.workcenter'].create({'name': 'TINTORERIA T1'})
        cls.wc2 = cls.env['mrp.workcenter'].create({'name': 'TINTORERIA T2'})

    def setUp(self):
        super().setUp()
        # Relación de baño por jet (fichas técnicas): T1 = 7 L/kg, T2 = 6 L/kg. Si ``rb_articulo`` tiene
        # valor, la del artículo en el jet gana sobre la de la máquina (``_ft_relacion_bano``).
        self.rb_articulo = None
        rb_maquina = {self.wc1.id: 7.0, self.wc2.id: 6.0}

        def relacion_bano(workcenter, product=None, day=None):
            if self.rb_articulo is not None and product and product == self.tela:
                return self.rb_articulo, 'relación de baño del artículo en este jet'
            valor = rb_maquina.get(workcenter.id, 0.0)
            return valor, ('relación de baño de la máquina' if valor else 'sin relación de baño')

        self.patch(self.env.registry['mrp.workcenter'], '_ft_relacion_bano', relacion_bano)

        self.formula = self.env['tintoreria.formula'].create({
            'name': 'NATURAL T05',
            'line_ids': [
                (0, 0, {'product_id': self.detergente.id, 'gramos_por_litro': 1.5}),
                (0, 0, {'product_id': self.secuestrante.id, 'gramos_por_litro': 1.0}),
                (0, 0, {'product_id': self.amarillo.id, 'porcentaje_colorante': 0.034}),
            ],
        })
        self.bom = self.env['mrp.bom'].create({
            'product_tmpl_id': self.tela.product_tmpl_id.id,
            'product_qty': 1.0,
            'product_uom_id': self.kg.id,
            'operation_ids': [(0, 0, {'name': 'TINTORERIA', 'workcenter_id': self.wc1.id})],
            'bom_line_ids': [(0, 0, {'product_id': self.otro.id, 'product_qty': 1.0})],
            'formula_tintoreria_id': self.formula.id,
        })
        self.bom.action_cargar_formula()

    def _mov(self, mo, producto):
        return mo.move_raw_ids.filtered(lambda m: m.product_id == producto)

    def _crear_mo(self, kg=100.0):
        return self.env['mrp.production'].create({
            'product_id': self.tela.id, 'product_qty': kg,
            'product_uom_id': self.kg.id, 'bom_id': self.bom.id,
        })

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

    def test_operacion_de_tintoreria_por_nombre(self):
        self.assertEqual(self.bom._operacion_tintoreria().workcenter_id, self.wc1)

    def test_qty_teorica_bom(self):
        # BoM de 1 kg, RB del work center de la operación = 7 L/kg: 1.5 g/L x 7 / 1000
        linea = self.bom.bom_line_ids.filtered(lambda l: l.product_id == self.detergente)
        self.assertAlmostEqual(linea.product_qty, 0.0105, places=6)

    def test_calculo_en_mo_y_recalculo_por_cambio_de_maquina(self):
        mo = self._crear_mo(100.0)
        # Químico: g/L x (RB x kg) / 1000 ; RB = 7
        self.assertAlmostEqual(self._mov(mo, self.detergente).product_uom_qty, 1.5 * 7 * 100 / 1000, places=6)
        self.assertAlmostEqual(self._mov(mo, self.secuestrante).product_uom_qty, 1.0 * 7 * 100 / 1000, places=6)
        # Colorante: % / 100 x kg (no depende de la RB)
        self.assertAlmostEqual(self._mov(mo, self.amarillo).product_uom_qty, 0.034 / 100 * 100, places=6)
        # Cambio de máquina: RB = 6
        mo.workorder_ids.filtered(lambda w: w.operation_id.name == 'TINTORERIA').write({'workcenter_id': self.wc2.id})
        self.assertAlmostEqual(self._mov(mo, self.detergente).product_uom_qty, 1.5 * 6 * 100 / 1000, places=6)
        self.assertAlmostEqual(self._mov(mo, self.amarillo).product_uom_qty, 0.034, places=6)

    def test_rb_del_articulo_gana_sobre_la_de_la_maquina(self):
        self.rb_articulo = 5.0
        mo = self._crear_mo(100.0)
        # Químicos con la RB del artículo (5), no la de la máquina (7); el colorante no cambia.
        self.assertAlmostEqual(self._mov(mo, self.detergente).product_uom_qty, 1.5 * 5 * 100 / 1000, places=6)
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
