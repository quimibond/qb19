# -*- coding: utf-8 -*-
from psycopg2 import IntegrityError

from odoo import Command
from odoo.tests import tagged
from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger


@tagged('post_install', '-at_install', 'quimibond_rollos_bitacora')
class TestRollos(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        # Códigos propios de la prueba: la base local ya trae los artículos reales (IWJ045Q22JNT160…).
        cls.producto_kg = cls._producto('IZZTEST045Q22JNT160')
        cls.producto_m = cls._producto('ZZTEST045Q22JNT160')
        cls.componente = cls._producto('COMP-ROLLOS-TEST')
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.producto_kg.product_tmpl_id.id,
            'product_qty': 1,
            'product_uom_id': cls.kg.id,
            'bom_line_ids': [Command.create({'product_id': cls.componente.id, 'product_qty': 1})],
        })
        cls.orden = cls.env['mrp.production'].create({
            'product_id': cls.producto_kg.id, 'product_qty': 100, 'bom_id': cls.bom.id})
        cls.Tara = cls.env['mrp.tara.producto']

    @classmethod
    def _producto(cls, codigo):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu', 'is_storable': True,
            'uom_id': cls.kg.id, 'tracking': 'lot'})

    def _bitacora(self, secuencia=1, **vals):
        valores = {'production_id': self.orden.id, 'secuencia': secuencia, 'kg_rama': 50.0}
        valores.update(vals)
        return self.env['mrp.rollo.bitacora'].create(valores)

    # ---------------------------------------------------------------- taras
    def test_01_tara_directa_y_por_par_kg_metros(self):
        tara = self.Tara.create({
            'product_tmpl_id': self.producto_m.product_tmpl_id.id, 'operacion': 'rama', 'tara_kg': 1.1})
        # La tara está capturada en el artículo en metros; la orden en kg (con «I») la encuentra.
        self.assertEqual(self.Tara._buscar_tara(self.producto_kg, 'rama'), tara)
        self.assertEqual(self.Tara._buscar_tara(self.producto_m, 'rama'), tara)
        self.assertFalse(self.Tara._buscar_tara(self.producto_kg, 'clasificacion_final'))

    def test_02_tara_unica_y_positiva(self):
        valores = {'product_tmpl_id': self.producto_kg.product_tmpl_id.id, 'operacion': 'rama', 'tara_kg': 1.1}
        self.Tara.create(valores)
        with self.assertRaises(IntegrityError), mute_logger('odoo.sql_db'), self.cr.savepoint():
            self.Tara.create(valores)
        with self.assertRaises(IntegrityError), mute_logger('odoo.sql_db'), self.cr.savepoint():
            self.Tara.create(dict(valores, operacion='clasificacion_final', tara_kg=0))

    def test_03_carga_inicial(self):
        # Artículo del archivo (Rama 1.1 / Clasificación final 1.5): el real si la base ya lo tiene.
        codigo = 'IWJ045Q22JNT160'
        producto = self.env['product.product'].search([('default_code', '=', codigo)], limit=1) \
            or self._producto(codigo)
        self.Tara.action_cargar_iniciales()
        rama = self.Tara._buscar_tara(producto, 'rama')
        final = self.Tara._buscar_tara(producto, 'clasificacion_final')
        self.assertEqual(rama.tara_kg, 1.1)
        self.assertEqual(final.tara_kg, 1.5)
        self.assertEqual(rama.product_tmpl_id, producto.product_tmpl_id)  # empata primero el «I…»
        # Repetirla no duplica.
        total = self.Tara.search_count([])
        self.Tara.action_cargar_iniciales()
        self.assertEqual(self.Tara.search_count([]), total)

    # ------------------------------------------------------------- bitácora
    def test_04_alertas_de_metros(self):
        rollo = self._bitacora(metros_insp=100.0, metros_final=100.5, kg_final=10.0)
        self.assertFalse(rollo.alerta_metros_insp)
        rollo.metros_final = 102.0
        self.assertTrue(rollo.alerta_metros_insp)
        self.assertAlmostEqual(rollo.diferencia_metros_insp, 2.0)
        self.assertAlmostEqual(rollo.rendimiento_real, 10.2)

    def test_05_reetiquetado_y_qc_laboratorio(self):
        rollo = self._bitacora(clasificacion_inicial='PQ')
        self.assertTrue(rollo.qc_laboratorio_aplica)
        rollo.clasificacion_final = 'FE'
        self.assertTrue(rollo.reetiquetado)
        rollo.clasificacion_inicial = 'FE'
        self.assertFalse(rollo.qc_laboratorio_aplica)
        self.assertFalse(rollo.reetiquetado)

    def test_06_rollo_unico_por_orden(self):
        self._bitacora(secuencia=1)
        with self.assertRaises(IntegrityError), mute_logger('odoo.sql_db'), self.cr.savepoint():
            self._bitacora(secuencia=1)

    def test_07_lote_inmutable(self):
        lote = self.env['stock.lot'].create({
            'name': 'ROLLOTEST-0001', 'product_id': self.producto_kg.id, 'company_id': self.env.company.id})
        rollo = self._bitacora(lot_id=lote.id)
        with self.assertRaises(Exception):
            lote.name = 'OTRO'
        with self.assertRaises(Exception):
            rollo.lot_id = self.env['stock.lot'].create({
                'name': 'ROLLOTEST-0002', 'product_id': self.producto_kg.id, 'company_id': self.env.company.id})

    # --------------------------------------------------------------- wizard
    def test_08_wizard_neto_y_tara_faltante(self):
        wiz = self.env['rollo.pesaje.rama.wizard'].new({'production_id': self.orden.id, 'kg_bruto': 53.0})
        self.assertTrue(wiz.tara_faltante)
        self.assertEqual(wiz.kg_neto, 0.0)
        self.Tara.create({
            'product_tmpl_id': self.producto_kg.product_tmpl_id.id, 'operacion': 'rama', 'tara_kg': 1.1})
        wiz = self.env['rollo.pesaje.rama.wizard'].new({'production_id': self.orden.id, 'kg_bruto': 53.0})
        self.assertFalse(wiz.tara_faltante)
        self.assertAlmostEqual(wiz.kg_neto, 51.9)
        self.assertEqual(wiz.secuencia, 1)
        self.assertEqual(wiz.lote_name, '%s-0001' % self.orden.name.split('/')[-1])

    def test_09_etiqueta_de_rama_68_x_38(self):
        """3.4.0: la etiqueta de Rama mide 68 x 38 mm y lleva artículo, orden, fecha, centro, kg y el código del lote."""
        rama = self.env['mrp.workcenter'].create({'name': 'RAMA ETIQUETA', 'code': 'RETQ'})
        lote = self.env['stock.lot'].create({'name': '00004-0001', 'product_id': self.producto_kg.id})
        rollo = self.env['mrp.rollo.bitacora'].create({
            'production_id': self.orden.id, 'secuencia': 1, 'lot_id': lote.id, 'workcenter_rama_id': rama.id,
            'kg_rama': 51.9})
        zpl = rollo._zpl_etiqueta_rama()
        self.assertTrue(zpl.startswith('^XA^PW544^LL304'))  # 68 x 38 mm a 203 dpi
        for texto in ('ARTÍCULO: %s' % (self.producto_kg.default_code or self.producto_kg.name),
                      'ORDEN: %s' % self.orden.name, 'FECHA: ', 'CENTRO: RAMA ETIQUETA', 'KG: 51.90'):
            self.assertIn(texto, zpl)
        datos, _modulos = rollo._code128_zpl('00004-0001')
        self.assertIn('^FD%s^FS' % datos, zpl)
        self.assertIn('^BY3,', zpl)

    # ------------------------------------------------- botón en la lista de operaciones
    def test_10_operacion_de_rama(self):
        Workcenter = self.env['mrp.workcenter']
        rama = Workcenter.create({'name': 'RAMA TEST 1', 'code': 'RTEST1'})
        devanadora = Workcenter.create({'name': 'DEVANADORA TEST 1', 'code': 'DTEST1'})
        self.assertTrue(self.env['mrp.workorder'].new({'workcenter_id': rama.id}).es_rama)
        self.assertFalse(self.env['mrp.workorder'].new({'workcenter_id': devanadora.id}).es_rama)
