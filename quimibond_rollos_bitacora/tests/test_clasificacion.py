# -*- coding: utf-8 -*-
from odoo import Command
from odoo.exceptions import UserError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase


@tagged('post_install', '-at_install', 'quimibond_rollos_bitacora')
class TestClasificacionFinal(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.metro = cls.env.ref('uom.product_uom_meter')
        cls.prod_a = cls._producto('IWJ038Q22JNT160', cls.kg)
        cls.prod_b = cls._producto('WJ038Q22JNT160', cls.metro)
        bom_a = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_a.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id})
        cls.orden_a = cls.env['mrp.production'].create({
            'product_id': cls.prod_a.id, 'product_qty': 100, 'bom_id': bom_a.id})
        bom_b = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_b.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.metro.id,
            'bom_line_ids': [Command.create({'product_id': cls.prod_a.id, 'product_qty': 0.0635})]})
        cls.orden_b = cls.env['mrp.production'].create({
            'product_id': cls.prod_b.id, 'product_qty': 100, 'bom_id': bom_b.id})
        cls.centro = cls.env['mrp.workcenter'].create({'name': 'EMPAQUE TEST', 'code': 'EMPTEST'})
        cls.workorder = cls.env['mrp.workorder'].create({
            'name': 'CLASIFICACION', 'production_id': cls.orden_b.id, 'workcenter_id': cls.centro.id})
        lote = cls.env['stock.lot'].create({
            'name': 'CLASTEST-0001', 'product_id': cls.prod_a.id, 'company_id': cls.env.company.id})
        cls.rollo = cls.env['mrp.rollo.bitacora'].create({
            'production_id': cls.orden_a.id, 'secuencia': 1, 'kg_rama': 33.0, 'lot_id': lote.id,
            'estado_inspeccion': 'inspeccionado', 'clasificacion_inicial': 'PQ', 'metros_insp': 520.0,
            'operador_insp': '99999999 - OPERADOR PRUEBA'})

    @classmethod
    def _producto(cls, codigo, uom):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu', 'is_storable': True,
            'uom_id': uom.id, 'tracking': 'lot'})

    def _final(self, clasificacion, defectos=()):
        self.rollo.write({
            'production_b_id': self.orden_b.id, 'clasificacion_final': clasificacion,
            'metros_final': 521.0, 'kg_final': 33.4, 'kg_bruto_final': 34.0,
            'siglas_operador_final': 'RI', 'lot_b_id': False,
            'defecto_ids': [Command.set(self.env['calidad.defecto.tela'].search([('clave', 'in', list(defectos))]).ids)],
        })
        return self.rollo._zpl_etiqueta_final()

    def test_01_operacion_de_clasificacion(self):
        self.assertTrue(self.workorder.es_clasificacion)
        self.assertTrue(self.orden_b.es_clasificacion_final)

    def test_02_datos_de_etiqueta(self):
        self.rollo.production_b_id = self.orden_b
        datos = self.rollo._datos_etiqueta_final()
        self.assertEqual(datos['color'], 'NATURAL')
        self.assertEqual(datos['ancho'], 160)

    def test_03_etiqueta_pq(self):
        zpl = self._final('PQ')
        self.assertIn('WJ038Q22JNT160', zpl)
        self.assertNotIn('WJ038Q22JNT160..', zpl)
        self.assertIn('PESO NETO: 33.40', zpl)
        self.assertIn('PESO BRUTO: 34.00', zpl)
        self.assertIn('521.00;33.400;CLASTEST;1', zpl)
        self.assertEqual(zpl.count('^BCN'), 1)

    def test_04_etiqueta_pnc_con_defectos(self):
        zpl = self._final('FE', defectos=('AD', 'AGU'))
        self.assertIn('WJ038Q22JNT160..', zpl)
        self.assertIn('KILOS: 33.40', zpl)
        self.assertIn('LIBRAS: 73.63', zpl)
        self.assertIn('DEFECTOS: AD AGU', zpl)
        self.assertIn('1 AD AGU', zpl)
        self.assertEqual(zpl.count('^BCN'), 2)
        self.assertIn('WJ038Q22JNT160.', self._final('SEGUNDA'))
        self.assertIn('WJ038Q22JNT160*', self._final('DESP'))

    def test_05_wizard_estados_del_rollo(self):
        wiz = self.env['rollo.clasificacion.wizard'].new({
            'production_id': self.orden_b.id, 'workorder_id': self.workorder.id, 'rollo_codigo': 'NO-EXISTE'})
        wiz.production_id._clasif_move_a()
        self.assertFalse(wiz.rollo_id)

    def test_06_siglas_requeridas(self):
        wiz = self.env['rollo.clasificacion.wizard'].new({
            'production_id': self.orden_b.id, 'workorder_id': self.workorder.id})
        with self.assertRaises(UserError):
            wiz._siglas_operador_insp(self.rollo)
