# -*- coding: utf-8 -*-
from odoo import Command
from odoo.exceptions import UserError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase


@tagged('post_install', '-at_install', 'quimibond_rollos_bitacora')
class TestInspeccion(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.producto = cls._producto('IWJ046Q22JNT160')
        cls.componente = cls._producto('COMP-INSP-TEST')
        bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.producto.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
            'bom_line_ids': [Command.create({'product_id': cls.componente.id, 'product_qty': 1})]})
        cls.orden = cls.env['mrp.production'].create({
            'product_id': cls.producto.id, 'product_qty': 100, 'bom_id': bom.id})
        cls.centro = cls.env['mrp.workcenter'].create({'name': 'INSPECCION TEST 5', 'code': 'ITEST5'})
        cls.centro9 = cls.env['mrp.workcenter'].create({'name': 'INSPECCION TEST 9', 'code': 'ITEST9'})
        cls.workorder = cls.env['mrp.workorder'].create({
            'name': 'INSPECCION', 'production_id': cls.orden.id, 'workcenter_id': cls.centro.id})
        lote = cls.env['stock.lot'].create({
            'name': 'INSPTEST-0001', 'product_id': cls.producto.id, 'company_id': cls.env.company.id})
        cls.rollo = cls.env['mrp.rollo.bitacora'].create({
            'production_id': cls.orden.id, 'secuencia': 1, 'kg_rama': 50.0, 'lot_id': lote.id})

    @classmethod
    def _producto(cls, codigo):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu', 'is_storable': True,
            'uom_id': cls.kg.id, 'tracking': 'lot'})

    def _wizard(self, **vals):
        valores = {'production_id': self.orden.id, 'workorder_id': self.workorder.id, 'workcenter_id': self.centro.id,
                   'empleado_numero': '1', 'rollo_codigo': self.rollo.name, 'clasificacion': 'PQ',
                   'no_empalmes': 2, 'metros': 123.45}
        valores.update(vals)
        return valores

    # ------------------------------------------------------------ catálogo
    def test_01_catalogo_de_defectos_cargado(self):
        Defecto = self.env['calidad.defecto.tela']
        self.assertGreaterEqual(Defecto.search_count([('tipo_tela', '=', 'tejido_acabado')]), 60)
        self.assertTrue(Defecto.search([('clave', '=', 'AD')]))

    def test_02_siglas_separadas_por_espacio(self):
        Defecto = self.env['calidad.defecto.tela']
        self.rollo.defecto_ids = Defecto.search([('clave', 'in', ['AD', 'AGU'])])
        self.assertEqual(self.rollo.siglas_defectos, 'AD AGU')

    # -------------------------------------------------------- restricciones
    def test_03_restricciones_por_centro(self):
        Restriccion = self.env['mrp.inspeccion.restriccion']
        Restriccion.create({
            'workcenter_id': self.centro9.id, 'prefijos_excluidos': 'WD',
            'codigos_exclusivos': 'WJ070Q66JBL205'})
        wd = self._producto('IWD038Q22JNT160')
        exclusivo = self._producto('IWJ070Q66JBL205')
        with self.assertRaises(UserError):
            Restriccion._validar_centro(self.centro9, wd)       # el 9 no revisa WD
        Restriccion._validar_centro(self.centro, wd)            # otro centro sí
        with self.assertRaises(UserError):
            Restriccion._validar_centro(self.centro, exclusivo)  # solo en el 9
        Restriccion._validar_centro(self.centro9, exclusivo)
        Restriccion._validar_centro(self.centro, self.producto)  # artículo sin reglas

    # ------------------------------------------------------------- wizard
    def test_04_escaneo_encuentra_el_rollo_de_la_orden(self):
        wiz = self.env['rollo.inspeccion.wizard'].new(self._wizard())
        self.assertEqual(wiz.rollo_id, self.rollo)
        self.assertFalse(wiz.rollo_no_encontrado)
        self.assertEqual(wiz.pendientes, 1)
        wiz.rollo_codigo = 'NO-EXISTE'
        self.assertTrue(wiz.rollo_no_encontrado)
        self.assertFalse(wiz.rollo_id)

    def test_05_registrar_inspeccion_y_etiqueta(self):
        wiz = self.env['rollo.inspeccion.wizard'].create(self._wizard(
            defecto_ids=[Command.set(self.env['calidad.defecto.tela'].search([('clave', '=', 'AD')]).ids)]))
        accion = wiz.action_confirmar()
        self.assertTrue(accion.get('close_on_report_download'))
        self.assertEqual(self.rollo.estado_inspeccion, 'inspeccionado')
        self.assertEqual(self.rollo.clasificacion_inicial, 'PQ')
        self.assertEqual(self.rollo.no_empalmes, 2)
        self.assertAlmostEqual(self.rollo.metros_insp, 123.45)
        self.assertEqual(self.rollo.workcenter_insp_id, self.centro)
        zpl = self.rollo.zpl_etiqueta_preliminar
        self.assertIn('^BCN', zpl)
        self.assertIn('INSPTEST-0001', zpl)
        self.assertIn('[X] PQ', zpl)
        self.assertIn('123.45', zpl)
        self.assertIn('50.00', zpl)  # Kgs = neto de Rama

    def test_06_datos_obligatorios(self):
        for cambio in ({'clasificacion': False}, {'metros': 0}, {'rollo_codigo': 'NO-EXISTE'}):
            wiz = self.env['rollo.inspeccion.wizard'].create(self._wizard(**cambio))
            with self.assertRaises(UserError):
                wiz.action_confirmar()

    # ----------------------------------------------------- cierre de la orden
    def test_07_no_cierra_con_rollos_pendientes(self):
        with self.assertRaises(UserError):
            self.orden._validar_rollos_inspeccionados()
        self.rollo.estado_inspeccion = 'inspeccionado'
        self.orden._validar_rollos_inspeccionados()

    # --------------------------------------------------------- desperdicio
    def test_08_desperdicio_requiere_subproducto(self):
        wo = self.env['mrp.workorder'].new({'production_id': self.orden.id})
        with self.assertRaises(UserError):
            self.orden._rama_registrar_desperdicio(wo, 3.5, '1', False)

    def test_09_operacion_de_inspeccion(self):
        self.assertTrue(self.env['mrp.workorder'].new({'workcenter_id': self.centro.id}).es_inspeccion)
        rama = self.env['mrp.workcenter'].create({'name': 'RAMA TEST 9', 'code': 'RTEST9'})
        self.assertFalse(self.env['mrp.workorder'].new({'workcenter_id': rama.id}).es_inspeccion)
