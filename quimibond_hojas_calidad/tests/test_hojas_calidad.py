# -*- coding: utf-8 -*-
from odoo import Command
from odoo.exceptions import UserError, ValidationError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase


@tagged('post_install', '-at_install', 'quimibond_hojas_calidad')
class TestHojasCalidad(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        kg = cls.env.ref('uom.product_uom_kgm')
        metro = cls.env.ref('uom.product_uom_meter')
        P = cls.env['product.product']
        vals = {'type': 'consu', 'is_storable': True, 'tracking': 'lot'}
        cls.prod_kg = P.create(dict(vals, name='IWJ038Q22JNT160', default_code='IWJ038Q22JNT160', uom_id=kg.id))
        cls.prod_m = P.create(dict(vals, name='WJ038Q22JNT160', default_code='WJ038Q22JNT160', uom_id=metro.id))
        cls.prod_otro = P.create(dict(vals, name='OTRO', default_code='IXX999', uom_id=kg.id))
        Doc = cls.env['documents.document']
        cls.blanco_rama = Doc.create({'name': 'Rama WJ038 blanco', 'handler': 'spreadsheet', 'type': 'binary'})
        cls.blanco_insp = Doc.create({'name': 'Inspeccion WJ038 blanco', 'handler': 'spreadsheet', 'type': 'binary'})
        Pl = cls.env['mrp.hoja.calidad.plantilla']
        cls.pl_rama = Pl.create({'articulo': 'wj038q22jnt160', 'tipo': 'rama', 'plantilla_id': cls.blanco_rama.id})
        cls.pl_insp = Pl.create({'articulo': 'WJ038Q22JNT160', 'tipo': 'inspeccion',
                                 'plantilla_id': cls.blanco_insp.id})
        centro = cls.env['mrp.workcenter'].create({'name': 'RAMA HC', 'code': 'RHC'})
        centro_e = cls.env['mrp.workcenter'].create({'name': 'EMPAQUE HC', 'code': 'EHC'})
        bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_kg.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.id})
        cls.orden_kg = cls.env['mrp.production'].create({'product_id': cls.prod_kg.id, 'product_qty': 10, 'bom_id': bom.id})
        bom_m = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_m.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': metro.id,
            'bom_line_ids': [Command.create({'product_id': cls.prod_kg.id, 'product_qty': 0.0635})]})
        cls.orden_m = cls.env['mrp.production'].create({'product_id': cls.prod_m.id, 'product_qty': 10, 'bom_id': bom_m.id})
        cls.wo_rama = cls.env['mrp.workorder'].create({
            'name': 'RAMA', 'production_id': cls.orden_kg.id, 'workcenter_id': centro.id})
        cls.wo_clas = cls.env['mrp.workorder'].create({
            'name': 'CLASIFICACION', 'production_id': cls.orden_m.id, 'workcenter_id': centro_e.id})

    def test_01_articulo_se_normaliza_y_es_unico(self):
        self.assertEqual(self.pl_rama.articulo, 'WJ038Q22JNT160')
        with self.assertRaises(ValidationError):
            self.env['mrp.hoja.calidad.plantilla'].create(
                {'articulo': 'WJ038Q22JNT160', 'tipo': 'rama', 'plantilla_id': self.blanco_rama.id})

    def test_02_registro_rama_crea_libro_desde_plantilla(self):
        accion = self.wo_rama.action_registro_rama()
        self.assertEqual(accion['tag'], 'action_open_spreadsheet')
        libro = self.env['mrp.hoja.calidad'].search([('articulo', '=', 'WJ038Q22JNT160'), ('tipo', '=', 'rama')])
        self.assertEqual(len(libro), 1)
        self.assertEqual(libro.numero, 1)
        self.assertEqual(accion['params']['spreadsheet_id'], libro.documento_id.id)
        self.assertNotEqual(libro.documento_id, self.blanco_rama)
        self.assertEqual(libro.documento_id.access_internal, 'edit')

    def test_03_segundo_clic_abre_el_mismo_libro(self):
        a = self.wo_rama.action_registro_rama()
        b = self.wo_rama.action_registro_rama()
        self.assertEqual(a['params']['spreadsheet_id'], b['params']['spreadsheet_id'])
        self.assertEqual(self.env['mrp.hoja.calidad'].search_count([('tipo', '=', 'rama')]), 1)

    def test_04_revision_usa_el_libro_de_inspeccion(self):
        wiz = self.env['mrp.hoja.revision.wizard'].create({'production_id': self.orden_m.id})
        accion = wiz.action_abrir()
        libro = self.env['mrp.hoja.calidad'].search([('tipo', '=', 'inspeccion')])
        self.assertEqual(accion['params']['spreadsheet_id'], libro.documento_id.id)
        self.assertEqual(self.env['mrp.hoja.calidad'].search_count([('tipo', '=', 'rama')]), 0)

    def test_05_sin_plantilla_avisa(self):
        orden = self.env['mrp.production'].create({
            'product_id': self.prod_otro.id, 'product_qty': 1,
            'bom_id': self.env['mrp.bom'].create({
                'product_tmpl_id': self.prod_otro.product_tmpl_id.id, 'product_qty': 1,
                'product_uom_id': self.prod_otro.uom_id.id}).id})
        wo = self.env['mrp.workorder'].create({'name': 'RAMA', 'production_id': orden.id,
                                               'workcenter_id': self.wo_rama.workcenter_id.id})
        with self.assertRaisesRegex(UserError, 'No hay plantilla'):
            wo.action_registro_rama()

    def test_06_botones_solo_en_su_operacion(self):
        with self.assertRaises(UserError):
            self.wo_clas.action_registro_rama()

    def test_07_cerrar_y_abrir_nuevo(self):
        self.wo_rama.action_registro_rama()
        viejo = self.env['mrp.hoja.calidad'].search([('tipo', '=', 'rama')])
        accion = viejo.with_user(self.env.ref('base.user_admin')).action_cerrar_y_abrir_nuevo()
        self.assertFalse(viejo.active)
        self.assertTrue(viejo.fecha_cierre)
        nuevo = self.env['mrp.hoja.calidad'].search([('tipo', '=', 'rama')])
        self.assertEqual(nuevo.numero, 2)
        self.assertNotEqual(nuevo, viejo)
        self.assertEqual(accion['params']['spreadsheet_id'], nuevo.documento_id.id)
        # el siguiente clic de Rama abre el nuevo
        self.assertEqual(self.wo_rama.action_registro_rama()['params']['spreadsheet_id'], nuevo.documento_id.id)
        with self.assertRaises(UserError):
            viejo.action_cerrar_y_abrir_nuevo()

    def test_08_operador_sin_permisos_de_documentos_abre_el_libro(self):
        user = self.env['res.users'].create({
            'name': 'Operador HC', 'login': 'op_hc', 'group_ids': [Command.set([
                self.env.ref('base.group_user').id, self.env.ref('mrp.group_mrp_user').id])]})
        accion = self.wo_rama.with_user(user).action_registro_rama()
        self.assertEqual(accion['tag'], 'action_open_spreadsheet')

    def test_09_articulo_desde_el_producto_del_catalogo(self):
        for prod in (self.prod_m, self.prod_kg):
            pl = self.env['mrp.hoja.calidad.plantilla'].new({'producto_id': prod.id, 'tipo': 'rama'})
            pl._compute_articulo()
            self.assertEqual(pl.articulo, 'WJ038Q22JNT160')
        nueva = self.env['mrp.hoja.calidad.plantilla'].create({
            'producto_id': self.prod_otro.id, 'tipo': 'rama', 'plantilla_id': self.blanco_rama.id})
        self.assertEqual(nueva.articulo, 'XX999')

    def test_10_menu_calidad_ordenes_y_usuario_de_calidad(self):
        user = self.env['res.users'].create({
            'name': 'Laboratorista HC', 'login': 'lab_hc', 'group_ids': [Command.set([
                self.env.ref('base.group_user').id, self.env.ref('quality.group_quality_user').id])]})
        wiz = self.env['mrp.hoja.revision.wizard'].with_user(user).create({'production_id': self.orden_m.id})
        # solo ofrece órdenes en metros con CLASIFICACION abierta
        self.assertIn(self.orden_m, wiz.production_domain_ids)
        self.assertNotIn(self.orden_kg, wiz.production_domain_ids)
        self.assertEqual(wiz.producto_id, self.prod_m)
        accion = wiz.action_abrir()
        self.assertEqual(accion['tag'], 'action_open_spreadsheet')
        self.assertTrue(self.env.ref('quimibond_hojas_calidad.menu_liberar_empaque'))
        self.assertTrue(self.env.ref('quimibond_hojas_calidad.menu_revision_tela_acabada'))
