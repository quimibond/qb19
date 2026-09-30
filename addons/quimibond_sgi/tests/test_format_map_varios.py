# -*- coding: utf-8 -*-
"""57.60.0 (bloque 2 de formularios): varios formatos por modelo en
``sgi.format.map``, elegidos por tipo de operación, centro de trabajo,
categoría de producto o filtro, con prioridad; el general sigue aplicando al
registro que no cumple ningún criterio."""
from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestFormatMapVarios(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.Map = cls.env['sgi.format.map']
        # Los mapeos de producción de estos modelos se archivan en la prueba
        # (se deshace al final): la prueba siembra los suyos.
        cls.Map.search([('model_name', 'in', ('mrp.production', 'stock.picking'))]).write(
            {'active': False})
        cls.m_mo = cls.env['ir.model']._get('mrp.production')
        cls.m_pick = cls.env['ir.model']._get('stock.picking')
        cls.wh = cls.env['stock.warehouse'].search(
            [('company_id', '=', cls.env.company.id)], limit=1)
        cls.type_a = cls.wh.manu_type_id
        cls.type_b = cls.type_a.copy({'name': 'Cocina prueba formato', 'sequence_code': 'ZCOC'})
        cls.categ = cls.env['product.category'].create({'name': 'Entretelas prueba formato'})
        cls.categ_child = cls.env['product.category'].create(
            {'name': 'Hija prueba formato', 'parent_id': cls.categ.id})
        cls.product = cls.env['product.product'].create(
            {'name': 'Tela prueba formato', 'type': 'consu'})
        cls.product_categ = cls.env['product.product'].create(
            {'name': 'Entretela prueba formato', 'type': 'consu',
             'categ_id': cls.categ_child.id})
        cls.general_mo = cls.Map.create({
            'model_id': cls.m_mo.id, 'sgi_code': 'F-IT-P-P01-08-01', 'note': 'General prueba'})
        cls.general_pick = cls.Map.create({
            'model_id': cls.m_pick.id, 'sgi_code': 'F-P-A16-01', 'note': 'General prueba'})

    def _mo(self, ptype=None, product=None):
        return self.env['mrp.production'].create({
            'product_id': (product or self.product).id, 'product_qty': 1,
            'picking_type_id': (ptype or self.type_a).id})

    def _picking(self, ptype, dest=None):
        return self.env['stock.picking'].create({
            'picking_type_id': ptype.id,
            'location_id': self.wh.lot_stock_id.id,
            'location_dest_id': (dest or ptype.default_location_dest_id or self.wh.lot_stock_id).id,
        })

    def test_01_general_sin_criterio(self):
        self.assertTrue(self.general_mo.is_general)
        self.assertEqual(self.general_mo.selector_label, "General del modelo")
        self.assertEqual(self._mo().sgi_format_info(), "F-IT-P-P01-08-01")

    def test_02_por_tipo_de_operacion(self):
        fmap = self.Map.create({
            'model_id': self.m_mo.id, 'sgi_code': 'F-IT-P-P01-02-02',
            'picking_type_ids': [(6, 0, self.type_b.ids)]})
        self.assertFalse(fmap.is_general)
        self.assertIn('Cocina prueba formato', fmap.selector_label)
        self.assertEqual(self._mo(self.type_b).sgi_format_info(), "F-IT-P-P01-02-02")
        self.assertEqual(self._mo(self.type_a).sgi_format_info(), "F-IT-P-P01-08-01",
                         "Otro tipo de operación sigue con el general.")

    def test_03_prioridad(self):
        self.Map.create({
            'model_id': self.m_mo.id, 'sgi_code': 'F-P-P02-01', 'sequence': 20,
            'picking_type_ids': [(6, 0, self.type_b.ids)]})
        self.Map.create({
            'model_id': self.m_mo.id, 'sgi_code': 'F-IT-P-P01-12-01', 'sequence': 5,
            'picking_type_ids': [(6, 0, self.type_b.ids)]})
        self.assertEqual(self._mo(self.type_b).sgi_format_info(), "F-IT-P-P01-12-01")

    def test_04_categoria_con_subcategorias(self):
        self.Map.create({
            'model_id': self.m_mo.id, 'sgi_code': 'F-P-P02-01',
            'product_categ_ids': [(6, 0, self.categ.ids)]})
        self.assertEqual(self._mo(product=self.product_categ).sgi_format_info(), "F-P-P02-01")
        self.assertEqual(self._mo().sgi_format_info(), "F-IT-P-P01-08-01")

    def test_05_transferencia_interna_con_su_clave(self):
        internal = self.wh.int_type_id
        # Sin mapeo propio, la interna no lleva la clave (el general es solo
        # para salidas, como antes).
        self.assertFalse(self._picking(internal).sgi_format_banner)
        self.Map.create({
            'model_id': self.m_pick.id, 'sgi_code': 'F-IT-P-A07-01-02',
            'picking_type_ids': [(6, 0, internal.ids)]})
        self.assertEqual(self._picking(internal).sgi_format_info(), "F-IT-P-A07-01-02")
        out = self._picking(self.wh.out_type_id, self.env.ref('stock.stock_location_customers'))
        self.assertEqual(out.sgi_format_info(), "F-P-A16-01")

    def test_06_filtro_adicional(self):
        supplier = self.env.ref('stock.stock_location_suppliers')
        self.Map.create({
            'model_id': self.m_pick.id, 'sgi_code': 'F-IT-P-A05-01-06',
            'record_domain': "[('location_dest_id.usage', '=', 'supplier')]"})
        ret = self._picking(self.wh.out_type_id, supplier)
        self.assertEqual(ret.sgi_format_info(), "F-IT-P-A05-01-06")
        out = self._picking(self.wh.out_type_id, self.env.ref('stock.stock_location_customers'))
        self.assertEqual(out.sgi_format_info(), "F-P-A16-01")

    def test_07_un_general_por_modelo(self):
        with self.assertRaises(ValidationError):
            self.Map.create({'model_id': self.m_mo.id, 'sgi_code': 'F-P-P01-01'})
        # Archivado el general, otro general sí entra.
        self.general_mo.active = False
        other = self.Map.create({'model_id': self.m_mo.id, 'sgi_code': 'F-P-P01-01'})
        self.assertTrue(other.is_general)

    def test_08_criterios_invalidos(self):
        with self.assertRaises(ValidationError):
            self.Map.create({'sgi_code': 'F-P-P01-01',
                             'picking_type_ids': [(6, 0, self.type_b.ids)]})
        m_lot = self.env['ir.model']._get('stock.lot')
        with self.assertRaises(ValidationError):
            self.Map.create({'model_id': m_lot.id, 'sgi_code': 'F-P-C04-02',
                             'picking_type_ids': [(6, 0, self.type_b.ids)]})
        with self.assertRaises(ValidationError):
            self.Map.create({'model_id': self.m_mo.id, 'sgi_code': 'F-P-P01-01',
                             'record_domain': "[('no_existe', '=', 1)]"})

    def test_09_pie_del_reporte(self):
        self.Map.create({
            'model_id': self.m_mo.id, 'sgi_code': 'F-IT-P-P01-02-02',
            'picking_type_ids': [(6, 0, self.type_b.ids)]})
        mo = self._mo(self.type_b)
        self.assertEqual(self.Map.sgi_footer_label(mo), "F-IT-P-P01-02-02")
        html = str(self.env['ir.qweb']._render(
            'quimibond_sgi.sgi_format_footer', {'sgi_rec': mo}))
        self.assertIn('F-IT-P-P01-02-02', html)
        self.assertTrue(self.env.ref('quimibond_sgi.report_mrporder_sgi'))

    def test_10_get_for_model_da_el_general(self):
        self.Map.create({
            'model_id': self.m_mo.id, 'sgi_code': 'F-IT-P-P01-02-02', 'sequence': 1,
            'picking_type_ids': [(6, 0, self.type_b.ids)]})
        self.assertEqual(self.Map._get_for_model('mrp.production'), self.general_mo)
