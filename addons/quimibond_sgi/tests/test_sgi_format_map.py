# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged
from odoo.exceptions import ValidationError
from odoo.tools import mute_logger


@tagged('post_install', '-at_install')
class TestSgiFormatMap(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Map = cls.env['sgi.format.map']
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente Formato'})
        cls.product = cls.env['product.product'].create({
            'name': 'Tela de prueba', 'type': 'consu', 'list_price': 10.0,
        })
        # Documento vigente para la clave de cotización (rev viva en Documentos)
        cls.doc_quote = cls.env['documents.document'].create({
            'name': 'F-P-A28-04 COTIZACION.xlsx',
            'type': 'binary',
            'sgi_is_controlled': True,
            'sgi_doc_type': 'formato',
            'sgi_code': 'F-P-A28-04',
            'sgi_revision': '03',
            'sgi_state': 'vigente',
        })

    def _new_sale(self):
        return self.env['sale.order'].create({
            'partner_id': self.partner.id,
            'order_line': [(0, 0, {'product_id': self.product.id, 'product_uom_qty': 1})],
        })

    def test_01_revision_viva_desde_documentos(self):
        order = self._new_sale()
        self.assertEqual(order.sgi_format_info(), "F-P-A28-04 · Rev. 03")
        # Sube la revisión en Documentos -> el registro la refleja sin tocar nada
        self.doc_quote.sgi_state = 'obsoleto'
        self.env['documents.document'].create({
            'name': 'F-P-A28-04 COTIZACION.xlsx',
            'type': 'binary',
            'sgi_is_controlled': True,
            'sgi_doc_type': 'formato',
            'sgi_code': 'F-P-A28-04',
            'sgi_revision': '04',
            'sgi_state': 'vigente',
        })
        order.invalidate_recordset()
        self.assertEqual(order.sgi_format_info(), "F-P-A28-04 · Rev. 04")

    def test_02_clave_sin_documento_vigente(self):
        # La OC está mapeada (F-P-A02-01) pero no cargamos su documento en el
        # test: debe degradar a la clave sola, sin excepción.
        po = self.env['purchase.order'].create({'partner_id': self.partner.id})
        self.assertEqual(po.sgi_format_info(), "F-P-A02-01")

    def test_03_venta_clave_por_estado(self):
        order = self._new_sale()
        self.assertTrue(order.sgi_format_banner.startswith("F-P-A28-04"))
        order.action_confirm()
        order.invalidate_recordset()
        self.assertTrue(order.sgi_format_banner.startswith("F-P-A28-03"))

    def test_04_picking_solo_salidas(self):
        wh = self.env['stock.warehouse'].search([], limit=1)
        pick_in = self.env['stock.picking'].create({
            'picking_type_id': wh.in_type_id.id,
            'location_id': self.env.ref('stock.stock_location_suppliers').id,
            'location_dest_id': wh.lot_stock_id.id,
        })
        self.assertFalse(pick_in.sgi_format_banner)
        pick_out = self.env['stock.picking'].create({
            'picking_type_id': wh.out_type_id.id,
            'location_id': wh.lot_stock_id.id,
            'location_dest_id': self.env.ref('stock.stock_location_customers').id,
        })
        self.assertTrue(pick_out.sgi_format_banner.startswith("F-P-A16-01"))

    def test_05_modelo_sin_mapeo(self):
        self.Map.search([('model_name', '=', 'sale.order')]).unlink()
        order = self._new_sale()
        self.assertFalse(order.sgi_format_banner)

    def test_06_mapeo_unico_por_modelo(self):
        model = self.env['ir.model']._get('sale.order')
        with self.assertRaises(Exception), self.cr.savepoint(), \
                mute_logger('odoo.sql_db'):
            self.Map.create({'model_id': model.id, 'sgi_code': 'F-P-A28-04'})
            self.env.flush_all()

    def test_07_clave_invalida(self):
        model = self.env['ir.model']._get('res.company')
        with self.assertRaises(ValidationError):
            self.Map.create({'model_id': model.id, 'sgi_code': 'FORMATO-XYZ'})

    def test_08_render_pie_de_pagina(self):
        order = self._new_sale()
        html = str(self.env['ir.qweb']._render(
            'quimibond_sgi.sgi_format_footer', {'sgi_rec': order}))
        self.assertIn('F-P-A28-04', html)
        self.assertIn('Rev. 03', html)
        # Sin registro -> el pie no pinta nada
        empty = str(self.env['ir.qweb']._render(
            'quimibond_sgi.sgi_format_footer', {'sgi_rec': False}))
        self.assertNotIn('Formato controlado', empty)
        # Las herencias de los reportes nativos existen
        for xmlid in ('quimibond_sgi.report_saleorder_document_sgi',
                      'quimibond_sgi.report_purchaseorder_document_sgi',
                      'quimibond_sgi.report_delivery_document_sgi'):
            self.assertTrue(self.env.ref(xmlid))
