# -*- coding: utf-8 -*-
"""C-006 (19.0.56.30.0): el pie de formato controlado y los formatos que el
código usa por referencia salen del DOCUMENTO ligado al mapeo, no del texto de
su clave. Un cambio de clave (clave nueva D-02) o de revisión se imprime solo."""
from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestFormatMapDocumento(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.Map = cls.env['sgi.format.map']
        cls.Doc = cls.env['documents.document']
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente C-006'})
        cls.product = cls.env['product.product'].create({
            'name': 'Tela C-006', 'type': 'consu', 'list_price': 10.0})
        cls.quote = cls._doc('F-P-A28-12', 3)
        cls.sale_map = cls.Map.search([('model_name', '=', 'sale.order')])
        cls.sale_map.write({'document_id': cls.quote.id, 'document_alt_id': False,
                            'sgi_code': 'F-P-A28-12', 'sgi_code_alt': False})

    @classmethod
    def _doc(cls, code, revision, state='vigente'):
        return cls.Doc.create({
            'name': '%s formato.xlsx' % code, 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'formulario_odoo',
            'sgi_code': code, 'sgi_revision': revision, 'sgi_state': state,
        })

    def _new_sale(self):
        return self.env['sale.order'].create({
            'partner_id': self.partner.id,
            'order_line': [(0, 0, {'product_id': self.product.id, 'product_uom_qty': 1})],
        })

    def _footer(self, record):
        return str(self.env['ir.qweb']._render(
            'quimibond_sgi.sgi_format_footer', {'sgi_rec': record}))

    def test_01_pie_con_clave_y_revision_vivas_tras_cambio_de_clave(self):
        order = self._new_sale()
        self.assertEqual(order.sgi_format_info(), "F-P-A28-12 · Rev. 03")
        # La clave del documento cambia (clave nueva): el pie la sigue sin
        # tocar el mapeo, aunque su «clave al ligar» siga siendo la vieja.
        self.quote.write({'sgi_code': 'F-ZT-12'})
        order.invalidate_recordset()
        self.assertEqual(order.sgi_format_info(), "F-ZT-12 · Rev. 03")
        html = self._footer(order)
        self.assertIn('F-ZT-12', html)
        self.assertIn('Rev. 03', html)
        self.assertNotIn('F-P-A28-12', html)
        self.assertEqual(self.sale_map.sgi_code, 'F-P-A28-12')

    def test_02_revision_nueva_se_imprime_y_el_mapeo_se_reapunta(self):
        order = self._new_sale()
        self.quote.sgi_state = 'obsoleto'
        new_rev = self._doc('F-P-A28-12', 4)
        order.invalidate_recordset()
        self.assertEqual(order.sgi_format_info(), "F-P-A28-12 · Rev. 04")
        self.assertEqual(self.sale_map.document_id, new_rev,
                         "Al entrar en vigor la revisión nueva, el mapeo apunta a ella.")

    def test_03_sin_vigente_imprime_la_clave_sola(self):
        order = self._new_sale()
        self.quote.sgi_state = 'obsoleto'
        order.invalidate_recordset()
        self.assertEqual(order.sgi_format_info(), "F-P-A28-12")

    def test_04_documento_ligado_no_se_borra(self):
        with self.assertRaises(Exception), self.cr.savepoint():
            self.quote.unlink()
            self.env.flush_all()

    def test_05_formatos_por_referencia(self):
        ref = self.env.ref('quimibond_sgi.format_ref_doc_change')
        self.assertFalse(ref.model_id, "Los formatos por referencia no llevan modelo.")
        doc = self._doc('F-P-G01-06', 2)
        ref.document_id = doc
        self.assertEqual(self.Map.sgi_ref_parts('format_ref_doc_change'), ('F-P-G01-06', '02'))
        doc.write({'sgi_code': 'F-ZT-06'})
        self.assertEqual(self.Map.sgi_ref_label('format_ref_doc_change'), "F-ZT-06 · Rev. 02")
        # Archivado el mapeo, nadie lo imprime.
        ref.active = False
        self.assertEqual(self.Map.sgi_ref_parts('format_ref_doc_change'), (False, False))
        self.assertFalse(self.Map.sgi_ref_label('no_existe'))

    def test_06_sin_documento_busca_por_clave_y_por_clave_anterior(self):
        ref = self.env.ref('quimibond_sgi.format_ref_epp_responsiva')
        ref.write({'document_id': False, 'sgi_code': 'F-P-S03-02'})
        doc = self._doc('F-P-S03-02', 1)
        self.assertEqual(self.Map.sgi_ref_label('format_ref_epp_responsiva'), "F-P-S03-02 · Rev. 01")
        doc.write({'sgi_code': 'F-ZT-02'})
        self.assertEqual(self.Map.sgi_ref_label('format_ref_epp_responsiva'), "F-ZT-02 · Rev. 01")

    def test_07_mapeo_exige_documento_o_clave(self):
        model = self.env['ir.model']._get('res.company')
        with self.assertRaises(ValidationError):
            self.Map.create({'model_id': model.id})
        fmap = self.Map.create({'model_id': model.id, 'document_id': self.quote.id})
        self.assertEqual(fmap.live_label, "F-P-A28-12 · Rev. 03")

    def test_08_reportes_por_referencia(self):
        # Solicitud de desarrollo: la clave sale del mapeo del tipo.
        doc = self._doc('F-P-D01-26', 5)
        self.env.ref('quimibond_sgi.format_ref_dev_carda').document_id = doc
        project = self.env['project.project'].create(
            {'name': 'FT-C006 prueba', 'sgi_dev_type': 'carda'})
        self.assertEqual(project.sgi_dev_format_code, 'F-P-D01-26')
        doc.write({'sgi_code': 'F-ZT-26'})
        project.invalidate_recordset()
        self.assertEqual(project.sgi_dev_format_info(), "F-ZT-26 · Rev. 05")
        # Pie del procedimiento impreso.
        g0102 = self._doc('F-P-G01-02', 7)
        self.env.ref('quimibond_sgi.format_ref_procedure_print').document_id = g0102
        process = self.env['sgi.process'].search([], limit=1)
        if process:
            self.assertEqual(process._sgi_format_parts('format_ref_procedure_print'),
                             ('F-P-G01-02', '07'))
