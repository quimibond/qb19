# -*- coding: utf-8 -*-
"""57.61.0 — mapeos de formato por tipo de operación de C4 y C6
(post-migrate): localiza formato por clave y tipos por nombre, no pisa lo que
existe, salta lo que no encuentra y es idempotente."""
import importlib.util
import os

from odoo.tests import TransactionCase, tagged

from ..models.sgi_document import SGI_CODE_REGEX
from ..models.sgi_format_map_seed import SGI_OPERATION_FORMAT_MAPS
from .common_documents import sgi_hide_real_documents

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@tagged('post_install', '-at_install')
class TestFormatMapOperaciones(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.Map = cls.env['sgi.format.map']
        # En una copia de producción ya existen los mapeos reales de estas
        # claves (57.61.0): se quitan dentro de la prueba (se deshace al final).
        cls.Map.with_context(active_test=False).search([('sgi_code', 'in', (
            'F-P-P02-01', 'F-IT-P-A07-01-02', 'F-IT-P-A05-01-06', 'F-IT-P-A07-01-01',
            'F-IT-P-P01-12-01'))]).unlink()
        cls.company = cls.env.company
        wh = cls.env['stock.warehouse'].search([('company_id', '=', cls.company.id)], limit=1)
        cls.wh = wh
        cls.t_carda = wh.manu_type_id.copy({'name': 'ZQ Carda prueba', 'sequence_code': 'ZQC'})
        cls.t_v10 = wh.manu_type_id.copy({'name': 'ZQ V10 prueba', 'sequence_code': 'ZQV'})
        cls.t_int = wh.int_type_id.copy({'name': 'ZQ Reetiquetado prueba', 'sequence_code': 'ZQR'})
        Doc = cls.env['documents.document']

        def doc(code):
            return Doc.create({'name': '%s prueba.xlsx' % code, 'type': 'binary',
                               'sgi_is_controlled': True, 'sgi_doc_type': 'formato',
                               'sgi_code': code, 'sgi_revision': 2, 'sgi_state': 'vigente'})
        cls.doc_ot = doc('F-P-P02-01')
        cls.doc_ree = doc('F-IT-P-A07-01-02')
        cls.doc_dev = doc('F-IT-P-A05-01-06')
        cls.table = (
            ('mrp.production', 'F-P-P02-01', ('ZQ Carda prueba', 'ZQ V10 prueba'), '', 10, 'OT'),
            ('stock.picking', 'F-IT-P-A07-01-02', ('ZQ Reetiquetado prueba',), '', 10, 'Reet.'),
            ('stock.picking', 'F-IT-P-A05-01-06', (),
             "[('location_dest_id.usage', '=', 'supplier')]", 20, 'Dev.'),
            ('stock.picking', 'F-IT-P-A07-01-01', ('ZQ Reetiquetado prueba',), '', 10, 'Sin doc'),
            ('mrp.production', 'F-IT-P-P01-12-01', ('ZQ No existe',), '', 10, 'Sin tipo'),
        )

    def test_01_crea_lo_que_falta_e_idempotente(self):
        created = self.Map._sgi_seed_operation_maps(self.table, company=self.company)
        self.assertEqual(created, ['F-P-P02-01', 'F-IT-P-A07-01-02', 'F-IT-P-A05-01-06'])
        ot = self.Map.search([('document_id', '=', self.doc_ot.id)])
        self.assertEqual(ot.picking_type_ids, self.t_carda | self.t_v10)
        self.assertFalse(ot.is_general)
        dev = self.Map.search([('document_id', '=', self.doc_dev.id)])
        self.assertEqual(dev.sequence, 20)
        self.assertIn('supplier', dev.record_domain)
        # La orden de carda imprime su clave con la revisión del documento.
        mo = self.env['mrp.production'].create({
            'product_id': self.env['product.product'].create(
                {'name': 'ZQ tela', 'type': 'consu'}).id,
            'product_qty': 1, 'picking_type_id': self.t_carda.id})
        self.assertEqual(mo.sgi_format_info(), "F-P-P02-01 · Rev. 02")
        # Segunda corrida: nada nuevo.
        self.assertEqual(self.Map._sgi_seed_operation_maps(self.table, company=self.company), [])

    def test_02_no_pisa_mapeo_existente(self):
        m_mo = self.env['ir.model']._get('mrp.production')
        mine = self.Map.create({'model_id': m_mo.id, 'document_id': self.doc_ot.id,
                                'sgi_code': 'F-P-P02-01', 'active': False,
                                'record_domain': "[('id', '=', 0)]"})
        created = self.Map._sgi_seed_operation_maps(self.table, company=self.company)
        self.assertNotIn('F-P-P02-01', created)
        self.assertEqual(mine.record_domain, "[('id', '=', 0)]")
        self.assertFalse(mine.active)

    def test_03_tabla_real(self):
        codes = [row[1] for row in SGI_OPERATION_FORMAT_MAPS]
        self.assertEqual(len(codes), len(set(codes)), "Clave repetida en la tabla.")
        for model_name, code, types, domain, _seq, note in SGI_OPERATION_FORMAT_MAPS:
            self.assertIn(model_name, self.env)
            self.assertTrue(SGI_CODE_REGEX.match(code), code)
            self.assertTrue(types or domain, "%s: sin criterio sería otro general." % code)
            self.assertTrue(note)

    def test_04_post_migrate_corre(self):
        path = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.61.0', 'post-migrate.py')
        spec = importlib.util.spec_from_file_location('sgi_mig_57_61_0', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.migrate(self.env.cr, '19.0.57.60.0')
        self.assertEqual(self.Map._sgi_seed_operation_maps(), [])
