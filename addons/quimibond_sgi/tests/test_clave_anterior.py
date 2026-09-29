# -*- coding: utf-8 -*-
"""Clave anterior y clave nueva (19.0.56.32.0): C-004 + L-004, C-005, C-007,
C-009 y J-006. La clave del Dropbox queda como clave anterior definitiva, se
busca siempre y un renombre posterior no la pisa; la clave nueva sigue los
patrones de D-02 (PR-{proceso} para procedimientos)."""
from dateutil.relativedelta import relativedelta

from odoo import fields
from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestClaveAnterior(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.Doc = cls.env['documents.document']
        cls.Process = cls.env['sgi.process']
        cls.proc = cls.Process.create({'code': 'XC1', 'name': 'Proceso XC1'})
        cls.proc2 = cls.Process.create({'code': 'XC2', 'name': 'Proceso XC2'})
        cls.t_proc = cls.env.ref('quimibond_sgi.sgi_doc_type_procedimiento')
        cls.t_fmt = cls.env.ref('quimibond_sgi.sgi_doc_type_formato')
        cls.t_form = cls.env.ref('quimibond_sgi.sgi_doc_type_formulario_odoo')
        cls.t_ext = cls.env.ref('quimibond_sgi.sgi_doc_type_externo')

    def _doc(self, code, dtype, name=None, process=None, **extra):
        vals = {'name': name or '%s documento.pdf' % code, 'type': 'binary',
                'sgi_is_controlled': True, 'sgi_doc_type_id': dtype.id,
                'sgi_code': code, 'sgi_state': 'vigente',
                'sgi_process_id': (process or self.proc).id}
        vals.update(extra)
        return self.Doc.create(vals)

    def test_01_migracion_idempotente(self):
        proc = self._doc('P-A71', self.t_proc)
        fmt = self._doc('F-P-A71-01', self.t_fmt)
        form = self._doc('F-P-A71-02', self.t_form)            # L-004: sí entra
        archived = self._doc('F-P-A71-03', self.t_fmt, active=False)
        external = self._doc('NOM-001', self.t_ext)           # fuera
        bad = self._doc('F-P-A71-04', self.t_fmt)            # excluido por id
        kept = self._doc('F-P-A71-05', self.t_fmt, sgi_previous_code='F-VIEJA-05')
        mine = proc | fmt | form | archived | external | bad | kept
        count = self.Doc._sgi_migrate_previous_codes(exclude_ids=bad.ids, ids=mine.ids)
        self.assertEqual(count, 4, "procedimiento, formato, formulario de Odoo y archivado.")
        for doc in (proc, fmt, form, archived):
            self.assertEqual(doc.sgi_previous_code, doc.sgi_code)
            self.assertFalse(doc.sgi_previous_code_date, "Sin fecha: clave del Dropbox.")
        self.assertFalse(external.sgi_previous_code)
        self.assertFalse(bad.sgi_previous_code, "El excluido (5556 en producción) no entra.")
        self.assertEqual(kept.sgi_previous_code, 'F-VIEJA-05', "No pisa una clave anterior.")
        self.assertEqual(proc.sgi_code, 'P-A71', "La clave vigente no cambia.")
        self.assertEqual(self.Doc._sgi_migrate_previous_codes(exclude_ids=bad.ids, ids=mine.ids), 0)

    def test_02_doble_renombre_conserva_la_clave_del_dropbox(self):
        doc = self._doc('F-P-A72-01', self.t_fmt)
        self.Doc._sgi_migrate_previous_codes(ids=doc.ids)
        new_code = doc._sgi_assign_new_code()
        self.assertEqual(new_code, 'F-XC1-01')
        self.assertEqual(doc.sgi_code, 'F-XC1-01')
        doc.write({'sgi_code': 'F-XC1-07'})
        self.assertEqual(doc.sgi_previous_code, 'F-P-A72-01', "El segundo renombre no la pisa.")
        self.assertFalse(doc.sgi_previous_code_date)
        self.assertEqual(self.Doc._sgi_find_by_code('F-P-A72-01'), doc)

    def test_03_busqueda_por_clave_anterior_sin_limite(self):
        doc = self._doc('F-P-A73-01', self.t_fmt)
        doc.write({'sgi_code': 'F-XC1-02'})       # renombre en Odoo: con fecha
        self.assertEqual(doc.sgi_previous_code, 'F-P-A73-01')
        self.assertTrue(doc.sgi_previous_code_date)
        doc.sgi_previous_code_date = fields.Date.context_today(doc) - relativedelta(months=13)
        self.assertEqual(self.Doc._sgi_find_by_code('F-P-A73-01'), doc,
                         "13 meses después la clave anterior sigue encontrando el documento.")
        found = self.Doc.search([('sgi_previous_code', 'ilike', 'F-P-A73-01')])
        self.assertEqual(found, doc)

    def test_04_clave_nueva_pr_valida_y_la_del_dropbox_no(self):
        self.assertTrue(self.t_proc._sgi_new_code_ok('PR-XC1', self.proc))
        self.assertFalse(self.t_proc._sgi_new_code_ok('P-C01', self.proc))
        self.assertFalse(self.t_proc._sgi_new_code_ok('PR-XC2', self.proc), "Otro proceso.")
        self.assertTrue(self.t_proc._sgi_code_ok('P-C01', self.proc),
                        "La del Dropbox solo vale como clave heredada de lo que ya existe.")
        doc = self._doc('P-A74', self.t_proc)
        with self.assertRaises(UserError):
            doc._sgi_assign_new_code('P-C01')
        self.assertEqual(doc._sgi_assign_new_code(), 'PR-XC1')
        other = self._doc('P-A75', self.t_proc)
        with self.assertRaises(UserError):
            other._sgi_assign_new_code('PR-XC1')   # repetida
        with self.assertRaises(ValidationError), self.cr.savepoint():
            self._doc('PX-1', self.t_proc)          # validación encendida

    def test_05_tipos_con_nomenclatura_d02(self):
        self.assertEqual(self.t_proc.prefix_pattern, 'PR-{process}')
        self.assertTrue(self.t_proc.code_required and self.t_proc.requires_process)
        self.assertTrue(self.t_fmt.code_required and self.t_fmt.requires_process)
        self.assertEqual(self.env.ref('quimibond_sgi.sgi_doc_type_formato_it').prefix_pattern,
                         'F-{process}-{seq:02d}')
        self.assertEqual(self.env.ref('quimibond_sgi.sgi_doc_type_dat').prefix_pattern,
                         'DA-{process}-{seq:02d}')

    def test_06_clave_nueva_a_todas_las_revisiones(self):
        old = self._doc('F-P-A76-01', self.t_fmt, sgi_state='obsoleto', sgi_revision=0)
        live = self._doc('F-P-A76-01', self.t_fmt, sgi_revision=1)
        self.Doc._sgi_migrate_previous_codes(ids=(old | live).ids)
        live._sgi_assign_new_code()
        self.assertEqual(old.sgi_code, live.sgi_code)
        self.assertEqual(old.sgi_previous_code, 'F-P-A76-01')
        # Los sustituidos, los formularios de Odoo y los obsoletos no reciben clave nueva.
        form = self._doc('F-P-A76-02', self.t_form)
        with self.assertRaises(UserError):
            form._sgi_assign_new_code()
        with self.assertRaises(UserError):
            old._sgi_assign_new_code()

    def test_07_titulo_limpio(self):
        doc = self._doc('P-A77', self.t_proc, name='P-A77 VENTAS.pdf')
        self.assertEqual(doc.sgi_title, 'VENTAS')
        loose = self._doc('F-P-A77-16', self.t_fmt, name='F-P-A-77-16 BITACORA.xlsx')
        self.assertEqual(loose.sgi_title, 'BITACORA', "Tolera «F-P-A-77» contra F-P-A77.")
        other = self._doc('F-P-A77-17', self.t_fmt, name='15. Lista de algo.xlsx')
        self.assertEqual(other.sgi_title, '15. Lista de algo')
        self.Doc._sgi_migrate_previous_codes(ids=doc.ids)
        doc._sgi_assign_new_code()
        self.env.flush_all()
        self.assertEqual(doc.sgi_title, 'VENTAS', "Con la clave nueva quita la del Dropbox.")
        self.assertEqual(doc.name, 'P-A77 VENTAS.pdf', "El archivo no se renombra.")
