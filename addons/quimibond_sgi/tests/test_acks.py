# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import ValidationError


@tagged('post_install', '-at_install')
class TestDocsAndAcks(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Doc = cls.env['documents.document']
        cls.job = cls.env['hr.job'].create({'name': 'Inspector de Calidad SGI'})
        cls.emp1 = cls.env['hr.employee'].create({'name': 'Emp Uno', 'job_id': cls.job.id})
        cls.emp2 = cls.env['hr.employee'].create({'name': 'Emp Dos', 'job_id': cls.job.id})
        cls.emp_other = cls.env['hr.employee'].create({'name': 'Emp Otro'})

    def _new_doc(self, **vals):
        base = {'name': 'Doc prueba', 'type': 'binary'}
        base.update(vals)
        return self.Doc.create(base)

    def test_01_invalid_code(self):
        with self.assertRaises(ValidationError):
            self._new_doc(sgi_is_controlled=True, sgi_doc_type='procedimiento', sgi_code='INVALIDO')

    def test_02_valid_code(self):
        doc = self._new_doc(sgi_is_controlled=True, sgi_doc_type='procedimiento', sgi_code='P-G01')
        self.assertTrue(doc.id)

    def test_03_unique_vigente_obsoletes_previous(self):
        doc1 = self._new_doc(sgi_is_controlled=True, sgi_doc_type='procedimiento',
                             sgi_code='P-C11', sgi_state='vigente')
        self.assertEqual(doc1.sgi_state, 'vigente')
        doc2 = self._new_doc(sgi_is_controlled=True, sgi_doc_type='procedimiento',
                             sgi_code='P-C11', sgi_state='vigente')
        self.assertEqual(doc2.sgi_state, 'vigente')
        self.assertEqual(doc1.sgi_state, 'obsoleto')

    def test_04_generate_acks_idempotent(self):
        doc = self._new_doc(sgi_is_controlled=True, sgi_doc_type='procedimiento',
                            sgi_code='IT-P-C11-01', sgi_state='vigente',
                            sgi_job_ids=[(6, 0, [self.job.id])])
        doc.action_generate_acks()
        acks = doc.sgi_ack_ids
        self.assertEqual(len(acks), 2)
        self.assertEqual(set(acks.mapped('employee_id')), {self.emp1, self.emp2})
        self.assertNotIn(self.emp_other, acks.mapped('employee_id'))
        # Idempotente
        doc.action_generate_acks()
        self.assertEqual(len(doc.sgi_ack_ids), 2)

    def test_05_mark_read(self):
        doc = self._new_doc(sgi_is_controlled=True, sgi_doc_type='procedimiento',
                            sgi_code='F-P-G05-01', sgi_state='vigente',
                            sgi_job_ids=[(6, 0, [self.job.id])])
        doc.action_generate_acks()
        ack = doc.sgi_ack_ids[0]
        # A2: el empleado no tiene usuario ligado → un usuario cualquiera no
        # puede firmarlo; MAST sí.
        from odoo.exceptions import UserError
        someone = self.env['res.users'].create({
            'name': 'Firmador ajeno', 'login': 'sgi_ack_test',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_user').id])]})
        with self.assertRaises(UserError):
            ack.with_user(someone).action_mark_read()
        manager = self.env['res.users'].create({
            'name': 'MAST acuses', 'login': 'sgi_ack_mgr_test',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_manager').id])]})
        ack.with_user(manager).action_mark_read()
        self.assertEqual(ack.state, 'leido')
        self.assertTrue(ack.ack_date)
