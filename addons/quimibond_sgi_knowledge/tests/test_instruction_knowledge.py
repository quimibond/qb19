# -*- coding: utf-8 -*-
"""DOC-5: el instructivo de una actividad escrito en Knowledge y publicado
como revisión del IT. Mudada de quimibond_sgi/tests/test_pr6_external.py
(test_05) en 57.9.0 (A-014, J-018); aquí Knowledge siempre está."""
import base64

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user

from odoo.addons.quimibond_sgi.tests.common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestInstructionKnowledge(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.process = cls.env['sgi.process'].create({'code': 'XP6', 'name': 'Proceso PR6'})

    def test_05_doc5_instructivo_desde_knowledge(self):
        manager = new_test_user(self.env, login='pr6_mast',
                                groups='base.group_user,quimibond_sgi.group_sgi_manager')
        article = self.env['knowledge.article'].create({
            'name': 'Cómo enhebrar la urdidora', 'body': '<p>Paso 1: apagar. Paso 2: enhebrar.</p>'})
        job = self.env['hr.job'].create({'name': 'URDIDOR PR6'})
        self.env['hr.employee'].create({'name': 'Urdidor PR6', 'job_id': job.id})
        activity = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Enhebrar urdidora',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})],
            'instruction_article_id': article.id})
        wiz = self.env['sgi.instruction.publish'].with_user(manager).create({
            'activity_id': activity.id, 'code': 'IT-P-C11-95'})
        self.assertEqual(wiz.job_ids, job, "Propone los puestos que ejecutan.")
        wiz.action_publish()
        doc = activity.instruction_id
        self.assertTrue(doc and doc.sgi_doc_type == 'instructivo')
        self.assertEqual(doc.sgi_code, 'IT-P-C11-95')
        self.assertEqual(doc.sgi_revision, 0)
        self.assertEqual(doc.sgi_article_id, article)
        self.assertTrue(base64.b64decode(doc.datas))
        self.assertTrue(doc.sgi_ack_ids, "Acuses para el puesto.")
        self.assertFalse(activity.instruction_article_stale)
        with self.assertRaises(UserError):
            self.env['sgi.instruction.publish'].with_user(manager).create({
                'activity_id': activity.id, 'code': 'IT-P-C11-95'}).action_publish()
        article.body = '<p>Paso 1: apagar. Paso 2: enhebrar. Paso 3: probar.</p>'
        self.assertTrue(activity.instruction_article_stale)
        self.env['sgi.instruction.publish'].with_user(manager).create({
            'activity_id': activity.id, 'code': 'IT-P-C11-95'}).action_publish()
        self.assertEqual(activity.instruction_id.sgi_revision, 1)
        self.assertEqual(doc.sgi_state, 'obsoleto')
