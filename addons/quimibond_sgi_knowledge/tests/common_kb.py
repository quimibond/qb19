# -*- coding: utf-8 -*-
"""Armado común de las pruebas de 1.2.0 (el SGI en Conocimiento).

La base del build es copia de producción: los documentos reales se ocultan
(``sgi_hide_real_documents``) y todo lo demás se crea con claves ZK… que no
existen. Ninguna prueba cuenta registros globales."""
import base64

from odoo.tests import TransactionCase, new_test_user

from odoo.addons.quimibond_sgi.tests.common_documents import sgi_hide_real_documents
from odoo.addons.quimibond_sgi.tests.common_users import sgi_set_mast

FAKE_PDF = b'%PDF-1.4\n% ZK SGI Conocimiento\n'


class KbCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        env = cls.env
        cls.Article = env['knowledge.article']
        cls.Doc = env['documents.document']
        cls.company = env['sgi.config']._sgi_company()
        cls.mast = sgi_set_mast(env, login='zk_kb_mast')
        cls.owner = new_test_user(env, login='zk_kb_owner', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.plain = new_test_user(env, login='zk_kb_plain', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner_emp = env['hr.employee'].create({
            'name': 'ZK Dueño', 'user_id': cls.owner.id, 'company_id': cls.company.id})
        cls.job = env['hr.job'].create({'name': 'ZK PUESTO CONOCIMIENTO'})
        cls.worker = env['hr.employee'].create({'name': 'ZK Operador', 'job_id': cls.job.id})
        cls.process = env['sgi.process'].create({
            'code': 'ZK2', 'name': 'Proceso ZK Conocimiento', 'process_type': 'cop',
            'owner_id': cls.owner_emp.id, 'company_id': cls.company.id})

    @classmethod
    def _doc(cls, code, doc_type, title, **vals):
        return cls.Doc.create(dict({
            'name': '%s %s.pdf' % (code, title), 'type': 'binary',
            'datas': base64.b64encode(FAKE_PDF), 'mimetype': 'application/pdf',
            'sgi_is_controlled': True, 'sgi_doc_type': doc_type, 'sgi_code': code,
            'sgi_state': 'vigente', 'sgi_revision': 0, 'sgi_process_id': cls.process.id,
            'company_id': cls.company.id}, **vals))

    @classmethod
    def _activity(cls, name, doc=None, job=None, **vals):
        return cls.env['sgi.process.activity'].create(dict({
            'process_id': cls.process.id, 'name': name,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': (job or cls.job).id})],
            'instruction_id': doc.id if doc else False}, **vals))

    def _import(self, docs, user=None):
        wizard = self.env['sgi.knowledge.import'].with_user(user or self.mast).create({
            'preset': 'manual', 'document_ids': [(6, 0, docs.ids)]})
        wizard.action_import()
        return wizard

    def _article(self, code):
        return self.Article.with_context(active_test=False).search([('sgi_document_code', '=', code)])
