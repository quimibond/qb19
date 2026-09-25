# -*- coding: utf-8 -*-
"""PR 4 del plan (19.0.51.0.0): DOC-1 publicación en un paso, DOC-3 lista
maestra en PDF y DIR-1 requisitos legales con evaluación y vencimiento."""
import base64
from datetime import date, timedelta

from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestPr4DocsLegal(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.category = cls.env['approval.category'].create({
            'name': 'Cambio documental PR4', 'sgi_is_doc_change': True, 'approval_minimum': 1})
        cls.job = cls.env['hr.job'].create({'name': 'OPERADOR PR4'})
        cls.user_emp = new_test_user(cls.env, login='pr4_emp',
                                     groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Emp PR4', 'job_id': cls.job.id, 'user_id': cls.user_emp.id})
        cls.owner_user = new_test_user(cls.env, login='pr4_owner',
                                       groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner = cls.env['hr.employee'].create({'name': 'Dueño PR4', 'user_id': cls.owner_user.id})
        cls.process = cls.env['sgi.process'].create({
            'code': 'XPR4', 'name': 'Proceso PR4', 'owner_id': cls.owner.id})
        cls.doc = cls.env['documents.document'].create({
            'name': 'P-A81 Procedimiento PR4.pdf', 'type': 'binary',
            'datas': base64.b64encode(b'%PDF-1.4 rev0'),
            'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-A81',
            'sgi_revision': 0, 'sgi_state': 'vigente', 'sgi_process_id': cls.process.id,
            'sgi_job_ids': [(6, 0, cls.job.ids)]})
        cls.doc.action_generate_acks()
        cls.doc.sgi_ack_ids.sudo().write({'state': 'leido', 'ack_date': '2026-01-01 10:00:00'})

    def _request(self, **vals):
        req = self.env['approval.request'].create(dict({
            'name': 'Cambio PR4', 'category_id': self.category.id,
            'request_owner_id': self.env.user.id, 'sgi_change_kind': 'modificacion',
            'sgi_document_id': self.doc.id, 'sgi_new_revision': 1}, **vals))
        req.approver_ids = [(0, 0, {'user_id': self.env.user.id, 'required': True})]
        req.action_confirm()
        return req

    def _activities(self, record):
        return self.env['mail.activity'].search(
            [('res_model', '=', record._name), ('res_id', '=', record.id)]).mapped('summary')

    def test_01_doc1_con_archivo_publica_revision_nueva(self):
        req = self._request()
        self.env['ir.attachment'].create({
            'name': 'P-A81 rev1.pdf', 'res_model': 'approval.request', 'res_id': req.id,
            'datas': base64.b64encode(b'%PDF-1.4 rev1'), 'mimetype': 'application/pdf'})
        req.action_approve()
        self.assertTrue(req.sgi_applied)
        new_doc = req.sgi_new_document_id
        self.assertTrue(new_doc, "Con archivo, la revisión nueva es un documento nuevo.")
        self.assertEqual(new_doc.sgi_code, 'P-A81')
        self.assertEqual(new_doc.sgi_revision, 1)
        self.assertEqual(new_doc.sgi_state, 'vigente')
        self.assertEqual(new_doc.sgi_process_id, self.process)
        self.assertEqual(new_doc.sgi_job_ids, self.job)
        self.assertEqual(new_doc.sgi_doc_change_id, req)
        self.assertEqual(base64.b64decode(new_doc.datas), b'%PDF-1.4 rev1')
        self.assertEqual(self.doc.sgi_state, 'obsoleto', "La anterior queda obsoleta.")
        # Acuse pendiente para quien debe leerla; el dueño del proceso recibe la difusión.
        acks = new_doc.sgi_ack_ids
        self.assertEqual(acks.mapped('employee_id'), self.emp)
        self.assertEqual(acks.mapped('state'), ['pendiente'])
        self.assertTrue(any('publicada' in s for s in self._activities(new_doc)))

    def test_02_doc1_sin_archivo_revisa_en_sitio_y_reabre_acuses(self):
        req = self._request()
        req.action_approve()
        self.assertFalse(req.sgi_new_document_id)
        self.assertEqual(self.doc.sgi_revision, 1)
        self.assertEqual(self.doc.sgi_state, 'vigente')
        self.assertEqual(self.doc.sgi_ack_ids.mapped('state'), ['pendiente'],
                         "Una revisión nueva vuelve a pedir la firma.")
        self.assertFalse(self.doc.sgi_ack_ids.ack_date)

    def test_03_doc1_sin_puestos_toma_los_del_proceso(self):
        self.doc.sgi_job_ids = [(5, 0, 0)]
        self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Operar PR4',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job.id})]})
        self._request().action_approve()
        self.assertEqual(self.doc.sgi_job_ids, self.job)
        self.assertTrue(self.doc.sgi_ack_ids.filtered(lambda a: a.state == 'pendiente'))

    def test_04_doc3_lista_maestra(self):
        action = self.process.action_print_master_list()
        self.assertEqual(action['report_name'], 'quimibond_sgi.report_master_list_document')
        self.assertEqual(self.process._sgi_master_list_documents(), self.doc)
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_master_list_document', self.process.ids)[0].decode()
        for text in ('Lista maestra', 'XPR4', 'P-A81', 'Procedimiento PR4', 'Próxima revisión'):
            self.assertIn(text, html)

    def test_05_dir1_evaluacion_con_evidencia_y_vencimiento(self):
        Req = self.env['sgi.legal.requirement']
        req = Req.create({'name': 'Programa de protección civil', 'reference': 'STPS-PC',
                          'system': 'sst', 'responsible_id': self.user_emp.id})
        self.assertEqual(req.responsible_id, self.user_emp)
        self.assertEqual(Req.create({'name': 'Sin responsable explícito'}).responsible_id,
                         self.env.user, "Responsable obligatorio: por omisión, quien captura.")
        wiz = self.env['sgi.legal.evaluate'].create({
            'requirement_id': req.id, 'result': 'cumple', 'evidence': 'Dictamen 2026 vigente'})
        self.assertTrue(wiz.next_date and wiz.next_date > date.today(), "Propone última + frecuencia.")
        wiz.next_date = date.today() + timedelta(days=45)
        wiz.action_confirm()
        self.assertEqual(req.compliance_state, 'cumple')
        self.assertEqual(req.last_eval_date, date.today())
        self.assertEqual(req.next_eval_date, date.today() + timedelta(days=45))
        self.assertEqual(len(req.evaluation_ids), 1)
        self.assertEqual(req.evaluation_ids.evidence, 'Dictamen 2026 vigente')
        self.assertEqual(req.evaluation_ids.next_date, req.next_eval_date)
        # No aplica también se registra; no cumple abre NC.
        self.env['sgi.legal.evaluate'].create({
            'requirement_id': req.id, 'result': 'no_aplica', 'evidence': 'Planta sin caldera'}).action_confirm()
        self.assertEqual(req.compliance_state, 'no_aplica')
        self.env['sgi.legal.evaluate'].create({
            'requirement_id': req.id, 'result': 'no_cumple', 'evidence': 'Sin simulacro'}).action_confirm()
        self.assertTrue(req.alert_id)
        self.assertEqual(len(req.evaluation_ids), 3)
        # Aviso 60 días antes al responsable y en Mis pendientes.
        req.next_eval_date = date.today() + timedelta(days=40)
        self.env['sgi.cron'].cron_legal_requirements()
        self.assertTrue(any('vence el' in s for s in self._activities(req)))
        mp = self.env['sgi.my.procedure'].with_user(self.user_emp).create({'employee_id': self.emp.id})
        self.assertIn(req, mp.pending_legal_ids)
        # PDF de la matriz legal.
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_legal_matrix_document', req.ids)[0].decode()
        for text in ('Matriz de requisitos legales', 'STPS-PC', 'Programa de protección civil'):
            self.assertIn(text, html)
