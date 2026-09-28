# -*- coding: utf-8 -*-
"""PR 6 del plan (19.0.53.0.0): proveedores, clientes y firmas.
NC-6 NC a proveedor por el portal, AU-4 auditorías de cliente y a proveedor,
AU-5 programa sugerido, DOC-4 documentos por revisar en Mis pendientes,
DOC-5 instructivo desde Knowledge, REG-1 firmas ligadas al registro,
REG-2 encuesta como entregable. (NC-7 8D y DOC-4 aviso ya existían.)"""
import base64
from datetime import date, timedelta

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestPr6External(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.team_int = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.team_ext = cls.env.ref('quimibond_sgi.sgi_quality_team_external')
        cls.supplier = cls.env['res.partner'].create({
            'name': 'Hilos del Norte', 'is_company': True, 'supplier_rank': 1,
            'email': 'calidad@hilosdelnorte.test'})
        cls.customer = cls.env['res.partner'].create({'name': 'Automotriz Cliente', 'is_company': True})
        cls.process = cls.env['sgi.process'].create({'code': 'XP6', 'name': 'Proceso PR6'})
        cls.buyer = new_test_user(cls.env, login='pr6_buyer',
                                  groups='base.group_user,quimibond_sgi.group_sgi_user')

    # ---- NC-6 -------------------------------------------------------
    def test_01_nc6_enviar_al_proveedor_y_respuesta_por_portal(self):
        nc = self.env['quality.alert'].create({
            'title': 'Hilo con título fuera de especificación', 'team_id': self.team_int.id,
            'partner_id': self.supplier.id, 'user_id': self.buyer.id,
            'sgi_deviation': 'Título 28/2 en vez de 30/2'})
        self.assertEqual(nc.sgi_supplier_id, self.supplier)
        self.assertEqual(nc.sgi_supplier_state, 'no_enviada')
        nc.action_sgi_send_to_supplier()
        self.assertEqual(nc.sgi_supplier_state, 'enviada')
        self.assertEqual(nc.sgi_supplier_sent_date, date.today())
        self.assertGreater(nc.sgi_supplier_due_date, date.today())
        self.assertTrue(nc.access_token)
        self.assertIn('/my/nc/%d' % nc.id, nc.access_url)
        mail = self.env['mail.mail'].search([('model', '=', 'quality.alert'), ('res_id', '=', nc.id)], limit=1)
        self.assertTrue(mail, "El correo al proveedor queda en cola.")
        self.assertIn('calidad@hilosdelnorte.test', mail.email_to)
        # Vencida sin respuesta: aviso al comprador y escalamiento a MAST.
        nc.sudo().write({'sgi_supplier_due_date': date.today() - timedelta(days=2)})
        self.assertTrue(nc.sgi_supplier_overdue)
        nc._sgi_supplier_escalation(date.today())
        summaries = self.env['mail.activity'].search(
            [('res_model', '=', 'quality.alert'), ('res_id', '=', nc.id)]).mapped('summary')
        self.assertTrue(any('Respuesta del proveedor vence' in s for s in summaries))
        self.assertTrue(any('escalada a MAST' in s for s in summaries))
        # El proveedor contesta por el portal (sin correo de por medio).
        with self.assertRaises(UserError):
            nc.sgi_supplier_answer('', 'algo')
        nc.sgi_supplier_answer('Lote de hilo de otro proveedor mezclado', 'Segregar y reponer el 30/09')
        self.assertEqual(nc.sgi_supplier_state, 'contestada')
        self.assertTrue(nc.sgi_supplier_response_date)
        self.assertIn('Segregar', nc.sgi_supplier_action)
        summaries = self.env['mail.activity'].search(
            [('res_model', '=', 'quality.alert'), ('res_id', '=', nc.id)]).mapped('summary')
        self.assertTrue(any('contestó' in s for s in summaries))
        self.assertFalse(any('Respuesta del proveedor vence' in s for s in summaries))
        # Cuenta en la evaluación del proveedor (S1.08).
        ev = self.env['sgi.supplier.eval'].create({
            'partner_id': self.supplier.id, 'date_from': date.today() - timedelta(days=30),
            'date_to': date.today() + timedelta(days=1)})
        self.assertGreaterEqual(ev._sgi_count_ncs(), 1)

    # ---- AU-4 / AU-5 ----------------------------------------------------
    def test_02_au4_auditorias_de_cliente_y_a_proveedor(self):
        Audit = self.env['sgi.audit']
        with self.assertRaises(ValidationError):
            Audit.create({'audit_type': 'cliente', 'process_ids': [(6, 0, self.process.ids)]})
        audit = Audit.create({
            'audit_type': 'cliente', 'partner_id': self.customer.id,
            'external_report_ref': 'NCR-2026-117', 'process_ids': [(6, 0, self.process.ids)],
            'state': 'informe'})
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'nc_menor', 'process_id': self.process.id,
            'description': 'Etiqueta sin lote'})
        finding.action_generate_nc()
        nc = finding.alert_id
        self.assertEqual(nc.team_id, self.team_ext)
        self.assertEqual(nc.sgi_origin_type, 'auditoria_externa')
        self.assertEqual(nc.partner_id, self.customer)
        self.assertEqual(nc.sgi_external_ref, 'NCR-2026-117')
        supplier_audit = Audit.create({
            'audit_type': 'proveedor', 'partner_id': self.supplier.id,
            'process_ids': [(6, 0, self.process.ids)], 'state': 'informe'})
        f2 = self.env['sgi.audit.finding'].create({
            'audit_id': supplier_audit.id, 'finding_type': 'nc_mayor', 'description': 'Sin certificado'})
        f2.action_generate_nc()
        self.assertEqual(f2.alert_id.sgi_supplier_id, self.supplier, "Va a la NC a proveedor (NC-6).")
        self.assertEqual(f2.alert_id.team_id, self.team_ext)

    def test_03_au5_programa_sugerido(self):
        Process = self.env['sgi.process']
        macro = Process.create({'code': 'XP6M', 'name': 'Macro PR6'})
        p1 = Process.create({'code': 'XP6A', 'name': 'Sub A', 'parent_id': macro.id, 'state': 'vigente'})
        p2 = Process.create({'code': 'XP6B', 'name': 'Sub B', 'parent_id': macro.id, 'state': 'piloto'})
        Process.create({'code': 'XP6C', 'name': 'Sub C borrador', 'parent_id': macro.id})
        self.env['quality.alert'].create({
            'title': 'NC abierta en A', 'team_id': self.team_int.id, 'sgi_process_id': p1.id})
        program = self.env['sgi.audit.program'].create({'year': 2099})
        program.action_suggest_lines()
        lines = program.line_ids
        self.assertEqual(set(lines.mapped('process_id')), {p1, p2}, "Solo vigentes o en piloto.")
        self.assertEqual(len(lines.filtered(lambda l: l.process_id == p1)), 2,
                         "Con NC abierta se audita dos veces al año.")
        self.assertEqual(len(lines.filtered(lambda l: l.process_id == p2)), 1)
        program.action_suggest_lines()
        self.assertEqual(len(program.line_ids), 3, "Idempotente.")
        program.action_approve()
        with self.assertRaises(UserError):
            program.action_suggest_lines()

    # ---- DOC-4 / DOC-5 --------------------------------------------------
    def test_04_doc4_documentos_por_revisar_en_mis_pendientes(self):
        job = self.env['hr.job'].create({'name': 'DOC PR6'})
        emp = self.env['hr.employee'].create({'name': 'Dueño doc PR6', 'job_id': job.id, 'user_id': self.buyer.id})
        doc = self.env['documents.document'].create({
            'name': 'P-A84 Prueba.pdf', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-A84', 'sgi_state': 'vigente',
            'sgi_owner_id': self.buyer.id, 'sgi_next_review_date': date.today() + timedelta(days=45)})
        wiz = self.env['sgi.my.procedure'].with_user(self.buyer).create({'employee_id': emp.id})
        self.assertIn(doc, wiz.pending_doc_review_ids)
        self.env['sgi.cron'].cron_documents()
        summaries = self.env['mail.activity'].search(
            [('res_model', '=', 'documents.document'), ('res_id', '=', doc.id),
             ('user_id', '=', self.buyer.id)]).mapped('summary')
        self.assertTrue(summaries, "El aviso de próxima revisión llega al dueño (60 días).")

    def test_05_doc5_instructivo_desde_knowledge(self):
        if 'knowledge.article' not in self.env:
            self.skipTest("Knowledge no instalado")
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

    # ---- REG-1 / REG-2 ----------------------------------------------------
    def test_06_reg1_firmas_ligadas_al_registro(self):
        po = self.env['purchase.order'].create({'partner_id': self.supplier.id})
        self.assertEqual(po.sgi_sign_count, 0)
        self.assertFalse(po.sgi_signed)
        action = po.action_sgi_sign()
        self.assertEqual(action['res_model'], 'sgi.sign.request.wizard')
        self.assertEqual(action['context']['default_res_model'], 'purchase.order')
        self.assertEqual(action['context']['default_partner_id'], self.supplier.id)
        self.assertNotIn(po, self.env['purchase.order'].search([('sgi_signed', '=', True)]))
        self.assertIn(po, self.env['purchase.order'].search([('sgi_signed', '=', False), ('id', '=', po.id)]))
        deliverable = self.env['sgi.deliverable'].create({
            'name': 'OC firmada', 'odoo_model_id': self.env['ir.model']._get_id('purchase.order'),
            'measure_domain': "[('partner_id', '=', %d)]" % self.supplier.id,
            'require_signed': True})
        self.assertEqual(deliverable._sgi_signed_ids('purchase.order', po.ids), [])

    def test_07_reg2_encuesta_como_entregable(self):
        survey = self.env['survey.survey'].create({'title': 'Satisfacción PR6'})
        deliverable = self.env['sgi.deliverable'].new({'name': 'Encuesta contestada', 'survey_id': survey.id})
        deliverable._onchange_survey_id()
        self.assertEqual(deliverable.odoo_model_id.model, 'survey.user_input')
        self.assertIn("('survey_id', '=', %d)" % survey.id, deliverable.measure_domain)
        self.assertEqual(deliverable.measure_date_field, 'end_datetime')
