# -*- coding: utf-8 -*-
"""PR 3 del plan (19.0.50.0.0): auditoría lista para octubre.
AU-1 checklist generado del proceso, AU-2 independencia del auditor por su
puesto, AU-3 informe F-P-G03-07 archivado al cerrar."""
from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestAuditPr3(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # 57.66.0: el pie del informe sale del mapeo por referencia
        # (noupdate, MAST lo edita y lo liga a su documento) y del documento
        # vigente: en la copia de producción la prueba fija el suyo, como
        # test_laboratorio y test_etiquetas_lote.
        sgi_hide_real_documents(cls.env)
        cls.env.ref('quimibond_sgi.format_ref_audit_report').write(
            {'sgi_code': 'F-P-G03-07', 'document_id': False, 'active': True})
        cls.job_exec = cls.env['hr.job'].create({'name': 'PLANEADOR AUD PRUEBA'})
        cls.job_other = cls.env['hr.job'].create({'name': 'CONTADOR AUD PRUEBA'})
        cls.family = cls.env['sgi.job.family'].create({
            'code': 'FAM-AUD', 'name': 'Familia AUD', 'job_ids': [(6, 0, cls.job_exec.ids)]})
        cls.process = cls.env['sgi.process'].create({'code': 'XAU1', 'name': 'Proceso auditado AU'})
        cls.other_process = cls.env['sgi.process'].create({'code': 'XAU2', 'name': 'Otro proceso AU'})
        Activity = cls.env['sgi.process.activity']
        cls.deliverable = cls.env['sgi.deliverable'].create({
            'name': 'Programa semanal AU',
            'odoo_model_id': cls.env['ir.model']._get_id('res.partner')})
        cls.act1 = Activity.create({
            'process_id': cls.process.id, 'name': 'Programar la semana',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_exec.id})],
            'output_deliverable_ids': [(6, 0, cls.deliverable.ids)]})
        cls.act2 = Activity.create({
            'process_id': cls.process.id, 'name': 'Aprobar el programa',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_exec.id}),
                         (0, 0, {'role': 'aprueba', 'target_type': 'family', 'family_id': cls.family.id})]})
        cls.act_other = Activity.create({
            'process_id': cls.other_process.id, 'name': 'Conciliar bancos',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_other.id})]})
        cls.auditor_user = new_test_user(
            cls.env, login='au3_auditor', groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.env['hr.employee'].create({
            'name': 'Auditor AU', 'job_id': cls.job_other.id, 'user_id': cls.auditor_user.id})
        cls.exec_user = new_test_user(
            cls.env, login='au3_exec', groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.env['hr.employee'].create({
            'name': 'Planeador AU', 'job_id': cls.job_exec.id, 'user_id': cls.exec_user.id})

    def _audit(self, **vals):
        return self.env['sgi.audit'].create(dict({
            'audit_type': 'interna', 'process_ids': [(6, 0, self.process.ids)],
            'lead_auditor_id': self.auditor_user.id}, **vals))

    def test_01_checklist_generado_del_proceso(self):
        audit = self._audit()
        self.assertFalse(audit.checklist_line_ids)
        audit.action_plan()
        self.assertEqual(audit.state, 'planificada')
        self.assertEqual(audit.checklist_line_ids.mapped('activity_id'), self.act1 | self.act2)
        line = audit.checklist_line_ids.filtered(lambda l: l.activity_id == self.act1)
        self.assertIn('¿Se cumple XAU1', line.question)
        self.assertIn('Programar la semana', line.question)
        self.assertIn('en plazo y con evidencia', line.question)
        self.assertEqual(line.executor, 'PLANEADOR AUD PRUEBA')
        self.assertEqual(line.deliverables, 'Programa semanal AU')
        self.assertTrue(line.can_open_records)
        action = line.action_open_records()
        self.assertEqual(action['res_model'], 'res.partner')
        # Idempotente y con las actividades nuevas.
        audit.action_generate_checklist()
        self.assertEqual(len(audit.checklist_line_ids), 2)
        act3 = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Publicar el programa',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_other.id})]})
        audit.action_generate_checklist()
        self.assertIn(act3, audit.checklist_line_ids.mapped('activity_id'))
        self.assertEqual(audit.checklist_count, 3)

    def test_02_respuesta_no_conforme_crea_su_hallazgo(self):
        audit = self._audit()
        audit.action_plan()
        audit.action_start()
        line = audit.checklist_line_ids.filtered(lambda l: l.activity_id == self.act1)
        line.write({'answer': 'nc_menor', 'evidence': 'Sin programa las semanas 30 y 31'})
        self.assertTrue(line.finding_id)
        self.assertEqual(line.finding_id.finding_type, 'nc_menor')
        self.assertEqual(line.finding_id.process_id, self.process)
        self.assertIn('semanas 30 y 31', line.finding_id.description)
        self.assertEqual(line.finding_id.checklist_line_id, line)
        line.answer = 'nc_mayor'
        self.assertEqual(line.finding_id.finding_type, 'nc_mayor')
        self.assertEqual(audit.checklist_nonconforming_count, 1)
        # Conforme retira el hallazgo mientras no tenga NC.
        finding = line.finding_id
        line.answer = 'conforme'
        self.assertFalse(finding.exists())
        self.assertFalse(line.finding_id)
        self.assertEqual(audit.checklist_answered_count, 1)
        line.answer = 'observacion'
        self.assertEqual(line.finding_id.finding_type, 'observacion')

    def test_03_independencia_por_puesto(self):
        # El planeador ejecuta una actividad del proceso: no puede auditarlo.
        with self.assertRaises(ValidationError):
            self._audit(lead_auditor_id=self.exec_user.id)
        with self.assertRaises(ValidationError):
            self._audit(auditor_ids=[(6, 0, self.exec_user.ids)])
        # Aprobar por la familia de su puesto también cuenta.
        approve_process = self.env['sgi.process'].create({'code': 'XAU3', 'name': 'Proceso aprobado AU'})
        self.env['sgi.process.activity'].create({
            'process_id': approve_process.id, 'name': 'Liberar el lote',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_other.id}),
                         (0, 0, {'role': 'aprueba', 'target_type': 'family', 'family_id': self.family.id})]})
        with self.assertRaises(ValidationError):
            self._audit(lead_auditor_id=self.exec_user.id, process_ids=[(6, 0, approve_process.ids)])
        # En otro proceso sí puede.
        audit = self._audit(lead_auditor_id=self.exec_user.id,
                            process_ids=[(6, 0, self.other_process.ids)])
        self.assertTrue(audit.folio)
        # El contador ejecuta en XAU2 pero no en XAU1: audita XAU1.
        self.assertTrue(self._audit().folio)

    def test_04_informe_archivado_al_cerrar(self):
        audit = self._audit()
        audit.action_plan()
        audit.action_start()
        audit.write({'opening_minutes': 'Alcance confirmado', 'closing_minutes': 'Dos observaciones',
                     'conclusion': 'El proceso opera conforme.'})
        line = audit.checklist_line_ids[:1]
        line.write({'answer': 'observacion', 'evidence': 'Registro tardío'})
        line.finding_id.write({'disposition': 'sin_accion', 'reason_no_action': 'Aislado'})
        audit.action_report()
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_audit_report_document', audit.ids)[0].decode()
        for text in ('F-P-G03-07', 'Alcance confirmado', 'Dos observaciones', 'Checklist por actividad',
                     'Registro tardío', 'Observaciones', 'El proceso opera conforme', 'Firmas'):
            self.assertIn(text, html)
        with self.assertRaises(UserError):
            audit.action_open_report_document()
        audit.action_close()
        self.assertEqual(audit.state, 'cerrada')
        self.assertTrue(audit.report_document_id, "Al cerrar queda el PDF en la auditoría.")
        self.assertEqual(audit.report_document_id.mimetype, 'application/pdf')
        self.assertIn(audit.folio, audit.report_document_id.name)
        self.assertTrue(any('F-P-G03-07' in (m.body or '') for m in audit.message_ids))

    def test_09_flujo_completo_con_el_proceso_real(self):
        """4.5: programa → auditoría → checklist → hallazgo → NC con un proceso
        real de la copia de producción (C2 «Pedido a entrega»). En una base
        vacía se usa el proceso de prueba."""
        process = self.env['sgi.process'].search([('code', '=', 'C2')], limit=1) or self.process
        if not process.procedure_activity_ids.filtered('active'):
            process = self.process
        mast = new_test_user(self.env, login='au3_mast',
                             groups='base.group_user,quimibond_sgi.group_sgi_manager')
        program = self.env['sgi.audit.program'].create({'year': 2097, 'line_ids': [
            (0, 0, {'process_id': process.id, 'planned_month': '10',
                    'lead_auditor_id': self.auditor_user.id})]})
        program.with_user(mast).action_approve()
        program.line_ids.with_user(mast).action_create_audit()
        audit = program.line_ids.audit_id
        self.assertEqual(audit.process_ids, process)
        audit.with_user(self.auditor_user).action_plan()
        self.assertTrue(audit.checklist_count, "Una línea por actividad del proceso.")
        audit.with_user(self.auditor_user).action_start()
        line = audit.checklist_line_ids[:1].with_user(self.auditor_user)
        line.write({'answer': 'nc_menor', 'evidence': 'Sin evidencia en el periodo auditado'})
        finding = line.finding_id
        self.assertEqual((finding.finding_type, finding.process_id), ('nc_menor', process))
        finding.with_user(mast).action_generate_nc()
        self.assertTrue(finding.alert_id.sgi_folio, "El hallazgo abre su NC con folio.")
        self.assertEqual(finding.alert_id.sgi_process_id, process)
