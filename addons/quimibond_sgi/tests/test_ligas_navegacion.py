# -*- coding: utf-8 -*-
"""57.150.0 (auditoría UX 2026-10-13, fase 3): ligas de navegación NC ↔
hallazgo ↔ auditoría, proceso → auditorías, hallazgos y periodos atrasados,
documento → riesgos que controla, indicador → NC generadas."""
from datetime import timedelta
from unittest.mock import patch

from odoo import fields
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_exec import sgi_exec_start_open


@tagged('post_install', '-at_install')
class TestLigasNavegacion(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_exec_start_open(cls.env)
        cls.today = fields.Date.context_today(cls.env.user)
        cls.process = cls.env['sgi.process'].create({'code': 'ZLN', 'name': 'Proceso ligas 150'})
        cls.audit = cls.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, cls.process.ids)], 'state': 'informe'})
        cls.finding = cls.env['sgi.audit.finding'].create({
            'audit_id': cls.audit.id, 'finding_type': 'nc_menor', 'process_id': cls.process.id,
            'description': 'Registro sin firma (ligas 150)'})
        cls.finding.action_generate_nc()
        cls.alert = cls.finding.alert_id

    def test_01_nc_hallazgo_auditoria(self):
        """El inverso de sgi.audit.finding.alert_id y la auditoría del hallazgo."""
        self.assertTrue(self.alert)
        self.alert.invalidate_recordset()
        self.assertEqual(self.alert.sgi_finding_ids, self.finding)
        self.assertEqual(self.alert.sgi_finding_count, 1)
        self.assertEqual(self.alert.sgi_audit_id, self.audit)
        action = self.alert.action_sgi_open_findings()
        self.assertEqual((action['res_model'], action['res_id']), ('sgi.audit.finding', self.finding.id))
        action = self.alert.action_sgi_open_audit()
        self.assertEqual((action['res_model'], action['res_id']), ('sgi.audit', self.audit.id))
        # Una NC sin hallazgo no tiene botones.
        other = self.env['quality.alert'].create({'title': 'NC sin hallazgo 150'})
        self.assertEqual(other.sgi_finding_count, 0)
        self.assertFalse(other.sgi_audit_id)

    def test_02_proceso_auditorias_hallazgos_y_atrasos(self):
        self.process.invalidate_recordset()
        self.assertEqual(self.process.audit_count, 1)
        self.assertEqual(self.process.finding_count, 1)
        action = self.process.action_sgi_open_audits()
        self.assertEqual(self.env['sgi.audit'].search(action['domain']), self.audit)
        action = self.process.action_sgi_open_findings()
        self.assertEqual(self.env['sgi.audit.finding'].search(action['domain']), self.finding)
        # Periodos atrasados: abiertos, vencidos y desde el arranque del registro.
        job = self.env['hr.job'].create({'name': 'PUESTO LIGAS 150'})
        employee = self.env['hr.employee'].create({'name': 'ZS Ligas 150', 'job_id': job.id})
        activity = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Revisar ligas 150', 'number': '15.1',
            'measure_cadence': 'mensual', 'due_business_day': 1,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})]})
        Exec = self.env['sgi.activity.execution'].sudo()
        start = self.today.replace(day=1) - timedelta(days=62)
        late = Exec.create({'activity_id': activity.id, 'employee_id': employee.id,
                            'period_start': start, 'period_end': start + timedelta(days=27),
                            'date_due': self.today - timedelta(days=3)})
        Exec.create({'activity_id': activity.id, 'employee_id': employee.id,
                     'period_start': start + timedelta(days=31), 'period_end': start + timedelta(days=58),
                     'date_due': self.today + timedelta(days=3)})
        self.process.invalidate_recordset()
        self.assertEqual(self.process.execution_late_count, 1)
        action = self.process.action_sgi_open_late_executions()
        self.assertEqual(Exec.search(action['domain']), late)
        # Antes del arranque del registro no cuenta.
        sgi_exec_start_open(self.env, start=fields.Date.to_string(self.today))
        self.process.invalidate_recordset()
        self.assertEqual(self.process.execution_late_count, 0)

    def test_03_documento_riesgos_que_controla(self):
        doc = self.env['documents.document'].create({
            'name': 'Control operacional ligas 150', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-ZLN-01', 'sgi_state': 'vigente',
            'sgi_process_id': self.process.id})
        self.assertEqual(doc.sgi_controlled_risk_count, 0)
        risk = self.env['sgi.risk'].create({
            'name': 'Derrame ligas 150', 'process_id': self.process.id,
            'operational_control_id': doc.id})
        doc.invalidate_recordset()
        self.assertEqual(doc.sgi_controlled_risk_count, 1)
        action = doc.action_sgi_open_controlled_risks()
        self.assertEqual(self.env['sgi.risk'].search(action['domain']), risk)

    def test_04_indicador_nc_generadas(self):
        indicator = self.env['sgi.indicator'].create({
            'code': 'ZLN-I', 'name': 'KPI ligas 150', 'calc_mode': 'manual', 'frequency': 'monthly',
            'process_id': self.process.id})
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': indicator.id, 'period_date': self.today.replace(day=1)})
        self.assertEqual(indicator.nc_generated_count, 0)
        alert = self.env['quality.alert'].create({
            'title': 'NC de indicador 150', 'sgi_indicator_measure_id': measure.id})
        indicator.invalidate_recordset()
        self.assertEqual(indicator.nc_generated_count, 1)
        self.assertEqual(alert.sgi_indicator_id, indicator)
        action = indicator.action_sgi_open_generated_ncs()
        self.assertEqual(self.env['quality.alert'].search(action['domain']), alert)


@tagged('post_install', '-at_install')
class TestCorreoDiarioPendientes(TransactionCase):
    """57.150.0 (fase 2): correo diario con lo atrasado y por vencer."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.soon = new_test_user(cls.env, login='d150_soon', email='d150.a@example.com', groups=groups)
        cls.off = new_test_user(cls.env, login='d150_off', email='d150.b@example.com', groups=groups)
        cls.off.sgi_daily_pending_mail = False
        cls.calm = new_test_user(cls.env, login='d150_calm', email='d150.c@example.com', groups=groups)
        cls.Pending = type(cls.env['sgi.my.pending'])

    def _fake_values(self, users):
        today = fields.Date.context_today(self.env.user)
        soon = {'kind': 'accion', 'name': 'Acción por vencer D150', 'state': 'por_vencer',
                'date_due': today + timedelta(days=2), 'process_id': False}
        ok = dict(soon, name='Acción al día D150', state='al_dia')
        return {user.id: ([soon, ok] if user in (self.soon | self.off) else [ok]) for user in users}

    def _run(self):
        fake = self._fake_values
        with patch.object(self.Pending, '_sgi_pending_values', autospec=True,
                          side_effect=lambda _self, users: fake(users)):
            return self.env['sgi.cron'].cron_daily_pending_mail()

    def test_01_solo_con_algo_por_vencer_o_atrasado(self):
        self.assertTrue(self.calm.sgi_daily_pending_mail, "Encendido por default.")
        sent = self._run()
        self.assertIn(self.soon.id, sent)
        self.assertNotIn(self.off.id, sent, "Quien lo apagó no recibe correo.")
        self.assertNotIn(self.calm.id, sent, "Sin nada atrasado ni por vencer no hay correo.")
        mails = self.env['mail.mail'].sudo().search([('subject', 'ilike', 'pendiente(s) para hoy')])

        def addresses(mail):
            return ' '.join([mail.email_to or ''] + mail.recipient_ids.mapped('email'))
        body = ' '.join(mails.filtered(lambda m: 'd150.a@' in addresses(m)).mapped('body_html'))
        self.assertIn('Acción por vencer D150', body)
        self.assertNotIn('Acción al día D150', body)

    def test_02_cada_quien_lo_apaga_y_el_cron_sale_apagado(self):
        self.soon.with_user(self.soon).write({'sgi_daily_pending_mail': False})
        self.assertNotIn(self.soon.id, self._run())
        cron = self.env.ref('quimibond_sgi.sgi_cron_daily_pending_mail')
        self.assertIn('cron_daily_pending_mail', cron.code)
        self.assertFalse(cron.active, "Jose lo enciende a mano.")
