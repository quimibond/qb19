# -*- coding: utf-8 -*-

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError, ValidationError


@tagged('post_install', '-at_install')
class TestAudit(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Audit = cls.env['sgi.audit']
        cls.user = cls.env['res.users'].create({
            'name': 'Auditor Uno', 'login': 'sgi_auditor_test'})
        cls.employee = cls.env['hr.employee'].create({
            'name': 'Dueño Proceso', 'user_id': cls.user.id})
        cls.process = cls.env['sgi.process'].create({
            'code': 'AUD-TEST', 'name': 'Proceso auditado', 'owner_id': cls.employee.id})
        cls.other_process = cls.env['sgi.process'].create({
            'code': 'AUD-TEST2', 'name': 'Otro proceso'})

    def test_01_auditor_independence(self):
        # El auditor no puede auditar un proceso del que es dueño.
        with self.assertRaises(ValidationError):
            self.Audit.create({
                'audit_type': 'interna',
                'lead_auditor_id': self.user.id,
                'process_ids': [(6, 0, self.process.ids)],
            })

    def test_02_folio_sequence(self):
        audit = self.Audit.create({
            'audit_type': 'interna',
            'process_ids': [(6, 0, self.other_process.ids)],
        })
        self.assertTrue(audit.folio.startswith('AUD-'))

    def test_03_close_blocked_without_disposition(self):
        audit = self.Audit.create({
            'audit_type': 'interna',
            'process_ids': [(6, 0, self.other_process.ids)],
            'state': 'informe',
        })
        self.env['sgi.audit.finding'].create({
            'audit_id': audit.id,
            'finding_type': 'nc_menor',
            'process_id': self.other_process.id,
            'description': 'Hallazgo sin disposición',
        })
        with self.assertRaises(UserError):
            audit.action_close()

    def test_04_generate_nc_links_alert(self):
        audit = self.Audit.create({
            'audit_type': 'interna',
            'process_ids': [(6, 0, self.other_process.ids)],
            'state': 'informe',
        })
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id,
            'finding_type': 'nc_mayor',
            'process_id': self.other_process.id,
            'description': 'Desviación mayor',
        })
        finding.action_generate_nc()
        self.assertTrue(finding.alert_id)
        self.assertEqual(finding.alert_id.sgi_origin_type, 'auditoria_interna')
        self.assertEqual(finding.alert_id.sgi_classification, 'mayor')
        self.assertEqual(finding.disposition, 'genera_nc')
        # Ahora la auditoría sí cierra
        audit.action_close()
        self.assertEqual(audit.state, 'cerrada')
