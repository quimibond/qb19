# -*- coding: utf-8 -*-
"""Pendientes del SAT en Mis pendientes: corre en el build de Odoo.sh (el CI
no instala el SGI, que es Enterprise)."""
from odoo import Command
from odoo.tests import tagged

from odoo.addons.quimibond_sat.tests.common import SatCommon, syntage_invoice

UUID_A = '66666666-0000-4000-8000-000000000001'


@tagged('post_install', '-at_install', 'quimibond_sat_sgi')
class TestSatSgi(SatCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.sgi_company_id', cls.company.id)
        cls.job = cls.env['hr.job'].create({'name': 'CONTADOR DE PRUEBA SAT', 'company_id': cls.company.id})
        cls.user = cls.env['res.users'].create({
            'name': 'Contador SAT', 'login': 'contador.sat@example.com',
            'company_id': cls.company.id, 'company_ids': [Command.set(cls.company.ids)],
            'group_ids': [Command.link(cls.env.ref('account.group_account_user').id),
                          Command.link(cls.env.ref('quimibond_sgi.group_sgi_user').id)],
        })
        cls.env['hr.employee'].create({'name': 'Contador SAT', 'job_id': cls.job.id, 'user_id': cls.user.id,
                                       'company_id': cls.company.id})
        process = cls.env['sgi.process'].create({'name': 'Compra a pago (prueba)', 'code': 'S1T',
                                                 'company_id': cls.company.id})
        cls.activity = cls.env['sgi.process.activity'].create({
            'name': 'Registrar la factura del proveedor desde el SAT', 'number': 'S1.21',
            'process_id': process.id, 'company_id': cls.company.id,
            'responsible_job_ids': [Command.set(cls.job.ids)],
        })

    def _notices(self):
        return self.env['mail.activity'].sudo().search([
            ('res_model', '=', 'sgi.process.activity'), ('res_id', '=', self.activity.id),
            ('sgi_cron_kind', '=', 'sat_falta_odoo')])

    def test_missing_cfdi_notice_goes_to_job_holder_and_closes(self):
        cfdi = self.env['sat.cfdi']._upsert_from_syntage(
            syntage_invoice(UUID_A, issuedAt='2026-01-10 12:00:00'), self.company, event_type='pull')
        self.assertEqual(cfdi.match_status, 'solo_sat')
        self.env.flush_all()
        self.env['sgi.cron'].cron_sat_pendientes()
        notice = self._notices()
        self.assertEqual(len(notice), 1)
        self.assertEqual(notice.user_id, self.user)
        self.assertIn('CFDI de proveedores', notice.summary)
        action = self.env['sgi.cron']._sat_pending_action('sat_falta_odoo')
        self.assertEqual(action['res_model'], 'sat.compare.line')
        # Ya en Odoo: la siguiente corrida cierra el aviso
        bill = self._invoice(self.proveedor, 3287.86, day='2026-01-10')
        cfdi.write({'move_id': bill.id})
        self.env.flush_all()
        self.env['sgi.cron'].cron_sat_pendientes()
        self.assertFalse(self._notices().filtered('active'))
