# -*- coding: utf-8 -*-
"""56.24.0: Mi procedimiento y Mis pendientes sin procesos archivados,
categorías de Aprobaciones archivadas ni aprobaciones ya decididas."""
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestArchivedFilters(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.user = new_test_user(cls.env, login='zs_arch_user', groups='base.group_user')
        cls.job = cls.env['hr.job'].create({'name': 'PUESTO ARCHIVADOS PRUEBA'})
        cls.env['hr.employee'].create({'name': 'ZS Archivados', 'job_id': cls.job.id,
                                       'user_id': cls.user.id})
        Process = cls.env['sgi.process']
        cls.live = Process.create({'code': 'XAR1', 'name': 'Proceso vivo'})
        cls.old = Process.create({'code': 'XAR2', 'name': 'Proceso viejo'})
        Activity = cls.env['sgi.process.activity']
        cls.act_live = Activity.create({
            'process_id': cls.live.id, 'name': 'Hacer lo vigente',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})]})
        cls.act_old = Activity.create({
            'process_id': cls.old.id, 'name': 'Hacer lo del proceso viejo',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})]})

    def test_01_mi_procedimiento_sin_procesos_archivados(self):
        roles = self.env['sgi.activity.role'].search(self.job._sgi_roles_domain())
        self.assertEqual(roles.activity_id, self.act_live | self.act_old)
        self.old.active = False
        roles = self.env['sgi.activity.role'].search(self.job._sgi_roles_domain())
        self.assertEqual(roles.activity_id, self.act_live)
        data = self.job._sgi_my_procedure_data()
        names = [e['name'] for s in data['sections'] for e in s['entries']]
        self.assertNotIn('Hacer lo del proceso viejo', names)
        employee = self.env['hr.employee'].search([('user_id', '=', self.user.id)])
        employee.invalidate_recordset()
        self.assertEqual(employee.sgi_mp_role_ids.activity_id, self.act_live,
                         "Archivar el proceso recalcula lo guardado en el empleado.")

    def test_02_pendientes_sin_nc_de_procesos_archivados(self):
        Alert = self.env['quality.alert']
        if 'sgi_responsible_ids' not in Alert._fields:
            self.skipTest("Sin responsables de NC en este modelo.")
        alert = Alert.create({'name': 'ZS NC de proceso viejo', 'sgi_process_id': self.old.id,
                              'sgi_responsible_ids': [(6, 0, self.user.ids)]})
        Pending = self.env['sgi.my.pending']
        self.assertIn(alert, Pending._sgi_pending_records(self.user)['nc'])
        self.old.active = False
        self.assertNotIn(alert, Pending._sgi_pending_records(self.user)['nc'])

    def test_03_pendientes_sin_categorias_archivadas(self):
        category = self.env['approval.category'].create({
            'name': 'ZS Categoría vieja', 'approval_minimum': 1,
            'approver_ids': [(0, 0, {'user_id': self.user.id, 'required': True})]})
        request = self.env['approval.request'].create({
            'name': 'ZS solicitud', 'category_id': category.id,
            'request_owner_id': self.env.user.id})
        request.action_confirm()
        Pending = self.env['sgi.my.pending']
        mine = Pending._sgi_pending_records(self.user)['solicitud']
        self.assertIn(request, mine.request_id)
        category.active = False
        mine = Pending._sgi_pending_records(self.user)['solicitud']
        self.assertNotIn(request, mine.request_id)
