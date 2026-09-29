# -*- coding: utf-8 -*-
"""Limpieza del bloque 10 (56.15.0): se retira ``sgi.employer.obligation``
(10.1) y los roles de actividades archivadas dejan de contar en el puesto,
la familia y la tortuga (10.3; en producción eran 223)."""
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestCleanupB10(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Job = cls.env['hr.job']
        cls.job = Job.create({'name': 'PUESTO LIMPIEZA B10'})
        cls.job_family = Job.create({'name': 'PUESTO FAMILIA B10'})
        cls.family = cls.env['sgi.job.family'].create({
            'code': 'FAM-B10', 'name': 'Familia B10', 'job_ids': [(6, 0, cls.job.ids)]})
        cls.process = cls.env['sgi.process'].create({'code': 'B10', 'name': 'Proceso B10'})
        Activity = cls.env['sgi.process.activity']
        cls.act_live = Activity.create({
            'process_id': cls.process.id, 'name': 'Actividad viva B10',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id}),
                         (0, 0, {'role': 'informa', 'target_type': 'family', 'family_id': cls.family.id})]})
        cls.act_old = Activity.create({
            'process_id': cls.process.id, 'name': 'Actividad archivada B10',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': cls.job.id}),
                         (0, 0, {'role': 'participa', 'target_type': 'family', 'family_id': cls.family.id}),
                         (0, 0, {'role': 'ejecuta', 'job_id': cls.job_family.id})]})
        cls.act_old.write({'active': False})
        cls.user = new_test_user(cls.env, login='b10_sgi_user',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')

    def test_01_obligacion_patronal_retirada(self):
        self.assertNotIn('sgi.employer.obligation', self.env.registry)
        self.assertFalse(self.env['ir.model'].search([('model', '=', 'sgi.employer.obligation')]))
        self.assertFalse(self.env['ir.model.data'].search_count(
            [('module', '=', 'quimibond_sgi'), ('name', 'like', 'employer_obligation')]),
            "Sin vistas, acción ni menú de obligaciones patronales.")
        self.assertFalse(self.env['ir.actions.act_window'].search_count(
            [('res_model', '=', 'sgi.employer.obligation')]))

    def test_02_roles_archivados_no_cuentan(self):
        roles = self.env['sgi.activity.role'].with_context(active_test=False).search(
            [('activity_id', '=', self.act_old.id)])
        self.assertEqual(len(roles), 4, "Los roles de la actividad archivada siguen en la base.")
        self.assertFalse(any(roles.mapped('activity_active')))
        job = self.job.with_user(self.user)
        job.invalidate_recordset()
        self.assertEqual(
            (job.sgi_execute_count, job.sgi_approve_count, job.sgi_participate_count,
             job.sgi_inform_count, job.sgi_role_count), (1, 0, 0, 1, 2),
            "Solo la actividad viva (ejecuta propio + se entera por familia), leído por un usuario SGI.")
        self.assertEqual(job.sgi_all_role_ids.activity_id, self.act_live)
        self.assertEqual(self.job_family.sgi_role_count, 0)
        listed = self.env['sgi.activity.role'].search(self.job.action_sgi_view_roles()['domain'])
        self.assertEqual(listed.activity_id, self.act_live)

    def test_03_tortuga_cuenta_puestos_de_actividades_activas(self):
        data = self.env['sgi.diagram'].with_user(self.user).data('sipoc', self.process.id)
        key = 'sgi.process,%d' % self.process.id
        center = next(item for lane in data['lanes'] for item in lane.get('items', [])
                      if item.get('key') == key)
        jobs = next(m['value'] for m in center['meta'] if m['label'] == "Puestos")
        self.assertEqual(jobs, 1, "Solo el puesto de la actividad viva; el de la archivada no.")
