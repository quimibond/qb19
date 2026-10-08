# -*- coding: utf-8 -*-
"""57.135.0 (C1, Jose 5.5): paso pendiente del desarrollo, reloj sin materia prima y avisos por nivel."""
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_escalation import PARAM_EXECUTOR_HOURS, PARAM_OWNER_HOURS
from ..models.sgi_dev_process import PARAM_ESCALATION_DAYS


@tagged('post_install', '-at_install')
class TestDevEscalation(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Esc = cls.env['sgi.dev.escalation']
        Process = cls.env['sgi.process']
        Job = cls.env['hr.job']
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        grp = cls.env.ref('base.group_user').id
        cls.process = Process.search([('code', '=', 'C1')], limit=1) or Process.create({'code': 'C1', 'name': 'C1 prueba'})
        # Actividad C1.02 (si ya existe en la base se usa su puesto ejecutor).
        Activity = cls.env['sgi.process.activity']
        cls.a02 = Activity._sgi_dev_c1_activity('C1.02')
        if not cls.a02:
            job = Job.create({'name': 'DISEÑO Y DESARROLLO DE PRODUCTO (prueba esc)'})
            cls.a02 = Activity.create({'process_id': cls.process.id, 'name': 'Analizar la muestra (prueba esc)',
                                       'step': 2, 'sequence': 20,
                                       'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})]})
        exec_jobs = cls.a02.role_ids.filtered(lambda r: r.role == 'ejecuta')._sgi_jobs()
        cls.job_exec = exec_jobs[:1] if exec_jobs else Job.create({'name': 'DISEÑO Y DESARROLLO DE PRODUCTO (prueba esc)'})
        if not exec_jobs:
            cls.a02.write({'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_exec.id})]})
        cls.job_director = Job.create({'name': 'DIRECTOR DE OPERACIONES (prueba esc)'})
        cls.job_owner = Job.create({'name': 'ADMINISTRADOR DE VENTAS Y MARKETING (prueba esc)'})
        cls.u_exec = Users.create({'name': 'Ejecutor esc', 'login': 'sgi_dev_esc_exec', 'group_ids': [(6, 0, [grp])]})
        cls.u_owner = Users.create({'name': 'Dueña esc', 'login': 'sgi_dev_esc_owner', 'group_ids': [(6, 0, [grp])]})
        cls.u_dir = Users.create({'name': 'Director esc', 'login': 'sgi_dev_esc_dir', 'group_ids': [(6, 0, [grp])]})
        Emp = cls.env['hr.employee']
        Emp.create({'name': 'Ejecutor esc', 'job_id': cls.job_exec.id, 'user_id': cls.u_exec.id})
        cls.owner = Emp.create({'name': 'Dueña esc', 'job_id': cls.job_owner.id, 'user_id': cls.u_owner.id})
        Emp.create({'name': 'Director esc', 'job_id': cls.job_director.id, 'user_id': cls.u_dir.id})
        cls.process.sudo().write({'owner_id': cls.owner.id})
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE ESCALAMIENTO PRUEBA', 'is_company': True})
        Param = cls.env['ir.config_parameter'].sudo()
        for key in (PARAM_EXECUTOR_HOURS, PARAM_OWNER_HOURS, PARAM_ESCALATION_DAYS):
            Param.set_param(key, '')

    def _dev(self):
        Project = self.env['project.project']
        stage = Project._sgi_dev_stage('solicitud')
        dev = Project.create({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                              'sgi_dev_product_name': 'Jersey escalamiento', 'stage_id': stage.id})
        if not dev.sgi_dev_stage_log_ids:
            dev._sgi_dev_open_stage_log()
        return dev

    def _stage_started(self, dev, when):
        log = dev.sgi_dev_stage_log_ids.filtered(lambda l: not l.date_end)
        self.assertTrue(log, "El reloj de la etapa está abierto")
        log.sudo().write({'date_start': when})

    def test_01_paso_pendiente_y_horas_sin_materia_prima(self):
        dev = self._dev()
        now = fields.Datetime.now()
        step = dev._sgi_dev_pending_step()
        self.assertEqual(step['code'], 'C1.02', "Sin resultado del análisis el paso es C1.02")
        self._stage_started(dev, now - timedelta(hours=5))
        dev.invalidate_recordset()
        step = dev._sgi_dev_pending_step()
        self.assertAlmostEqual(dev._sgi_dev_hours_since(step['since'], now), 5.0, places=2)
        self.env['sgi.dev.mp.wait'].create({'project_id': dev.id, 'date_start': now - timedelta(hours=3),
                                            'date_end': now - timedelta(hours=2)})
        dev.invalidate_recordset()
        self.assertAlmostEqual(dev._sgi_dev_hours_since(step['since'], now), 4.0, places=2,
                               msg="La hora con materia prima pendiente no cuenta")
        self.assertIn('C1.02', dev.sgi_dev_pending_step)
        dev.write({'sgi_dev_analysis_result': 'nuevo'})
        step = dev._sgi_dev_pending_step()
        self.assertEqual(step['code'], 'C1.03', "Con resultado y sin revisión de Ventas: C1.03")
        self.assertTrue(dev.sgi_dev_analysis_date)
        self.assertEqual(step['since'], dev.sgi_dev_analysis_date)
        dev.write({'sgi_dev_review_state': 'aprobado', 'sgi_dev_review_date': now})
        self.assertEqual(dev._sgi_dev_pending_step()['code'], 'C1.05')
        liberado = self.env['project.project']._sgi_dev_stage('liberado')
        dev.with_context(sgi_dev_migration=True).write({'stage_id': liberado.id})
        self.assertIsNone(dev._sgi_dev_pending_step(), "Liberado: sin paso pendiente")

    def test_02_cron_por_niveles_y_cierre_al_avanzar(self):
        dev = self._dev()
        t0 = fields.Datetime.now() - timedelta(hours=1)
        self._stage_started(dev, t0)
        dev.invalidate_recordset()
        self.assertFalse(self.Esc.cron_sgi_dev_escalation(t0 + timedelta(hours=3)), "Sin parámetros no se avisa")
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_EXECUTOR_HOURS, '2')
        Param.set_param(PARAM_OWNER_HOURS, '4')
        created = self.Esc.cron_sgi_dev_escalation(t0 + timedelta(hours=3))
        mine = created.filtered(lambda r: r.project_id == dev)
        self.assertEqual(set(mine.mapped('level')), {'ejecutor'}, "A las 3 h solo el responsable")
        self.assertIn(self.u_exec, mine.user_id)
        row = mine.filtered(lambda r: r.user_id == self.u_exec)
        self.assertEqual(row.step_code, 'C1.02')
        self.assertEqual(row.since, t0)
        self.assertTrue(row.mail_activity_id.active)
        self.assertEqual(row.mail_activity_id.res_id, dev.id)
        again = self.Esc.cron_sgi_dev_escalation(t0 + timedelta(hours=3, minutes=30))
        self.assertFalse(again.filtered(lambda r: r.project_id == dev), "Segunda corrida: nada repetido")
        created = self.Esc.cron_sgi_dev_escalation(t0 + timedelta(hours=5)).filtered(lambda r: r.project_id == dev)
        self.assertEqual(set(created.mapped('level')), {'dueno'}, "A las 5 h entra el dueño del proceso")
        self.assertIn(self.u_owner, created.user_id)
        Param.set_param(PARAM_ESCALATION_DAYS, '1')
        created = self.Esc.cron_sgi_dev_escalation(t0 + timedelta(days=7)).filtered(lambda r: r.project_id == dev)
        self.assertEqual(set(created.mapped('level')), {'direccion'}, "Dirección solo al vencer sus días hábiles")
        self.assertIn(self.u_dir, created.user_id)
        open_rows = self.Esc.search([('project_id', '=', dev.id), ('state', '=', 'abierto')])
        self.assertGreaterEqual(len(open_rows), 3)
        # El paso avanza: los avisos del paso anterior se cierran solos con su actividad.
        dev.write({'sgi_dev_analysis_result': 'nuevo'})
        self.Esc.cron_sgi_dev_escalation(t0 + timedelta(days=7))
        closed = self.Esc.search([('project_id', '=', dev.id), ('step_code', '=', 'C1.02')])
        self.assertTrue(closed and all(r.state == 'cerrado' for r in closed))
        self.assertTrue(all(not r.mail_activity_id.exists() or not r.mail_activity_id.active for r in closed))
        self.assertFalse(self.Esc.search([('project_id', '=', dev.id), ('step_code', '=', 'C1.03'),
                                          ('state', '=', 'abierto')]), "C1.03 acaba de empezar: sin avisos")
