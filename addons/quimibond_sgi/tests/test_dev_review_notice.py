# -*- coding: utf-8 -*-
"""57.182.0 (C1, Administración de Ventas): avisos de la revisión del análisis."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from ..models.sgi_dev_review_notice import JOB_FALLBACK, NOTICE_KIND


@tagged('post_install', '-at_install')
class TestDevReviewNotice(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        groups = 'base.group_user,project.group_project_manager'
        users = {}
        # Los puestos salen de la ficha de C1 (si la base la tiene) o, sin ella, por nombre.
        for key, numbers, role in (('producto', ['C1.02'], 'ejecuta'), ('procesos', ['C1.03'], 'ejecuta'),
                                   ('ventas', ['C1.02', 'C1.03'], 'aprueba')):
            job = cls.Project._sgi_dev_c1_role_jobs(numbers, role, key)[:1]
            if not job:
                job = cls.env['hr.job'].create({'name': JOB_FALLBACK[key].upper() + ' (prueba avisos)'})
            user = new_test_user(cls.env, login='sgi_rev_%s' % key, groups=groups, name='%s prueba' % key)
            cls.env['hr.employee'].create({'name': '%s prueba' % key, 'job_id': job.id, 'user_id': user.id})
            users[key] = user
        cls.u_producto, cls.u_procesos, cls.u_ventas = users['producto'], users['procesos'], users['ventas']
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente avisos', 'is_company': True})
        cls.dev = cls.Project.create({'name': 'x', 'sgi_is_ft': True, 'partner_id': cls.partner.id,
                                      'sgi_dev_product_name': 'Jersey avisos'})
        cls.item = cls.env['sgi.dev.feasibility.item'].create(
            {'line': 'tejido_circular', 'name': 'Máquina (prueba avisos)'})

    def _open(self, step, user=None):
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'project.project'), ('res_id', '=', self.dev.id),
            ('sgi_cron_key', '=', '%s:%s' % (NOTICE_KIND, step))])
        return acts.filtered(lambda a: a.user_id == user) if user else acts

    def _done(self, step):
        return self.env['mail.activity'].with_context(active_test=False).search([
            ('res_model', '=', 'project.project'), ('res_id', '=', self.dev.id), ('active', '=', False),
            ('sgi_cron_key', '=', '%s:%s' % (NOTICE_KIND, step))])

    def test_01_full_cycle(self):
        self.assertIn(self.dev.sgi_dev_stage_key, ('solicitud', 'analisis'))
        # 1. Producto nuevo → Diseño de Procesos contesta el checklist, con el plazo de C1.03.
        self.dev.with_user(self.u_producto).write({'sgi_dev_analysis_result': 'nuevo'})
        checklist = self._open('checklist', self.u_procesos)
        self.assertEqual(len(checklist), 1)
        self.assertEqual(checklist.date_deadline, self.dev._sgi_dev_c1_deadline('C1.03'))
        self.assertFalse(self._open('revisar'))
        # 2. Checklist cargado y contestado → Ventas revisa; la del checklist queda hecha.
        self.dev.action_sgi_dev_load_feasibility()
        self.assertFalse(self._open('revisar'), "Con renglones pendientes Ventas todavía no revisa.")
        self.dev.sgi_dev_feasibility_ids.filtered(lambda l: l.answer == 'pendiente').with_user(
            self.u_procesos).write({'answer': 'si'})
        self.assertFalse(self._open('checklist'))
        self.assertTrue(self._done('checklist'))
        self.assertEqual(len(self._open('revisar', self.u_ventas)), 1)
        # 3. Ventas regresa → Producto y Procesos corrigen, con el motivo y un correo.
        self.dev.write({'sgi_dev_review_note': 'Falta la galga'})
        self.dev.with_user(self.u_ventas).action_sgi_dev_review_return()
        self.assertEqual(self.dev.sgi_dev_review_state, 'regresado')
        self.assertFalse(self._open('revisar'))
        fix = self._open('corregir')
        self.assertTrue(self.u_producto | self.u_procesos <= fix.user_id)
        self.assertIn('Falta la galga', fix[0].note)
        msg = self.dev.message_ids.filtered(lambda m: 'regresó el análisis a Diseño' in (m.body or ''))[:1]
        self.assertEqual(msg.subtype_id, self.env.ref('mail.mt_comment'))
        self.assertTrue((self.u_producto | self.u_procesos).partner_id <= msg.partner_ids)
        # 4. «Ya corregí»: solo Diseño; vuelve a pendiente y le llega a quien regresó.
        with self.assertRaises(UserError):
            self.dev.with_user(self.u_ventas).action_sgi_dev_review_fixed()
        self.dev.with_user(self.u_producto).write({'sgi_dev_fix_note': 'Galga 18'})
        self.dev.with_user(self.u_producto).action_sgi_dev_review_fixed()
        self.assertEqual(self.dev.sgi_dev_review_state, 'pendiente')
        self.assertFalse(self.dev.sgi_dev_fix_note)
        self.assertFalse(self._open('corregir'))
        self.assertTrue(self._done('corregir'))
        again = self._open('correccion', self.u_ventas)
        self.assertEqual(len(again), 1)
        self.assertIn('Galga 18', again.note)
        # 5. Ventas aprueba → todo el ciclo queda hecho.
        self.dev.with_user(self.u_ventas).action_sgi_dev_review_approve()
        self.assertEqual(self.dev.sgi_dev_review_state, 'aprobado')
        self.assertFalse(self._open('correccion') | self._open('revisar') | self._open('checklist'))
        self.assertTrue(self._done('correccion'))

    def test_02_line_product_goes_to_sales(self):
        self.dev.write({'sgi_dev_analysis_result': 'linea'})
        self.assertFalse(self._open('checklist'))
        self.assertEqual(len(self._open('revisar', self.u_ventas)), 1)
        # Cambiar a producto nuevo cierra la revisión y manda el checklist a Procesos.
        self.dev.write({'sgi_dev_analysis_result': 'nuevo'})
        self.assertFalse(self._open('revisar'))
        self.assertEqual(len(self._open('checklist', self.u_procesos)), 1)
