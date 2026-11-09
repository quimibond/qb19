# -*- coding: utf-8 -*-
"""57.197.0: cada botón de aprobación del desarrollo lo ve quien lo aprueba según el SGI; «Asignar folio FT» solo
Ventas, Dirección de Operaciones, Dirección del SGI y administradores. Corre también sobre una copia de producción:
los renglones del SGI de verdad no se tocan (método de prueba propio y el resto con ``patch``)."""
from unittest.mock import patch

from odoo.tests import TransactionCase, new_test_user, tagged

from ..models import sgi_dev_buttons


@tagged('post_install', '-at_install')
class TestDevButtons(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.job_dir = cls.env['hr.job'].create({'name': 'DIRECTOR BOTONES ZZ'})
        cls.director = new_test_user(cls.env, login='zz_btn_dir', groups='base.group_user,project.group_project_user')
        cls.designer = new_test_user(cls.env, login='zz_btn_dis', groups='base.group_user,project.group_project_user')
        cls.env['hr.employee'].create({'name': 'Director Botones', 'job_id': cls.job_dir.id,
                                       'user_id': cls.director.id})
        process = cls.env['sgi.process'].create({'code': 'ZBTN', 'name': 'Proceso Botones ZZ'})
        # Un solo «Ejecuta» (_sgi_check_roles), de otro puesto: quien ejecuta no aprueba.
        job_exec = cls.env['hr.job'].create({'name': 'DISEÑO BOTONES ZZ'})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Aprobar algo de prueba', 'number': '9.9',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job_exec.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': cls.job_dir.id,
                                 'approval_kind': 'boton', 'approval_method': 'zz_btn_metodo',
                                 'approval_model_id': cls.env['ir.model']._get('project.project').id})]})
        cls.project = cls.env['project.project'].create({'name': 'Proyecto botones ZZ'})

    def test_01_aprobadores_del_metodo(self):
        Project = self.env['project.project']
        self.assertIsNone(Project._sgi_dev_button_approvers('zz_sin_renglon'), "Sin renglón no se restringe")
        self.assertEqual(Project._sgi_dev_button_approvers('zz_btn_metodo'), self.director)
        self.activity.active = False
        self.assertIsNone(Project._sgi_dev_button_approvers('zz_btn_metodo'), "Actividad archivada: no cuenta")

    def _acceso(self, user, aprobadores):
        Project = type(self.env['project.project'])

        def fake(self_, method):
            return aprobadores.get(method)
        with patch.object(Project, '_sgi_dev_button_approvers', fake):
            project = self.project.with_user(user)
            project.invalidate_recordset()
            return (project.sgi_dev_can_approve_request, project.sgi_dev_can_review_approve,
                    project.sgi_dev_can_assign_folio)

    def test_02_quien_ve_cada_boton(self):
        Users = self.env['res.users']
        ventas = new_test_user(self.env, login='zz_btn_ven', groups='base.group_user,project.group_project_user')
        aprobadores = {sgi_dev_buttons.METHOD_APPROVE_REQUEST: self.director,
                       sgi_dev_buttons.METHOD_REVIEW_APPROVE: ventas}
        self.assertEqual(self._acceso(self.director, aprobadores), (True, False, True))
        self.assertEqual(self._acceso(ventas, aprobadores), (False, True, True))
        self.assertEqual(self._acceso(self.designer, aprobadores), (False, False, False),
                         "Diseño no ve botones que aprueban otros ni el folio")
        self.assertEqual(self._acceso(self.designer, {}), (True, True, True), "Sin renglones, como antes")
        self.assertEqual(self._acceso(self.designer, {sgi_dev_buttons.METHOD_APPROVE_REQUEST: Users,
                                                      sgi_dev_buttons.METHOD_REVIEW_APPROVE: Users}),
                         (False, False, False), "Renglón sin personas: nadie lo ve (no queda abierto a todos)")
