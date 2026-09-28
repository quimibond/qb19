# -*- coding: utf-8 -*-
"""Pantalla «Mi procedimiento» (54.1.0): encabezado de persona, botones
inteligentes con conteo y tarjetas compactas agrupadas por cadencia."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestMyProcedureUi(TransactionCase):

    def test_01_pantalla_con_botones(self):
        Screen = self.env['sgi.my.procedure']
        arch = Screen.get_views([(False, 'form')])['views']['form']['arch']
        # 56.7.0: ficha con pestañas. Los botones inteligentes solo para lo
        # que vive en otra app; las actividades se ven al abrir.
        for button in ('action_show_all', 'action_show_pending', 'action_show_acks',
                       'action_show_indicators', 'action_open_obligations',
                       'action_sign', 'action_propose_new_activity', 'action_print'):
            self.assertIn('name="%s"' % button, arch, button)
        # 56.3.0: un solo botón de pendientes; 56.7.0: publicar es de MAST y
        # vive en Administración SGI, no en la pantalla del empleado.
        for button in ('action_show_late', 'action_show_ok', 'action_show_unmeasured', 'action_focus_pending',
                       'action_show_nc', 'action_show_measures', 'action_show_legal', 'action_show_doc_reviews',
                       'action_publish', 'action_publish_all', 'action_precheck'):
            self.assertNotIn('name="%s"' % button, arch, button)
        for page in ('actividades', 'escalamientos', 'participa', 'documentos', 'epp', 'firmas', 'personas'):
            self.assertIn('name="%s"' % page, arch, page)
        self.assertLess(arch.index('name="actividades"'), arch.index('name="documentos"'),
                        "Mis actividades es la primera pestaña.")
        self.assertIn('name="employee_avatar"', arch)
        check = self.env['sgi.my.procedure.check'].action_open()
        self.assertEqual(check['res_model'], 'sgi.my.procedure.check')
        check_arch = self.env['sgi.my.procedure.check'].get_views([(False, 'form')])['views']['form']['arch']
        self.assertIn('name="action_publish_all"', check_arch)
        self.assertEqual(self.env.ref('quimibond_sgi.menu_sgi_my_procedure_publish').action,
                         self.env.ref('quimibond_sgi.sgi_my_procedure_action_publish'))
        job = self.env['hr.job'].create({'name': 'PUESTO UI'})
        wiz = Screen.create({'job_id': job.id})
        self.assertEqual((wiz.activity_count, wiz.pending_count), (0, 0))
        for method in ('action_show_all', 'action_show_late', 'action_show_ok', 'action_show_unmeasured'):
            action = getattr(wiz, method)()
            self.assertEqual(action['res_model'], 'sgi.activity.role', method)
            self.assertEqual(action['domain'], [('id', 'in', [])], method)
        self.assertEqual(wiz.action_focus_pending()['res_model'], 'sgi.action.line')
        self.assertEqual(wiz.action_show_acks()['res_model'], 'sgi.document.ack')
        self.assertEqual(wiz.action_show_documents()['res_model'], 'documents.document')
        self.assertEqual(wiz.action_show_nc()['res_model'], 'quality.alert')
        self.assertEqual(wiz.action_show_epp()['res_model'], 'sgi.epp.delivery')
        self.assertFalse(wiz.action_show_received()['context']['create'])
        kanban = self.env['sgi.activity.role'].get_view(
            self.env.ref('quimibond_sgi.sgi_activity_role_view_kanban_mp').id, 'kanban')['arch']
        self.assertIn('default_group_by="cadence"', kanban)
        self.assertIn('data-bs-toggle="collapse"', kanban, "El detalle largo va plegado.")

    def test_02_mi_equipo_abre_en_organigrama(self):
        views = self.env['hr.employee.public'].get_views([(False, 'hierarchy')])
        arch = views['views']['hierarchy']['arch']
        self.assertIn('parent_field="parent_id"', arch)
        self.assertIn('sgi_mp_pending_state', arch, "56.3.0: el semáforo de pendientes por persona.")
        action = self.env['sgi.my.procedure'].action_open_my_team()
        self.assertEqual(action['view_mode'].split(',')[0], 'hierarchy')
        self.assertEqual(action['views'][0][1], 'hierarchy')
