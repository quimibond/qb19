# -*- coding: utf-8 -*-
"""Pantalla «Mi procedimiento» (54.1.0): encabezado de persona, botones
inteligentes con conteo y tarjetas compactas agrupadas por cadencia."""
from lxml import etree

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
        # vive en Administración, no en la pantalla del empleado.
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
        self.assertEqual(wiz.activity_count, 0)
        for method in ('action_show_all',):
            action = getattr(wiz, method)()
            self.assertEqual(action['res_model'], 'sgi.activity.role', method)
            self.assertEqual(action['domain'], [('id', 'in', [])], method)
        self.assertEqual(wiz.action_show_acks()['res_model'], 'sgi.document.ack')
        # 57.8.0 (B-011, I-022): los botones sin vista y las listas de
        # pendientes se retiraron de la pantalla.
        for gone in ('action_show_late', 'action_show_ok', 'action_show_unmeasured',
                     'action_focus_pending', 'action_show_documents', 'action_show_nc',
                     'action_show_epp', 'action_precheck', 'pending_action_ids'):
            self.assertFalse(hasattr(wiz, gone), gone)
        self.assertFalse(wiz.action_show_received()['context']['create'])
        # 56.7.0: la lista es la vista principal; las tarjetas, en el celular.
        mine = wiz.action_show_all()
        self.assertEqual(mine['view_mode'].split(',')[0], 'list')
        self.assertEqual(mine['views'][0][1], 'list')
        self.assertEqual(mine['mobile_view_mode'], 'kanban')
        for model in ('hr.employee', 'hr.job'):
            tab = self.env[model].get_views([(False, 'form')])['views']['form']['arch']
            self.assertNotIn('sgi_activity_role_view_kanban_mp', tab, model)
        kanban = self.env['sgi.activity.role'].get_view(
            self.env.ref('quimibond_sgi.sgi_activity_role_view_kanban_mp').id, 'kanban')['arch']
        self.assertIn('default_group_by="cadence"', kanban)
        self.assertIn('data-bs-toggle="collapse"', kanban, "El detalle largo va plegado.")

    def test_02_mi_equipo_abre_en_organigrama(self):
        # La acción de Mi equipo abre SU organigrama (sgi_my_team_view_hierarchy,
        # 54.4.0), no el organigrama por omisión de hr.employee.public: en una
        # base nueva de Odoo 19 el por omisión es el nativo de RH
        # (js_class="hr_employee_hierarchy"), que no lleva el semáforo. Se
        # revisa la vista que devuelve la acción.
        action = self.env['hr.employee.public'].action_open_my_team()
        view = self.env.ref('quimibond_sgi.sgi_my_team_view_hierarchy')
        self.assertEqual(action['views'][0], (view.id, 'hierarchy'))
        views = self.env['hr.employee.public'].get_views([action['views'][0]])
        arch = views['views']['hierarchy']['arch']
        # Odoo 19 ya no escribe parent_field en el organigrama de empleados
        # (child_field="child_ids"; parent_field vale parent_id por omisión).
        root = etree.fromstring(arch)
        self.assertEqual(root.get('parent_field', 'parent_id'), 'parent_id')
        self.assertEqual(root.get('child_field', 'child_ids'), 'child_ids')
        self.assertIn('sgi_mp_pending_state', arch, "56.3.0: el semáforo de pendientes por persona.")
        self.assertEqual(action['view_mode'].split(',')[0], 'hierarchy')
        self.assertEqual(action['views'][0][1], 'hierarchy')
