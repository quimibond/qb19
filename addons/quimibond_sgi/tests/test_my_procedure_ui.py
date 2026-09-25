# -*- coding: utf-8 -*-
"""Pantalla «Mi procedimiento» (54.1.0): encabezado de persona, botones
inteligentes con conteo y tarjetas compactas agrupadas por cadencia."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestMyProcedureUi(TransactionCase):

    def test_01_pantalla_con_botones(self):
        Screen = self.env['sgi.my.procedure']
        arch = Screen.get_views([(False, 'form')])['views']['form']['arch']
        for button in ('action_show_all', 'action_show_late', 'action_show_ok', 'action_show_unmeasured',
                       'action_show_received', 'action_show_short', 'action_show_acks', 'action_show_documents',
                       'action_focus_pending', 'action_show_nc', 'action_show_measures', 'action_show_indicators',
                       'action_show_legal', 'action_show_doc_reviews', 'action_show_epp', 'action_open_obligations'):
            self.assertIn('name="%s"' % button, arch, button)
        self.assertNotIn('<notebook', arch, "Sin pestañas: cada lista es un botón.")
        self.assertIn('many2one_avatar_employee', arch)
        self.assertNotIn('string="Estado"', arch, "Los conteos ya no van en un grupo, van en botones.")
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
        self.assertIn('sgi_mp_late', arch)
        action = self.env['sgi.my.procedure'].action_open_my_team()
        self.assertEqual(action['view_mode'].split(',')[0], 'hierarchy')
        self.assertEqual(action['views'][0][1], 'hierarchy')
