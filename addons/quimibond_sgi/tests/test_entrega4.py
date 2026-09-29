# -*- coding: utf-8 -*-
"""Entrega 4 de la auditoría 2026-09: grupos Dirección y Auditor, incidentes,
revisión por la dirección y Documentos vigentes.

Cada restricción se prueba con with_user: el env de TransactionCase es
superusuario y no revisa permisos."""
from datetime import date

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools.safe_eval import safe_eval

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestEntrega4Groups(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        sgi_hide_real_documents(env)
        cls.user = new_test_user(env, login='e4_user', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.other = new_test_user(env, login='e4_other', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.auditor = new_test_user(env, login='e4_auditor',
                                    groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.director = new_test_user(env, login='e4_director',
                                     groups='base.group_user,quimibond_sgi.group_sgi_director')
        cls.mast = new_test_user(env, login='e4_mast', groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.health = new_test_user(env, login='e4_health',
                                   groups='base.group_user,quimibond_sgi.group_sgi_health')

    # ---- F-013 / E-009: Dirección consulta y aprueba, sin permisos de MAST ----
    def test_01_director_is_user_and_auditor_not_mast(self):
        self.assertFalse(self.director.has_group('quimibond_sgi.group_sgi_manager'))
        self.assertTrue(self.director.has_group('quimibond_sgi.group_sgi_user'))
        self.assertTrue(self.director.has_group('quimibond_sgi.group_sgi_auditor'))
        process = self.env['sgi.process'].create({'code': 'XE4D', 'name': 'Proceso E4'})
        self.assertEqual(process.with_user(self.director).name, 'Proceso E4', "Dirección lee.")
        with self.assertRaises(AccessError):
            process.with_user(self.director).write({'name': 'Editado por Dirección'})
        # Los menús que hoy usa Dirección (Revisión, Tablero, Administración)
        # los sigue viendo sin ser MAST.
        visible = self.env['ir.ui.menu'].with_user(self.director)._visible_menu_ids()
        for xmlid in ('menu_sgi_mgmt_review', 'menu_sgi_dashboard_health', 'menu_sgi_admin',
                      'menu_sgi_nc_all_actions', 'menu_sgi_doc_changes', 'menu_sgi_analysis',
                      'menu_sgi_approvals_native', 'menu_sgi_migration'):
            self.assertIn(self.env.ref('quimibond_sgi.' + xmlid).id, visible, xmlid)
        self.assertNotIn(self.env.ref('quimibond_sgi.menu_sgi_config').id, visible,
                         "Configuración es solo de MAST.")

    def test_02_director_approves_without_mast_write(self):
        """Aprobar: Dirección ya no escribe el presupuesto de ventas (era del
        Jefe MAST), pero lo aprueba: action_approve revisa el grupo y sella
        con sudo. El flujo completo lo cubre test_sales_budget (test_03 y
        test_04) con un usuario que solo tiene el grupo Dirección."""
        with self.assertRaises(AccessError):
            self.env['sgi.sales.budget'].with_user(self.director).check_access('write')
        # Sus ACL propias siguen: revisión por la dirección y tablero.
        self.env['sgi.management.review'].with_user(self.director).check_access('write')
        self.env['sgi.management.review.agreement'].with_user(self.director).check_access('write')
        self.env['sgi.direction.board'].with_user(self.director).check_access('create')

    def test_03_user_sees_direction_basics_only(self):
        visible = self.env['ir.ui.menu'].with_user(self.user)._visible_menu_ids()
        for xmlid in ('menu_sgi_direction', 'menu_sgi_policy', 'menu_sgi_objectives',
                      'menu_sgi_risks', 'menu_sgi_legal', 'menu_sgi_current_documents'):
            self.assertIn(self.env.ref('quimibond_sgi.' + xmlid).id, visible, xmlid)
        for xmlid in ('menu_sgi_dashboard_health', 'menu_sgi_mgmt_review', 'menu_sgi_interested_parties',
                      'menu_sgi_satisfaction', 'menu_sgi_admin'):
            self.assertNotIn(self.env.ref('quimibond_sgi.' + xmlid).id, visible, xmlid)

    # ---- F-012: el Auditor lee todo menos salud y salarios, y no escribe ----
    def test_04_auditor_no_health_no_salaries(self):
        for model in ('sgi.health.record', 'sgi.staff.efficiency', 'sgi.staff.efficiency.line'):
            with self.assertRaises(AccessError, msg=model):
                self.env[model].with_user(self.auditor).check_access('read')
        for model in ('sgi.incident', 'sgi.competence.gap', 'sgi.management.review', 'sgi.diagnostic.line'):
            self.env[model].with_user(self.auditor).check_access('read')

    def test_05_auditor_does_not_write_what_base_user_gave(self):
        project = self.env['project.project'].create({'name': 'Proyecto E4'})
        Char = self.env['sgi.dev.characteristic']
        with self.assertRaises(AccessError):
            Char.with_user(self.auditor).create({'project_id': project.id, 'name': 'Gramaje'})
        # Quien además es Usuario SGI (Dirección, MAST) sigue igual.
        char = Char.with_user(self.director).create({'project_id': project.id, 'name': 'Ancho'})
        self.assertTrue(char.exists())
        with self.assertRaises(AccessError):
            char.with_user(self.auditor).write({'name': 'Cambiado por el auditor'})

    def test_06_auditor_runs_diagnostic(self):
        action = self.env['sgi.diagnostic'].with_user(self.auditor).action_run()
        lines = self.env['sgi.diagnostic.line'].with_user(self.auditor).search(action['domain'])
        self.assertTrue(lines, "El Auditor lee el Diagnóstico (D-18).")

    # ---- D-06 / D-009: incidentes ----
    def _incident(self, user, **vals):
        return self.env['sgi.incident'].with_user(user).create(dict({
            'name': 'Resbalón E4', 'severity': 'leve', 'incident_type': 'casi_accidente'}, **vals))

    def test_07_incident_reporter_edits_while_reported_then_only_reads(self):
        inc = self._incident(self.user)
        inc.write({'description': 'Piso mojado'})
        with self.assertRaises(AccessError):
            inc.with_user(self.other).read(['name'])  # otro usuario no lo ve
        with self.assertRaises(UserError):
            inc.action_set_investigacion()  # el reportante no investiga
        inc.with_user(self.health).action_set_investigacion()
        inc.with_user(self.mast).write({
            'immediate_causes': 'Piso mojado', 'basic_causes': 'Sin procedimiento',
            'lack_of_control': 'Sin inspección'})
        self.env['sgi.action.line'].create({
            'incident_id': inc.id, 'name': 'Señalizar', 'responsible_id': self.mast.id,
            'date_commit': date.today(), 'date_done': date.today()})
        inc.with_user(self.mast).action_set_cerrado()
        # El reportante consulta cómo se cerró (causas y acciones), sin editar.
        data = inc.with_user(self.user).read(['basic_causes', 'action_line_ids', 'state'])[0]
        self.assertEqual(data['state'], 'cerrado')
        self.assertEqual(data['basic_causes'], 'Sin procedimiento')
        self.assertTrue(data['action_line_ids'])
        self.assertTrue(inc.action_line_ids.with_user(self.user).read(['name']))
        with self.assertRaises(UserError):  # candado de cerrado o regla: no edita
            inc.with_user(self.user).write({'basic_causes': 'Otra causa'})
        with self.assertRaises(UserError):
            inc.with_user(self.user).action_set_reportado()
        # Auditor y Dirección leen; no editan.
        self.assertTrue(inc.with_user(self.auditor).read(['name']))
        self.assertTrue(inc.with_user(self.director).read(['name']))
        with self.assertRaises((AccessError, UserError)):
            inc.with_user(self.director).action_set_reportado()
        # Salud ocupacional reabre (D-06), no solo el Jefe MAST.
        inc.with_user(self.health).action_set_reportado()
        self.assertEqual(inc.state, 'reportado')

    def test_08_incident_buttons_only_for_sst(self):
        arch = self.env['sgi.incident'].with_user(self.user).get_view(
            self.env.ref('quimibond_sgi.sgi_incident_view_form').id)['arch']
        for method in ('action_set_investigacion', 'action_set_cerrado', 'action_set_reportado'):
            self.assertNotIn(method, arch, "El Usuario SGI no ve «%s»." % method)
        arch = self.env['sgi.incident'].with_user(self.health).get_view(
            self.env.ref('quimibond_sgi.sgi_incident_view_form').id)['arch']
        self.assertIn('action_set_cerrado', arch)

    # ---- D-009: revisión por la dirección ----
    def test_09_management_review_close_only_mast(self):
        review = self.env['sgi.management.review'].create({
            'period_from': date(2047, 1, 1), 'period_to': date(2047, 6, 30), 'state': 'realizada'})
        with self.assertRaises(UserError):
            review.with_user(self.director).action_close()
        review.with_user(self.mast).action_close()
        self.assertEqual(review.state, 'cerrada')
        with self.assertRaises(UserError):
            review.with_user(self.director).action_draft()

    # ---- I-015 / D-21: Documentos vigentes con acuse ----
    def test_10_current_documents_and_ack(self):
        employee = self.env['hr.employee'].create({'name': 'Empleado E4', 'user_id': self.user.id})
        Doc = self.env['documents.document']
        vals = {'type': 'binary', 'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento'}
        if 'access_internal' in Doc._fields:
            vals['access_internal'] = 'view'
        current = Doc.create(dict(vals, name='Procedimiento vigente E4', sgi_code='P-G97', sgi_state='vigente'))
        draft = Doc.create(dict(vals, name='Procedimiento borrador E4', sgi_code='P-G96', sgi_state='borrador'))
        ack = self.env['sgi.document.ack'].create({'document_id': current.id, 'employee_id': employee.id})
        action = self.env.ref('quimibond_sgi.sgi_current_document_action')
        domain = safe_eval(action.domain)
        mine = Doc.with_user(self.user).search(domain + [('sgi_my_ack_state', '=', 'pendiente')])
        self.assertIn(current, mine)
        self.assertNotIn(draft, Doc.with_user(self.user).search(domain), "Solo vigentes.")
        list_arch = self.env['documents.document'].with_user(self.user).get_view(
            self.env.ref('quimibond_sgi.sgi_current_document_view_list').id, 'list')['arch']
        self.assertNotIn('sgi_code', list_arch, "Sin la clave del Dropbox (decisión 11).")
        with self.assertRaises(UserError):
            current.with_user(self.other).action_sgi_mark_my_ack_read()
        current.with_user(self.user).action_sgi_mark_my_ack_read()
        self.assertEqual(ack.state, 'leido')
        self.assertEqual(current.with_user(self.user).sgi_my_ack_state, 'leido')
