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
        """Dirección conserva sus ACL propias: revisión por la dirección y
        tablero. Lo del presupuesto de ventas (no lo escribe, lo aprueba) se
        mudó a quimibond_ventas_presupuesto en 57.11.0 (A-016, J-018)."""
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

    # ---- Decisión de Jose (2026-09-29): Dirección conserva tres administraciones ----
    def test_11_director_keeps_admin_groups_direct(self):
        director = new_test_user(self.env, login='e4_director_admin',
                                 groups='base.group_user,quimibond_sgi.group_sgi_director')
        admin_xmlids = ('project.group_project_manager', 'approvals.group_approval_manager',
                        'helpdesk.group_helpdesk_manager')
        self.env['sgi.config'].sudo()._sgi_director_keep_admin_groups()
        for xmlid in admin_xmlids:
            self.assertIn(self.env.ref(xmlid), director.group_ids, "%s directo." % xmlid)
        # Idempotente: una segunda corrida no agrega nada a este usuario.
        again = self.env['sgi.config'].sudo()._sgi_director_keep_admin_groups()
        self.assertNotIn(director.login, again)
        # Pierde solo lo de Jefe MAST (y no ve salarios).
        mast = self.env.ref('quimibond_sgi.group_sgi_manager')
        self.assertNotIn(mast, director.group_ids)
        self.assertNotIn(mast, director.all_group_ids)
        self.assertFalse(director.has_group('quimibond_sgi.group_sgi_salary'))
        with self.assertRaises(AccessError):
            self.env['sgi.document.type'].with_user(director).check_access('write')
        review = self.env['sgi.management.review'].create({
            'period_from': date(2048, 1, 1), 'period_to': date(2048, 6, 30), 'state': 'realizada'})
        with self.assertRaises(UserError):
            review.with_user(director).action_close()
        # Conserva lo de Dirección (test_01 y test_02).
        self.assertTrue(director.has_group('quimibond_sgi.group_sgi_user'))
        self.assertTrue(director.has_group('quimibond_sgi.group_sgi_auditor'))
        self.env['sgi.management.review'].with_user(director).check_access('write')
        self.env['sgi.direction.board'].with_user(director).check_access('create')
        visible = self.env['ir.ui.menu'].with_user(director)._visible_menu_ids()
        self.assertIn(self.env.ref('quimibond_sgi.menu_sgi_mgmt_review').id, visible)
        self.assertNotIn(self.env.ref('quimibond_sgi.menu_sgi_config').id, visible,
                         "Configuración sigue siendo solo de MAST.")

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


@tagged('post_install', '-at_install')
class TestEntrega4SensitiveModels(TransactionCase):
    """Los 5 modelos sensibles quedan restringidos a su grupo (decisión de
    Jose, 2026-09-29)."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.user = new_test_user(env, login='e4s_user', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.mast = new_test_user(env, login='e4s_mast', groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.capture = new_test_user(env, login='e4s_capture',
                                    groups='base.group_user,quimibond_sgi.group_sgi_efficiency_capture')
        cls.payroll = new_test_user(env, login='e4s_salary',
                                    groups='base.group_user,quimibond_sgi.group_sgi_salary')
        cls.hr = new_test_user(env, login='e4s_hr', groups='base.group_user,hr.group_hr_user')
        cls.csh = new_test_user(env, login='e4s_csh', groups='base.group_user,quimibond_sgi.group_sgi_csh')
        cls.health = new_test_user(env, login='e4s_health',
                                   groups='base.group_user,quimibond_sgi.group_sgi_health')
        cls.auditor = new_test_user(env, login='e4s_auditor',
                                    groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.dept = env['hr.department'].create({'name': 'E4 Tejido'})
        cls.boss_emp = env['hr.employee'].create({'name': 'E4 Jefe', 'user_id': cls.capture.id,
                                                  'department_id': cls.dept.id})
        cls.worker = env['hr.employee'].create({'name': 'E4 Tejedor', 'department_id': cls.dept.id})

    # ---- sgi.staff.efficiency(.line): salarios solo para «Salarios de eficiencias» ----
    def _sheet(self):
        sheet = self.env['sgi.staff.efficiency'].create({'period_date': date(2046, 5, 1),
                                                         'department_id': self.dept.id})
        line = self.env['sgi.staff.efficiency.line'].create({
            'sheet_id': sheet.id, 'employee_id': self.worker.id, 'wage_daily': 400.0,
            'attendance_pct': 5.5, 'efficiency_pct': 2.0})
        return sheet, line

    def test_01_salaries_only_for_salary_group(self):
        sheet, line = self._sheet()
        money = ['wage_daily', 'wage_monthly', 'amount']
        data = line.with_user(self.payroll).read(money)[0]
        self.assertEqual(data['wage_daily'], 400.0)
        self.assertTrue(sheet.with_user(self.payroll).read(['amount_total'])[0]['amount_total'])
        for user in (self.mast, self.capture, self.hr):
            with self.assertRaises(AccessError, msg=user.login):
                line.with_user(user).read(['amount'])
            with self.assertRaises(AccessError, msg=user.login):
                sheet.with_user(user).read(['amount_total'])
            # Ven la eficiencia sin el importe.
            self.assertEqual(line.with_user(user).read(['total_pct'])[0]['total_pct'], 7.5)
        # Exportar pasa por los mismos groups del campo: no aparece en la
        # lista de campos exportables de quien no es del grupo de salarios.
        self.assertNotIn('amount', line.with_user(self.mast).fields_get())
        Report = self.env['ir.actions.report']
        name = 'quimibond_sgi.report_staff_efficiency_document'
        self.assertNotIn(b'A pagar', Report.with_user(self.mast)._render_qweb_html(name, sheet.ids)[0],
                         "El PDF del Jefe MAST no lleva importes.")
        self.assertIn(b'A pagar', Report.with_user(self.payroll)._render_qweb_html(name, sheet.ids)[0])

    def test_01b_payroll_group_alone_does_not_see_salaries(self):
        """Nómina (hr_payroll.group_hr_payroll_user) ya no basta: en
        producción incluye a usuarios que no deben ver salarios."""
        payroll_group = self.env.ref('hr_payroll.group_hr_payroll_user', raise_if_not_found=False)
        if not payroll_group:
            self.skipTest("hr_payroll no está instalado.")
        payroll = new_test_user(self.env, login='e4s_payroll_only', groups='base.group_user')
        payroll.write({'group_ids': [(4, payroll_group.id)]})
        self.assertFalse(payroll.has_group('quimibond_sgi.group_sgi_salary'))
        sheet, line = self._sheet()
        self.assertTrue(line.with_user(self.payroll).read(['amount'])[0]['amount'],
                        "El grupo de salarios ve el importe.")
        for field in ('wage_daily', 'wage_monthly', 'amount'):
            self.assertEqual(line._fields[field].groups, 'quimibond_sgi.group_sgi_salary', field)
            self.assertNotIn(field, line.with_user(payroll).fields_get(), field)
        self.assertNotIn('amount_total', sheet.with_user(payroll).fields_get())
        self.assertFalse(sheet.with_user(payroll).sgi_show_money())

    # ---- hr.version: campos del SGI solo RH ----
    def test_02_hr_version_fields_only_hr(self):
        for name in ('sgi_departure_reason_id', 'sgi_departure_registered_at'):
            self.assertEqual(self.env['hr.version']._fields[name].groups, 'hr.group_hr_user', name)
        version = self.worker.version_id
        self.assertTrue(version.with_user(self.hr).read(['sgi_departure_reason_id']))

    # ---- account.move.line: diferencia contra la OC solo para Contabilidad ----
    def test_03_move_line_field_only_accounting(self):
        field = self.env['account.move.line']._fields['sgi_po_price_diff']
        self.assertEqual(field.groups, 'account.group_account_readonly')
        with self.assertRaises(AccessError):
            self.env['account.move.line'].with_user(self.user).search_read(
                [('id', '=', 0)], ['sgi_po_price_diff'])

    # ---- sgi.competence.gap: lo suyo y lo de su equipo ----
    def test_04_competence_gaps_own_and_team(self):
        Gap = self.env['sgi.competence.gap']
        total = Gap.search_count([])
        self.assertEqual(Gap.with_user(self.hr).search_count([]), total, "RH ve todas.")
        self.assertEqual(Gap.with_user(self.mast).search_count([]), total, "MAST ve todas.")
        self.assertEqual(Gap.with_user(self.auditor).search_count([]), total, "El Auditor ve todas.")
        for user in (self.user, self.capture):
            for gap in Gap.with_user(user).search([]).sudo():
                self.assertIn(user, gap.employee_id.user_id | gap.employee_id.parent_id.user_id
                              | gap.department_id.manager_id.user_id,
                              "%s solo ve sus brechas y las de su equipo." % user.login)
        rule = self.env.ref('quimibond_sgi.rule_sgi_competence_gap_user_own')
        self.assertIn(self.env.ref('quimibond_sgi.group_sgi_user'), rule.groups)

    # ---- sgi.csh.finding: Comisión, Salud, MAST (y el Auditor lee) ----
    def test_05_csh_findings_restricted(self):
        inspection = self.env['sgi.csh.inspection'].create({
            'date': date(2046, 5, 5), 'area': 'Tejido',
            'finding_ids': [(0, 0, {'description': 'Operador sin guantes', 'severity': 'media'})]})
        finding = inspection.finding_ids
        for user in (self.csh, self.health, self.mast):
            self.assertTrue(finding.with_user(user).read(['description']), user.login)
        finding.with_user(self.csh).write({'disposition': 'corregido'})
        self.assertTrue(finding.with_user(self.auditor).read(['description']))
        with self.assertRaises(AccessError):
            finding.with_user(self.auditor).write({'description': 'x'})
        with self.assertRaises(AccessError):
            finding.with_user(self.user).read(['description'])
        # El Usuario SGI ve el recorrido y su conteo, no los hallazgos.
        data = inspection.with_user(self.user).read(['finding_count', 'name'])[0]
        self.assertEqual(data['finding_count'], 1)
        with self.assertRaises(AccessError):
            inspection.with_user(self.user).read(['finding_ids'])
        with self.assertRaises(UserError):
            inspection.with_user(self.user).action_close()
        inspection.with_user(self.csh).action_close()
        self.assertEqual(inspection.state, 'cerrado')
        group = self.env.ref('quimibond_sgi.group_sgi_csh')
        self.assertIn(self.env.ref('quimibond_sgi.group_sgi_user'), group.implied_ids)
