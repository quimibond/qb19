# -*- coding: utf-8 -*-
"""«Mi procedimiento» por puesto: secciones por cadencia real, detalle solo
de lo que ejecuta o aprueba, lista corta de participa / se entera,
escalamientos, huella de contenido, PDF archivado como documento controlado
del puesto y acuses que se renuevan con cada revisión."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_calendar import sgi_test_calendar
from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestMyProcedure(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_test_calendar(cls.env)
        Job = cls.env['hr.job']
        cls.job = Job.create({'name': 'PLANEADOR PRUEBA MP'})
        cls.job_boss = Job.create({'name': 'GERENTE PRUEBA MP'})
        cls.job_other = Job.create({'name': 'OTRO PUESTO MP'})
        cls.family = cls.env['sgi.job.family'].create({
            'code': 'FAM-MP', 'name': 'Familia MP', 'job_ids': [(6, 0, cls.job.ids)]})
        cls.user_emp = new_test_user(cls.env, login='mp_emp',
                                     groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.manager = new_test_user(cls.env, login='mp_manager',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.emp1 = cls.env['hr.employee'].create({
            'name': 'Emp MP Uno', 'job_id': cls.job.id, 'user_id': cls.user_emp.id})
        cls.emp2 = cls.env['hr.employee'].create({'name': 'Emp MP Dos', 'job_id': cls.job.id})
        cls.Process = cls.env['sgi.process']
        cls.Activity = cls.env['sgi.process.activity']
        cls.p_a = cls.Process.create({'code': 'MPA', 'name': 'Proceso A MP'})
        cls.p_b = cls.Process.create({'code': 'MPB', 'name': 'Proceso B MP'})

        def act(process, name, roles, **vals):
            return cls.Activity.create(dict({
                'process_id': process.id, 'name': name,
                'role_ids': [(0, 0, r) for r in roles],
            }, **vals))

        # Semanal, vence viernes; ejecuta el puesto; escala al gerente a los 2 días.
        cls.a_weekly_fri = act(cls.p_b, 'Programa semanal', [
            {'role': 'ejecuta', 'job_id': cls.job.id},
            {'role': 'escala', 'job_id': cls.job_boss.id, 'after_days': 2},
        ], measure_cadence='semanal', due_weekday='4',
            done_criteria='El programa está publicado', on_fail='Avisar a Ventas')
        # Semanal, vence lunes; llega por la familia.
        cls.a_weekly_mon = act(cls.p_a, 'Revisar pedidos', [
            {'role': 'ejecuta', 'target_type': 'family', 'family_id': cls.family.id},
        ], measure_cadence='semanal', due_weekday='0')
        # Mensual, día hábil 3; el puesto solo aprueba.
        cls.a_monthly = act(cls.p_a, 'Cerrar el mes', [
            {'role': 'ejecuta', 'job_id': cls.job_other.id},
            {'role': 'aprueba', 'job_id': cls.job.id},
        ], measure_cadence='mensual', due_business_day=3)
        # Trimestral sin agrupar con mensual.
        cls.a_quarterly = act(cls.p_a, 'Revisión trimestral', [
            {'role': 'ejecuta', 'job_id': cls.job.id},
        ], measure_cadence='trimestral')
        # Por evento; el puesto solo participa.
        cls.a_event = act(cls.p_b, 'Atender reclamación', [
            {'role': 'ejecuta', 'job_id': cls.job_other.id},
            {'role': 'participa', 'job_id': cls.job.id},
        ], measure_cadence='evento')
        # Otro puesto ejecuta y escala a este puesto.
        cls.a_received = act(cls.p_b, 'Embarcar', [
            {'role': 'ejecuta', 'job_id': cls.job_other.id},
            {'role': 'escala', 'job_id': cls.job.id, 'after_days': 1},
        ], measure_cadence='diaria')

    # Los calculados sin depends se cachean en la transacción: se invalidan
    # antes de leerlos de nuevo (en la interfaz cada petición recalcula).
    def _ack_state(self):
        self.emp1.invalidate_recordset(['sgi_my_procedure_ack_state'])
        return self.emp1.sgi_my_procedure_ack_state

    def _stale(self):
        self.job.invalidate_recordset(['sgi_my_procedure_doc_id', 'sgi_my_procedure_stale'])
        return self.job.sgi_my_procedure_stale

    # ------------------------------------------------------------------
    def test_01_secciones_por_cadencia_real_y_orden(self):
        data = self.job._sgi_my_procedure_data()
        labels = [s['label'] for s in data['sections']]
        self.assertEqual(labels, ['Semanal', 'Mensual', 'Trimestral'],
                         "Solo las cadencias con actividades, sin agrupar, en orden fijo.")
        weekly = data['sections'][0]['entries']
        self.assertEqual([e['name'] for e in weekly], ['Revisar pedidos', 'Programa semanal'],
                         "Dentro de la sección: por cuándo (lunes antes que viernes).")
        self.assertEqual(weekly[0]['when'], 'Cada lunes')
        self.assertEqual(weekly[1]['when'], 'Cada viernes')
        self.assertEqual(weekly[1]['escalates_to'], ['GERENTE PRUEBA MP (a los 2 días hábiles)'])
        monthly = data['sections'][1]['entries'][0]
        self.assertEqual(monthly['roles'], ['Aprueba'])
        self.assertEqual(monthly['when'], 'Día hábil 3 del mes')
        labels_in_parts = [label for label, _t in weekly[1]['parts']]
        self.assertIn('Terminada cuando', labels_in_parts)
        self.assertIn('Si no se puede', labels_in_parts)
        self.assertNotIn(None, labels_in_parts, "El «Vence cada…» sale en la columna, no repetido.")

    def test_02_lista_corta_y_escalamientos_recibidos(self):
        data = self.job._sgi_my_procedure_data()
        self.assertEqual([e['name'] for e in data['short']], ['Atender reclamación'])
        self.assertEqual(data['short'][0]['roles'], ['Participa'])
        self.assertEqual([e['name'] for e in data['received']], ['Embarcar'])
        self.assertEqual(data['received'][0]['from'], 'OTRO PUESTO MP')
        self.assertEqual(data['received'][0]['after_days'], 1)
        cover = data['cover']
        self.assertEqual(set(cover['employees']), {self.emp1, self.emp2})
        self.assertEqual([p.code for p in cover['processes']], ['MPA', 'MPB'])
        self.assertEqual(dict(cover['counts']),
                         {'Ejecuta': 3, 'Aprueba': 1, 'Participa': 1, 'Escala': 1})
        self.assertEqual(data['total'], 5)

    def test_03_huella_estable_y_sensible(self):
        first = self.job._sgi_my_procedure_data()['hash']
        self.assertEqual(first, self.job._sgi_my_procedure_data()['hash'], "Estable.")
        self.a_weekly_fri.done_criteria = 'Otro criterio'
        self.assertNotEqual(first, self.job._sgi_my_procedure_data()['hash'],
                            "Cambia lo que la persona debe hacer → cambia la huella.")

    def test_04_publicar_archiva_pdf_y_pide_acuses(self):
        job = self.job.with_user(self.manager)
        with self.assertRaises(UserError, msg="Solo el Jefe MAST publica."):
            self.job.with_user(self.user_emp).action_sgi_publish_my_procedure()
        job.action_sgi_publish_my_procedure()
        doc = self.job._sgi_my_procedure_current_doc()
        self.assertTrue(doc, "Queda un documento controlado vigente del puesto.")
        self.assertEqual(doc.sgi_code, 'MP-%03d' % self.job.id)
        self.assertEqual(doc.sgi_doc_type, 'mi_procedimiento')
        self.assertEqual(doc.sgi_revision, 0)
        self.assertTrue(doc.datas, "Trae el PDF.")
        self.assertEqual(doc.sgi_job_ids, self.job)
        self.assertEqual(set(doc.sgi_ack_ids.mapped('employee_id')), {self.emp1, self.emp2})
        self.assertTrue(all(a.state == 'pendiente' for a in doc.sgi_ack_ids))
        self.assertEqual(self._ack_state(), 'pendiente')
        # Sin cambios: no hay revisión nueva.
        job.action_sgi_publish_my_procedure()
        self.assertEqual(self.job._sgi_my_procedure_current_doc(), doc)
        self.assertFalse(self._stale())
        # El empleado firma.
        ack = doc.sgi_ack_ids.filtered(lambda a: a.employee_id == self.emp1)
        ack.with_user(self.user_emp).action_mark_read()
        self.assertEqual(self._ack_state(), 'leido')
        # Cambian las actividades → revisión nueva, la anterior obsoleta y
        # TODOS vuelven a quedar pendientes.
        self.a_weekly_fri.how_steps = 'Abrir el programa → publicar'
        self.assertTrue(self._stale())
        job.action_sgi_publish_my_procedure()
        new_doc = self.job._sgi_my_procedure_current_doc()
        self.assertNotEqual(new_doc, doc)
        self.assertEqual((new_doc.sgi_revision, doc.sgi_state), (1, 'obsoleto'))
        self.assertEqual(set(new_doc.sgi_ack_ids.mapped('employee_id')), {self.emp1, self.emp2})
        self.assertTrue(all(a.state == 'pendiente' for a in new_doc.sgi_ack_ids))
        self.assertEqual(self._ack_state(), 'pendiente')

    def test_05_empleado_y_vista(self):
        action = self.emp1.action_sgi_my_procedure_view()
        Role = self.env['sgi.activity.role']
        roles = Role.search(action['domain'])
        self.assertEqual(set(roles.activity_id),
                         {self.a_weekly_fri, self.a_weekly_mon, self.a_monthly,
                          self.a_quarterly, self.a_event, self.a_received},
                         "Todos los roles del puesto y de su familia, incluido el escalamiento.")
        self.assertEqual(roles.filtered(lambda r: r.activity_id == self.a_weekly_fri
                                        and r.role == 'ejecuta').activity_when, 'Cada viernes')
        report = self.emp1.action_sgi_print_my_procedure()
        self.assertEqual(report['report_name'], 'quimibond_sgi.report_my_procedure_document')
        no_job = self.env['hr.employee'].create({'name': 'Sin puesto MP'})
        with self.assertRaises(UserError):
            no_job.action_sgi_print_my_procedure()

    def test_06_mis_actividades_todos_los_roles(self):
        Activity = self.Activity.with_user(self.user_emp)
        view = self.env.ref('quimibond_sgi.sgi_process_activity_view_search')
        arch = Activity.get_view(view.id, 'search')['arch']
        self.assertIn("role_ids.job_id.employee_ids.user_id", arch)
        domain = ['|', '|', ('responsible_job_ids.employee_ids.user_id', '=', self.user_emp.id),
                  ('role_ids.job_id.employee_ids.user_id', '=', self.user_emp.id),
                  ('role_ids.family_id.job_ids.employee_ids.user_id', '=', self.user_emp.id)]
        mine = Activity.search(domain)
        self.assertIn(self.a_monthly, mine, "Aprueba: ahora sí aparece en «Mis actividades».")
        self.assertIn(self.a_event, mine, "Participa también.")
        self.assertIn(self.a_weekly_mon, mine, "Y lo que llega por la familia.")

    # ------------------------------------------------------------------
    # Pantalla (Inicio → Mi procedimiento)
    # ------------------------------------------------------------------
    def test_07_pantalla_secciones_tarjetas_y_botones(self):
        menu = self.env['ir.ui.menu'].search(
            [('action', 'like', 'ir.actions.act_window,')], limit=1)
        self.a_weekly_fri.write({'odoo_menu_id': menu.id, 'how_steps': 'Abrir → publicar',
                                 'check_against': 'Pedidos confirmados'})
        self.a_weekly_fri.measure_state = 'verde'
        self.a_quarterly.measure_state = 'rojo'
        Wiz = self.env['sgi.my.procedure'].with_user(self.user_emp)
        action = Wiz.action_open_mine()
        wiz = Wiz.browse(action['res_id'])
        self.assertEqual((wiz.employee_id.id, wiz.job_id), (self.emp1.id, self.job))
        self.assertFalse(wiz.can_pick, "Sin equipo ni permisos: solo su puesto.")
        self.assertEqual(wiz.ack_state, 'sin_publicar')
        html = wiz.content
        for text in ('Semanal', 'Mensual', 'Trimestral', 'Programa semanal', 'Cada viernes',
                     'Día hábil 3 del mes', '<details', 'Ir a hacerlo', '/odoo/action-%d' % menu.action.id,
                     'Abrir → publicar', 'Pedidos confirmados', 'El programa está publicado',
                     'Avisar a Ventas', 'GERENTE PRUEBA MP (a los 2 días hábiles)',
                     'Participa o se entera', 'Atender reclamación', 'Escalamientos que recibe',
                     'Embarcar', 'Al día', 'Atrasada', 'Ver actividad'):
            self.assertIn(text, html, text)
        self.assertNotIn('Diario <span', html,
                         "Sin actividades diarias del puesto no hay sección Diario.")

    def test_08_pantalla_firma_contra_revision_vigente(self):
        Wiz = self.env['sgi.my.procedure'].with_user(self.user_emp)
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        with self.assertRaises(UserError, msg="Sin revisión publicada no hay qué firmar."):
            wiz.action_sign()
        self.job.with_user(self.manager).action_sgi_publish_my_procedure()
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        self.assertEqual(wiz.ack_state, 'pendiente')
        self.assertTrue(wiz.is_me)
        wiz.action_sign()
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        self.assertEqual(wiz.ack_state, 'leido')
        doc = self.job._sgi_my_procedure_current_doc()
        ack = doc.sgi_ack_ids.filtered(lambda a: a.employee_id == self.emp1)
        self.assertEqual(ack.state, 'leido')
        # Otro no puede firmar por él.
        other_user = new_test_user(self.env, login='mp_other',
                                   groups='base.group_user,quimibond_sgi.group_sgi_user')
        self.emp2.user_id = other_user
        wiz2 = self.env['sgi.my.procedure'].with_user(other_user).create({'employee_id': self.emp1.id})
        with self.assertRaises(UserError):
            wiz2.action_sign()

    def test_09_pantalla_quien_ve_a_quien(self):
        boss_user = new_test_user(self.env, login='mp_boss',
                                  groups='base.group_user,quimibond_sgi.group_sgi_user')
        boss = self.env['hr.employee'].create({
            'name': 'Jefe MP', 'job_id': self.job_boss.id, 'user_id': boss_user.id})
        self.emp1.parent_id = boss
        Wiz = self.env['sgi.my.procedure'].with_user(boss_user)
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        self.assertTrue(wiz.can_pick, "Un jefe elige entre su equipo.")
        self.assertIn(self.emp1.id, wiz.allowed_employee_ids.ids)
        self.assertNotIn(self.emp2.id, wiz.allowed_employee_ids.ids, "Emp Dos no le reporta.")
        self.assertIn(self.job, wiz.allowed_job_ids)
        # Dueño de proceso: ve a los puestos con rol en su proceso.
        owner_user = new_test_user(self.env, login='mp_owner',
                                   groups='base.group_user,quimibond_sgi.group_sgi_user')
        owner = self.env['hr.employee'].create({'name': 'Dueño MP', 'user_id': owner_user.id})
        self.p_b.owner_id = owner
        team = owner._sgi_mp_team_employees()
        self.assertIn(self.emp2, team, "Emp Dos ocupa un puesto con rol en Proceso B.")
        # Jefe MAST: cualquiera.
        wiz_m = self.env['sgi.my.procedure'].with_user(self.manager).create({'employee_id': self.emp2.id})
        self.assertTrue(wiz_m.can_pick)
        self.assertTrue(wiz_m.can_publish)
        self.assertEqual(wiz_m.job_id, self.job)
        self.assertEqual(wiz_m.ack_state, 'sin_publicar')
        self.assertFalse(wiz_m.is_me)
