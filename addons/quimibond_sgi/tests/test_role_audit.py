# -*- coding: utf-8 -*-
"""56.7.0: auditoría funcional por rol (28-sep-2026). Cada prueba entra como
un usuario real del rol (with_user) y revisa lo que ese rol debe poder y no
debe poder hacer."""
import functools
import sys
from contextlib import ExitStack
from datetime import date
from unittest.mock import patch

from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


class _RoleArchiveDiag:
    """57.87.0: diagnóstico de test_07 (falla en base nueva y en staging sin
    causa visible en el código). Envuelve, sin cambiar lo que hacen,
    ``hr.employee._sgi_mp_touch_jobs``, ``hr.employee._compute_sgi_mp_roles_stored``
    y ``sgi.activity.role._sgi_mp_jobs``, y anota qué pasó con el empleado y
    el rol de la prueba. Solo lee la caché por dentro (``_get_cache``) y la
    base con SQL directo: no dispara cálculos ni flush que cambien el orden
    de lo que se quiere observar."""

    def __init__(self, env, emp, role, activity):
        self.env, self.emp_id, self.role_id, self.act_id = env, emp.id, role.id, activity.id
        self.f_roles = env['hr.employee']._fields['sgi_mp_role_ids']
        self.f_act_active = env['sgi.activity.role']._fields['activity_active']
        self.f_active = env['sgi.process.activity']._fields['active']
        self.events = []
        self.snaps = []

    # -- lectura sin efectos -------------------------------------------
    def _cache(self, field, rec_id, env=None):
        try:
            value = field._get_cache(env or self.env).get(rec_id, 'sin caché')
        except Exception as exc:  # noqa: BLE001 (solo diagnóstico)
            value = 'error %r' % exc
        return list(value) if isinstance(value, tuple) else value

    def _db_roles(self):
        self.env.cr.execute('SELECT role_id FROM hr_employee_sgi_mp_role_rel '
                            'WHERE employee_id = %s ORDER BY role_id', (self.emp_id,))
        return [r[0] for r in self.env.cr.fetchall()]

    def _db_activity(self):
        self.env.cr.execute('SELECT active FROM sgi_process_activity WHERE id = %s', (self.act_id,))
        act = self.env.cr.fetchone()
        self.env.cr.execute('SELECT activity_active FROM sgi_activity_role WHERE id = %s', (self.role_id,))
        role = self.env.cr.fetchone()
        return (act and act[0], role and role[0])

    def _pending(self):
        tocompute = self.env.transaction.tocompute
        return {
            'emp.sgi_mp_role_ids': self.emp_id in tocompute.get(self.f_roles, ()),
            'role.activity_active': self.role_id in tocompute.get(self.f_act_active, ()),
            'hr.employee': sorted(f.name for f, ids in tocompute.items()
                                  if f.model_name == 'hr.employee' and ids),
            'sgi.activity.role': sorted(f.name for f, ids in tocompute.items()
                                        if f.model_name == 'sgi.activity.role' and ids),
        }

    def _state(self, env=None):
        env = env or self.env
        return {
            'caché emp.roles': self._cache(self.f_roles, self.emp_id, env),
            'base emp.roles': self._db_roles(),
            'caché act.active': self._cache(self.f_active, self.act_id, env),
            'caché role.activity_active': self._cache(self.f_act_active, self.role_id, env),
            'base (act.active, role.activity_active)': self._db_activity(),
            'pendiente': self._pending(),
        }

    def snap(self, label):
        self.snaps.append((label, self._state()))

    @staticmethod
    def _caller(depth=2, limit=4):
        frames = []
        frame = sys._getframe(depth)
        while frame and len(frames) < limit:
            code = frame.f_code
            frames.append('%s:%s %s' % (code.co_filename.rsplit('/', 1)[-1], frame.f_lineno, code.co_name))
            frame = frame.f_back
        return ' < '.join(frames)

    # -- envolturas ----------------------------------------------------
    def patches(self):
        Emp = type(self.env['hr.employee'])
        Role = type(self.env['sgi.activity.role'])
        orig_touch = Emp._sgi_mp_touch_jobs
        orig_compute = Emp._compute_sgi_mp_roles_stored
        orig_jobs = Role._sgi_mp_jobs
        diag = self

        @functools.wraps(orig_touch)
        def touch(model, jobs):
            before = diag._pending()['emp.sgi_mp_role_ids']
            res = orig_touch(model, jobs)
            diag.events.append(('touch', {
                'jobs': list(jobs.ids) if jobs else [], 'emp pendiente antes': before,
                'emp pendiente después': diag._pending()['emp.sgi_mp_role_ids'],
                'desde': diag._caller()}))
            return res

        @functools.wraps(orig_compute)
        def compute(records):
            mine = diag.emp_id in records.ids
            before = diag._state(records.env) if mine else None
            res = orig_compute(records)
            entry = {'ids': list(records.ids)[:10], 'n': len(records), 'incluye emp': mine,
                     'su': records.env.su, 'ctx': dict(records.env.context),
                     'desde': diag._caller(limit=12)}
            if mine:
                entry['antes'] = before
                entry['después'] = diag._state(records.env)
            diag.events.append(('compute', entry))
            return res

        @functools.wraps(orig_jobs)
        def role_jobs(roles):
            res = orig_jobs(roles)
            diag.events.append(('_sgi_mp_jobs', {'roles': list(roles.ids)[:10], 'puestos': list(res.ids)}))
            return res

        stack = ExitStack()
        stack.enter_context(patch.object(Emp, '_sgi_mp_touch_jobs', new=touch))
        stack.enter_context(patch.object(Emp, '_compute_sgi_mp_roles_stored', new=compute))
        stack.enter_context(patch.object(Role, '_sgi_mp_jobs', new=role_jobs))
        return stack

    def report(self, extra):
        lines = ['', '--- diagnóstico test_07 (57.87.0): emp=%s rol=%s actividad=%s ---' % (
            self.emp_id, self.role_id, self.act_id)]
        for label, state in self.snaps:
            lines.append('[%s] %s' % (label, state))
        computes = [e for k, e in self.events if k == 'compute']
        lines.append('compute llamado %d veces, %d con el empleado' % (
            len(computes), sum(1 for e in computes if e['incluye emp'])))
        for kind, entry in self.events:
            lines.append('%s: %s' % (kind, entry))
        for key, value in extra.items():
            lines.append('%s: %s' % (key, value))
        return '\n'.join(lines)


@tagged('post_install', '-at_install')
class TestRoleAudit(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.user = new_test_user(cls.env, login='rol_usuario_sgi',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.other = new_test_user(cls.env, login='rol_otro_sgi',
                                  groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.mast = new_test_user(cls.env, login='rol_mast_sgi',
                                 groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.job = cls.env['hr.job'].create({'name': 'PUESTO AUDITORIA ROL'})
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Empleado Auditoría Rol', 'job_id': cls.job.id, 'user_id': cls.user.id})
        Indicator = cls.env['sgi.indicator']
        cls.mine = Indicator.create({
            'code': 'ZROL-01', 'name': 'Mío en prueba', 'calc_mode': 'manual',
            'responsible_id': cls.user.id, 'target_objective': 1.0, 'target_acceptable': 0.5})
        cls.theirs = Indicator.create({
            'code': 'ZROL-02', 'name': 'De otro', 'calc_mode': 'manual',
            'responsible_id': cls.other.id, 'target_objective': 1.0, 'target_acceptable': 0.5})

    def test_01_mis_indicadores_aunque_esten_en_prueba(self):
        self.assertNotEqual(self.mine.status, 'oficial')
        screen = self.env['sgi.my.procedure'].with_user(self.user).create({'employee_id': self.emp.id})
        self.assertIn(self.mine, screen.indicator_ids, "Antes solo contaban los oficiales: nadie veía los suyos.")
        self.assertNotIn(self.theirs, screen.indicator_ids)
        self.assertEqual(screen.indicator_count, len(screen.indicator_ids))
        self.assertEqual(screen.action_show_indicators()['res_model'], 'sgi.indicator')
        arch = self.env['sgi.my.procedure'].get_views([(False, 'form')])['views']['form']['arch']
        self.assertIn('name="indicadores"', arch)
        action = self.env.ref('quimibond_sgi.sgi_indicator_action_mine')
        self.assertEqual(self.env.ref('quimibond_sgi.menu_sgi_my_indicators').action, action)
        self.assertEqual(self.mine.action_sgi_measures()['domain'], [('indicator_id', '=', self.mine.id)])

    def test_02_usuario_captura_solo_lo_suyo(self):
        Measure = self.env['sgi.indicator.measure']
        own = Measure.with_user(self.user).create({'indicator_id': self.mine.id, 'period_date': date(2046, 1, 1)})
        own.write({'value': 1.0})
        foreign = Measure.create({'indicator_id': self.theirs.id, 'period_date': date(2046, 1, 1)})
        with self.assertRaises(AccessError):
            foreign.with_user(self.user).write({'value': 5.0})
        with self.assertRaises(AccessError):
            Measure.with_user(self.user).create({'indicator_id': self.theirs.id, 'period_date': date(2046, 2, 1)})
        foreign.with_user(self.mast).write({'value': 2.0})
        self.assertEqual(foreign.value, 2.0, "MAST captura cualquiera.")
        # Lee todo el SGI: la medición ajena sí la ve.
        self.assertEqual(foreign.with_user(self.user).indicator_id, self.theirs)

    def test_03_usuario_no_edita_catalogo_y_no_aprueba_todo(self):
        process = self.env['sgi.process'].create({'code': 'ZROL', 'name': 'Proceso rol'})
        with self.assertRaises(AccessError):
            process.with_user(self.user).write({'name': 'Cambiado'})
        self.assertFalse(self.user.has_group('approvals.group_approval_user'),
                         "Usuario SGI ya no implica «aprobar todas las solicitudes».")

    def test_04_escalaciones_van_al_jefe_mast_directo(self):
        Cron = self.env['sgi.cron']
        group = self.env.ref('quimibond_sgi.group_sgi_manager')
        self.assertIn(Cron._sgi_manager_user_id(), group.user_ids.ids,
                      "Primero un miembro directo, no el primero heredado (el CEO).")
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mast_user_id', str(self.mast.id))
        self.assertEqual(Cron._sgi_manager_user_id(), self.mast.id)

    def test_05_actividad_de_captura_se_cierra_sola(self):
        Cron = self.env['sgi.cron']
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': self.mine.id, 'period_date': date(2046, 3, 1)})
        summary = "Capturar indicador ZROL-01 (03/2046)"
        Cron._sgi_schedule(self.mine, summary, '', self.user.id)
        opened = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.indicator'), ('res_id', '=', self.mine.id), ('summary', '=', summary)])
        self.assertTrue(opened)
        Cron._sgi_close_resolved_activities()
        self.assertEqual(self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.indicator'), ('res_id', '=', self.mine.id), ('summary', '=', summary)]),
            opened, "Con la medición pendiente sigue abierta.")
        measure.write({'value': 1.0, 'state': 'capturado'})
        Cron._sgi_close_resolved_activities()
        still = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.indicator'), ('res_id', '=', self.mine.id), ('summary', '=', summary)])
        self.assertFalse(still, "Capturada la medición, la actividad se marca hecha.")

    def test_06_mis_pendientes_en_el_menu(self):
        menu = self.env.ref('quimibond_sgi.menu_sgi_my_pending')
        self.assertEqual(menu.parent_id, self.env.ref('quimibond_sgi.menu_sgi_panel'))
        self.assertEqual(menu.action, self.env.ref('quimibond_sgi.sgi_my_pending_action_mine'))
        action = self.env['sgi.my.pending'].with_user(self.user).action_open_mine()
        self.assertEqual(action['res_model'], 'sgi.my.pending')
        self.assertEqual(action['context'].get('search_default_group_state'), 1)
        # Sin empleado ligado también abre (vacía o con lo del usuario).
        action = self.env['sgi.my.pending'].with_user(self.other).action_open_mine()
        self.assertEqual(action['res_model'], 'sgi.my.pending')

    def test_07_procedimiento_guardado_en_el_empleado(self):
        """1.1 / 1.8: el procedimiento vive en campos guardados del empleado
        (se buscan y se leen por API) y se recalcula al cambiar los roles."""
        process = self.env['sgi.process'].create({'code': 'ZROLG', 'name': 'Proceso guardado'})
        activity = self.env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Revisar lo guardado', 'number': '1.1',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job.id})]})
        role = activity.role_ids
        Employee = self.env['hr.employee']
        self.assertEqual(self.emp.sgi_mp_job_id, self.job)
        self.assertIn(role, self.emp.sgi_mp_role_ids)
        self.assertIn(self.emp, Employee.search([('sgi_mp_role_ids', 'in', role.ids)]),
                      "Guardado: se busca por rol.")
        self.assertIn(self.emp, Employee.search([('sgi_mp_process_ids', 'in', process.ids)]))
        data = self.emp.read(['sgi_mp_role_ids', 'sgi_mp_process_ids', 'sgi_my_procedure_ack_state'])[0]
        self.assertIn(role.id, data['sgi_mp_role_ids'])
        self.assertEqual(data['sgi_my_procedure_ack_state'], 'sin_publicar')
        groups = Employee._read_group([('id', '=', self.emp.id)], ['sgi_my_procedure_ack_state'], ['__count'])
        self.assertEqual(groups[0][0], 'sin_publicar', "Guardado: se agrupa por estado de firma.")
        # Archivar la actividad lo saca del procedimiento guardado.
        # Lo guardado se recalcula al bajar a la base (flush); en la misma
        # transacción la caché conserva la lista anterior, así que se baja y
        # se vuelve a leer (en la interfaz cada petición lee de la base).
        # 57.87.0: el archivo y el flush corren con las envolturas de
        # _RoleArchiveDiag; si la aserción de lo guardado falla, su mensaje
        # trae qué se llamó, con qué y qué había en caché y en la base.
        diag = _RoleArchiveDiag(self.env, self.emp, role, activity)
        with diag.patches():
            diag.snap('antes de archivar')
            activity.active = False
            diag.snap('archivada, antes del flush')
            self.env.flush_all()
            diag.snap('después del flush')
            # 57.13.1: la corrida real (build 38916808) falló abajo sin decir en
            # qué paso. Primero lo que se calcula (el rol ya no cuenta para el
            # puesto) y luego lo guardado: si falla solo lo guardado, el
            # recálculo no se disparó (_sgi_mp_touch_jobs), no la lista.
            self.assertFalse(role.activity_active)
            self.assertNotIn(role, self.job.with_context(
                sgi_mp_employee_id=self.emp.id)._sgi_mp_role_lists()['detail'])
            self.assertIn(self.emp, Employee.sudo().with_context(active_test=False).search(
                [('sgi_mp_job_id', 'in', role._sgi_mp_jobs().ids)]),
                "El empleado se encuentra por el puesto del rol (a quién se recalcula).")
            self.emp.invalidate_recordset(['sgi_mp_role_ids'])
            diag.snap('tras invalidate_recordset')
            orm_value = self.emp.sgi_mp_role_ids.ids
            self.env.invalidate_all()
            why = diag.report({
                'ORM tras invalidate_recordset': orm_value,
                'ORM tras invalidate_all': self.emp.sgi_mp_role_ids.ids,
                'base al final': diag._db_roles(),
                'lista del puesto ahora': self.job.with_context(
                    sgi_mp_employee_id=self.emp.id)._sgi_mp_role_lists()['detail'].ids,
                'puesto del empleado (guardado)': self.emp.sgi_mp_job_id.id,
                'puesto del rol': role.job_id.id,
            })
        self.emp.invalidate_recordset(['sgi_mp_role_ids'])
        self.assertNotIn(role, self.emp.sgi_mp_role_ids,
                         "La lista del puesto ya no trae el rol, pero lo guardado no se recalculó." + why)
        activity.active = True
        self.env.flush_all()
        self.emp.invalidate_recordset(['sgi_mp_role_ids'])
        self.assertIn(role, self.emp.sgi_mp_role_ids)
        # La pantalla (como Usuario SGI) muestra lo mismo que está guardado.
        screen = self.env['sgi.my.procedure'].with_user(self.user).create({'employee_id': self.emp.id})
        self.assertEqual(screen.job_id, self.job)
        self.assertEqual(set(screen.role_ids.ids), set(self.emp.sgi_mp_role_ids.ids))
        # Indicador: último semáforo guardado y buscable; difusión guardada.
        self.env['sgi.indicator.measure'].create({
            'indicator_id': self.mine.id, 'period_date': date(2046, 5, 1), 'value': 0.1, 'state': 'capturado'})
        self.assertEqual(self.mine.last_semaphore, 'rojo')
        self.assertIn(self.mine, self.env['sgi.indicator'].search([('last_semaphore', '=', 'rojo')]))
        self.assertTrue(self.env['documents.document']._fields['sgi_ack_count'].store)
        self.assertTrue(self.env['sgi.indicator']._fields['spec_missing'].store)

    def test_08_accion_terminada_exige_100_y_fecha_pasada(self):
        """3.6 (como Usuario SGI): terminar pone 100 %; no se puede terminar en
        el futuro ni con avance menor a 100 %."""
        from datetime import timedelta
        from odoo import fields as ofields
        from odoo.exceptions import ValidationError
        today = ofields.Date.today()
        risk = self.env['sgi.risk'].create({'name': 'Riesgo acción', 'instrument': 'ryo',
                                            'eval_probability': '1', 'eval_impact': '1'})
        line = self.env['sgi.action.line'].create({
            'risk_id': risk.id, 'name': 'Acción propia', 'responsible_id': self.user.id,
            'date_commit': today})
        mine = line.with_user(self.user)
        with self.assertRaises(ValidationError):
            mine.write({'date_done': today + timedelta(days=30)})
        with self.assertRaises(ValidationError):
            mine.write({'date_done': today, 'progress': '0'})
        mine.write({'date_done': today})
        self.assertEqual((line.progress, line.state), ('100', 'terminada'))
        mine.write({'date_done': False})
        self.assertEqual(line.progress, '50', "Reabrir la regresa a 50 %.")

    def test_09_programa_de_auditorias_sin_auditor_no_se_aprueba(self):
        """4.4: el programa no se aprueba con auditorías internas sin auditor
        líder, y solo MAST lo aprueba (el Usuario SGI ni el auditor)."""
        from odoo.exceptions import UserError
        process = self.env['sgi.process'].create({'code': 'ZROLA', 'name': 'Proceso auditado'})
        program = self.env['sgi.audit.program'].create({'year': 2098, 'line_ids': [
            (0, 0, {'process_id': process.id, 'planned_month': '10'})]})
        # Odoo no acepta tuplas en assertRaises (issubclass); AccessError es
        # subclase de UserError, así que UserError cubre ambos casos.
        with self.assertRaises(UserError):
            program.with_user(self.user).action_approve()
        with self.assertRaises(UserError):
            program.with_user(self.mast).action_approve()
        program.line_ids.lead_auditor_id = self.mast
        program.with_user(self.mast).action_approve()
        self.assertEqual(program.state, 'aprobado')

    def test_10_indicador_que_no_calcula_guarda_el_motivo(self):
        """6.1: el cron guarda por qué no calculó y avisa al responsable; el
        aviso se cierra solo cuando vuelve a calcular. 6.5: el «NC sin
        acción» se cierra al cancelar o al registrar acción."""
        Cron = self.env['sgi.cron']
        indicator = self.env['sgi.indicator'].create({
            'code': 'ZROL-CFG', 'name': 'Configurable sin fórmula', 'calc_mode': 'configurable',
            'responsible_id': self.user.id, 'target_objective': 1.0, 'target_acceptable': 0.5})
        Cron._sgi_generate_measures(indicator, date(2045, 1, 1), date(2045, 1, 1), date(2045, 1, 31),
                                    date(2045, 2, 5), "01/2045")
        self.assertIn(indicator.calc_status, ('sin_formula', 'sin_datos', 'error'))
        self.assertTrue(indicator.calc_message)
        notice = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.indicator'), ('res_id', '=', indicator.id),
            ('summary', '=', 'Indicador ZROL-CFG no calculó')])
        self.assertEqual(notice.user_id, self.user, "Avisa al responsable.")
        self.assertIn(indicator, self.env['sgi.indicator'].with_user(self.user).search(
            [('calc_status', 'in', ('error', 'sin_formula', 'sin_datos'))]), "El usuario lo filtra.")
        indicator.calc_status = 'ok'
        Cron._sgi_close_resolved_activities()
        self.assertFalse(notice.exists() and self.env['mail.activity'].search([('id', '=', notice.id)]))
