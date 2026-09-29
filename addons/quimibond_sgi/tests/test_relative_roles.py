# -*- coding: utf-8 -*-
"""57.13.0 (entrega 8, primer bloque): los roles relativos se resuelven a
personas y quien aprueba o recibe el escalamiento nunca es quien ejecuta o
pide (J-010). Datos propios, nada de producción."""
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestRelativeRoles(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Employee = cls.env['hr.employee']
        cls.job_exec = cls.env['hr.job'].create({'name': 'EJECUTOR RELATIVOS QR'})
        cls.job_other = cls.env['hr.job'].create({'name': 'OTRO PUESTO RELATIVOS QR'})
        cls.u_boss = new_test_user(cls.env, login='qr_boss', groups='base.group_user')
        cls.u_owner = new_test_user(cls.env, login='qr_owner', groups='base.group_user')
        cls.u_owner2 = new_test_user(cls.env, login='qr_owner2', groups='base.group_user')
        cls.u_exec = new_test_user(cls.env, login='qr_exec', groups='base.group_user')
        cls.u_asker = new_test_user(cls.env, login='qr_asker', groups='base.group_user')
        cls.boss = Employee.create({'name': 'Jefe QR', 'user_id': cls.u_boss.id,
                                    'job_id': cls.job_other.id})
        cls.owner = Employee.create({'name': 'Dueño QR', 'user_id': cls.u_owner.id,
                                     'parent_id': cls.boss.id, 'job_id': cls.job_other.id})
        cls.owner2 = Employee.create({'name': 'Dueño QR2', 'user_id': cls.u_owner2.id,
                                      'job_id': cls.job_other.id})
        cls.executor = Employee.create({'name': 'Ejecutor QR', 'user_id': cls.u_exec.id,
                                        'parent_id': cls.boss.id, 'job_id': cls.job_exec.id})
        cls.asker = Employee.create({'name': 'Solicitante QR', 'user_id': cls.u_asker.id,
                                     'parent_id': cls.owner.id, 'job_id': cls.job_other.id})
        cls.department = cls.env['hr.department'].create(
            {'name': 'Depto QR', 'manager_id': cls.owner2.id})
        Process = cls.env['sgi.process']
        cls.p_activity = Process.create({'code': 'ZQRA', 'name': 'Proceso de la actividad QR',
                                         'owner_id': cls.owner.id})
        cls.p_record = Process.create({'code': 'ZQRB', 'name': 'Proceso del registro QR',
                                       'owner_id': cls.owner2.id})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': cls.p_activity.id, 'name': 'Aprobar lo que pide otro', 'number': '9.7',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_exec.id}),
                         (0, 0, {'role': 'aprueba', 'target_type': 'relative',
                                 'relative_role': 'dueno_proceso'}),
                         (0, 0, {'role': 'escala', 'target_type': 'relative',
                                 'relative_role': 'dueno_proceso', 'after_days': 2})]})
        cls.approver = cls.activity.role_ids.filtered(lambda r: r.role == 'aprueba')
        cls.escalation = cls.activity.role_ids.filtered(lambda r: r.role == 'escala')

    def _relative(self, relative):
        """El rol «Aprueba» de la actividad, cambiado al relativo pedido."""
        self.approver.relative_role = relative
        return self.approver

    def test_01_dueno_del_proceso_del_registro(self):
        """«Dueño del proceso» sale del proceso del REGISTRO; sin él, el de la
        actividad (antes siempre el de la actividad)."""
        incident = self.env['sgi.incident'].new({'name': 'QR', 'process_id': self.p_record.id})
        employees, note = self.approver._sgi_resolve_employees(incident)
        self.assertEqual(employees, self.owner2)
        self.assertFalse(note)
        without = self.env['sgi.incident'].new({'name': 'QR sin proceso'})
        self.assertEqual(self.approver._sgi_resolve_employees(without)[0], self.owner)
        self.assertEqual(self.approver._sgi_resolve_employees()[0], self.owner,
                         "Sin registro, el dueño del proceso de la actividad.")

    def test_02_proceso_por_many2many_y_por_actividad_ligada(self):
        """Un cambio que afecta varios procesos lo aprueban sus dueños; una
        solicitud ligada a una actividad toma el proceso de la actividad."""
        Role = self.env['sgi.activity.role']
        Request = self.env['approval.request']
        if 'sgi_affected_process_ids' in Request._fields:
            moc = Request.new({'name': 'Cambio QR',
                               'sgi_affected_process_ids': [(6, 0, (self.p_activity | self.p_record).ids)]})
            self.assertEqual(Role._sgi_record_process(moc), self.p_activity | self.p_record)
            self.assertEqual(self.approver._sgi_resolve_employees(moc)[0], self.owner | self.owner2)
        if 'sgi_activity_id' in Request._fields:
            linked = Request.new({'name': 'Ligada QR', 'sgi_activity_id': self.activity.id})
            self.assertEqual(Role._sgi_record_process(linked), self.p_activity,
                             "Un salto por la actividad ligada.")

    def test_03_solicitante_y_su_jefe(self):
        request = self.env['approval.request'].new({
            'name': 'Pide QR', 'request_owner_id': self.u_asker.id})
        role = self._relative('solicitante')
        self.assertEqual(role._sgi_resolve_employees(request)[0], self.asker)
        role = self._relative('jefe_del_solicitante')
        self.assertEqual(role._sgi_resolve_employees(request)[0], self.owner,
                         "El jefe (parent_id) de quien pide.")
        employees, note = role._sgi_resolve_employees()
        self.assertFalse(employees, "Sin registro no hay a quién.")
        self.assertTrue(note)

    def test_04_quien_detecta_y_area_responsable(self):
        area = self.env['sgi.area'].create({'code': 'ZQR', 'name': 'Área QR',
                                            'department_id': self.department.id})
        incident = self.env['sgi.incident'].new({
            'name': 'Detectado QR', 'reporter_id': self.u_asker.id, 'sgi_area_id': area.id})
        role = self._relative('quien_detecta')
        self.assertEqual(role._sgi_resolve_employees(incident)[0], self.asker)
        role = self._relative('area_responsable')
        self.assertEqual(role._sgi_resolve_employees(incident)[0], self.owner2,
                         "El responsable del departamento del área.")
        employees, note = role._sgi_resolve_employees(self.env['sgi.incident'].new({'name': 'x'}))
        self.assertFalse(employees)
        self.assertIn("área", note)

    def test_05_aprobador_igual_a_ejecutor_sube_al_jefe(self):
        """El caso S6.07: el dueño del proceso es quien ejecuta. No se asigna
        en silencio: sube a su jefe directo."""
        self.p_activity.owner_id = self.executor
        employees, note = self.approver._sgi_target_employees()
        self.assertEqual(employees, self.boss)
        self.assertIn("jefe", note)
        self.approver.invalidate_recordset(['approval_user_ids'])
        self.assertEqual(self.approver.approval_user_ids, self.u_boss)
        # Sin jefe: vacío y aviso, nunca el mismo.
        self.executor.parent_id = False
        employees, note = self.approver._sgi_target_employees()
        self.assertFalse(employees)
        self.assertIn("nadie aprueba", note)

    def test_06_aprueba_solicitante_nunca_es_quien_pide(self):
        request = self.env['approval.request'].new({
            'name': 'Mi propia solicitud', 'request_owner_id': self.u_asker.id})
        role = self._relative('solicitante')
        employees, _note = role._sgi_target_employees(request)
        self.assertEqual(employees, self.owner, "Quien pide no se aprueba: su jefe.")

    def test_07_escalamiento_al_dueno_en_mis_pendientes(self):
        """Los escalamientos a «Dueño del proceso» sí le llegan a alguien: al
        dueño, o a su jefe si el dueño también ejecuta."""
        Role = self.env['sgi.activity.role']
        found = Role._sgi_relative_escalations(self.owner | self.boss)
        self.assertIn(self.escalation, found.get(self.owner.id, Role))
        self.p_activity.owner_id = self.executor
        found = Role._sgi_relative_escalations(self.executor | self.boss)
        self.assertNotIn(self.escalation, found.get(self.executor.id, Role))
        self.assertIn(self.escalation, found.get(self.boss.id, Role))

    def test_08_jefe_del_solicitante_en_aprobaciones_es_nativo(self):
        role = self._relative('jefe_del_solicitante')
        role.invalidate_recordset(['approval_state', 'approval_user_ids'])
        self.assertEqual(role.approval_state, 'relativo',
                         "En un botón no hay aprobador fijo: depende de cada registro.")
        role.approval_kind = 'solicitud'
        role.action_sgi_sync_approval()
        category = role.approval_category_id
        self.assertTrue(category)
        self.assertEqual(category.manager_approval, 'required')
        self.assertFalse(category.approver_ids)
        role.invalidate_recordset(['approval_state', 'approval_user_ids'])
        self.assertEqual(role.approval_state, 'activa')

    def test_09_ejecutor_relativo_cuenta_en_adherencia(self):
        act = self.env['sgi.process.activity'].create({
            'process_id': self.p_activity.id, 'name': 'Pedir', 'number': '9.8',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'target_type': 'relative',
                                 'relative_role': 'solicitante'})]})
        self.assertIsNone(act._sgi_executor_jobs())
        self.assertTrue(act._sgi_executor_is_relative())
        self.assertFalse(self.activity._sgi_executor_is_relative())

    def test_10_carga_avisa_aprueba_solicitante(self):
        payload = {'processes': [{'code': 'ZQRC', 'name': 'Proceso QR carga'}],
                   'activities': [{'process': 'ZQRC', 'number': 'ZQRC.1', 'name': 'Pedir y aprobar',
                                   'roles': [{'role': 'ejecuta', 'job': 'EJECUTOR RELATIVOS QR'},
                                             {'role': 'aprueba', 'relative': 'solicitante'}]}]}
        result = self.env['sgi.process'].load_payload(payload, dry_run=True)
        self.assertTrue(result['ok'], result['errors'])
        self.assertTrue([w for w in result['warnings'] if w['kind'] == 'role'
                         and 'Solicitante' in w['message']])

    def test_11_reporte(self):
        rows = self.env['sgi.activity.role'].sgi_relative_roles_report()
        mine = [r for r in rows if r['role_id'] in self.activity.role_ids.ids]
        self.assertEqual({r['role'] for r in mine}, {'aprueba', 'escala'})
        self.assertTrue(all(r['employees'] == [self.owner.name] for r in mine))
