# -*- coding: utf-8 -*-
"""57.94.0 «SGI en planta» (auditoría 2026-10: U-01, U-08, I-03, I-05).

- La gente de planta firma en la tableta compartida A SU NOMBRE: foto, PIN
  (el de Asistencias) y su menú. Cada acción vuelve a validar el PIN en el
  servidor; lo firmado guarda al empleado y la tableta, nunca solo la cuenta
  compartida. La cuenta de la tableta no tiene acceso propio a acuses,
  incidentes, EPP ni checklist.
- Un solo validador de PIN para el checklist y la tableta (sin límite de
  intentos, D-08 / F-018).
- Checklist: «Marcar el resto como Bien» y una hoja firmada no se reescribe.
- Kanban móvil en las 6 acciones de piso.
- RH: lista de empleados sin puesto, sin PIN o sin correo y aviso semanal por
  departamento en Mis pendientes."""
from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools.safe_eval import safe_eval

from ..models.sgi_calendar import sgi_today
from .common_documents import sgi_hide_real_documents
from .common_users import sgi_set_mast

# Misma clave que models/sgi_floor_kiosk.py (se escribe aquí para que este
# archivo importe aunque el modelo todavía no exista: el fallo esperado antes
# del código es por prueba, no del paquete de pruebas completo).
HR_GAPS_KEY = 'rh_empleados_incompletos'
# Prefijo de los xmlid que llegan con el código (lista de RH, kanban de Mis
# pendientes): con el xmlid completo y literal en la llamada, tools/check_addons.py
# marca error mientras el registro todavía no existe.
SGI = 'quimibond_sgi.'

LONG_TEXT = 'Se cayó una bobina del montacargas junto al pasillo 3'


class _PlantaCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        # Claves reales de la copia de producción fuera del camino (patrón de
        # test_bandeja / test_acks): las de esta prueba son F-P-A92-xx.
        sgi_hide_real_documents(env)
        cls.mast = sgi_set_mast(env, login='zk_mast')
        cls.company = env['sgi.config']._sgi_company()
        Dept = env['hr.department']
        cls.dept = Dept.create({'name': 'ZK Tejido', 'company_id': cls.company.id})
        cls.subdept = Dept.create({'name': 'ZK Tejido turno 2', 'parent_id': cls.dept.id,
                                   'company_id': cls.company.id})
        cls.other_dept = Dept.create({'name': 'ZK Almacén', 'company_id': cls.company.id})
        cls.job = env['hr.job'].create({'name': 'ZK Tejedor'})
        cls.worker_user = new_test_user(
            env, login='zk_worker_user', groups='base.group_user,quimibond_sgi.group_sgi_user')
        Emp = env['hr.employee']
        base = {'company_id': cls.company.id}
        cls.worker = Emp.create(dict(base, name='ZK Operadora', department_id=cls.dept.id,
                                     job_id=cls.job.id, pin='1357'))
        cls.worker2 = Emp.create(dict(base, name='ZK Operador turno 2', department_id=cls.subdept.id,
                                      job_id=cls.job.id, pin='2468', user_id=cls.worker_user.id))
        cls.no_pin = Emp.create(dict(base, name='ZK Sin PIN', department_id=cls.dept.id))
        cls.outsider = Emp.create(dict(base, name='ZK Almacenista', department_id=cls.other_dept.id,
                                       pin='9753'))
        other_company = env['res.company'].create({'name': 'ZK Otra empresa'})
        foreign_dept = Dept.create({'name': 'ZK Tejido B', 'company_id': other_company.id})
        cls.foreign = Emp.create({'name': 'ZK De otra empresa', 'company_id': other_company.id,
                                  'department_id': foreign_dept.id, 'pin': '8642'})
        cls.tablet_user = new_test_user(
            env, login='zk_tableta', groups='base.group_user,quimibond_sgi.group_sgi_floor_tablet')
        cls.tablet = env['sgi.floor.tablet'].create({
            'name': 'ZK Tableta Tejido', 'user_id': cls.tablet_user.id,
            'department_ids': [(6, 0, cls.dept.ids)]})
        cls.Kiosk = env['sgi.floor.kiosk'].with_user(cls.tablet_user)

    # La clave debe cumplir la nomenclatura del tipo «formato»
    # (F-P-[AGCDEIMPSV]NN-NN, sgi_document.py:23 y _check_sgi_code): «F-P-ZK-01»
    # truena con ValidationError al crear. A92 no la usa ninguna otra prueba.
    def _doc(self, code='F-P-A92-01'):
        return self.env['documents.document'].create({
            'name': 'ZK Instructivo %s' % code, 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': code, 'sgi_state': 'vigente'})

    def _sheet(self, employees=None):
        equipment = self.env['maintenance.equipment'].create({'name': 'ZK Telar 1'})
        template = self.env['sgi.checklist.template'].create({
            'name': 'ZK Arranque', 'frequency': 'diaria',
            'equipment_ids': [(6, 0, equipment.ids)],
            'employee_ids': [(6, 0, (employees or self.env['hr.employee']).ids)],
            'item_ids': [(0, 0, {'name': 'Fugas'}), (0, 0, {'name': 'Guardas'}),
                         (0, 0, {'name': 'Paro de emergencia'})]})
        self.tablet.checklist_template_ids = [(4, template.id)]
        return template._sgi_generate(sgi_today(self.env))


@tagged('post_install', '-at_install')
class TestPinComun(_PlantaCase):

    def test_01_un_solo_validador(self):
        Pin = self.env['sgi.pin']
        self.assertTrue(Pin._sgi_check_pin(self.worker, '1357'))
        with self.assertRaisesRegex(UserError, 'PIN incorrecto'):
            Pin._sgi_check_pin(self.worker, '0000')
        with self.assertRaisesRegex(UserError, 'Pida a RH'):
            Pin._sgi_check_pin(self.no_pin, '')
        self.assertFalse(Pin._sgi_check_pin(self.no_pin, '', required=False),
                         "Sin PIN y sin exigirlo (checklist con el parámetro apagado).")
        with self.assertRaisesRegex(UserError, 'no es de'):
            Pin._sgi_check_pin(self.foreign, '8642')
        with self.assertRaisesRegex(UserError, 'lista'):
            Pin._sgi_check_pin(self.worker, '1357', allowed=self.outsider)
        self.worker.active = False
        with self.assertRaisesRegex(UserError, 'activa'):
            Pin._sgi_check_pin(self.worker, '1357')


@tagged('post_install', '-at_install')
class TestKiosco(_PlantaCase):

    def test_02_mosaico_del_departamento_sin_pin(self):
        data = self.Kiosk.kiosk_employees()
        self.assertEqual(data['tablet']['name'], 'ZK Tableta Tejido')
        cards = {card['id']: card for card in data['employees']}
        self.assertEqual(set(cards), {self.worker.id, self.worker2.id, self.no_pin.id},
                         "El departamento y sus subdepartamentos; ni otra área ni otra empresa.")
        self.assertTrue(cards[self.worker.id]['has_pin'])
        self.assertFalse(cards[self.no_pin.id]['has_pin'])
        plain = str([{k: v for k, v in card.items() if k != 'avatar'} for card in data['employees']])
        for pin in ('1357', '2468'):
            self.assertNotIn(pin, plain, "El PIN nunca sale del servidor.")
        only_sub = self.Kiosk.kiosk_employees(self.subdept.id)
        self.assertEqual({c['id'] for c in only_sub['employees']}, {self.worker2.id})
        with self.assertRaisesRegex(UserError, 'departamento'):
            self.Kiosk.kiosk_employees(self.other_dept.id)

    def test_03_solo_tabletas_registradas(self):
        sgi_user = new_test_user(self.env, login='zk_sgi_user',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        with self.assertRaises(AccessError):
            self.env['sgi.floor.kiosk'].with_user(sgi_user).kiosk_employees()
        lonely = new_test_user(self.env, login='zk_tableta_sin_alta',
                               groups='base.group_user,quimibond_sgi.group_sgi_floor_tablet')
        with self.assertRaisesRegex(AccessError, 'Tabletas de planta'):
            self.env['sgi.floor.kiosk'].with_user(lonely).kiosk_employees()
        self.tablet.active = False
        with self.assertRaises(AccessError):
            self.Kiosk.kiosk_check_pin(self.worker.id, '1357')

    def test_04_pin_valido_invalido_y_rechazos(self):
        res = self.Kiosk.kiosk_check_pin(self.worker.id, '1357')
        self.assertEqual(res['employee']['id'], self.worker.id)
        self.assertIn('docs', res['counts'])
        cases = ((self.worker, '0000', 'PIN incorrecto'),
                 (self.no_pin, '', 'Pida a RH'),
                 (self.outsider, '9753', 'departamentos de esta tableta'),
                 (self.foreign, '8642', 'departamentos de esta tableta'))
        for employee, pin, message in cases:
            with self.assertRaisesRegex(UserError, message):
                self.Kiosk.kiosk_check_pin(employee.id, pin)

    def test_05_acuse_a_nombre_del_empleado(self):
        doc = self._doc()
        ack = self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.worker.id})
        other = self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.worker2.id})
        self.assertEqual([d['ack_id'] for d in self.Kiosk.kiosk_pending_docs(self.worker.id, '1357')],
                         [ack.id])
        with self.assertRaisesRegex(UserError, 'PIN incorrecto'):
            self.Kiosk.kiosk_ack_document(self.worker.id, '0000', ack.id)
        self.assertEqual(ack.state, 'pendiente')
        with self.assertRaisesRegex(UserError, 'no es suyo'):
            self.Kiosk.kiosk_ack_document(self.worker.id, '1357', other.id)
        with self.assertRaisesRegex(UserError, 'no tiene archivo'):
            self.Kiosk.kiosk_document_file(self.worker.id, '1357', ack.id)
        self.Kiosk.kiosk_ack_document(self.worker.id, '1357', ack.id)
        self.assertEqual(ack.state, 'leido')
        self.assertTrue(ack.ack_date)
        self.assertEqual(ack.employee_id, self.worker)
        self.assertEqual(ack.sgi_pin_tablet_id, self.tablet)
        self.assertEqual(ack.sgi_pin_tablet_id.user_id, self.tablet_user)
        self.assertIn('ZK Operadora', ack.sgi_pin_signature)
        self.assertIn('ZK Tableta Tejido', ack.sgi_pin_signature)
        with self.assertRaisesRegex(UserError, 'ya estaba firmado'):
            self.Kiosk.kiosk_ack_document(self.worker.id, '1357', ack.id)
        self.assertEqual(other.state, 'pendiente')
        # La cuenta de la tableta, por sí sola, no firma nada en el backend.
        with self.assertRaises(AccessError):
            other.with_user(self.tablet_user).action_mark_read()

    def test_06_casi_accidente_a_nombre_del_empleado(self):
        Incident = self.env['sgi.incident']
        before = Incident.search_count([])
        with self.assertRaisesRegex(UserError, 'PIN incorrecto'):
            self.Kiosk.kiosk_report_near_miss(self.worker2.id, '0000', {'description': LONG_TEXT})
        self.assertEqual(Incident.search_count([]), before)
        with self.assertRaisesRegex(UserError, 'Describa'):
            self.Kiosk.kiosk_report_near_miss(self.worker2.id, '2468', {'description': 'x'})
        res = self.Kiosk.kiosk_report_near_miss(
            self.worker2.id, '2468', {'description': LONG_TEXT, 'location': 'Pasillo 3'})
        incident = Incident.search([('reporter_employee_id', '=', self.worker2.id)])
        self.assertEqual(len(incident), 1)
        self.assertEqual(res['folio'], incident.folio)
        self.assertEqual(incident.incident_type, 'casi_accidente')
        self.assertEqual(incident.reporter_id, self.worker_user, "El usuario de la persona, no la tableta.")
        self.assertEqual(incident.sgi_pin_tablet_id, self.tablet)
        self.assertEqual(incident.location, 'Pasillo 3')
        self.assertTrue(incident.activity_ids.filtered(lambda a: (a.summary or '').startswith('Revisar')),
                        "Salud ocupacional o el Jefe MAST reciben el reporte.")
        # Sin usuario: el reporte queda a nombre del empleado y sin usuario.
        self.Kiosk.kiosk_report_near_miss(self.worker.id, '1357', {'description': LONG_TEXT})
        second = Incident.search([('reporter_employee_id', '=', self.worker.id)])
        self.assertFalse(second.reporter_id)
        self.assertNotEqual(second.reporter_id, self.tablet_user)
        # La cuenta de la tableta no lee incidentes por el backend.
        with self.assertRaises(AccessError):
            Incident.with_user(self.tablet_user).search([])

    def test_07_epp_a_nombre_del_empleado(self):
        delivery = self.env['sgi.epp.delivery'].create({
            'employee_id': self.worker.id, 'items': 'Casco y lentes'})
        self.assertEqual([r['id'] for r in self.Kiosk.kiosk_epp(self.worker.id, '1357')], [delivery.id])
        with self.assertRaisesRegex(UserError, 'no es suya'):
            self.Kiosk.kiosk_sign_epp(self.worker2.id, '2468', delivery.id)
        self.Kiosk.kiosk_sign_epp(self.worker.id, '1357', delivery.id)
        self.assertEqual(delivery.state, 'firmada')
        self.assertEqual(delivery.sgi_pin_tablet_id, self.tablet)
        body = "".join(delivery.message_ids.mapped('body'))
        self.assertIn('ZK Operadora', body)
        self.assertIn('ZK Tableta Tejido', body)
        self.assertFalse(self.Kiosk.kiosk_epp(self.worker.id, '1357'))

    def test_08_checklist_en_la_tableta(self):
        sheet = self._sheet()
        self.assertEqual([s['id'] for s in self.Kiosk.kiosk_checklists(self.worker.id, '1357')], [sheet.id])
        first = sheet.sgi_checklist_line_ids.sorted('sequence')[0]
        self.Kiosk.kiosk_checklist_save(self.worker.id, '1357', sheet.id,
                                        {str(first.id): {'answer': 'falla', 'note': 'Gotea'}})
        self.assertEqual((first.answer, first.note), ('falla', 'Gotea'))
        with self.assertRaisesRegex(UserError, 'no válida'):
            self.Kiosk.kiosk_checklist_save(self.worker.id, '1357', sheet.id,
                                            {str(first.id): {'answer': 'quizá'}})
        with self.assertRaisesRegex(UserError, 'Faltan'):
            self.Kiosk.kiosk_checklist_save(self.worker.id, '1357', sheet.id, {}, False, True)
        self.Kiosk.kiosk_checklist_save(self.worker.id, '1357', sheet.id, {}, True, True)
        self.assertEqual(first.answer, 'falla', "«El resto» no toca lo ya contestado.")
        self.assertEqual(set(sheet.sgi_checklist_line_ids.mapped('answer')), {'falla', 'ok'})
        self.assertEqual(sheet.sgi_checklist_employee_id, self.worker)
        self.assertEqual(sheet.sgi_pin_tablet_id, self.tablet)
        self.assertFalse(self.Kiosk.kiosk_checklists(self.worker.id, '1357'))
        with self.assertRaisesRegex(UserError, 'no está pendiente'):
            self.Kiosk.kiosk_checklist_save(self.worker.id, '1357', sheet.id,
                                            {str(first.id): {'answer': 'ok'}})
        with self.assertRaisesRegex(UserError, 'firmada'):
            first.with_user(self.worker_user).write({'answer': 'ok'})

    def test_09_plantilla_de_otras_personas(self):
        sheet = self._sheet(employees=self.worker2)
        self.assertFalse(self.Kiosk.kiosk_checklists(self.worker.id, '1357'))
        with self.assertRaisesRegex(UserError, 'no está pendiente'):
            self.Kiosk.kiosk_checklist_save(self.worker.id, '1357', sheet.id, {}, True, True)
        self.assertEqual([s['id'] for s in self.Kiosk.kiosk_checklists(self.worker2.id, '2468')],
                         [sheet.id])

    def test_10_no_se_falsifica_la_firma(self):
        ack = self.env['sgi.document.ack'].create({'document_id': self._doc('F-P-A92-02').id,
                                                   'employee_id': self.worker.id})
        with self.assertRaisesRegex(UserError, 'sistema'):
            ack.with_user(self.mast).write({'sgi_pin_tablet_id': self.tablet.id})
        # «Lo llenó» de la hoja tampoco se escribe por RPC (el campo es
        # readonly solo en la vista; base.group_user escribe maintenance.request).
        sheet = self._sheet()
        with self.assertRaisesRegex(UserError, 'sistema'):
            sheet.with_user(self.worker_user).write({'sgi_checklist_employee_id': self.worker.id})
        with self.assertRaisesRegex(UserError, 'a su nombre'):
            self.env['sgi.incident'].with_user(self.worker_user).create({
                'name': 'ZK reporte ajeno', 'reporter_employee_id': self.worker.id})
        own = self.env['sgi.incident'].with_user(self.worker_user).create({'name': 'ZK reporte propio'})
        self.assertEqual(own.reporter_employee_id, self.worker2)


@tagged('post_install', '-at_install')
class TestChecklistBackend(_PlantaCase):

    def test_11_marcar_el_resto_como_bien_y_un_toque(self):
        sheet = self._sheet()
        lines = sheet.sgi_checklist_line_ids.sorted('sequence')
        lines[0].with_context(sgi_answer='falla').action_sgi_answer()
        lines[1].with_context(sgi_answer='na').action_sgi_answer()
        with self.assertRaisesRegex(UserError, 'no válida'):
            lines[2].with_context(sgi_answer='tal vez').action_sgi_answer()
        sheet.action_sgi_checklist_all_ok()
        self.assertEqual(lines.mapped('answer'), ['falla', 'na', 'ok'])
        self.env['sgi.checklist.finish'].create({
            'request_id': sheet.id, 'employee_id': self.worker.id, 'pin': '1357'}).action_confirm()
        self.assertEqual(sheet.sgi_checklist_employee_id, self.worker)
        self.assertTrue(sheet.sgi_pin_signed_at, "Firmado con PIN aunque no sea desde la tableta.")
        self.assertFalse(sheet.sgi_pin_tablet_id)
        with self.assertRaisesRegex(UserError, 'firmada'):
            sheet.action_sgi_checklist_all_ok()


@tagged('post_install', '-at_install')
class TestRhFaltantes(_PlantaCase):

    def _notices(self, department):
        return self.env['mail.activity'].sudo().with_context(active_test=False).search([
            ('res_model', '=', 'hr.department'), ('res_id', '=', department.id),
            ('sgi_cron_key', '=', HR_GAPS_KEY)])

    def test_12_aviso_semanal_por_departamento(self):
        rh = new_test_user(self.env, login='zk_rh',
                           groups='base.group_user,hr.group_hr_user,quimibond_sgi.group_sgi_user')
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.hr_user_id', str(rh.id))
        Cron = self.env['sgi.cron']
        Cron.cron_hr_employee_gaps()
        notice = self._notices(self.dept).filtered('active')
        self.assertEqual(len(notice), 1)
        self.assertEqual(notice.user_id, rh)
        # worker sin correo de trabajo; no_pin sin PIN, sin puesto y sin correo.
        self.assertIn('ZK Tejido: 2', notice.summary)
        # La lista de RH los trae.
        action = self.env.ref(SGI + 'sgi_hr_employee_gaps_action')
        found = self.env['hr.employee'].with_user(rh).search(safe_eval(action.domain))
        self.assertIn(self.no_pin, found)
        self.assertEqual(self.no_pin.with_user(rh).sgi_missing_data, 'puesto, PIN, correo')
        # Mis pendientes: «Ir» abre la lista de RH filtrada por el departamento.
        Pending = self.env['sgi.my.pending'].with_user(rh)
        rows = Pending.browse(Pending.action_open_mine()['domain'][0][2])
        row = rows.filtered(lambda r: r.kind == 'aviso' and r.res_id == notice.id)
        self.assertTrue(row)
        go = row.action_open()
        self.assertEqual(go['res_model'], 'hr.employee')
        self.assertEqual(go['context']['search_default_department_id'], self.dept.id)
        # «Hecho» y siguen faltando: el lunes siguiente llega otro.
        notice.with_user(rh).action_feedback(feedback='Ya les pedí los datos')
        Cron.cron_hr_employee_gaps()
        self.assertEqual(len(self._notices(self.dept).filtered('active')), 1)
        # Completos: el aviso del departamento se cierra.
        self.worker.work_email = 'zk.operadora@example.com'
        self.no_pin.write({'pin': '1111', 'job_id': self.job.id, 'work_email': 'zk.sinpin@example.com'})
        Cron.cron_hr_employee_gaps()
        self.assertFalse(self._notices(self.dept).filtered('active'))


@tagged('post_install', '-at_install')
class TestVistasDePiso(_PlantaCase):

    ACTIONS = {
        'quimibond_sgi.sgi_incident_action': 'sgi.incident',
        'quimibond_sgi.sgi_indicator_action_mine': 'sgi.indicator',
        'quimibond_sgi.sgi_current_document_action': 'documents.document',
        'quimibond_sgi.sgi_epp_delivery_action': 'sgi.epp.delivery',
        'quimibond_sgi.sgi_work_permit_action': 'sgi.work.permit',
    }

    def test_13_kanban_movil_en_las_seis_acciones(self):
        for xmlid, model in self.ACTIONS.items():
            action = self.env.ref(xmlid)
            self.assertEqual(action.mobile_view_mode, 'kanban', xmlid)
            self.assertIn('kanban', action.view_mode.split(','), xmlid)
            kanban = action.view_ids.filtered(lambda v: v.view_mode == 'kanban').view_id
            arch = self.env[model].get_view(kanban.id or None, 'kanban')['arch']
            self.assertIn('<kanban', arch, xmlid)
        user = new_test_user(self.env, login='zk_piso',
                             groups='base.group_user,quimibond_sgi.group_sgi_user')
        action = self.env['sgi.my.pending'].with_user(user).action_open_mine()
        self.assertEqual(action.get('mobile_view_mode'), 'kanban')
        kanban_id = dict((mode, vid) for vid, mode in action['views'])['kanban']
        self.assertEqual(kanban_id, self.env.ref(SGI + 'sgi_my_pending_view_kanban').id)
        for xmlid in ('sgi_my_pending_view_kanban', 'sgi_indicator_view_kanban_mine',
                      'sgi_current_document_view_kanban', 'sgi_epp_delivery_view_kanban',
                      'sgi_work_permit_view_kanban'):
            self.assertIn('btn-lg', self.env.ref(SGI + xmlid).arch, xmlid)
