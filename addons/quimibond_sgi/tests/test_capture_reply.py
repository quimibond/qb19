# -*- coding: utf-8 -*-
"""56.22.0: eficiencias por departamento (C4.25 → S4.35), checklists firmados
por el empleado con su PIN y acuse / respuesta al cliente en la NC (C5.19,
C5.20)."""
from datetime import date

from odoo import fields
from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestCaptureReply(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Dept = cls.env['hr.department']
        cls.dept_a = Dept.create({'name': 'ZS Tejido'})
        cls.dept_b = Dept.create({'name': 'ZS Acabado'})
        cls.jefe = new_test_user(cls.env, login='zs_eff_jefe',
                                 groups='base.group_user,quimibond_sgi.group_sgi_efficiency_capture')
        cls.rh = new_test_user(cls.env, login='zs_eff_rh', groups='base.group_user,hr.group_hr_user')
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.rh_user_id', cls.rh.id)
        Employee = cls.env['hr.employee']
        cls.jefe_emp = Employee.create({'name': 'ZS Jefe Tejido', 'user_id': cls.jefe.id,
                                        'department_id': cls.dept_a.id})
        cls.op_a = Employee.create({'name': 'ZS Tejedor A', 'department_id': cls.dept_a.id})
        cls.op_b = Employee.create({'name': 'ZS Acabador B', 'department_id': cls.dept_b.id})

    # ------------------------------------------------------------------
    # 1. Eficiencias: el jefe captura solo su departamento, cierra, RH recibe
    # ------------------------------------------------------------------
    def test_01_jefe_captura_su_area_y_rh_recibe(self):
        Sheet = self.env['sgi.staff.efficiency']
        other = Sheet.create({'period_date': date(2046, 3, 1), 'department_id': self.dept_b.id})
        mine = Sheet.with_user(self.jefe).create({'period_date': date(2046, 3, 1)})
        self.assertEqual(mine.department_id, self.dept_a, "Por omisión, el área del jefe.")
        mine.action_load_employees()
        self.assertEqual(mine.line_ids.employee_id, self.op_a | self.jefe_emp)
        self.assertNotIn(other, Sheet.with_user(self.jefe).search([]), "No ve otras áreas.")
        with self.assertRaises(AccessError):
            Sheet.with_user(self.jefe).create({'period_date': date(2046, 3, 1), 'department_id': self.dept_b.id})
        with self.assertRaises(AccessError):
            mine.line_ids[:1].read(['amount'])  # los importes solo los ven RH y MAST
        mine.line_ids.write({'attendance_pct': 5.5, 'efficiency_pct': 1.5})
        mine.action_close()
        self.assertEqual(mine.state, 'cerrado')
        activity = mine.sudo().activity_ids.filtered(lambda a: a.user_id == self.rh)
        self.assertTrue(activity, "RH recibe la hoja cerrada.")
        with self.assertRaises(AccessError):
            mine.line_ids[:1].write({'quality_pct': 2.0})  # cerrada: ya no la edita
        with self.assertRaises(UserError):
            other.with_user(self.rh).action_receive()  # sigue en borrador
        sheet_rh = mine.with_user(self.rh)
        sheet_rh.action_receive()
        self.assertEqual(sheet_rh.state, 'recibido')
        self.assertEqual(sheet_rh.received_by_id, self.rh)
        self.assertFalse(sheet_rh.sudo().activity_ids, "Al recibirla se cierra el aviso.")
        html = self.env['ir.actions.report'].with_user(self.jefe)._render_qweb_html(
            'quimibond_sgi.report_staff_efficiency_document', mine.ids)[0]
        self.assertNotIn(b'A pagar', html, "El PDF del jefe no lleva importes.")

    # ------------------------------------------------------------------
    # 2. Checklist firmado por el empleado con su PIN
    # ------------------------------------------------------------------
    def test_02_checklist_firmado_con_pin(self):
        self.op_a.pin = '4321'
        truck = self.env['maintenance.equipment'].create({'name': 'ZS Unidad 7'})
        template = self.env['sgi.checklist.template'].create({
            'name': 'Unidades', 'frequency': 'semanal', 'equipment_ids': [(6, 0, truck.ids)],
            'employee_ids': [(6, 0, self.op_a.ids)],
            'item_ids': [(0, 0, {'name': 'Llantas'}), (0, 0, {'name': 'Luces'})],
        })
        request = template._sgi_generate(date(2046, 3, 5))
        Finish = self.env['sgi.checklist.finish']
        with self.assertRaises(UserError):
            Finish.create({'request_id': request.id, 'employee_id': self.op_a.id, 'pin': '4321'}).action_confirm()
        request.sgi_checklist_line_ids.write({'answer': 'ok'})
        with self.assertRaises(UserError):
            Finish.create({'request_id': request.id, 'employee_id': self.op_b.id}).action_confirm()
        with self.assertRaises(UserError):
            Finish.create({'request_id': request.id, 'employee_id': self.op_a.id, 'pin': '0000'}).action_confirm()
        Finish.create({'request_id': request.id, 'employee_id': self.op_a.id, 'pin': '4321'}).action_confirm()
        self.assertEqual(request.sgi_checklist_employee_id, self.op_a)
        self.assertTrue(request.sgi_checklist_done_at)
        with self.assertRaises(UserError):
            Finish.create({'request_id': request.id, 'employee_id': self.op_a.id, 'pin': '4321'}).action_confirm()

    # ------------------------------------------------------------------
    # 3. Acuse y respuesta al cliente (C5.19) y aviso de embarcado (C5.20)
    # ------------------------------------------------------------------
    def _closable(self, vals):
        alert = self.env['quality.alert'].create(dict({
            'name': 'ZS NC cliente', 'sgi_root_cause': 'Tensión de urdido',
            # 57.93.0 (N-02): resultado «Eficaz» y fecha no futura (la acción termina hoy).
            'sgi_effective': 'eficaz', 'sgi_effectiveness_note': 'Sin reincidencia',
            'sgi_effectiveness_date': fields.Date.context_today(self),
            'sgi_classification': 'menor',
        }, **vals))
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'Ajuste', 'action_type': 'correccion',
            'responsible_id': self.env.user.id, 'date_commit': date(2046, 3, 20),
            'date_done': fields.Date.context_today(self), 'progress': '100',
        })
        return alert

    def test_03_acuse_y_respuesta_al_cliente(self):
        industrial = self.env['crm.team'].create({'name': 'ZS Industrial', 'sgi_complaint_response_days': 20})
        alert = self._closable({'sgi_origin_type': 'reclamacion', 'sgi_sale_team_id': industrial.id,
                                'sgi_customer_received_date': date(2046, 3, 5)})
        self.assertTrue(alert.sgi_customer_reply_required)
        self.assertTrue(alert.sgi_customer_ack_due > date(2046, 3, 5))
        self.assertTrue(alert.sgi_customer_response_due > alert.sgi_customer_ack_due)
        alert.sgi_customer_deadline = date(2046, 3, 9)
        self.assertEqual(alert.sgi_customer_response_due, date(2046, 3, 9), "Manda el plazo del cliente.")
        with self.assertRaises(UserError):
            alert._sgi_check_can_close()
        alert.write({'sgi_customer_ack_date': date(2046, 3, 6), 'sgi_customer_response_date': date(2046, 3, 12)})
        self.assertTrue(alert.sgi_customer_ack_on_time)
        self.assertFalse(alert.sgi_customer_response_on_time)
        alert._sgi_check_can_close()
        alert.sgi_shipped_status = 'posible'
        with self.assertRaises(UserError):
            alert._sgi_check_can_close()
        alert.sgi_customer_notice_date = date(2046, 3, 6)
        alert._sgi_check_can_close()

    def test_04_aviso_de_vencimiento(self):
        alert = self.env['quality.alert'].create({'name': 'ZS NC sin acuse', 'sgi_origin_type': 'reclamacion',
                                                  'user_id': self.env.user.id,
                                                  'sgi_customer_received_date': date(2046, 3, 5)})
        alert._sgi_customer_reply_escalation(date(2046, 4, 30))
        summaries = alert.activity_ids.mapped('summary')
        self.assertTrue(any(s.startswith("Acusar recibo al cliente") for s in summaries))
        self.assertTrue(any(s.startswith("Responder al cliente") for s in summaries))
