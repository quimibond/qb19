# -*- coding: utf-8 -*-
"""Pantallas de lo que seguía en papel (56.21.0): estudios y exámenes por
trabajador, recorrido de la Comisión de Seguridad e Higiene y checklists de
planta y unidades como hojas de mantenimiento."""
from datetime import date

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, freeze_time, tagged, new_test_user

from .common_calendar import sgi_test_calendar


@tagged('post_install', '-at_install')
class TestHseRecords(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.rh = new_test_user(cls.env, login='zs_hse_rh', groups='base.group_user,hr.group_hr_user')
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.rh_user_id', cls.rh.id)
        cls.employee = cls.env['hr.employee'].create({'name': 'ZS Tejedor'})
        # 57.66.0: días hábiles de lunes a viernes sin festivos cargados (el
        # calendario de producción tiene su zona y sus festivos).
        sgi_test_calendar(cls.env)

    def test_01_examen_vence_y_avisa_a_rh(self):
        Health = self.env['sgi.health.record']
        old = Health.create({'employee_id': self.employee.id, 'name': 'Audiometría',
                             'date': date(2045, 1, 10), 'validity_months': 12})
        new = Health.create({'employee_id': self.employee.id, 'name': 'Audiometría',
                             'date': date(2046, 1, 10), 'validity_months': 12, 'result': 'apto'})
        self.assertEqual(new.next_date, date(2047, 1, 10))
        # 57.66.0: el cron usa sgi_today (57.15.0), no context_today.
        with freeze_time('2046-12-20 12:00:00'):
            Health._sgi_expiry_notices()
        self.assertEqual(new.state, 'por_vencer')
        self.assertEqual(new.activity_ids.user_id, self.rh)
        self.assertFalse(old.activity_ids, "El anterior ya fue sustituido por el nuevo: no avisa.")

    def test_02_recorrido_csh(self):
        inspection = self.env['sgi.csh.inspection'].create({
            'date': date(2046, 3, 5), 'area': 'Tejido',
            'finding_ids': [(0, 0, {'description': 'Guarda de máquina suelta', 'severity': 'alta',
                                     'responsible_id': self.rh.id}),
                            (0, 0, {'description': 'Pasillo obstruido', 'severity': 'baja'})],
        })
        self.assertEqual(inspection.name, 'CSH-2046-03-05')
        with self.assertRaises(UserError):
            inspection.action_close()
        guard, aisle = inspection.finding_ids.sorted('id')
        guard.action_generate_nc()
        self.assertEqual(guard.alert_id.sgi_origin_type, 'recorrido_csh')
        self.assertEqual(guard.alert_id.sgi_classification, 'mayor')
        # 57.102.0: el responsable del hallazgo contesta la NC.
        self.assertEqual(guard.alert_id.sgi_responsible_ids, self.rh)
        self.assertEqual(guard.alert_id.user_id, self.rh)
        self.assertEqual(guard.alert_id.sgi_requester_id, self.env.user)
        aisle.disposition = 'corregido'
        inspection.action_close()
        self.assertEqual(inspection.state, 'cerrado')
        self.assertEqual(inspection.nc_count, 1)
        with self.assertRaises(UserError):
            guard.action_generate_nc()

    def test_03_checklist_como_hoja_de_mantenimiento(self):
        truck = self.env['maintenance.equipment'].create({'name': 'ZS Camioneta 1'})
        template = self.env['sgi.checklist.template'].create({
            'name': 'Checklist semanal de unidades', 'code': 'S5.20', 'frequency': 'semanal',
            'equipment_ids': [(6, 0, truck.ids)],
            'item_ids': [(0, 0, {'name': 'Llantas', 'hint': 'Sin cortes'}),
                         (0, 0, {'name': 'Luces'})],
        })
        monday, tuesday = date(2046, 3, 5), date(2046, 3, 6)
        saturday, next_tuesday = date(2046, 3, 10), date(2046, 3, 13)
        self.assertTrue(template._sgi_due_today(monday))
        self.assertFalse(template._sgi_due_today(saturday), "Día inhábil: no sale.")
        # 57.15.0 (G-022, decisión 14): la semanal sale el primer día hábil
        # de la semana en que corra el cron (si el lunes falló, sale el
        # martes), una vez por semana.
        self.assertTrue(template._sgi_due_today(tuesday), "El lunes no corrió: sale el martes.")
        request = template._sgi_generate(monday)
        self.assertEqual(len(request), 1)
        self.assertFalse(template._sgi_due_today(tuesday), "Semanal: una vez por semana.")
        self.assertTrue(template._sgi_due_today(next_tuesday), "La semana siguiente sí.")
        self.assertEqual(request.maintenance_type, 'preventive')
        self.assertEqual(len(request.sgi_checklist_line_ids), 2)
        self.assertFalse(template._sgi_generate(monday), "No duplica la del día.")
        self.assertEqual(request.sgi_checklist_state, 'pendiente')
        tires, lights = request.sgi_checklist_line_ids.sorted('id')
        tires.answer = 'ok'
        lights.write({'answer': 'falla', 'note': 'Direccional izquierda fundida'})
        self.assertEqual(request.sgi_checklist_state, 'con_fallas')
        request.action_sgi_create_correctives()
        self.assertEqual(lights.corrective_request_id.maintenance_type, 'corrective')
        self.assertEqual(lights.corrective_request_id.equipment_id, truck)
