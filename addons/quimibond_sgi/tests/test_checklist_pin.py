# -*- coding: utf-8 -*-
"""57.18.0 (entrega 8, I-005, D-08): PIN obligatorio para firmar checklists,
detrás de ``quimibond_sgi.checklist_pin_required`` (apagado). Datos propios."""
from datetime import date

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestChecklistPin(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Employee = cls.env['hr.employee']
        cls.no_pin = Employee.create({'name': 'ZS Sin PIN'})
        cls.with_pin = Employee.create({'name': 'ZS Con PIN', 'pin': '2468'})
        equipment = cls.env['maintenance.equipment'].create([
            {'name': 'ZS Caldera %d' % n} for n in range(3)])
        cls.template = cls.env['sgi.checklist.template'].create({
            'name': 'ZS Planta PIN', 'frequency': 'diaria',
            'equipment_ids': [(6, 0, equipment.ids)],
            'item_ids': [(0, 0, {'name': 'Fugas'})],
        })
        cls.requests = cls.template._sgi_generate(date(2046, 3, 6))
        cls.requests.sgi_checklist_line_ids.write({'answer': 'ok'})
        cls.Finish = cls.env['sgi.checklist.finish']
        cls.Param = cls.env['ir.config_parameter'].sudo()

    def _sign(self, request, employee, pin=None):
        return self.Finish.create({'request_id': request.id, 'employee_id': employee.id,
                                   'pin': pin}).action_confirm()

    def test_01_apagado_por_default_firma_sin_pin(self):
        self.assertFalse(self.Finish._sgi_pin_required(), "Apagado por default (D-08).")
        request = self.requests[0]
        self._sign(request, self.no_pin)
        self.assertEqual(request.sgi_checklist_employee_id, self.no_pin)
        self.assertTrue(any('sin PIN registrado' in (m.body or '') for m in request.message_ids))

    def test_02_encendido_sin_pin_no_se_firma(self):
        self.Param.set_param('quimibond_sgi.checklist_pin_required', 'True')
        self.assertTrue(self.Finish._sgi_pin_required())
        request = self.requests[1]
        with self.assertRaises(UserError) as caught:
            self._sign(request, self.no_pin)
        self.assertIn('PIN', str(caught.exception))
        self.assertFalse(request.sgi_checklist_employee_id)
        with self.assertRaises(UserError):
            self._sign(request, self.with_pin, '0000')
        self._sign(request, self.with_pin, '2468')
        self.assertEqual(request.sgi_checklist_employee_id, self.with_pin)

    def test_03_ajuste_en_pantalla(self):
        settings = self.env['res.config.settings'].create({'sgi_checklist_pin_required': True})
        settings.execute()
        self.assertTrue(self.Finish._sgi_pin_required())
        settings = self.env['res.config.settings'].create({'sgi_checklist_pin_required': False})
        settings.execute()
        self.assertFalse(self.Finish._sgi_pin_required())
