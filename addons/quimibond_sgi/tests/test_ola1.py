# -*- coding: utf-8 -*-
"""OLA 1 — Motor de Mejora (ISO 10).

Paso 1: causa raíz antes que acción correctiva/preventiva (H8) y cierre real de
NC mayor (5 porqués + acción correctiva terminada, refinamiento H1).
"""
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError, ValidationError


@tagged('post_install', '-at_install')
class TestOla1RootCause(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id)]
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.Alert = cls.env['quality.alert']
        cls.Line = cls.env['sgi.action.line']

    def _nc(self, **vals):
        base = {'title': 'NC prueba', 'team_id': self.team.id}
        base.update(vals)
        alert = self.Alert.create(base)
        self.assertTrue(alert.sgi_folio, "Debe recibir folio SGI.")
        return alert

    def _line(self, alert, **vals):
        base = {'alert_id': alert.id, 'name': 'Acción',
                'responsible_id': self.env.user.id, 'date_commit': date.today()}
        base.update(vals)
        return self.Line.create(base)

    # --- H8: causa raíz antes que correctiva/preventiva -------------------
    def test_01_correction_allowed_without_root_cause(self):
        alert = self._nc()
        # La corrección (contención) sí se permite antes de la causa raíz.
        line = self._line(alert, action_type='correccion')
        self.assertTrue(line)

    def test_02_corrective_blocked_without_root_cause(self):
        alert = self._nc()
        with self.assertRaises(ValidationError):
            self._line(alert, action_type='correctiva')

    def test_03_preventive_blocked_without_root_cause(self):
        alert = self._nc()
        with self.assertRaises(ValidationError):
            self._line(alert, action_type='preventiva')

    def test_04_corrective_allowed_with_root_cause(self):
        alert = self._nc(sgi_root_cause='Causa raíz')
        line = self._line(alert, action_type='correctiva')
        self.assertTrue(line)

    def test_05_change_type_to_corrective_blocked(self):
        alert = self._nc()
        line = self._line(alert, action_type='correccion')
        with self.assertRaises(ValidationError):
            line.write({'action_type': 'correctiva'})

    # --- H1: cierre real de NC mayor --------------------------------------
    def _mayor_ready(self):
        alert = self._nc(sgi_classification='mayor', sgi_root_cause='Causa',
                         sgi_effectiveness_note='Eficaz',
                         sgi_effectiveness_date=date.today())
        return alert

    def test_06_mayor_needs_five_whys(self):
        alert = self._mayor_ready()
        self._line(alert, action_type='correctiva', date_done=date.today())
        # Faltan los 5 porqués.
        with self.assertRaises(UserError):
            alert.write({'stage_id': self.stage_closed.id})

    def test_07_mayor_needs_corrective_done(self):
        alert = self._mayor_ready()
        alert.write({'sgi_why_1': '1', 'sgi_why_2': '2', 'sgi_why_3': '3',
                     'sgi_why_4': '4', 'sgi_why_5': '5'})
        # Solo una corrección terminada, sin acción correctiva.
        self._line(alert, action_type='correccion', date_done=date.today())
        with self.assertRaises(UserError):
            alert.write({'stage_id': self.stage_closed.id})

    def test_08_mayor_closes_with_whys_and_corrective(self):
        alert = self._mayor_ready()
        alert.write({'sgi_why_1': '1', 'sgi_why_2': '2', 'sgi_why_3': '3',
                     'sgi_why_4': '4', 'sgi_why_5': '5'})
        self._line(alert, action_type='correctiva', date_done=date.today())
        alert.write({'stage_id': self.stage_closed.id})
        self.assertEqual(alert.stage_id, self.stage_closed)
