# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import ValidationError


@tagged('post_install', '-at_install')
class TestRisk(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Risk = cls.env['sgi.risk']

    def test_01_ryo_score_and_level(self):
        # 5x5 = 25 -> Inmediata
        risk = self.Risk.create({
            'name': 'Riesgo alto', 'instrument': 'ryo',
            'eval_probability': '5', 'eval_impact': '5'})
        self.assertEqual(risk.score, 25)
        self.assertEqual(risk.attention_level, 'inmediata')
        # 2x2 = 4 -> Intermedia; 1x2 = 2 -> Baja
        risk.write({'eval_probability': '2', 'eval_impact': '2'})
        self.assertEqual(risk.score, 4)
        self.assertEqual(risk.attention_level, 'intermedia')

    def test_02_iper_scale(self):
        risk = self.Risk.create({
            'name': 'Peligro', 'instrument': 'iper',
            'eval_probability': '3', 'eval_impact': '3'})
        self.assertEqual(risk.score, 9)
        self.assertEqual(risk.attention_level, 'alto')
        # IPER no admite valores > 3
        with self.assertRaises(ValidationError):
            self.Risk.create({
                'name': 'Peligro 2', 'instrument': 'iper',
                'eval_probability': '5', 'eval_impact': '2'})
        # Tampoco en la evaluación residual (editar solo residual dispara la validación)
        with self.assertRaises(ValidationError):
            risk.write({'residual_probability': '4', 'residual_impact': '2'})

    def test_03_patrimonial_scale(self):
        risk = self.Risk.create({
            'name': 'Patrimonial', 'instrument': 'patrimonial',
            'eval_probability': '5', 'eval_impact': '4'})
        self.assertEqual(risk.score, 20)
        self.assertEqual(risk.attention_level, 'alto')
        risk.write({'eval_probability': '2', 'eval_impact': '2'})
        self.assertEqual(risk.score, 4)
        self.assertEqual(risk.attention_level, 'bajo')

    def test_04_foda_no_score(self):
        risk = self.Risk.create({
            'name': 'Fortaleza X', 'instrument': 'foda', 'foda_type': 'fortaleza',
            'eval_probability': '5', 'eval_impact': '5'})
        self.assertEqual(risk.score, 0)
        self.assertFalse(risk.attention_level)
        # FODA requiere tipo
        with self.assertRaises(ValidationError):
            self.Risk.create({'name': 'FODA sin tipo', 'instrument': 'foda'})

    def test_05_action_xor_constraint(self):
        risk = self.Risk.create({'name': 'Riesgo con acción', 'instrument': 'ryo'})
        # Acción sólo con risk_id: OK
        line = self.env['sgi.action.line'].create({
            'risk_id': risk.id,
            'name': 'Mitigar',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
        })
        self.assertTrue(line)
        # Acción con ambos: rechazada
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        alert = self.env['quality.alert'].create({'title': 'NC', 'team_id': team.id})
        with self.assertRaises(ValidationError):
            self.env['sgi.action.line'].create({
                'risk_id': risk.id, 'alert_id': alert.id,
                'name': 'Ambos', 'responsible_id': self.env.user.id,
                'date_commit': date.today(),
            })
        # Acción con ninguno: rechazada
        with self.assertRaises(ValidationError):
            self.env['sgi.action.line'].create({
                'name': 'Ninguno', 'responsible_id': self.env.user.id,
                'date_commit': date.today(),
            })

    def test_06_reopen_or_delete_action_revalidates_closed_risk(self):
        from odoo.exceptions import UserError
        risk = self.Risk.create({
            'name': 'Riesgo alto controlado', 'instrument': 'ryo',
            'eval_probability': '5', 'eval_impact': '5',
            'residual_probability': '1', 'residual_impact': '1'})
        line = self.env['sgi.action.line'].create({
            'risk_id': risk.id, 'name': 'Tratamiento',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today(),
        })
        risk.state = 'controlado'
        # Reabrir la única acción terminada invalida el cierre (H11).
        with self.assertRaises(UserError):
            line.write({'date_done': False})
        # Borrar la única acción terminada también.
        with self.assertRaises(UserError):
            line.unlink()
        # Un riesgo de atención baja no se ve afectado.
        low = self.Risk.create({
            'name': 'Riesgo bajo', 'instrument': 'ryo',
            'eval_probability': '1', 'eval_impact': '1'})
        low_line = self.env['sgi.action.line'].create({
            'risk_id': low.id, 'name': 'Menor',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(), 'date_done': date.today(),
        })
        low.state = 'controlado'
        low_line.write({'date_done': False})
        low_line.unlink()
