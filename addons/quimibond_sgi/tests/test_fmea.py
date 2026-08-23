# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestFmea(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Fmea = cls.env['sgi.fmea']
        cls.Line = cls.env['sgi.fmea.line']

    def test_01_npr_computed(self):
        fmea = self.Fmea.create({'name': 'PFMEA extrusión'})
        line = self.Line.create({
            'fmea_id': fmea.id,
            'step': 'Extrusión',
            'severity': '10', 'occurrence': '5', 'detection': '4',
        })
        self.assertEqual(line.npr, 200)
        self.assertTrue(line.requires_action)
        self.assertEqual(fmea.max_npr, 200)

    def test_02_low_npr_no_action(self):
        fmea = self.Fmea.create({'name': 'PFMEA bajo riesgo'})
        line = self.Line.create({
            'fmea_id': fmea.id,
            'step': 'Corte',
            'severity': '2', 'occurrence': '2', 'detection': '2',
        })
        self.assertEqual(line.npr, 8)
        self.assertFalse(line.requires_action)

    def test_03_vigente_lock_requires_action(self):
        fmea = self.Fmea.create({'name': 'PFMEA con NPR alto'})
        line = self.Line.create({
            'fmea_id': fmea.id,
            'step': 'Sellado',
            'severity': '9', 'occurrence': '6', 'detection': '3',
        })
        self.assertTrue(line.requires_action)
        with self.assertRaises(UserError):
            fmea.action_set_vigente()

        # Una acción registrada pero SIN terminar tampoco basta (IATF).
        action = self.env['sgi.action.line'].create({
            'fmea_line_id': line.id,
            'name': 'Poka-yoke de sellado',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
        })
        with self.assertRaises(UserError):
            fmea.action_set_vigente()

        # Acción TERMINADA pero SIN re-evaluación (S/O/D post) -> bloqueado.
        action.write({'date_done': date.today()})
        with self.assertRaises(UserError):
            fmea.action_set_vigente()

        # Re-evaluación capturada pero el NPR post NO baja (162 = inicial)
        # y sin justificación -> bloqueado.
        line.write({'severity_post': '9', 'occurrence_post': '6',
                    'detection_post': '3'})
        with self.assertRaises(UserError):
            fmea.action_set_vigente()

        # Con el NPR post a la baja (9*2*3 = 54 < 162), sí pasa a vigente.
        line.write({'occurrence_post': '2'})
        fmea.action_set_vigente()
        self.assertEqual(fmea.state, 'vigente')

    def test_04_vigente_npr_post_no_baja_con_justificacion(self):
        fmea = self.Fmea.create({'name': 'PFMEA severidad de diseño'})
        line = self.Line.create({
            'fmea_id': fmea.id,
            'step': 'Impregnado',
            'severity': '10', 'occurrence': '5', 'detection': '2',
        })
        self.env['sgi.action.line'].create({
            'fmea_line_id': line.id,
            'name': 'Control redundante',
            'responsible_id': self.env.user.id,
            'date_commit': date.today(),
            'date_done': date.today(),
        })
        # NPR post igual al inicial: solo pasa con justificación escrita.
        line.write({'severity_post': '10', 'occurrence_post': '5',
                    'detection_post': '2'})
        with self.assertRaises(UserError):
            fmea.action_set_vigente()
        line.post_note = "La severidad no baja por diseño; se refuerza detección aguas abajo."
        fmea.action_set_vigente()
        self.assertEqual(fmea.state, 'vigente')
