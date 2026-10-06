# -*- coding: utf-8 -*-
"""Vencimiento por mes y día para cadencias trimestral, semestral y anual
(56.20.0): validador, frase del procedimiento, «cuándo» de Mi procedimiento,
medición a tiempo y carga por API."""
from datetime import date

from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_load import SgiLoadReport, _SgiLoader


@tagged('post_install', '-at_install')
class TestDueLong(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.process = cls.env['sgi.process'].create({'code': 'ZDL', 'name': 'ZS Vencimientos'})
        # Toda actividad no automática lleva exactamente un puesto que la
        # ejecuta (_sgi_check_roles); en una base nueva no hay de dónde tomarlo.
        cls.job = cls.env['hr.job'].create({'name': 'ZS PUESTO VENCIMIENTOS'})

    def _act(self, **vals):
        return self.env['sgi.process.activity'].create(dict({
            'process_id': self.process.id, 'name': 'Revisar la matriz legal',
            'done_criteria': 'La matriz quedó revisada', 'on_fail': 'Avisar a MAST',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job.id})]}, **vals))

    def test_01_anual(self):
        act = self._act(measure_cadence='anual', due_month='3', due_day=15)
        self.assertEqual(act._sgi_due_months(), [3])
        self.assertEqual(act._sgi_periodic_due(date(2046, 1, 10)), date(2046, 3, 15))
        self.assertEqual(act._sgi_periodic_due(date(2046, 11, 2)), date(2046, 3, 15),
                         "El periodo es el año: después de marzo ya está vencida.")
        self.assertEqual(act._sgi_due_label(), "15 de marzo")
        codes = [code for code, _msg in act._sgi_spec_problems()]
        self.assertNotIn('no_timing', codes, "Mes y día cuentan como plazo.")
        self.assertNotIn('due_mismatch', codes)
        self.assertIn("Vence el 15 de marzo", [text for _l, text in act._sgi_sentence_parts()])
        _key, text = self.env['hr.job']._sgi_mp_when(act)
        self.assertEqual(text, "Cada 15 de marzo")

    def test_02_trimestral_y_semestral(self):
        act = self._act(measure_cadence='trimestral', due_month='2', due_day=31)
        self.assertEqual(act._sgi_due_months(), [2, 5, 8, 11])
        self.assertEqual(act._sgi_periodic_due(date(2046, 4, 3)), date(2046, 5, 31))
        self.assertEqual(act._sgi_periodic_due(date(2046, 1, 3)), date(2046, 2, 28),
                         "Febrero corto: el último día.")
        self.assertEqual(act._sgi_due_label(), "31 de febrero, mayo, agosto y noviembre")
        act.write({'measure_cadence': 'semestral', 'due_month': '8', 'due_day': 1})
        self.assertEqual(act._sgi_due_months(), [2, 8])
        self.assertEqual(act._sgi_periodic_due(date(2046, 9, 30)), date(2046, 8, 1))

    def test_03_validador(self):
        act = self._act(measure_cadence='mensual', due_month='3', due_day=15)
        self.assertIn('due_mismatch', [code for code, _m in act._sgi_spec_problems()])
        act.write({'measure_cadence': 'anual', 'due_day': 0})
        messages = [m for code, m in act._sgi_spec_problems() if code == 'due_mismatch']
        self.assertTrue(any('día' in m for m in messages), "Falta el día.")
        with self.assertRaises(ValidationError):
            act.due_day = 40

    def test_04_carga_por_api(self):
        loader = _SgiLoader(self.env['sgi.process'], self.env.company, SgiLoadReport(True), {})
        self.assertEqual(loader._due_vals({'month': 7, 'day': 10}),
                         {'due_weekday': False, 'due_business_day': 0, 'due_month': '7', 'due_day': 10})
        with self.assertRaises(ValidationError):
            loader._due_vals({'month': 13, 'day': 1})
        with self.assertRaises(ValidationError):
            loader._due_vals({'month': 3})
