# -*- coding: utf-8 -*-
"""57.111.0: «Quién lo hizo: quien lo pasó a su estado (historial)»."""
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestMedicionHistorial(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.closer = new_test_user(cls.env, login='mh_cierra', groups='base.group_user')
        cls.editor = new_test_user(cls.env, login='mh_edita', groups='base.group_user')
        cls.process = cls.env['sgi.process'].create({'code': 'ZMH', 'name': 'Proceso historial'})
        cls.Inspection = cls.env['sgi.csh.inspection']

    def _activity(self, **vals):
        return self.env['sgi.process.activity'].create(dict({
            'process_id': self.process.id, 'number': 'ZMH.01', 'name': 'Cerrar el recorrido',
            'measure_method': 'odoo', 'measure_model_name': 'sgi.csh.inspection',
            'measure_domain': "[('area', '=', 'ZMH'), ('state', '=', 'cerrado')]",
            'measure_date_field': 'date', 'measure_user_field': 'write_uid',
        }, **vals))

    def _closed_inspection(self):
        inspection = self.Inspection.create({'area': 'ZMH'})
        inspection.with_user(self.closer).sudo().write({'state': 'cerrado'})
        self.env.cr.precommit.run()   # el seguimiento se escribe al confirmar
        # Alguien más edita después: «write_uid» ya no dice quién cerró.
        inspection.with_user(self.editor).sudo().write({'notes': 'Acta corregida'})
        self.env.flush_all()
        return inspection

    def test_01_atribuye_a_quien_cerro_no_al_ultimo_que_edito(self):
        inspection = self._closed_inspection()
        self.assertEqual(inspection.write_uid, self.editor)
        activity = self._activity(measure_user_history=True)
        self.assertEqual(activity.measure_history_field, 'state')
        Model = self.Inspection.sudo()
        domain = activity._sgi_measure_domain_strict()
        since = fields.Date.today() - timedelta(days=7)
        groups = activity._sgi_history_groups(Model, domain, 'date', since)
        users = {user.id: count for user, _day, count in groups}
        self.assertEqual(users, {self.closer.id: 1},
                         "Cuenta quien lo pasó a «Cerrado», no quien lo editó al último.")
        self.assertTrue(activity._sgi_executor_attributable(Model))

    def test_02_sin_casilla_sigue_el_campo_de_usuario(self):
        self._closed_inspection()
        activity = self._activity()
        Model = self.Inspection.sudo()
        since = fields.Date.today() - timedelta(days=7)
        groups = activity._sgi_executor_groups(
            Model, activity._sgi_measure_domain_strict(), 'date', since)
        self.assertEqual({user.id for user, _day, _count in groups}, {self.editor.id})

    def test_03_quita_el_aviso_de_write_uid(self):
        weak = self._activity()
        self.assertTrue(any('«write_uid»' in msg for msg in weak._sgi_weak_attribution()))
        weak.measure_user_history = True
        self.assertFalse(any('«write_uid»' in msg for msg in weak._sgi_weak_attribution()))

    def test_04_modelo_sin_historial_lo_dice(self):
        activity = self._activity(measure_model_name='res.partner', measure_domain='[]',
                                  measure_date_field='create_date', measure_user_history=True)
        self.assertFalse(activity.measure_history_field)
        codes = [code for code, _msg in activity._sgi_spec_problems()]
        self.assertIn('weak_attribution', codes)
