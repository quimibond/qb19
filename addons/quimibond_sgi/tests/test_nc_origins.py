# -*- coding: utf-8 -*-
"""Orígenes nuevos de la NC y acta administrativa (56.20.0)."""
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestNcOrigins(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.rh = new_test_user(cls.env, login='zs_nc_rh', groups='base.group_user')
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.rh_user_id', cls.rh.id)

    def test_01_origenes_nuevos(self):
        selection = dict(self.env['quality.alert']._fields['sgi_origin_type'].selection)
        for code in ('incidente_sst', 'emergencia', 'scorecard', 'recorrido_csh', 'riesgo'):
            self.assertIn(code, selection)
        alert = self.env['quality.alert'].create({'name': 'ZS NC scorecard', 'sgi_origin_type': 'scorecard'})
        self.assertEqual(alert.sgi_origin_type, 'scorecard')

    def _admin_activities(self, alert):
        return alert.activity_ids.filtered(lambda a: a.summary == "Levantar acta administrativa (S4.32)")

    def test_02_accion_administrativa_pide_acta_a_rh(self):
        alert = self.env['quality.alert'].create({'name': 'ZS NC administrativa'})
        self.assertFalse(self._admin_activities(alert))
        alert.sgi_followup_action = 'administrativa'
        activity = self._admin_activities(alert)
        self.assertEqual(activity.user_id, self.rh)
        alert.write({'sgi_followup_action': 'administrativa'})
        self.assertEqual(len(self._admin_activities(alert)), 1, "Una sola por NC.")
        created = self.env['quality.alert'].create({'name': 'ZS NC al crear',
                                                    'sgi_followup_action': 'administrativa'})
        self.assertTrue(self._admin_activities(created))
