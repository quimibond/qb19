# -*- coding: utf-8 -*-
"""57.109.0: reportar un riesgo u oportunidad con preguntas sencillas."""
from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestRiesgoReportar(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.plain = new_test_user(cls.env, login='rr_plain',
                                  groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.manager = new_test_user(cls.env, login='rr_mast',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mast_user_id', str(cls.manager.id))
        cls.process = cls.env['sgi.process'].create({'code': 'ZRR', 'name': 'Proceso riesgo reportado'})

    def test_01_las_respuestas_proponen_la_evaluacion(self):
        report = self.env['sgi.risk.report'].with_user(self.plain).create({
            'kind': 'riesgo', 'name': 'Que el proveedor único de hilo deje de surtir',
            'consequence': 'Se para el tejido una semana', 'process_id': self.process.id,
            'how_often': '3', 'how_bad': '4', 'existing_controls': 'Ninguno'})
        self.assertIn('= <b>12</b>', report.preview_html)
        report.action_report()
        risk = self.env['sgi.risk'].search([('name', '=', 'Que el proveedor único de hilo deje de surtir')])
        self.assertEqual(len(risk), 1)
        self.assertEqual((risk.eval_probability, risk.eval_impact, risk.score, risk.instrument),
                         ('3', '4', 12, 'ryo'))
        self.assertEqual(risk.state, 'identificado')
        todo = risk.activity_ids.filtered(lambda a: a.summary == 'Evaluar riesgo reportado')
        self.assertEqual(todo.user_id, self.manager)

    def test_02_oportunidad_usa_el_beneficio(self):
        report = self.env['sgi.risk.report'].with_user(self.plain).create({
            'kind': 'oportunidad', 'name': 'Vender tela técnica a un cliente nuevo de colchones',
            'how_often': '2', 'how_good': '4'})
        report.action_report()
        risk = self.env['sgi.risk'].search([('name', '=', 'Vender tela técnica a un cliente nuevo de colchones')])
        self.assertEqual((risk.kind, risk.eval_impact), ('oportunidad', '4'))
