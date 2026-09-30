# -*- coding: utf-8 -*-
"""57.50.0 — matriz de aspectos e impactos ambientales (ISO 14001 6.1.2,
E2.23): cálculo de significancia, evaluación con control operacional, riesgo
de tratamiento y reporte con pie de formato."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestEnvAspect(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.process = cls.env['sgi.process'].create({'code': 'XAA', 'name': 'Aspectos prueba'})
        cls.Aspect = cls.env['sgi.env.aspect']

    def _aspect(self, **vals):
        base = {'process_id': self.process.id, 'activity': 'Lavado de tambos XAA',
                'name': 'Descarga de agua con residuos XAA', 'impact': 'Contaminación del agua',
                'aspect_type': 'descarga'}
        base.update(vals)
        return self.Aspect.create(base)

    def test_01_significancia(self):
        low = self._aspect(severity='2', frequency='2')
        self.assertTrue(low.folio)
        self.assertEqual((low.score, low.level, low.significant), (4, 'bajo', False))
        moderate = self._aspect(severity='1', frequency='5')
        self.assertEqual((moderate.score, moderate.level, moderate.significant), (5, 'moderado', True))
        severe = self._aspect(severity='5', frequency='2')
        self.assertEqual(severe.level, 'severo')
        critical = self._aspect(severity='4', frequency='4')
        self.assertEqual(critical.level, 'critico')
        # Un requisito legal vuelve significativo un aspecto de nivel bajo.
        requirement = self.env['sgi.legal.requirement'].create({'name': 'NOM-001 XAA'})
        low.legal_requirement_ids = [(4, requirement.id)]
        self.assertTrue(low.legal_requirement)
        self.assertTrue(low.significant)
        self.assertEqual(low.level, 'bajo')
        # Sin criterios no hay nivel.
        empty = self._aspect()
        self.assertEqual((empty.score, empty.level, empty.significant), (0, False, False))

    def test_02_umbral_por_parametro(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.aspect_moderado', 7)
        aspect = self._aspect(severity='2', frequency='3')
        self.assertEqual(aspect.level, 'bajo')
        self.assertFalse(aspect.significant)

    def test_03_evaluar_exige_control_si_es_significativo(self):
        aspect = self._aspect(severity='4', frequency='3')
        self.assertTrue(aspect.significant)
        with self.assertRaises(UserError):
            aspect.action_evaluate()
        aspect.control_description = 'Trampa de grasas y análisis semestral'
        aspect.action_evaluate()
        self.assertEqual(aspect.state, 'evaluado')
        self.assertTrue(aspect.last_review_date)
        self.assertEqual(aspect.next_review_date.year - aspect.last_review_date.year, 1)
        # Sin criterios tampoco se evalúa.
        with self.assertRaises(UserError):
            self._aspect().action_evaluate()
        # No significativo: se evalúa sin control.
        low = self._aspect(severity='1', frequency='1')
        low.action_evaluate()
        self.assertEqual(low.state, 'evaluado')

    def test_04_riesgo_de_tratamiento(self):
        aspect = self._aspect(severity='5', frequency='4', control_description='Dique')
        aspect.action_create_risk()
        risk = aspect.risk_id
        self.assertEqual(risk.instrument, 'ambiental')
        self.assertEqual((risk.eval_probability, risk.eval_impact), ('4', '5'))
        self.assertEqual(risk.process_id, self.process)
        aspect.action_create_risk()
        self.assertEqual(aspect.risk_id, risk, "No crea un segundo riesgo.")

    def test_05_ya_no_aplica_archiva(self):
        aspect = self._aspect(severity='1', frequency='1')
        aspect.action_set_obsoleto()
        self.assertEqual(aspect.state, 'obsoleto')
        self.assertFalse(aspect.active)

    def test_06_reporte_con_pie_de_formato(self):
        aspects = self._aspect(severity='3', frequency='3') | self._aspect(severity='1', frequency='2')
        self.assertTrue(aspects[0].sgi_format_info(), "El mapeo de formato del modelo existe.")
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_env_aspect_document', aspects.ids)[0].decode()
        self.assertIn('Matriz de aspectos e impactos ambientales', html)
        self.assertIn('Formato controlado del SGI', html)
        self.assertIn(aspects[0].folio, html)
