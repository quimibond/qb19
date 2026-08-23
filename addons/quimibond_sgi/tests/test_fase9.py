# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestFase9Forms(TransactionCase):
    """Formularios que sustituyen formatos (ronda sin piso): sello de
    embarque, minutas de auditoría y encuestas sembradas."""

    def test_01_picking_seal_field(self):
        self.assertIn('sgi_seal_number', self.env['stock.picking']._fields,
                      "La entrega debe tener el campo de sello (F-IT-P-A07-01-07/08).")

    def test_02_audit_minutes_fields(self):
        audit = self.env['sgi.audit'].create({
            'opening_minutes': "Alcance confirmado; agenda acordada.",
            'closing_minutes': "2 hallazgos presentados; plazo 15 días.",
        })
        self.assertTrue(audit.opening_minutes and audit.closing_minutes)

    def test_03_auditor_eval_survey(self):
        survey = self.env.ref('quimibond_sgi.sgi_survey_auditor_eval')
        self.assertGreaterEqual(len(survey.question_ids), 5,
                                "La evaluación del auditor trae sus preguntas.")
        audit = self.env['sgi.audit'].create({})
        action = audit.action_evaluate_auditors()
        self.assertEqual(action['type'], 'ir.actions.act_url')
        self.assertIn('/survey/', action['url'])

    def test_04_consulta_survey_seeded(self):
        survey = self.env.ref('quimibond_sgi.sgi_survey_consulta')
        self.assertGreaterEqual(len(survey.question_ids), 5,
                                "La consulta y participación (F-P-A10-05) trae sus preguntas.")
