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

    def test_05_product_spec_and_job_epp(self):
        # Spec del producto (C04-06/C14-02) y EPP por puesto (S03-01).
        tmpl = self.env['product.template'].create({
            'name': 'Fibra spec', 'sgi_packaging_notes': "No apilar > 3 camas."})
        self.assertTrue(tmpl.sgi_packaging_notes)
        from odoo.exceptions import UserError as UE
        with self.assertRaises(UE):
            tmpl.action_sgi_open_spec()  # sin spec ligada -> error claro
        variant = tmpl.product_variant_id
        with self.assertRaises(UE):
            variant.action_sgi_open_spec()  # resuelve también en la variante
        job = self.env['hr.job'].create({
            'name': 'Operador spec', 'sgi_epp_required': "Casco, lentes."})
        self.assertTrue(job.sgi_epp_required)
