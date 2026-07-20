# -*- coding: utf-8 -*-
"""Mini-fase Procedimiento vivo — paso 1: modelo y ficha."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestProcedureModel(TransactionCase):
    """Paso 1: alcance, responsabilidades y actividades como datos del proceso."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id)]
        cls.process = cls.env['sgi.process'].create({
            'code': 'PROC-TST', 'name': 'Proceso de prueba',
            'process_type': 'cop'})
        cls.job = cls.env['hr.job'].create({'name': 'Rol de prueba'})

    def test_01_scope_and_norms(self):
        norm = self.env['sgi.norm'].search([], limit=1)
        self.process.write({
            'scope': 'Aplica a todas las áreas.',
            'env_aspects': 'Generación de residuos.',
            'norm_ids': [(6, 0, norm.ids)],
        })
        self.assertEqual(self.process.scope, 'Aplica a todas las áreas.')
        self.assertIn(norm, self.process.norm_ids)

    def test_02_responsibilities_are_lines(self):
        resp = self.env['sgi.process.responsibility'].create({
            'process_id': self.process.id, 'job_id': self.job.id,
            'name': 'Director de ventas', 'responsibilities': 'Presupuesto anual.'})
        self.assertIn(resp, self.process.job_responsibility_ids)
        self.assertEqual(resp.name, 'Director de ventas')

    def test_03_activities_and_count(self):
        for i in range(3):
            self.env['sgi.process.activity'].create({
                'process_id': self.process.id, 'sequence': i,
                'number': '4.1.%d' % i, 'block': 'inicial',
                'name': 'Actividad %d' % i})
        self.assertEqual(self.process.activity_count, 3)
        self.assertEqual(len(self.process.activity_ids), 3)

    def test_04_activity_format_domain_holds_controlled_doc(self):
        doc = self.env['documents.document'].create({
            'name': 'F-P-TST-01.pdf', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'formato',
            'sgi_code': 'F-P-A28-01', 'sgi_state': 'vigente'})
        act = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'number': '4.3.3.1',
            'block': 'final', 'name': 'Reclamación',
            'format_document_ids': [(6, 0, doc.ids)]})
        self.assertIn(doc, act.format_document_ids)
        self.assertTrue(act.display_name.startswith('4.3.3.1'))
