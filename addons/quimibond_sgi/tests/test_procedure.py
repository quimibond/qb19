# -*- coding: utf-8 -*-
"""Mini-fase Procedimiento vivo — paso 1: modelo y ficha; paso 2: reporte."""
from datetime import date

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


@tagged('post_install', '-at_install')
class TestProcedureReport(TransactionCase):
    """Paso 2: el reporte F-P-G01-02 rinde y arma la sección 8 sin duplicados."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id)]
        cls.Doc = cls.env['documents.document']
        cls.process = cls.env['sgi.process'].create({
            'code': 'RPT-01', 'name': 'Proceso reporte',
            'process_type': 'cop', 'purpose': 'Propósito.',
            'scope': 'Alcance.', 'env_aspects': 'Residuos.'})
        # Procedimiento vigente que encabeza el proceso (clave/fecha/rev en vivo).
        cls.proc_doc = cls.Doc.create({
            'name': 'P-RPT.pdf', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-A28',
            'sgi_revision': '15', 'sgi_issue_date': date(2022, 11, 1),
            'sgi_state': 'vigente', 'sgi_process_id': cls.process.id})
        # Familia FK del procedimiento (para sección 8).
        cls.fam = cls.Doc.create({
            'name': 'Alta de cliente', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': 'F-P-A28-21',
            'sgi_state': 'vigente', 'sgi_parent_document_id': cls.proc_doc.id})
        # Referencia cruzada (otra familia).
        cls.ref = cls.Doc.create({
            'name': 'Crédito y cobranza', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-A22',
            'sgi_state': 'vigente'})
        cls.proc_doc.sgi_reference_ids = [(6, 0, cls.ref.ids)]
        # Formato usado en una actividad (para sección 8, sin duplicar).
        cls.fmt = cls.Doc.create({
            'name': 'Cotización de producto', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'formato',
            'sgi_code': 'F-P-A28-12', 'sgi_state': 'vigente'})
        cls.env['sgi.process.activity'].create([
            {'process_id': cls.process.id, 'sequence': 1, 'number': '4.1.1',
             'block': 'inicial', 'section': '4.1 Actividades iniciales',
             'name': 'Retro de crédito', 'responsible_role': 'Vendedor'},
            {'process_id': cls.process.id, 'sequence': 2, 'number': '4.2.3.1',
             'block': 'desarrollo', 'section': '4.2.3 Cotización',
             'name': 'Cotización', 'format_document_ids': [(6, 0, cls.fmt.ids)]},
            {'process_id': cls.process.id, 'sequence': 3, 'number': '4.3.1',
             'block': 'final', 'section': '4.3.1 Entrega', 'name': 'Entrega'},
        ])

    def test_01_report_renders_html(self):
        report = self.env.ref('quimibond_sgi.action_report_procedure')
        html, _ = report._render_qweb_html(
            'quimibond_sgi.report_procedure_document', self.process.ids)
        text = html.decode() if isinstance(html, bytes) else html
        # Encabezado en vivo del documento controlado.
        self.assertIn('P-A28', text)
        self.assertIn('PRODUCTORA DE NO TEJIDOS QUIMIBOND', text)
        # Las 8 secciones y la leyenda de copia no controlada.
        self.assertIn('8. Información documentada', text)
        self.assertIn('copia no controlada', text)

    def test_02_documented_info_dedup_and_union(self):
        docs = self.process._sgi_documented_info()
        codes = docs.mapped('sgi_code')
        # Unión: formato de actividad + familia FK + referencia cruzada.
        self.assertIn('F-P-A28-12', codes)
        self.assertIn('F-P-A28-21', codes)
        self.assertIn('P-A22', codes)
        # Sin duplicados y ordenado por clave.
        self.assertEqual(len(codes), len(set(codes)))
        self.assertEqual(codes, sorted(codes))

    def test_03_header_reads_live_procedure(self):
        self.assertEqual(self.process._sgi_procedure_document(), self.proc_doc)
        self.assertEqual(self.process._sgi_procedure_document().sgi_revision, '15')


@tagged('post_install', '-at_install')
class TestProcedureVentasSeed(TransactionCase):
    """Paso 3: el piloto P-A28 VENTAS Rev.15 cargado como datos."""

    # Claves de formato del P-A28 (se crean como documentos vigentes para que la
    # semilla los enlace y la sección 8 los liste).
    FORMAT_CODES = [
        'IT-P-A28-01', 'IT-P-A28-02', 'F-P-A28-21', 'F-P-A28-13', 'F-P-A31-01',
        'F-P-A28-17', 'F-P-A31-02', 'F-P-A28-18', 'F-P-A28-12', 'F-P-A28-04',
        'F-P-A28-16', 'F-P-A28-15', 'F-P-A28-20', 'F-P-D01-09', 'F-P-D01-11',
        'F-P-A28-03', 'F-P-A28-19', 'F-P-A28-01', 'F-P-A28-11',
    ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env.user.group_ids = [
            (4, cls.env.ref('quimibond_sgi.group_sgi_manager').id)]
        cls.process = cls.env.ref('quimibond_sgi.proc_ventas')
        Doc = cls.env['documents.document']
        # Procedimiento que encabeza (clave/fecha/rev en vivo).
        cls.proc_doc = Doc.create({
            'name': 'P-A28 Ventas.pdf', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-A28',
            'sgi_revision': '15', 'sgi_state': 'vigente',
            'sgi_process_id': cls.process.id})
        # Formatos referenciados, vigentes.
        for i, code in enumerate(cls.FORMAT_CODES):
            doc_type = 'instructivo' if code.startswith('IT-') else 'formato'
            Doc.create({
                'name': 'Doc %s' % code, 'type': 'binary',
                'sgi_is_controlled': True, 'sgi_doc_type': doc_type,
                'sgi_code': code, 'sgi_state': 'vigente'})

    def test_01_seed_loads_ventas(self):
        self.env['sgi.config'].seed_procedure_ventas()
        # Actividades ≥ 25 y 7 responsabilidades.
        self.assertGreaterEqual(self.process.activity_count, 25)
        self.assertEqual(len(self.process.job_responsibility_ids), 7)
        self.assertTrue(self.process.scope)
        self.assertEqual(len(self.process.norm_ids), 4)

    def test_02_documented_info_has_15_plus(self):
        self.env['sgi.config'].seed_procedure_ventas()
        documented = self.process._sgi_documented_info()
        codes = [c for c in documented.mapped('sgi_code') if c]
        self.assertGreaterEqual(len(codes), 15,
                                "La sección 8 debe listar ≥15 claves.")
        self.assertEqual(len(codes), len(set(codes)), "Sin duplicados.")

    def test_03_report_renders_for_ventas(self):
        self.env['sgi.config'].seed_procedure_ventas()
        report = self.env.ref('quimibond_sgi.action_report_procedure')
        html, _ = report._render_qweb_html(
            'quimibond_sgi.report_procedure_document', self.process.ids)
        text = html.decode() if isinstance(html, bytes) else html
        self.assertIn('4.2.3.1', text)
        self.assertIn('F-P-A28-12', text)
        self.assertIn('copia no controlada', text)

    def test_04_seed_is_idempotent(self):
        self.env['sgi.config'].seed_procedure_ventas()
        first = self.process.activity_count
        self.env['sgi.config'].seed_procedure_ventas()
        self.assertEqual(self.process.activity_count, first,
                         "Re-ejecutar la semilla no duplica actividades.")
