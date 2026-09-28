# -*- coding: utf-8 -*-
"""Actividades ↔ puntos de la norma (56.23.0): «Cumple con», evidencia en el
punto, Matriz de cumplimiento y checklist de auditoría por requisito."""
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestNormCompliance(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.norm = cls.env['sgi.norm'].create({
            'code': 'ISO 9999:2046', 'name': 'Norma de prueba',
            'clause_ids': [(0, 0, {'code': '8.5.1', 'name': 'Control de la producción'}),
                           (0, 0, {'code': '8.10', 'name': 'Mejora'}),
                           (0, 0, {'code': '8.9', 'name': 'Punto sin actividad'})]})
        cls.c851, cls.c810, cls.c89 = [cls.norm.clause_ids.filtered(lambda c, k=k: c.code == k)
                                       for k in ('8.5.1', '8.10', '8.9')]
        cls.job = cls.env['hr.job'].create({'name': 'TEJEDOR NORMA PRUEBA'})
        cls.process = cls.env['sgi.process'].create({
            'code': 'XNR1', 'name': 'Proceso norma', 'norm_ids': [(6, 0, cls.norm.ids)]})
        cls.other = cls.env['sgi.process'].create({'code': 'XNR2', 'name': 'Otro proceso norma'})
        Activity = cls.env['sgi.process.activity']
        cls.act1 = Activity.create({
            'process_id': cls.process.id, 'name': 'Tejer según orden',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})],
            'norm_clause_ids': [(6, 0, cls.c851.ids)]})
        cls.act2 = Activity.create({
            'process_id': cls.other.id, 'name': 'Registrar la mejora',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})],
            'norm_clause_ids': [(6, 0, (cls.c851 | cls.c810).ids)]})
        cls.auditor = new_test_user(cls.env, login='zs_norm_aud',
                                    groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.env['hr.employee'].create({
            'name': 'Auditor norma', 'user_id': cls.auditor.id,
            'job_id': cls.env['hr.job'].create({'name': 'AUDITOR NORMA PRUEBA'}).id})

    def test_01_evidencia_en_el_punto(self):
        self.assertEqual(self.c851.short_label, '9999 8.5.1')
        self.assertEqual(self.c851.activity_count, 2)
        self.assertEqual(self.c851.process_ids, self.process | self.other)
        self.assertFalse(self.c89.covered)
        Clause = self.env['sgi.norm.clause']
        self.assertEqual(Clause.search([('norm_id', '=', self.norm.id), ('covered', '=', False)]), self.c89)
        self.act2.active = False
        self.c810.invalidate_recordset()
        self.assertFalse(self.c810.covered, "Una actividad archivada no cuenta como evidencia.")
        self.assertEqual(Clause._sgi_find('ISO 9999:2046 8.10'), self.c810)
        self.assertEqual(Clause._sgi_find('9999 8.5.1'), self.c851)
        self.assertFalse(Clause._sgi_find('9999 1.1'))

    def test_05_requisitos_de_clientes(self):
        norm = self.env['sgi.norm'].create({
            'code': 'CLIENTES-PRUEBA', 'name': 'Requisitos de clientes',
            'clause_ids': [(0, 0, {'code': 'ZCLI-01', 'name': 'PPAP'}),
                           (0, 0, {'code': 'ZCLI-02', 'name': 'Respuesta en plazo'})]})
        ppap = norm.clause_ids.filtered(lambda c: c.code == 'ZCLI-01')
        self.assertEqual(ppap.short_label, 'ZCLI-01')
        self.assertEqual(self.env['sgi.norm.clause']._sgi_find('ZCLI-01'), ppap)
        self.act1.norm_clause_ids = [(4, ppap.id)]
        matrix = norm._sgi_compliance_matrix()
        self.assertIn(self.process, matrix['processes'])
        self.assertEqual(matrix['uncovered'], 1)
        entry = next(e for s in self.job._sgi_my_procedure_data()['sections'] for e in s['entries']
                     if e['activity'] == self.act1)
        self.assertIn('ZCLI-01', entry['norms'])

    def test_02_matriz_de_cumplimiento(self):
        matrix = self.norm._sgi_compliance_matrix()
        self.assertEqual([r['clause'].code for r in matrix['rows']], ['8.5.1', '8.9', '8.10'],
                         "8.10 va después de 8.9.")
        self.assertEqual(matrix['processes'], self.process | self.other)
        self.assertEqual(matrix['uncovered'], 1)
        row = matrix['rows'][0]
        self.assertEqual(row['total'], 2)
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_compliance_matrix_document', self.norm.ids)[0]
        self.assertIn(b'Matriz de cumplimiento', html)
        self.assertIn(b'Sin ninguna actividad', html)

    def test_03_mi_procedimiento_muestra_cumple_con(self):
        data = self.job._sgi_my_procedure_data()
        entries = [e for s in data['sections'] for e in s['entries']]
        entry = next(e for e in entries if e['activity'] == self.act2)
        self.assertEqual(entry['norms'], '9999 8.5.1, 9999 8.10')
        before = data['hash']
        self.act1.norm_clause_ids = [(4, self.c810.id)]
        self.assertEqual(self.job._sgi_my_procedure_data()['hash'], before,
                         "Ligar puntos de la norma no obliga a republicar.")

    def test_04_checklist_por_requisito(self):
        audit = self.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, self.process.ids)],
            'norm_ids': [(6, 0, self.norm.ids)], 'lead_auditor_id': self.auditor.id})
        audit.action_plan()
        line = audit.checklist_line_ids.filtered(lambda l: l.activity_id == self.act1)
        self.assertEqual(line.norm_clause_ids, self.c851)
        self.assertIn('Requisito: 9999 8.5.1', line.question)
        gap = audit.checklist_line_ids.filtered(lambda l: not l.activity_id)
        self.assertEqual(gap.norm_clause_ids, self.c89, "Una pregunta por punto sin actividad.")
        self.assertIn('9999 8.9', gap.question)
        audit.action_generate_checklist()
        self.assertEqual(len(audit.checklist_line_ids.filtered(lambda l: not l.activity_id)), 1,
                         "Idempotente.")
        audit.action_start()
        line.write({'answer': 'nc_menor', 'evidence': 'Sin orden en el telar 4'})
        self.assertEqual(line.finding_id.norm_clause_id, self.c851)
        gap.write({'answer': 'observacion', 'evidence': 'No hay procedimiento'})
        self.assertEqual(gap.finding_id.norm_clause_id, self.c89)
