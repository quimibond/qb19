# -*- coding: utf-8 -*-
"""PR 5 del plan (19.0.52.0.0): DIR-2 riesgos con evaluación periódica, DIR-3
informe de revisión por la dirección con acuerdos como acciones, DIR-4
tablero de dirección (I-9) y PER-3 matriz de competencias."""
from datetime import date, timedelta

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestPr5Direction(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.owner_user = new_test_user(cls.env, login='pr5_owner',
                                       groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner = cls.env['hr.employee'].create({'name': 'Dueño PR5', 'user_id': cls.owner_user.id})
        cls.parent = cls.env['sgi.process'].create({'code': 'XP5', 'name': 'Macro PR5'})
        cls.process = cls.env['sgi.process'].create({
            'code': 'XP5A', 'name': 'Proceso PR5', 'owner_id': cls.owner.id, 'parent_id': cls.parent.id})

    def _activities(self, record):
        return self.env['mail.activity'].search(
            [('res_model', '=', record._name), ('res_id', '=', record.id)]).mapped('summary')

    def test_01_dir2_evaluacion_periodica_y_alto_sin_accion(self):
        Risk = self.env['sgi.risk']
        risk = Risk.create({'name': 'Caída de rollo', 'instrument': 'ryo',
                            'process_id': self.process.id})
        with self.assertRaises(UserError):
            risk.action_evaluate()
        risk.write({'eval_probability': '5', 'eval_impact': '5'})
        self.assertEqual(risk.score, 25)
        self.assertEqual(risk.semaphore, 'rojo')
        self.assertTrue(risk.high_without_action, "Alto, abierto y sin acción → marcado.")
        risk.action_evaluate()
        self.assertEqual(risk.last_eval_date, date.today())
        self.assertIn(risk.next_review_date.month, (1, 7))
        self.assertGreater(risk.next_review_date, date.today())
        self.assertEqual(risk.next_review_date.day, 1)
        # Una acción abierta quita la marca; terminarla la regresa.
        line = self.env['sgi.action.line'].create({
            'risk_id': risk.id, 'name': 'Instalar guarda', 'responsible_id': self.owner_user.id,
            'date_commit': date.today() + timedelta(days=30)})
        self.assertFalse(risk.high_without_action)
        line.action_mark_done()
        self.assertTrue(risk.high_without_action)
        # Cron: reevaluación vencida y alto sin acción → actividades al dueño del proceso.
        risk.next_review_date = date.today() - timedelta(days=1)
        self.env['sgi.cron'].cron_risk_review()
        summaries = self._activities(risk)
        self.assertTrue(any('Revisar riesgo' in s for s in summaries))
        self.assertTrue(any('sin acción' in s for s in summaries))
        low = Risk.create({'name': 'Bajo', 'instrument': 'ryo', 'process_id': self.process.id,
                           'eval_probability': '1', 'eval_impact': '1'})
        self.assertEqual(low.semaphore, 'verde')
        self.assertFalse(low.high_without_action)
        # Matriz de riesgos del proceso en PDF.
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_risk_matrix_document', self.process.ids)[0].decode()
        for text in ('Matriz de riesgos', 'XP5A', 'Caída de rollo', 'alto sin acción'):
            self.assertIn(text, html)

    def test_02_dir3_acuerdos_como_acciones_e_informe(self):
        review = self.env['sgi.management.review'].create({
            'period_from': date(2026, 1, 1), 'period_to': date(2026, 6, 30)})
        review.action_load_inputs()
        self.assertTrue(review.objectives_summary)
        self.assertTrue(review.satisfaction_summary)
        self.env['sgi.management.review.agreement'].create({
            'review_id': review.id, 'name': 'Comprar el medidor de energía',
            'responsible_id': self.owner_user.id, 'deadline': date.today() - timedelta(days=3)})
        review.action_mark_done()
        agreement = review.agreement_ids
        line = agreement.action_line_id
        self.assertTrue(line and line.review_id == review)
        self.assertEqual(line.state, 'vencida')
        self.assertEqual(agreement.status_label, 'Vencida')
        self.assertIn('Comprar el medidor', self._activities(review)[0] if self._activities(review) else '')
        # E1-02: acuerdos cumplidos a tiempo.
        indicator = self.env['sgi.indicator'].create({
            'code': 'TST-E1-02', 'name': 'Acuerdos RxD', 'calc_mode': 'acuerdos_rxd'})
        self.assertEqual(indicator._sgi_compute_value(
            date.today() - timedelta(days=30), date.today()), 0.0)
        line.write({'date_done': line.date_commit})
        self.assertEqual(indicator._sgi_compute_value(
            date.today() - timedelta(days=30), date.today()), 100.0)
        self.assertEqual(agreement.status_label, 'Terminada')
        # La siguiente revisión lee los acuerdos previos desde las acciones.
        nxt = self.env['sgi.management.review'].create({
            'date': date.today() + timedelta(days=1),
            'period_from': date(2026, 7, 1), 'period_to': date(2026, 12, 31)})
        self.assertIn('100.0% cerrados', nxt._sgi_load_prev_agreements())
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_mgmt_review_document', review.ids)[0].decode()
        for text in ('13. Objetivos e indicadores', '14. Satisfacción del cliente',
                     'Comprar el medidor', 'Terminada'):
            self.assertIn(text, html)

    def test_03_dir4_tablero_de_direccion(self):
        Indicator = self.env['sgi.indicator']
        top = Indicator.create({'code': 'TST-DIR', 'name': 'KPI dirección', 'calc_mode': 'manual',
                                'status': 'oficial', 'level': 'direccion'})
        Indicator.create({'code': 'TST-PROC', 'name': 'KPI proceso', 'calc_mode': 'manual',
                          'status': 'oficial', 'level': 'proceso'})
        for i, value in enumerate((10, 20, 30, 40, 50, 60, 70)):
            self.env['sgi.indicator.measure'].create({
                'indicator_id': top.id, 'period_date': date(2026, 1 + i, 1),
                'value': value, 'state': 'capturado'})
        self.assertEqual(top.last_six.count('·'), 5, "Seis periodos separados por ·.")
        self.assertNotIn('01/26', top.last_six)
        action = self.env['sgi.direction.board'].action_open()
        board = self.env['sgi.direction.board'].browse(action['res_id'])
        self.assertIn(top, board.indicator_ids)
        self.assertNotIn(Indicator.search([('code', '=', 'TST-PROC')]), board.indicator_ids)
        self.assertEqual(board.indicator_count, len(board.indicator_ids))
        self.assertTrue(board.action_open_spreadsheet()['tag'])

    def test_04_per3_matriz_de_competencias(self):
        dept = self.env['hr.department'].create({'name': 'Área PR5'})
        job = self.env['hr.job'].create({'name': 'TEJEDOR PR5'})
        emp = self.env['hr.employee'].create({
            'name': 'Tejedor PR5', 'job_id': job.id, 'department_id': dept.id})
        it = self.env['documents.document'].create({
            'name': 'IT-P-C11-90.pdf', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-P-C11-90', 'sgi_state': 'vigente',
            'sgi_job_ids': [(6, 0, job.ids)]})
        it.action_generate_acks()
        rows = dept._sgi_competence_matrix()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['employee'], emp)
        self.assertEqual(rows[0]['instructions'], '0 de 1')
        self.assertEqual(rows[0]['procedure'], 'Sin publicar')
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_competence_matrix_document', dept.ids)[0].decode()
        for text in ('Matriz de competencias', 'Área PR5', 'Tejedor PR5', 'TEJEDOR PR5', '0 de 1'):
            self.assertIn(text, html)
