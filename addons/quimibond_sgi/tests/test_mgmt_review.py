# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestManagementReview(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Review = cls.env['sgi.management.review']

    def _review(self):
        return self.Review.create({
            'date': date(2026, 6, 30),
            'period_from': date(2026, 1, 1),
            'period_to': date(2026, 6, 30),
        })

    def test_01_folio(self):
        review = self._review()
        self.assertTrue(review.folio.startswith('RD-'))

    def test_02_load_inputs_fills_snapshot(self):
        review = self._review()
        review.action_load_inputs()
        # Los resúmenes de texto se llenan (no vacíos)
        self.assertTrue(review.nc_summary)
        self.assertTrue(review.supplier_summary)
        self.assertTrue(review.audit_summary)
        self.assertTrue(review.doc_changes_summary)

    def test_02b_load_inputs_links_period_audits(self):
        process = self.env['sgi.process'].create({'code': 'XRD', 'name': 'Revisión X'})
        inside = self.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, process.ids)],
            'date_start': date(2026, 3, 10), 'date_end': date(2026, 3, 11)})
        self.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, process.ids)],
            'date_start': date(2026, 9, 1), 'date_end': date(2026, 9, 2)})
        review = self._review()
        review.action_load_inputs()
        self.assertIn(inside, review.audit_ids, "La auditoría del periodo queda ligada (P-3).")
        self.assertTrue(all(date(2026, 1, 1) <= a.date_start <= date(2026, 6, 30)
                            for a in review.audit_ids))

    def test_03_done_blocked_without_agreements(self):
        review = self._review()
        with self.assertRaises(UserError):
            review.action_mark_done()

    def test_04_agreements_create_tasks(self):
        review = self._review()
        self.env['sgi.management.review.agreement'].create({
            'review_id': review.id,
            'name': 'Acuerdo de prueba',
            'responsible_id': self.env.user.id,
            'deadline': date(2026, 7, 31),
        })
        review.action_mark_done()
        self.assertEqual(review.state, 'realizada')
        agreement = review.agreement_ids
        # DIR-3 (52.0.0): el acuerdo es una acción del SGI con responsable y
        # compromiso (actividad nativa al responsable), no una tarea.
        self.assertTrue(agreement.action_line_id)
        self.assertEqual(agreement.action_line_id.date_commit, date(2026, 7, 31))
        self.assertEqual(agreement.action_line_id.review_id, review)
        self.assertTrue(agreement.action_line_id.activity_id)
        self.assertEqual(agreement.status_label, 'Abierta')
