# -*- coding: utf-8 -*-
"""57.107.0: revisión mensual de la medición por el dueño del proceso."""
from dateutil.relativedelta import relativedelta

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from ..models.sgi_calendar import sgi_today


@tagged('post_install', '-at_install')
class TestRevisionMedicion(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.owner_user = new_test_user(cls.env, login='rv_owner',
                                       groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.other_user = new_test_user(cls.env, login='rv_other',
                                       groups='base.group_user,quimibond_sgi.group_sgi_user')
        owner = cls.env['hr.employee'].create({'name': 'Dueño revisión', 'user_id': cls.owner_user.id})
        cls.process = cls.env['sgi.process'].create({
            'code': 'ZRV', 'name': 'Proceso revisión', 'owner_id': owner.id})
        cls.act = cls.env['sgi.process.activity'].create({
            'process_id': cls.process.id, 'name': 'Dar de alta al cliente',
            'measure_method': 'odoo', 'measure_cadence': 'evento',
            'measure_model_id': cls.env['ir.model']._get('res.partner').id,
            'measure_user_field': 'create_uid',
            'measure_domain': "[('name', '=like', 'ZRV muestra%')]"})
        cls.env['res.partner'].create([{'name': 'ZRV muestra %d' % n} for n in range(5)])
        cls.today = sgi_today(cls.env)
        cls.Review = cls.env['sgi.measure.review']

    def _review(self):
        return self.Review.search([('activity_id', '=', self.act.id)])

    def test_01_una_revision_por_mes_con_tres_muestras(self):
        self.Review._sgi_generate_month(self.today)
        review = self._review()
        self.assertEqual(len(review), 1)
        self.assertEqual(review.user_id, self.owner_user)
        self.assertEqual(len(review.line_ids), 3)
        self.assertTrue(all(line.name.startswith('ZRV muestra') for line in review.line_ids))
        self.Review._sgi_generate_month(self.today)
        self.assertEqual(len(self._review()), 1, "Correr de nuevo no duplica.")
        # Sale en Mis pendientes del dueño; «Ir» abre la revisión.
        Pending = self.env['sgi.my.pending'].with_user(self.owner_user)
        rows = Pending.search(Pending.action_open_mine()['domain']).filtered(
            lambda r: r.kind == 'revision' and r.res_id == review.id)
        self.assertEqual(len(rows), 1)
        opened = rows.action_open()
        self.assertEqual((opened['res_model'], opened['res_id']), ('sgi.measure.review', review.id))

    def test_02_contestar(self):
        self.Review._sgi_generate_month(self.today)
        review = self._review()
        with self.assertRaises(AccessError):
            review.with_user(self.other_user).action_confirm()
        with self.assertRaises(UserError):
            review.with_user(self.owner_user)._sgi_answer('no_corresponde', '')
        review.with_user(self.owner_user)._sgi_answer('no_corresponde', 'Cuenta también los proveedores')
        self.assertEqual(review.state, 'no_corresponde')
        self.act.invalidate_recordset(['spec_gap_ids'])
        gap = self.act.spec_gap_ids.filtered(lambda g: g.code == 'review_rejected')
        self.assertIn('proveedores', gap.message)
        with self.assertRaises(UserError):
            review.with_user(self.owner_user).action_confirm()
        # El mes siguiente: una nueva; confirmada, el faltante se va.
        next_month = self.today + relativedelta(months=1)
        self.Review._sgi_generate_month(next_month)
        new = self._review().filtered(lambda r: r.state == 'pendiente')
        self.assertEqual(len(new), 1)
        new.with_user(self.owner_user).action_confirm()
        self.act.invalidate_recordset(['spec_gap_ids'])
        self.assertNotIn('review_rejected', self.act.spec_gap_ids.mapped('code'))

    def test_03_sin_respuesta_al_mes_siguiente(self):
        self.Review._sgi_generate_month(self.today)
        review = self._review()
        self.Review._sgi_generate_month(self.today + relativedelta(months=1))
        self.assertEqual(review.state, 'sin_respuesta')
        self.assertEqual(len(self._review()), 2)
