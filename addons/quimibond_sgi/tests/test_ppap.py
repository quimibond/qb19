# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestPpap(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Ppap = cls.env['sgi.ppap']
        cls.partner = cls.env['res.partner'].create({'name': 'Continental', 'is_company': True})
        cls.product = cls.env['product.template'].create({'name': 'Fieltro técnico A'})

    def _new_ppap(self):
        return self.Ppap.create({
            'partner_id': self.partner.id,
            'product_tmpl_id': self.product.id,
        })

    def test_01_generates_18_elements(self):
        ppap = self._new_ppap()
        self.assertEqual(len(ppap.element_ids), 18,
                         "El PPAP debe generar los 18 elementos AIAG.")
        # Idempotente: regenerar no duplica.
        ppap._sgi_generate_elements()
        self.assertEqual(len(ppap.element_ids), 18)

    def test_02_enviado_requires_no_pending(self):
        ppap = self._new_ppap()
        with self.assertRaises(UserError):
            ppap.action_mark_enviado()
        # Marca todo como N/A o listo.
        ppap.element_ids.write({'state': 'listo'})
        ppap.action_mark_enviado()
        self.assertEqual(ppap.state, 'enviado')
        self.assertTrue(ppap.date_submitted)

    def test_02b_nivel_1_solo_bloquean_los_S(self):
        # Nivel 1: solo AAR (13) y PSW (18) se presentan; el resto se retiene.
        ppap = self.Ppap.create({
            'partner_id': self.partner.id,
            'product_tmpl_id': self.product.id,
            'level': '1',
        })
        submits = ppap.element_ids.filtered(lambda e: e.submission == 'submit')
        self.assertEqual(sorted(submits.mapped('sequence')), [13, 18])
        # Los retenidos pendientes NO bloquean el envío; los S sí.
        with self.assertRaises(UserError):
            ppap.action_mark_enviado()
        submits.write({'state': 'listo'})
        ppap.action_mark_enviado()
        self.assertEqual(ppap.state, 'enviado')

    def test_02c_cambiar_nivel_reaplica_tabla(self):
        ppap = self._new_ppap()  # nivel 3: casi todo S
        el15 = ppap.element_ids.filtered(lambda e: e.sequence == 15)
        self.assertEqual(el15.submission, 'retain',
                         "La muestra maestra (15) se retiene incluso en nivel 3.")
        n_submit_l3 = len(ppap.element_ids.filtered(
            lambda e: e.submission == 'submit'))
        ppap.level = '5'
        self.assertFalse(ppap.element_ids.filtered(
            lambda e: e.submission == 'submit'),
            "Nivel 5 retiene todo (revisión en planta).")
        ppap.level = '3'
        self.assertEqual(len(ppap.element_ids.filtered(
            lambda e: e.submission == 'submit')), n_submit_l3)

    def test_03_approve_requires_psw(self):
        ppap = self._new_ppap()
        ppap.element_ids.write({'state': 'listo'})
        psw = ppap.element_ids.filtered(lambda e: e.template_id.is_psw)
        self.assertTrue(psw, "Debe existir el elemento 18 (PSW).")
        ppap.action_mark_enviado()
        # PSW en pendiente/na -> no aprueba
        psw.state = 'na'
        with self.assertRaises(UserError):
            ppap.action_approve()
        # PSW listo -> aprueba
        psw.state = 'aprobado'
        ppap.action_approve()
        self.assertEqual(ppap.state, 'aprobado')
        self.assertTrue(ppap.date_decision)
