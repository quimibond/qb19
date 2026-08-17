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
