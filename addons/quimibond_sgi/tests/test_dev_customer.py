# -*- coding: utf-8 -*-
"""57.128.0: la aprobación del cliente abre la etapa «Muestra»."""
import base64

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevCustomerApproval(TransactionCase):

    def _dev(self):
        partner = self.env['res.partner'].create({'name': 'CLIENTE APROBACION PRUEBA', 'is_company': True})
        return self.env['project.project'].create({'name': 'x', 'sgi_is_ft': True, 'partner_id': partner.id,
                                                   'sgi_dev_product_name': 'Jersey aprobación'})

    def test_01_sin_registro_no_hay_muestra(self):
        dev = self._dev()
        muestra = self.env.ref('quimibond_sgi.sgi_dev_stage_muestra')
        with self.assertRaises(UserError):
            dev.write({'stage_id': muestra.id})
        with self.assertRaises(UserError, msg="Faltan medio, fecha y evidencia"):
            dev.action_sgi_dev_register_customer_approval()
        dev.write({'sgi_dev_customer_approval_medium': 'oc', 'sgi_dev_customer_approval_date': '2026-10-08',
                   'sgi_dev_customer_approval_ref': 'OC-123',
                   'sgi_dev_customer_approval_file': base64.b64encode(b'oc'), 'sgi_dev_customer_approval_filename': 'oc.pdf'})
        dev.action_sgi_dev_register_customer_approval()
        self.assertTrue(dev.sgi_dev_customer_approved_at)
        self.assertEqual(dev.sgi_dev_customer_approved_by_id, self.env.user)
        self.assertEqual(dev.sgi_dev_stage_key, 'muestra', "Con la aprobación el proyecto pasa a Muestra")
        self.assertTrue(dev.sgi_ft_folio, "Y recibe su folio FT")
        cerrado = self.env.ref('quimibond_sgi.sgi_dev_stage_cerrado_sin_producto')
        otro = self._dev()
        otro.write({'stage_id': cerrado.id})
        self.assertEqual(otro.sgi_dev_stage_key, 'cerrado_sin_producto', "Cerrar sin producto no pide aprobación")

    def test_02_los_que_ya_estan_en_muestra_no_se_tocan(self):
        dev = self._dev()
        muestra = self.env.ref('quimibond_sgi.sgi_dev_stage_muestra')
        dev.with_context(sgi_dev_migration=True).write({'stage_id': muestra.id})
        respuesta = self.env.ref('quimibond_sgi.sgi_dev_stage_respuesta_cliente')
        dev.write({'stage_id': respuesta.id})
        self.assertEqual(dev.sgi_dev_stage_key, 'respuesta_cliente')
