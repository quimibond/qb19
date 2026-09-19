# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesCompras(SenalesCommon):

    def _oc(self, hace_aprobada, ref=False):
        prod = self.env['product.product'].create({'name': 'Colorante', 'type': 'consu'})
        po = self.env['purchase.order'].create({'partner_id': self.proveedor.id, 'partner_ref': ref,
                                                'order_line': [(0, 0, {'product_id': prod.id, 'product_qty': 1, 'price_unit': 10})]})
        po.button_confirm()
        self.env.cr.execute('UPDATE purchase_order SET date_approve = %s WHERE id = %s', (self.hace(hace_aprobada), po.id))
        po.invalidate_recordset(['date_approve'])
        return po

    def test_oc_sin_confirmacion(self):
        po = self._oc(8)
        self._oc(8, ref='ACK-1')  # con acuse del proveedor
        self._oc(1)               # reciente
        f = self.fila_de(self.filas('oc_sin_confirmacion', {'umbrales': {'dias': 5}}), 'oc_sin_confirmacion:partner:%d' % self.proveedor.id)
        self.assertEqual([d['id'] for d in f['documentos']], [po.id])

    def test_actividad_vencida_oc_por_usuario(self):
        po = self._oc(1)
        act = self.env['mail.activity'].create({'res_model_id': self.env['ir.model']._get_id('purchase.order'), 'res_id': po.id,
                                                'activity_type_id': self.env.ref('mail.mail_activity_data_todo').id,
                                                'user_id': self.env.user.id, 'date_deadline': self.hace(2), 'summary': 'Llamar'})
        f = self.fila_de(self.filas('actividad_vencida_oc'), 'actividad_vencida_oc:user:%d' % self.env.user.id)
        self.assertIn(act.id, [d['id'] for d in f['documentos']])
