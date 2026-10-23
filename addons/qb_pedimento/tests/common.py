# -*- coding: utf-8 -*-
"""Base de los tests: un proveedor extranjero, una agencia aduanal y el
usuario de Logística con el grupo de pedimentos."""
from odoo.addons.account.tests.common import AccountTestInvoicingCommon


class PedimentoCommon(AccountTestInvoicingCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # El usuario de AccountTestInvoicingCommon no es admin: se le da el grupo del módulo.
        cls.env.user.group_ids = [(4, cls.env.ref('qb_pedimento.group_pedimento_manager').id),
                                  (4, cls.env.ref('stock.group_stock_manager').id),
                                  (4, cls.env.ref('purchase.group_purchase_manager').id)]
        cls.Pedimento = cls.env['qb.pedimento']
        cls.logistica = cls.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Logística', 'login': 'logistica@test.local', 'email': 'logistica@test.local',
            'group_ids': [(6, 0, [cls.env.ref('qb_pedimento.group_pedimento_user').id,
                                  cls.env.ref('stock.group_stock_user').id,
                                  cls.env.ref('purchase.group_purchase_user').id])],
        })
        cls.proveedor = cls.env['res.partner'].create({'name': 'NINGBO MH INDUSTRY CO., LTD', 'is_company': True})
        cls.agencia = cls.env['res.partner'].create({'name': 'JORGE REYES HIDALGO Y SUCESORES SC', 'is_company': True})
        cls.cliente_virtual = cls.env['res.partner'].create({
            'name': 'LEAR MEXICAN TRIM OPERATIONS', 'is_company': True, 'qb_pedimento_virtual': True})
        cls.clave_a1 = cls.env.ref('qb_pedimento.clave_a1')
        cls.clave_v1 = cls.env.ref('qb_pedimento.clave_v1')
        cls.clave_r1 = cls.env.ref('qb_pedimento.clave_r1')

    def _pedimento(self, **vals):
        base = {'operacion': 'importacion', 'clave_id': self.clave_a1.id, 'partner_id': self.proveedor.id,
                'agencia_id': self.agencia.id, 'user_id': self.logistica.id}
        base.update(vals)
        return self.Pedimento.create(base)

    def _completar_documentos(self, pedimento):
        attachment = self.env['ir.attachment'].create({'name': 'pedimento.pdf', 'raw': b'%PDF-1.4'})
        for doc in pedimento.documento_ids.filtered('required'):
            doc.attachment_ids = [(6, 0, attachment.ids)]
