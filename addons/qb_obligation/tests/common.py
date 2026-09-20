# -*- coding: utf-8 -*-
"""Base de los tests: compañía con dueños, dos clientes y facturas de venta
vencidas para anclar obligaciones. La moneda de generic_coa es USD."""
from odoo import Command, fields
from odoo.addons.account.tests.common import AccountTestInvoicingCommon


class ObligationCommon(AccountTestInvoicingCommon):
    chart_template = 'generic_coa'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        # Con acceso a facturas: la actividad espejo se agenda sobre el documento
        # ancla solo si el dueño puede leerlo (mail.activity lo exige).
        groups = [(6, 0, [cls.env.ref('base.group_user').id, cls.env.ref('account.group_account_invoice').id])]
        cls.cxc = Users.create({'name': 'Sandra CXC', 'login': 'cxc@test.local', 'email': 'cxc@test.local',
                                'group_ids': groups})
        cls.direccion = Users.create({'name': 'Jose Dirección', 'login': 'dir@test.local', 'email': 'dir@test.local',
                                      'group_ids': groups})
        cls.conta = Users.create({'name': 'Conta', 'login': 'conta@test.local', 'email': 'conta@test.local',
                                  'group_ids': groups})
        cls.cliente = cls.env['res.partner'].create({'name': 'BELSUEÑO', 'is_company': True})
        cls.cliente2 = cls.env['res.partner'].create({'name': 'BLANCOS MILENIUM', 'is_company': True})
        cls.Obligation = cls.env['qb.obligation']

    def _configure(self, owner=None, escalation=None, **vals):
        self.company.write({
            'obligation_collection_user_id': (owner or self.cxc).id,
            'obligation_escalation_user_id': escalation.id if escalation else False,
            **vals,
        })

    def _invoice(self, partner, amount, day='2026-04-01', due='2026-04-30', post=True, move_type='out_invoice'):
        move = self.env['account.move'].create({
            'move_type': move_type,
            'partner_id': partner.id,
            'invoice_date': fields.Date.from_string(day),
            'date': fields.Date.from_string(day),
            'invoice_date_due': fields.Date.from_string(due),
            'invoice_line_ids': [Command.create({
                'name': 'Tela', 'quantity': 1.0, 'price_unit': amount,
                'account_id': self.company_data['default_account_revenue'].id, 'tax_ids': [Command.clear()],
            })],
        })
        if post:
            move.action_post()
        return move

    def _pay(self, move, amount=None, day='2026-05-05'):
        wizard = self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=move.ids).create({
                'payment_date': fields.Date.from_string(day),
                **({'amount': amount} if amount is not None else {}),
            })
        wizard.action_create_payments()
        move.invalidate_recordset()

    def _run(self):
        self.Obligation._cron_collection()
        self.env.flush_all()
        self.env.invalidate_all()

    def _open_for(self, move):
        return self.Obligation.search([('res_model', '=', 'account.move'), ('res_id', '=', move.id)])

    def _promise_on(self, move, confirm=True, **extra):
        """Promesa de pago (correo) anclada a una factura, confirmada por el dueño."""
        vals = {'obligation_type': 'collection.payment_promise', 'partner_id': move.partner_id.id,
                'description': 'Prometió pagar %s' % move.name, 'name': 'Pago %s' % move.name,
                'res_model': 'account.move', 'res_id': move.id, 'source': 'email',
                'source_ref': 'thread:%s:promise' % move.id, 'date_deadline': move.invoice_date_due}
        vals.update(extra)
        rec = self.Obligation.create_candidate(vals)
        if rec and confirm:
            rec.action_confirm()
        return rec
