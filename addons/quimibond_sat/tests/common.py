# -*- coding: utf-8 -*-
"""Base de los tests: compañía con RFC, un proveedor y un cliente con RFC real
(el validador de RFC exige dígito verificador correcto) y helpers para crear
facturas publicadas con su folio fiscal."""
from odoo import Command, fields
from odoo.addons.account.tests.common import AccountTestInvoicingCommon

RFC_QUIMIBOND = 'PNT920218IW5'
RFC_PROVEEDOR = 'PSA0602073Q1'   # PAPELERA SANDOVAL
RFC_CLIENTE = 'ETE790612A92'     # EUROTECNICA TEXTIL


def syntage_invoice(uuid, **overrides):
    """Payload de un CFDI como lo manda Syntage (camelCase, inglés)."""
    obj = {
        'id': 'a1aa3c35-%s' % uuid[:8],
        'uuid': uuid,
        'type': 'I',
        'status': 'VIGENTE',
        'canceledAt': None,
        'isIssuer': False,
        'isReceiver': True,
        'issuedAt': '2026-04-29 22:42:37',
        'certifiedAt': '2026-04-29 22:42:49',
        'issuer': {'rfc': RFC_PROVEEDOR, 'name': 'PAPELERA SANDOVAL', 'blacklistStatus': None},
        'receiver': {'rfc': RFC_QUIMIBOND, 'name': 'PRODUCTORA DE NO TEJIDOS QUIMIBOND', 'blacklistStatus': None},
        'subtotal': 2834.36,
        'discount': 0,
        'total': 3287.86,
        'currency': 'MXN',
        'exchangeRate': None,
        'transferredTaxes': {'total': 453.5},
        'retainedTaxes': {'total': 0},
        'paymentType': 'PUE',
        'paymentMethod': '03',
        'usage': 'G03',
        'serie': 'A',
        'folio': '1234',
    }
    obj.update(overrides)
    return obj


class SatCommon(AccountTestInvoicingCommon):
    chart_template = 'generic_coa'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        cls.company.write({'vat': RFC_QUIMIBOND, 'sat_sync_enabled': True})
        cls.proveedor = cls.env['res.partner'].create({
            'name': 'PAPELERA SANDOVAL', 'is_company': True, 'vat': RFC_PROVEEDOR})
        cls.cliente = cls.env['res.partner'].create({
            'name': 'EUROTECNICA TEXTIL', 'is_company': True, 'vat': RFC_CLIENTE})

    def _invoice(self, partner, amount, move_type='in_invoice', uuid=None, post=True, day='2026-04-29'):
        if move_type.startswith('in_'):
            account = self.company_data['default_account_expense']
        else:
            account = self.company_data['default_account_revenue']
        move = self.env['account.move'].create({
            'move_type': move_type,
            'partner_id': partner.id,
            'invoice_date': fields.Date.from_string(day),
            'date': fields.Date.from_string(day),
            'invoice_line_ids': [Command.create({
                'name': 'Línea', 'quantity': 1.0, 'price_unit': amount,
                'account_id': account.id, 'tax_ids': [Command.clear()],
            })],
        })
        if post:
            move.action_post()
        if uuid:
            self._set_uuid(move, uuid)
        return move

    def _set_uuid(self, move, uuid):
        """Folio fiscal como lo dejaría Odoo al recibir el XML: un documento
        CFDI ligado al asiento. attachment_uuid se calcula del adjunto, así
        que en el test se fija por SQL."""
        doc = self.env['l10n_mx_edi.document'].create({
            'move_id': move.id,
            'state': 'invoice_received' if move.move_type.startswith('in_') else 'invoice_sent',
            'sat_state': 'valid',
            'datetime': fields.Datetime.now(),
        })
        self.env.cr.execute(
            "UPDATE l10n_mx_edi_document SET attachment_uuid = %s WHERE id = %s", (uuid.upper(), doc.id))
        doc.invalidate_recordset(['attachment_uuid'])
        move.invalidate_recordset()
        return doc
