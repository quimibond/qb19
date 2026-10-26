# -*- coding: utf-8 -*-
"""Folio fiscal en la factura de proveedor cuando una persona liga el CFDI.

El documento CFDI es de l10n_mx_edi (Enterprise): sin él solo se prueba que
no hace nada; con él (build de Odoo.sh), que reemplaza el XML cruzado."""
from unittest.mock import patch

from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_A = '99999999-0000-4000-8000-000000000001'
UUID_B = '99999999-0000-4000-8000-000000000002'


def cfdi_xml(uuid):
    return (b'<?xml version="1.0" encoding="UTF-8"?>'
            b'<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" '
            b'xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital" Version="4.0" Total="3287.86">'
            b'<cfdi:Complemento><tfd:TimbreFiscalDigital Version="1.1" UUID="%s"/></cfdi:Complemento>'
            b'</cfdi:Comprobante>' % uuid.upper().encode())


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatFolio(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def test_without_mx_edi_does_nothing(self):
        if self.has_mx_edi:
            self.skipTest('con l10n_mx_edi se prueba el reemplazo')
        bill = self._invoice(self.proveedor, 3287.86)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        cfdi.write({'move_id': bill.id})
        self.assertIn('localización mexicana', cfdi._write_fiscal_folio())
        action = cfdi.action_write_fiscal_folio()
        self.assertEqual(action['tag'], 'display_notification')

    def test_crossed_xml_is_replaced(self):
        self._require_mx_edi()
        bill = self._invoice(self.proveedor, 3287.86, uuid=UUID_B)   # trae el XML de otro CFDI
        cfdi = self._upsert(syntage_invoice(UUID_A))
        with patch.object(type(cfdi), '_fetch_xml', lambda rec: cfdi_xml(rec.uuid)):
            cfdi._reconcile_with(bill)
        docs = self.env['l10n_mx_edi.document'].search([('move_id', '=', bill.id), ('state', '=', 'invoice_received')])
        self.assertEqual(docs.mapped('attachment_uuid'), [UUID_A.upper()])
        self.assertTrue(any('cambiado' in str(body) for body in bill.message_ids.mapped('body')))
