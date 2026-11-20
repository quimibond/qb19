from odoo.tests import BaseCase, tagged

from ..models.cfdi import CfdiInvalido, clabe_valida, leer_cfdi, limpiar_rfc
from .common import RFC_ALFA, RFC_EMPRESA, cfdi_xml

UUID = '11111111-2222-4333-8444-555555555555'


@tagged('post_install', '-at_install')
class TestCfdi(BaseCase):

    def test_lee_un_cfdi(self):
        datos = leer_cfdi(cfdi_xml(UUID.lower(), subtotal=1000, total=1160))
        self.assertEqual(datos['uuid'], UUID)
        self.assertEqual(datos['emisor_rfc'], RFC_ALFA)
        self.assertEqual(datos['receptor_rfc'], RFC_EMPRESA)
        self.assertEqual(datos['tipo'], 'I')
        self.assertEqual(datos['fecha'], '2026-11-10')
        self.assertEqual((datos['subtotal'], datos['total'], datos['moneda']), (1000.0, 1160.0, 'MXN'))
        self.assertEqual((datos['serie'], datos['folio']), ('F', '100'))

    def test_rechaza_lo_que_no_es_cfdi(self):
        for malo in (b'', b'no es xml', b'<a/>', b'<html><body/></html>'):
            with self.assertRaises(CfdiInvalido):
                leer_cfdi(malo)
        with self.assertRaises(CfdiInvalido):
            leer_cfdi(cfdi_xml(UUID, timbrado=False))

    def test_no_resuelve_entidades(self):
        bomba = (b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY e SYSTEM "file:///etc/passwd">]>'
                 b'<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Serie="&e;">'
                 b'<cfdi:Emisor Rfc="A"/><cfdi:Receptor Rfc="B"/></cfdi:Comprobante>')
        with self.assertRaises(CfdiInvalido):
            leer_cfdi(bomba)

    def test_rfc_y_clabe(self):
        self.assertEqual(limpiar_rfc(' alf-010203 xy1 '), RFC_ALFA)
        self.assertEqual(limpiar_rfc('MX' + RFC_ALFA), RFC_ALFA)
        self.assertTrue(clabe_valida('002010077777777771'))
        self.assertFalse(clabe_valida('002010077777777772'))
        self.assertFalse(clabe_valida('12345'))
