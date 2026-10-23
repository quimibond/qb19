# -*- coding: utf-8 -*-
"""Cargar el expediente completo de una vez. Los nombres de archivo son los
que mandan las agencias y Logística (memoria de correo, 2025-10 a 2026-10)."""
import io
import zipfile

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import PedimentoCommon
from ..models.qb_pedimento_carga import consecutivos_en_nombre


@tagged('post_install', '-at_install')
class TestCarga(PedimentoCommon):

    def _adjunto(self, nombre, datos=b'%PDF-1.4'):
        return self.env['ir.attachment'].create({'name': nombre, 'raw': datos})

    def _zip(self, nombre, archivos):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            for n, d in archivos:
                zf.writestr(n, d)
        return self._adjunto(nombre, buf.getvalue())

    def _carga(self, ped, adjuntos, **vals):
        return self.env['qb.pedimento.carga'].create(dict(
            pedimento_id=ped.id, archivo_ids=[(6, 0, adjuntos.ids)], **vals))

    def _tipo(self, xmlid):
        return self.env.ref('qb_pedimento.%s' % xmlid)

    def test_consecutivos_en_nombre(self):
        self.assertEqual(consecutivos_en_nombre('26-07-1612-6000590.pdf'), {'6000590'})
        self.assertEqual(consecutivos_en_nombre('17404306000661NORMAL_PAGADO.PDF'), {'6000661'})
        self.assertEqual(consecutivos_en_nombre('Doda5001894.PDF'), {'5001894'})
        self.assertEqual(consecutivos_en_nombre('C-INV2025090016-MX-Invoice-4.0.pdf'), set())

    def test_patrones_sembrados(self):
        self.assertTrue(self._tipo('doc_pedimento').patron_archivo)
        self.assertTrue(self._tipo('doc_doda_exp').patron_archivo)

    def test_clasifica_importacion_por_nombre(self):
        ped = self._pedimento(numero='26 43 1740 6000661', referencia_agencia='I601')
        adjuntos = (self._adjunto('17404306000661NORMAL_PAGADO.PDF') | self._adjunto('17404306000661SIMPDEC_PAGADO.PDF')
                    | self._adjunto('INVOICE AND PL BIANCO.pdf') | self._adjunto('JJ28104.XML')
                    | self._adjunto('CARTA 3.1.8 FIRMADA.pdf') | self._adjunto('HOJACALCULO.pdf')
                    | self._adjunto('COVE2680NPTX4.pdf') | self._adjunto('foto.jpg'))
        carga = self._carga(ped, adjuntos)
        carga.action_clasificar()
        tipos = {l.nombre: l.tipo_ids for l in carga.linea_ids}
        self.assertIn(self._tipo('doc_pedimento'), tipos['17404306000661NORMAL_PAGADO.PDF'])
        self.assertIn(self._tipo('doc_pedimento'), tipos['17404306000661SIMPDEC_PAGADO.PDF'])
        # Un PDF con factura y packing cubre los dos documentos.
        self.assertEqual(tipos['INVOICE AND PL BIANCO.pdf'],
                         self._tipo('doc_factura_proveedor') | self._tipo('doc_packing_imp'))
        self.assertEqual(tipos['JJ28104.XML'], self._tipo('doc_cuenta_gastos'))
        self.assertEqual(tipos['CARTA 3.1.8 FIRMADA.pdf'], self._tipo('doc_carta_318'))
        self.assertEqual(tipos['HOJACALCULO.pdf'], self._tipo('doc_mv'))
        self.assertEqual(tipos['COVE2680NPTX4.pdf'], self._tipo('doc_cove'))
        self.assertFalse(tipos['foto.jpg'])
        # Solo tipos de importación.
        self.assertFalse(carga.linea_ids.tipo_ids.filtered(lambda t: t.operacion == 'exportacion'))
        self.assertFalse(carga.linea_ids.filtered('aviso'))

    def test_guardar_cumple_documentos_y_deja_sin_tipo_como_adjunto(self):
        ped = self._pedimento(numero='26 43 1740 6000661')
        carga = self._carga(ped, self._adjunto('17404306000661NORMAL_PAGADO.PDF') | self._adjunto('foto.jpg'))
        carga.action_clasificar()
        carga.action_guardar()
        doc = ped.documento_ids.filtered(lambda d: d.tipo_id == self._tipo('doc_pedimento'))
        self.assertTrue(doc.cumplido)
        self.assertEqual(doc.attachment_ids.name, '17404306000661NORMAL_PAGADO.PDF')
        adjuntos = self.env['ir.attachment'].search([('res_model', '=', 'qb.pedimento'), ('res_id', '=', ped.id)])
        self.assertEqual(set(adjuntos.mapped('name')), {'17404306000661NORMAL_PAGADO.PDF', 'foto.jpg'})

    def test_zip_se_abre(self):
        ped = self._pedimento(numero='26 07 1612 6000590', operacion='exportacion', clave_id=self.clave_a1.id)
        zipf = self._zip('26-03-26.zip', [
            ('26-07-1612-6000590.pdf', b'x'), ('Doda6000590.PDF', b'x'),
            ('C-INV2026030081-MX-Invoice-4.0.xml', b'<x/>'), ('CEO TMEC 2026 SHAWMUT.pdf', b'x'),
            ('__MACOSX/._basura', b'x'), ('carpeta/', b''),
        ])
        carga = self._carga(ped, zipf)
        carga.action_clasificar()
        tipos = {l.nombre: l.tipo_ids for l in carga.linea_ids}
        self.assertEqual(set(tipos), {'26-07-1612-6000590.pdf', 'Doda6000590.PDF',
                                      'C-INV2026030081-MX-Invoice-4.0.xml', 'CEO TMEC 2026 SHAWMUT.pdf'})
        self.assertIn(self._tipo('doc_pedimento'), tipos['26-07-1612-6000590.pdf'])
        self.assertEqual(tipos['Doda6000590.PDF'], self._tipo('doc_doda_exp'))
        self.assertIn(self._tipo('doc_factura_cfdi'), tipos['C-INV2026030081-MX-Invoice-4.0.xml'])
        self.assertEqual(tipos['CEO TMEC 2026 SHAWMUT.pdf'], self._tipo('doc_certificado_origen_exp'))

    def test_aviso_si_el_archivo_es_de_otro_pedimento(self):
        ped = self._pedimento(numero='26 16 3774 6000206')
        carga = self._carga(ped, self._adjunto('37741606000417NORMAL_PAGADO.PDF'))
        carga.action_clasificar()
        self.assertIn('6000417', carga.linea_ids.aviso)

    def test_expediente_unico_cubre_todo(self):
        ped = self._pedimento(numero='26 16 3774 6000206')
        carga = self._carga(ped, self._adjunto('EXPEDIENTE 83272.pdf'), expediente_unico=True)
        carga.action_clasificar()
        carga.action_guardar()
        self.assertTrue(ped.documentos_completos)
        self.assertEqual(ped.documentos_faltantes, 0)

    def test_linea_omitida_no_se_guarda(self):
        ped = self._pedimento(numero='26 16 3774 6000206')
        carga = self._carga(ped, self._adjunto('COVE1.pdf') | self._adjunto('COVE2.pdf'))
        carga.action_clasificar()
        carga.linea_ids.filtered(lambda l: l.nombre == 'COVE2.pdf').omitir = True
        carga.action_guardar()
        doc = ped.documento_ids.filtered(lambda d: d.tipo_id == self._tipo('doc_cove'))
        self.assertEqual(doc.attachment_ids.mapped('name'), ['COVE1.pdf'])

    def test_zip_invalido(self):
        ped = self._pedimento()
        carga = self._carga(ped, self._adjunto('roto.zip', b'no es zip'))
        with self.assertRaises(UserError):
            carga.action_clasificar()

    def test_sin_archivos(self):
        with self.assertRaises(UserError):
            self._carga(self._pedimento(), self.env['ir.attachment']).action_clasificar()

    def test_usuario_logistica_puede_cargar(self):
        ped = self._pedimento(numero='26 16 3774 6000206')
        adj = self.env['ir.attachment'].with_user(self.logistica).create({'name': 'COVE.pdf', 'raw': b'x'})
        carga = self.env['qb.pedimento.carga'].with_user(self.logistica).create(
            {'pedimento_id': ped.id, 'archivo_ids': [(6, 0, adj.ids)]})
        carga.action_clasificar()
        carga.action_guardar()
        self.assertTrue(ped.documento_ids.filtered(lambda d: d.tipo_id == self._tipo('doc_cove')).cumplido)

    def test_boton_abre_asistente(self):
        ped = self._pedimento()
        action = ped.action_cargar_expediente()
        self.assertEqual(action['res_model'], 'qb.pedimento.carga')
        self.assertEqual(action['context']['default_pedimento_id'], ped.id)
