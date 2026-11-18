# -*- coding: utf-8 -*-
"""Traer los archivos del expediente desde la memoria de correo. La memoria
(Supabase) se simula: se prueban la selección de archivos, la clasificación
y que no se repitan ni pisen archivos."""
from unittest.mock import patch

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import PedimentoCommon

PDF = b'%PDF-1.4 pedimento'
DODA = b'%PDF-1.4 doda'


@tagged('post_install', '-at_install')
class TestMemoria(PedimentoCommon):

    def setUp(self):
        super().setUp()
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_intelligence.supabase_url', 'https://memoria.test')
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_intelligence.supabase_service_key', 'llave')
        self.consultas = []

    def _get(self, tabla, params):
        self.consultas.append((tabla, dict(params)))
        params = dict(params)
        if tabla == 'email_attachments' and 'texto_tsv' in params:
            return [{'email_id': 10}, {'email_id': 11}]
        if tabla == 'emails' and 'id' in params:
            return [{'id': 10}, {'id': 11}]
        if tabla == 'emails':
            return []
        return [
            {'id': 1, 'filename': '37741606001184NORMAL_PAGADO.PDF', 'mime_type': 'application/pdf',
             'size_bytes': 80000, 'sha256': 'a', 'storage_path': 'a.pdf'},
            # La misma copia en otro buzón.
            {'id': 2, 'filename': '37741606001184NORMAL_PAGADO.PDF', 'mime_type': 'application/pdf',
             'size_bytes': 80000, 'sha256': 'a', 'storage_path': 'a.pdf'},
            {'id': 3, 'filename': 'Doda6001184.PDF', 'mime_type': 'application/pdf',
             'size_bytes': 30000, 'sha256': 'b', 'storage_path': 'b.pdf'},
            {'id': 4, 'filename': 'image001.png', 'mime_type': 'image/png',
             'size_bytes': 9000, 'sha256': 'c', 'storage_path': 'c.png'},
            {'id': 5, 'filename': 'RE: embarque.eml', 'mime_type': 'message/rfc822',
             'size_bytes': 90000, 'sha256': 'd', 'storage_path': 'd.eml'},
        ]

    def _descargar(self, storage_path):
        return {'a.pdf': PDF, 'b.pdf': DODA}.get(storage_path, b'x')

    def _patch(self):
        Ped = type(self.env['qb.pedimento'])
        return patch.multiple(Ped, _memoria_get=lambda s, t, p: self._get(t, p),
                              _memoria_descargar=lambda s, sp: self._descargar(sp))

    def test_un_expediente_abre_la_carga_clasificada(self):
        ped = self._pedimento(numero='26 16 3774 6001184', fecha_pago='2026-11-13')
        with self._patch():
            action = ped.action_traer_memoria()
        carga = self.env['qb.pedimento.carga'].browse(action['res_id'])
        self.assertEqual(sorted(carga.linea_ids.mapped('nombre')),
                         ['37741606001184NORMAL_PAGADO.PDF', 'Doda6001184.PDF'])
        linea = carga.linea_ids.filtered(lambda l: l.nombre.startswith('3774'))
        self.assertIn(self.env.ref('qb_pedimento.doc_pedimento'), linea.tipo_ids)
        self.assertTrue(ped.memoria_fecha)
        # La búsqueda va por el consecutivo y dentro de la ventana de fechas.
        tsv = [p for t, p in self.consultas if 'texto_tsv' in p][0]
        self.assertEqual(tsv['texto_tsv'], 'fts(simple).6001184')

    def test_varios_guardan_solos_y_no_repiten(self):
        ped = self._pedimento(numero='26 16 3774 6001184', fecha_pago='2026-11-13')
        otro = self._pedimento(numero='26 16 3774 6001154', fecha_pago='2026-10-31')
        with self._patch():
            (ped | otro).action_traer_memoria()
        doc = ped.documento_ids.filtered(lambda d: d.tipo_id == self.env.ref('qb_pedimento.doc_pedimento'))
        self.assertTrue(doc.cumplido)
        # En «otro» los nombres traen el pedimento 6001184: se dejan fuera.
        self.assertFalse(self.env['ir.attachment'].search([('res_model', '=', 'qb.pedimento'),
                                                           ('res_id', '=', otro.id)]))
        # Otra vuelta no duplica lo que ya está.
        antes = self.env['ir.attachment'].search_count([('res_model', '=', 'qb.pedimento'), ('res_id', '=', ped.id)])
        with self._patch(), self.assertRaisesRegex(UserError, 'no tiene archivos nuevos'):
            ped.action_traer_memoria()
        despues = self.env['ir.attachment'].search_count([('res_model', '=', 'qb.pedimento'), ('res_id', '=', ped.id)])
        self.assertEqual(antes, despues)

    def test_cron_marca_la_busqueda(self):
        ped = self._pedimento(numero='26 16 3774 6001184', fecha_pago='2026-11-13')
        with self._patch():
            self.env['qb.pedimento']._cron_traer_memoria()
        self.assertTrue(ped.memoria_fecha)
        self.assertTrue(self.env['ir.attachment'].search([('res_model', '=', 'qb.pedimento'), ('res_id', '=', ped.id)]))

    def test_sin_numero(self):
        ped = self._pedimento()
        with self._patch(), self.assertRaisesRegex(UserError, 'número de pedimento'):
            ped.action_traer_memoria()
