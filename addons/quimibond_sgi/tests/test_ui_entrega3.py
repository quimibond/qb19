# -*- coding: utf-8 -*-
"""57.172.0 — Interfaz, entrega 3 (auditoría docs/audit/ui-sgi-2026-10-20.md).

La navegación va en botones inteligentes (con su número cuando lo hay) y la
barra queda para los pasos del flujo."""
from lxml import etree

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestUiEntrega3(TransactionCase):

    def _arch(self, xmlid):
        return etree.fromstring(self.env.ref(xmlid).arch)

    def test_01_navegacion_fuera_de_la_barra(self):
        for xmlid, button in (
                ('quimibond_sgi.sgi_action_line_view_form', 'action_open_origin'),
                ('quimibond_sgi.sgi_audit_view_form', 'action_open_report_document'),
                ('quimibond_sgi.sgi_dev_shipment_view_form', 'action_open_picking'),
                ('quimibond_sgi.sgi_epp_delivery_view_form', 'action_open_sign_request'),
                ('quimibond_sgi.sgi_miid_view_form', 'action_open_request'),
                ('quimibond_sgi.sgi_norm_view_form', 'action_view_uncovered_clauses'),
                ('quimibond_sgi.sgi_process_flow_view_form', 'action_view_records'),
                ('quimibond_sgi.sgi_direction_board_view_form', 'action_open_spreadsheet')):
            arch = self._arch(xmlid)
            self.assertFalse(arch.xpath("//header/button[@name='%s']" % button), xmlid)
            self.assertTrue(arch.xpath("//div[@name='button_box']/button[@name='%s']" % button), xmlid)

    def test_02_conteos(self):
        norm = self.env['sgi.norm'].create({'name': 'Norma ZU3', 'code': 'ZU3'})
        self.env['sgi.norm.clause'].create({'norm_id': norm.id, 'code': '4.1', 'name': 'Contexto ZU3'})
        self.assertEqual(norm.uncovered_clause_count, 1, "Sin actividad, regla ni exclusión: hueco.")
        self.assertEqual(norm.uncovered_clause_count, len(self.env['sgi.norm.clause'].search(
            norm.action_view_uncovered_clauses()['domain'])), "El número es el de la lista que abre.")
        deliverable = self.env['sgi.deliverable'].create({'name': 'Entregable ZU3'})
        self.assertEqual(deliverable.link_count, 0)

    def test_03_faltantes_y_advertencias_por_separado(self):
        arch = self._arch('quimibond_sgi.sgi_process_view_form')
        gaps = arch.xpath("//div[@name='button_box']/button[@name='action_view_spec_gaps']")
        self.assertEqual({b.get('invisible') for b in gaps},
                         {'spec_error_count == 0', 'spec_warning_count == 0'})
