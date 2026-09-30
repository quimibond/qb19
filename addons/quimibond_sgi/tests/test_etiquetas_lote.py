# -*- coding: utf-8 -*-
"""57.63.0 — etiquetas de material liberado, rechazado y detenido desde el
lote y desde la NC, con el pie del formato controlado de cada una."""
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestEtiquetasLote(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        for xmlid, code in (('format_ref_lot_released', 'F-P-C04-02'),
                            ('format_ref_lot_rejected', 'F-P-C04-03'),
                            ('format_ref_lot_held', 'F-P-C04-04')):
            cls.env.ref('quimibond_sgi.%s' % xmlid).write({'sgi_code': code, 'active': True})
        cls.product = cls.env['product.product'].create({
            'name': 'ZE Entretela etiqueta', 'type': 'consu', 'is_storable': True,
            'tracking': 'lot', 'default_code': 'ZE-001'})
        cls.lot = cls.env['stock.lot'].create({'name': 'ZE-LOTE-01', 'product_id': cls.product.id})
        team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.alert = cls.env['quality.alert'].create({
            'title': 'ZE NC etiqueta', 'team_id': team.id, 'product_id': cls.product.id,
            'lot_ids': [(6, 0, cls.lot.ids)]})
        cls.alert_no_lot = cls.env['quality.alert'].create({
            'title': 'ZE NC sin lote', 'team_id': team.id, 'product_id': cls.product.id})

    def _html(self, report_xmlid, records):
        report = self.env.ref('quimibond_sgi.%s' % report_xmlid)
        self.assertEqual(report.paperformat_id, self.env.ref('quimibond_sgi.paperformat_sgi_lot_label'))
        return self.env['ir.actions.report']._render_qweb_html(
            report.report_name, records.ids)[0].decode()

    def test_01_desde_el_lote(self):
        for xmlid, title, code in (
                ('action_report_lot_label_released', 'MATERIAL LIBERADO', 'F-P-C04-02'),
                ('action_report_lot_label_rejected', 'MATERIAL RECHAZADO', 'F-P-C04-03'),
                ('action_report_lot_label_held', 'MATERIAL DETENIDO', 'F-P-C04-04')):
            html = self._html(xmlid, self.lot)
            self.assertIn(title, html)
            self.assertIn('ZE-LOTE-01', html)
            self.assertIn('ZE-001', html)
            self.assertIn(code, html, "El pie lleva la clave del formato.")

    def test_02_desde_la_nc(self):
        html = self._html('action_report_alert_label_held', self.alert)
        self.assertIn('MATERIAL DETENIDO', html)
        self.assertIn('ZE-LOTE-01', html)
        self.assertIn(self.alert.name, html)
        self.assertIn('F-P-C04-04', html)
        html = self._html('action_report_alert_label_rejected', self.alert_no_lot)
        self.assertIn('Sin lote', html)
        self.assertIn('F-P-C04-03', html)

    def test_03_en_el_menu_imprimir(self):
        for xmlid, model in (('action_report_lot_label_released', 'stock.lot'),
                             ('action_report_alert_label_held', 'quality.alert')):
            report = self.env.ref('quimibond_sgi.%s' % xmlid)
            self.assertEqual(report.binding_model_id.model, model)
