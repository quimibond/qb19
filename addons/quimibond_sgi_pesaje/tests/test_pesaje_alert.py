# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestPesajeAlert(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.product = cls.env['product.product'].create({
            'name': 'Tela pesaje test', 'type': 'consu', 'tracking': 'lot'})
        cls.wc = cls.env['mrp.workcenter'].create({'name': 'Circular pesaje test'})
        cls.mo = cls.env['mrp.production'].create({
            'product_id': cls.product.id, 'product_qty': 100.0})
        cls.wo = cls.env['mrp.workorder'].create({
            'name': 'TEJIDO test', 'production_id': cls.mo.id, 'workcenter_id': cls.wc.id})
        # Estándar: 40 kg ± 3 kg
        cls.env['mrp.rollo.estandar'].create({
            'product_id': cls.product.id, 'rollo_teorico': 40.0})

    def _wizard(self, weight):
        return self.env['mrp.weigh.roll.wizard'].create({
            'workorder_id': self.wo.id, 'employee_number': '1', 'weight': weight})

    def test_01_out_of_tolerance_detected(self):
        self.assertTrue(self._wizard(50.0)._sgi_weight_out_of_tolerance())
        self.assertFalse(self._wizard(41.0)._sgi_weight_out_of_tolerance())

    def test_02_alert_created_once_per_roll(self):
        wizard = self._wizard(50.0)
        wizard._sgi_create_weight_alert()
        wizard._sgi_create_weight_alert()  # idempotente: mismo rollo, no duplica
        alerts = self.env['quality.alert'].search([('production_id', '=', self.mo.id)])
        self.assertEqual(len(alerts), 1, "Debe crearse una sola alerta por rollo.")
        self.assertEqual(alerts.product_id, self.product)
        self.assertEqual(alerts.production_id, self.mo)

    def test_03_alert_stamped_with_its_source(self):
        self._wizard(50.0)._sgi_create_weight_alert()
        alert = self.env['quality.alert'].search([('production_id', '=', self.mo.id)])
        self.assertEqual(
            alert.sgi_source_id,
            self.env.ref('quimibond_sgi_pesaje.sgi_alert_source_pesaje'),
            "La NC debe quedar estampada con la fuente de pesaje.")

    def test_04_source_disabled_stops_the_nc(self):
        """Apagar la fuente deja de generar NC sin desinstalar el módulo."""
        source = self.env.ref('quimibond_sgi_pesaje.sgi_alert_source_pesaje')
        source.enabled = False
        wizard = self._wizard(50.0)
        # La condición de fuera de tolerancia se sigue detectando: lo que se
        # apaga es el expediente de NC, no el control de piso.
        self.assertTrue(wizard._sgi_weight_out_of_tolerance())
        self.assertFalse(wizard._sgi_create_weight_alert())
        self.assertFalse(
            self.env['quality.alert'].search([('production_id', '=', self.mo.id)]),
            "Con la fuente apagada no debe crearse NC por peso.")
        self.assertEqual(source.suppressed_count, 1)
