# -*- coding: utf-8 -*-
"""57.112.0: «Por qué se mide a mano» quita el aviso «Se hace en Odoo, se mide a mano»."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestMedicionManualMotivo(TransactionCase):

    def test_01_el_motivo_quita_el_aviso(self):
        process = self.env['sgi.process'].create({'code': 'ZMM', 'name': 'Proceso manual'})
        activity = self.env['sgi.process.activity'].create({
            'process_id': process.id, 'number': 'ZMM.01',
            'name': 'Revisar cada mes la balanza', 'exec_channel': 'odoo',
            'measure_method': 'manual'})
        codes = [code for code, _msg in activity._sgi_spec_problems()]
        self.assertIn('odoo_measured_manual', codes)
        activity.manual_reason = "Se revisa la balanza; la decisión queda en el registro de cumplimiento."
        codes = [code for code, _msg in activity._sgi_spec_problems()]
        self.assertNotIn('odoo_measured_manual', codes)
        self.assertNotIn('odoo_measured_manual', activity.spec_gap_ids.mapped('code'),
                         "Guardar el motivo recalcula los faltantes.")
