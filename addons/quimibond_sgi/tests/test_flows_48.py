# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestFlows48(TransactionCase):
    """Mini-fase 4.8: mapa de entradas/salidas completo por proceso."""

    def _flow(self, xmlid):
        return self.env.ref('quimibond_sgi.%s' % xmlid)

    def test_01_flujos_nuevos_con_modelo(self):
        expected = {
            'flow_almacenmp_compras_req': 'approval.request',
            'flow_plan_prodent': 'mrp.production',
            'flow_prodtac_tintoreria': 'mrp.production',
            'flow_tintoreria_prodtac': 'mrp.production',
            'flow_prodent_inspeccion': 'mrp.production',
            'flow_almacenmp_lab': 'quality.check',
            'flow_lab_almacenmp': 'quality.check',
            'flow_prodtac_mto': 'maintenance.request',
            'flow_inspeccion_sgi_nc': 'quality.alert',
            'flow_ventas_cal_reclamo': 'helpdesk.ticket',
            'flow_sgi_compras_prov': 'sgi.supplier.eval',
            'flow_direccion_sgi_acuerdos': 'sgi.management.review',
            'flow_mfg_sgi_incidente': 'sgi.incident',
            'flow_diseno_mfg': 'mrp.bom',
        }
        for xmlid, model in expected.items():
            flow = self._flow(xmlid)
            self.assertEqual(flow.odoo_model_name, model,
                             "El flujo %s debe materializarse en %s" % (xmlid, model))

    def test_02_ver_registros_abre_el_modelo(self):
        action = self._flow('flow_prodtac_mto').action_view_records()
        self.assertEqual(action['res_model'], 'maintenance.request')

    def test_03_todo_proceso_operativo_conectado(self):
        """Ningún proceso hoja del mapa queda aislado (sin entradas ni salidas)."""
        Process = self.env['sgi.process']
        leaves = Process.search([('child_ids', '=', False)])
        for proc in leaves:
            self.assertTrue(
                proc.in_flow_ids or proc.out_flow_ids,
                "El proceso %s quedó aislado en el mapa (sin flujos)" % proc.name)

    def test_04_tintoreria_encadenada(self):
        """El caso que faltaba: tintorería con entrada (tejido) y salida (acabado)."""
        tint = self.env.ref('quimibond_sgi.proc_tintoreria')
        self.assertTrue(tint.in_flow_ids)
        self.assertTrue(tint.out_flow_ids)
