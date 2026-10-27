# -*- coding: utf-8 -*-
"""Estado del CFDI consultado directo al SAT (1.27.0): Syntage no vuelve a
traer un CFDI que se cancela después de emitido."""
from unittest.mock import patch

from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_A = 'eeeeeeee-0000-4000-8000-000000000001'
UUID_B = 'eeeeeeee-0000-4000-8000-000000000002'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatEstado(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def test_cancelado_en_el_sat_y_syntage_atrasado(self):
        bill = self._invoice(self.proveedor, 4640.0)
        cfdi = self._upsert(syntage_invoice(UUID_A, total=4640.0))
        cfdi.write({'move_id': bill.id})
        bill.button_cancel()
        otro = self._upsert(syntage_invoice(UUID_B, id='b', total=100.0))
        Cfdi = type(self.env['sat.cfdi'])
        respuestas = {UUID_A.upper(): 'cancelado', UUID_B.upper(): 'vigente'}
        with patch.object(Cfdi, '_sat_query', lambda self, e, r, t, u: respuestas[u.upper()]):
            # La que Odoo tiene cancelada va primero
            self.assertEqual(self.env['sat.cfdi']._verify_candidates(1), cfdi)
            self.env['sat.cfdi']._cron_verify_sat_status(limit=10)
        self.assertEqual(cfdi.estado_sat, 'cancelado')
        self.assertEqual(cfdi.sat_estado_verificado, 'cancelado')
        self.assertTrue(cfdi.fecha_cancelacion)
        self.assertEqual(otro.estado_sat, 'vigente')
        self.assertEqual(otro.sat_estado_verificado, 'vigente')
        # Una descarga atrasada de Syntage que todavía dice VIGENTE no lo regresa
        self._upsert(syntage_invoice(UUID_A, total=4640.0))
        self.assertEqual(cfdi.estado_sat, 'cancelado')

    def test_sin_respuesta_no_cambia_nada(self):
        cfdi = self._upsert(syntage_invoice(UUID_A, total=50.0))
        Cfdi = type(self.env['sat.cfdi'])
        with patch.object(Cfdi, '_sat_query', lambda self, e, r, t, u: None):
            action = cfdi.action_verify_sat_status()
        self.assertEqual(cfdi.estado_sat, 'vigente')
        self.assertFalse(cfdi.sat_verificado)
        self.assertIn('sin respuesta', action['params']['message'])
