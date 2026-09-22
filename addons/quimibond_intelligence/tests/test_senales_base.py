# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged

from ..models.senales import base


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesBase(TransactionCase):

    def test_registro_tiene_las_senales_de_odoo(self):
        # Todas las señales fuente=odoo del catálogo (spec §4) menos delegacion_estado (plan B).
        esperadas = {
            'entrega_vencida', 'pedido_sin_fecha', 'lead_frio', 'venta_margen_negativo', 'producto_pierde', 'cliente_pierde', 'cotizacion_bajo_costo',
            'op_atrasada', 'op_sin_componentes', 'tiempos_excepcion', 'existencia_negativa', 'reorden_pendiente', 'transferencia_atorada', 'familia_saturada', 'mantenimiento_abierto',
            'recepcion_vencida', 'oc_sin_confirmacion', 'aprobacion_pendiente', 'precio_compra_subio', 'actividad_vencida_oc', 'proveedor_reprobado',
            'cartera_vencida', 'cxp_vencida', 'factura_proveedor_borrador', 'entregado_sin_facturar', 'banco_sin_conciliar', 'cfdi_cancelacion_pendiente',
            'sat_discrepancia', 'sat_complemento', 'sat_extraccion_detenida', 'nomina_borrador', 'cash_bajo_piso', 'indicador_financiero_rojo',
            'indicador_rojo', 'accion_correctiva_vencida', 'nc_abierta', 'calibracion_vencida', 'legal_incumplido', 'riesgo_sin_tratar', 'ppap_rechazado', 'auditoria_pendiente', 'fuente_sgi_apagada',
            'aprobacion_rh', 'evaluacion_vencida', 'ticket_abierto',
            'carga_actividades', 'firma_pendiente', 'acuse_documento', 'acuerdo_direccion_vencido', 'obligacion_legado',
        }
        self.assertEqual(esperadas - set(base.REGISTRO), set(), 'señales del catálogo sin consulta')

    def test_helpers(self):
        p = self.env['res.partner'].create({'name': 'Cliente Prueba', 'vat': 'XAXX010101000'})
        d = base.doc(p, monto=10.5)
        self.assertEqual(d, {'modelo': 'res.partner', 'id': p.id, 'nombre': 'Cliente Prueba', 'monto': 10.5})
        f = base.fila('x:1', [d], valor=3, partner=p, payload={'a': 1})
        self.assertEqual(f['odoo_partner_id'], p.commercial_partner_id.id)
        self.assertEqual(f['payload'], {'a': 1, 'rfc': 'XAXX010101000'})
        self.assertIsNone(f['responsable_odoo_user_id'])
        self.assertTrue(base.tiene_modelo(self.env, 'res.partner'))
        self.assertFalse(base.tiene_modelo(self.env, 'no.existe'))
        self.assertEqual(base.umbral({'umbrales': {'dias': '9'}}, 'dias', 3), 9)
        self.assertEqual(base.umbral({'umbrales': {}}, 'dias', 3), 3)
        self.assertEqual(base.umbral({}, 'dias', 3), 3)
