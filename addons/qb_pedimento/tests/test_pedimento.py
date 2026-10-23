# -*- coding: utf-8 -*-
from datetime import datetime

from odoo.exceptions import UserError, ValidationError
from odoo.tests import tagged
from odoo.tools import mute_logger

from .common import PedimentoCommon
from ..models.qb_pedimento import dias_habiles_entre, formato_nativo, normalizar_numero


@tagged('post_install', '-at_install')
class TestPedimento(PedimentoCommon):

    def test_numero_normalizado_y_desglose(self):
        """Las agencias y la localización escriben el número distinto; se
        guarda como Logística (un espacio) y de ahí salen aduana y patente."""
        self.assertEqual(normalizar_numero('26  16  3774  6000417'), '26 16 3774 6000417')
        self.assertEqual(normalizar_numero('26-16-3774-6000206'), '26 16 3774 6000206')
        self.assertEqual(formato_nativo('26 16 3774 6000417'), '26  16  3774  6000417')
        ped = self._pedimento(numero='261637746000206')
        self.assertEqual(ped.numero, '26 16 3774 6000206')
        self.assertEqual(ped.name, '26 16 3774 6000206')
        self.assertEqual(ped.aduana_id, self.env.ref('qb_pedimento.aduana_16'))
        self.assertEqual(ped.patente, '3774')

    def test_aduana_desconocida_se_da_de_alta(self):
        ped = self._pedimento(numero='26 39 3621 6011646', operacion='exportacion', clave_id=self.clave_v1.id)
        self.assertEqual(ped.aduana_id.code, '39')
        self.assertIn('por nombrar', ped.aduana_id.name)
        ped.numero = '26 16 3774 6000206'
        self.assertEqual(ped.aduana_id, self.env.ref('qb_pedimento.aduana_16'))
        ped.numero = False
        self.assertFalse(ped.aduana_id or ped.patente)

    def test_numero_invalido(self):
        with self.assertRaises(ValidationError):
            self._pedimento(numero='3774-6000206')
        with self.assertRaises(ValidationError):
            # El consecutivo de 2026 empieza con 6.
            self._pedimento(numero='26 16 3774 5000206')

    def test_rectificacion_admite_consecutivo_del_anio_anterior(self):
        original = self._pedimento(numero='25 16 3774 5000045')
        r1 = self._pedimento(numero='26 16 3774 5000046', clave_id=self.clave_r1.id, rectifica_id=original.id)
        self.assertEqual(original.rectificacion_ids, r1)

    def test_numero_unico_por_compania(self):
        self._pedimento(numero='26 16 3774 6000206')
        with self.assertRaises(Exception), self.cr.savepoint(), mute_logger('odoo.sql_db'):
            self._pedimento(numero='26 16 3774 6000206')

    def test_nombre_sin_numero(self):
        self.assertEqual(self._pedimento(referencia_agencia='83272').name, 'Ref. 83272')

    def test_checklist_por_operacion(self):
        imp = self._pedimento()
        nombres = imp.documento_ids.tipo_id.mapped('name')
        self.assertIn('COVE / VUCEM', nombres)
        self.assertIn('Carta 3.1.8', nombres)
        self.assertNotIn('Carta porte', nombres)
        exp = self._pedimento(operacion='exportacion', partner_id=self.cliente_virtual.id)
        self.assertIn('Carta porte', exp.documento_ids.tipo_id.mapped('name'))
        self.assertNotIn('Proforma autorizada (agente del cliente)', exp.documento_ids.tipo_id.mapped('name'))
        # Al pasar a V1 entra la proforma y salen los documentos del cruce físico (vacíos).
        exp.clave_id = self.clave_v1
        nombres = exp.documento_ids.tipo_id.mapped('name')
        self.assertIn('Proforma autorizada (agente del cliente)', nombres)
        self.assertNotIn('Carta porte', nombres)

    def test_cambiar_operacion_no_borra_documentos_con_archivo(self):
        ped = self._pedimento()
        cove = ped.documento_ids.filtered(lambda d: d.tipo_id == self.env.ref('qb_pedimento.doc_cove'))
        cove.attachment_ids = [(0, 0, {'name': 'cove.pdf', 'raw': b'x'})]
        ped.operacion = 'exportacion'
        self.assertIn(cove, ped.documento_ids)

    def test_etapas_y_cierre(self):
        ped = self._pedimento(referencia_agencia='83272')
        with self.assertRaises(UserError):
            ped.action_despachado()  # sin número
        ped.numero = '26 16 3774 6000206'
        ped.action_despachado()
        self.assertEqual(ped.state, 'despachado')
        self.assertTrue(ped.fecha_despacho and ped.fecha_pago)
        ped.action_entregado()
        with self.assertRaisesRegex(UserError, 'faltan documentos'):
            ped.action_cerrar()
        self._completar_documentos(ped)
        with self.assertRaisesRegex(UserError, 'cuenta de gastos'):
            ped.action_cerrar()
        ped.write({'cuenta_gastos_total': 543084.01, 'anticipo': 587000.0})
        ped.action_cerrar()
        self.assertEqual(ped.state, 'cerrado')
        self.assertTrue(ped.documentos_completos)

    def test_no_aplica_cumple_el_documento(self):
        ped = self._pedimento()
        ped.documento_ids.filtered('required').write({'no_aplica': True})
        self.assertEqual(ped.documentos_faltantes, 0)

    def test_saldo_como_en_el_excel(self):
        """F-P-A-16-05, cuenta 83272: total 543,084.01, anticipo 587,000 → -43,915.99 a favor."""
        ped = self._pedimento(cuenta_gastos_total=543084.01, anticipo=587000.0)
        self.assertAlmostEqual(ped.saldo, -43915.99, places=2)
        self.assertEqual(self._pedimento(anticipo=587000.0).saldo, 0.0)

    def test_contribuciones(self):
        """Pedimento 26 16 3774 6000206 (adjunto 271227): 344,760 en efectivo. Los 14,225 «otros» de la
        cuenta aduanera de garantía son una garantía aparte."""
        ped = self._pedimento(dta=5999, igi=187454, iva=150924, prv=330, iva_prv=53)
        self.assertAlmostEqual(ped.total_contribuciones, 344760)

    def test_igi_por_partida(self):
        ped = self._pedimento(partida_ids=[(0, 0, {'fraccion': '56039201', 'valor_aduana': 749822, 'tasa_igi': 25})])
        self.assertAlmostEqual(ped.partida_ids.igi, 187455.5)

    def test_dias_cierre(self):
        ped = self._pedimento(numero='26 16 3774 6000206', fecha_entrega=datetime(2026, 2, 19, 12))
        ped.fecha_cierre = '2026-02-26'
        self.assertEqual(ped.dias_cierre, 5)

    def test_dias_habiles(self):
        from datetime import date
        self.assertEqual(dias_habiles_entre(date(2026, 10, 23), date(2026, 10, 26)), 1)  # vie → lun
        self.assertEqual(dias_habiles_entre(date(2026, 10, 19), date(2026, 10, 23)), 4)
        self.assertEqual(dias_habiles_entre(None, date(2026, 10, 23)), 0)

    def test_usuario_logistica_puede_capturar(self):
        ped = self.Pedimento.with_user(self.logistica).create({
            'operacion': 'importacion', 'numero': '26 99 3774 6000001', 'partner_id': self.proveedor.id})
        self.assertEqual(ped.aduana_id.code, '99')
        self.assertTrue(ped.documento_ids)
