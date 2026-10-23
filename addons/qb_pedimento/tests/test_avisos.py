# -*- coding: utf-8 -*-
from datetime import date, datetime

from odoo.tests import tagged

from .common import PedimentoCommon


@tagged('post_install', '-at_install')
class TestAvisos(PedimentoCommon):

    def _avisos(self, record):
        return record.activity_ids.filtered(lambda a: a.summary and a.summary.startswith('[Pedimento]'))

    def test_despachado_sin_cruce(self):
        ped = self._pedimento(numero='26 16 3774 6000206', state='despachado',
                              fecha_despacho=datetime(2026, 10, 19, 15))
        self.Pedimento._cron_avisos(hoy=date(2026, 10, 21))  # 2 días hábiles: todavía no
        self.assertFalse(self._avisos(ped))
        self.Pedimento._cron_avisos(hoy=date(2026, 10, 22))
        self.assertEqual(len(self._avisos(ped)), 1)
        self.assertEqual(self._avisos(ped).user_id, self.logistica)
        self.Pedimento._cron_avisos(hoy=date(2026, 10, 23))  # no se repite
        self.assertEqual(len(self._avisos(ped)), 1)

    def test_entregado_sin_cerrar_y_cierre_lo_apaga(self):
        ped = self._pedimento(numero='26 16 3774 6000206', state='entregado',
                              fecha_entrega=datetime(2026, 10, 15, 15))
        self.Pedimento._cron_avisos(hoy=date(2026, 10, 22))
        aviso = self._avisos(ped)
        self.assertEqual(len(aviso), 1)
        self.assertIn('faltan documentos', aviso.note)
        self._completar_documentos(ped)
        ped.cuenta_gastos_total = 1000
        ped.action_cerrar()
        self.assertFalse(self._avisos(ped))

    def test_garantia_sin_devolver(self):
        self.env.company.qb_pedimento_responsable_id = self.logistica
        garantia = self.env['qb.pedimento.garantia'].create({
            'name': 'Garantía naviera NYKU0807898', 'partner_id': self.agencia.id,
            'importe': 1000, 'currency_id': self.env.ref('base.USD').id, 'fecha_pago': '2026-07-01'})
        self.assertEqual(garantia.state, 'pendiente')
        self.Pedimento._cron_avisos(hoy=date(2026, 8, 15))  # 45 días
        self.assertFalse(self._avisos(garantia))
        self.Pedimento._cron_avisos(hoy=date(2026, 9, 1))
        self.assertEqual(len(self._avisos(garantia)), 1)
        garantia.write({'devuelto': 400})
        self.assertEqual(garantia.state, 'parcial')
        self.assertAlmostEqual(garantia.pendiente, 600)
        garantia.write({'devuelto': 1000, 'fecha_devolucion': '2026-09-10'})
        self.assertEqual(garantia.state, 'devuelta')
        self.assertFalse(self._avisos(garantia))

    def test_garantia_perdida(self):
        garantia = self.env['qb.pedimento.garantia'].create({
            'name': 'Depósito', 'tipo': 'deposito', 'partner_id': self.agencia.id, 'importe': 5000})
        garantia.perdida = True
        self.assertEqual(garantia.state, 'perdida')
        self.assertEqual(garantia.pendiente, 0)

    def test_garantia_toma_responsable_del_pedimento(self):
        ped = self._pedimento()
        garantia = self.env['qb.pedimento.garantia'].create({
            'name': 'Garantía', 'pedimento_id': ped.id, 'partner_id': self.agencia.id, 'importe': 10})
        self.assertEqual(garantia.user_id, self.logistica)
        self.assertEqual(ped.garantia_pendiente, 10)
