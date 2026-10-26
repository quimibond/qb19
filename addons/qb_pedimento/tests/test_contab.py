# -*- coding: utf-8 -*-
from datetime import date

from odoo import Command
from odoo.tests import tagged

from .common import PedimentoCommon


@tagged('post_install', '-at_install')
class TestContab(PedimentoCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        company = cls.env.company
        Product = cls.env['product.product']
        company.qb_pedimento_product_igi_id = Product.create({'name': 'IGI', 'type': 'service'})
        company.qb_pedimento_product_dta_id = Product.create({'name': 'DTA', 'type': 'service'})
        company.qb_pedimento_product_prv_id = Product.create({'name': 'PRV', 'type': 'service'})
        cls.iva_importacion = cls.env['account.tax'].create({
            'name': 'IVA(16%) Importacion', 'amount': 16, 'amount_type': 'percent', 'type_tax_use': 'purchase'})

    def _factura_impuestos(self, igi, dta, iva):
        """Como la captura de contabilidad: IGI y DTA sin impuesto, y el IVA
        de importación como impuesto sobre el valor aduanal, que se anula con
        una línea igual en negativo sin impuesto."""
        company = self.env.company
        base = iva / 0.16
        bill = self.env['account.move'].create({
            'move_type': 'in_invoice', 'partner_id': self.proveedor.id, 'invoice_date': '2026-05-28',
            'invoice_line_ids': [
                Command.create({'product_id': company.qb_pedimento_product_igi_id.id, 'price_unit': igi,
                                'tax_ids': []}),
                Command.create({'product_id': company.qb_pedimento_product_dta_id.id, 'price_unit': dta,
                                'tax_ids': []}),
                Command.create({'name': 'Valor aduanal', 'price_unit': base,
                                'tax_ids': [Command.set(self.iva_importacion.ids)]}),
                Command.create({'name': 'Valor aduanal', 'price_unit': -base, 'tax_ids': []}),
            ]})
        bill.action_post()
        return bill

    def _avisos(self, ped):
        return ped.activity_ids.filtered(lambda a: a.summary and a.summary.startswith('[Pedimento]'))

    def test_sin_factura_luego_diferencia_luego_cuadra(self):
        ped = self._pedimento(numero='26 16 3774 6000675', state='despachado', fecha_pago=date(2026, 5, 28),
                              igi=265645, dta=12136, iva=287175, iva_prv=53)
        self.assertEqual(ped.contab_estado, 'sin_poliza')
        self.Pedimento._cron_avisos_contab(hoy=date(2026, 6, 1))  # 2 días hábiles: todavía no
        self.assertFalse(self._avisos(ped))
        self.Pedimento._cron_avisos_contab(hoy=date(2026, 6, 10))
        self.assertEqual(self._avisos(ped).summary, '[Pedimento] Sin factura de impuestos')

        # La factura de impuestos con el IVA sin el IVA/PRV: no cuadra
        bill = self._factura_impuestos(265645, 12136, 287175)
        ped.gasto_move_ids = [Command.link(bill.id)]
        self.assertEqual(ped.contab_igi, 265645)
        self.assertEqual(ped.contab_estado, 'diferencia')
        self.assertIn('IVA', ped.contab_detalle)
        self.Pedimento._cron_avisos_contab(hoy=date(2026, 6, 10))
        self.assertEqual(self._avisos(ped).summary, '[Pedimento] No cuadra con contabilidad')

        # Se corrige el expediente: cuadra y el aviso se cierra
        ped.iva_prv = 0
        self.assertEqual(ped.contab_estado, 'cuadra')
        self.Pedimento._cron_avisos_contab(hoy=date(2026, 6, 10))
        self.assertFalse(self._avisos(ped))

    def test_exportacion_no_aplica(self):
        ped = self._pedimento(operacion='exportacion', clave_id=self.clave_a1.id, numero='26 16 3774 6000676',
                              state='despachado', fecha_pago=date(2026, 5, 28), igi=10)
        self.assertFalse(ped.contab_estado)
