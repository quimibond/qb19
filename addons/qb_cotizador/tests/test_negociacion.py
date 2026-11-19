# -*- coding: utf-8 -*-
"""1.8.0: negociaciones de precio. Caso de aceptación: el programa Blockbuster de ContiTech (Polycotton con
escalera incremental medida por trimestre y Crep 90 con rebaja de 1 %)."""
from datetime import date

from odoo import Command, fields
from odoo.exceptions import UserError, ValidationError
from odoo.tests import tagged

from odoo.addons.account.tests.common import AccountTestInvoicingCommon

from ..models.negociacion import descuento_tramos

TRAMOS_CONTI = [(0, 50000, 0.0), (50000, 75000, 10.0), (75000, 95000, 12.0), (95000, 0, 15.0)]


@tagged('post_install', '-at_install')
class TestNegociacion(AccountTestInvoicingCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        cls.moneda = cls.company.currency_id
        # Tarifas activas, como en producción (Ajustes → Ventas → Tarifas).
        cls.env.ref('base.group_user').sudo().write(
            {'implied_ids': [(4, cls.env.ref('product.group_product_pricelist').id)]})
        cls.env.user.group_ids = [(4, cls.env.ref('sales_team.group_sale_manager').id),
                                  (4, cls.env.ref('account.group_account_invoice').id)]
        cls.ceo = cls.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'CEO negociación', 'login': 'qbneg_ceo', 'company_id': cls.company.id,
            'company_ids': [(6, 0, cls.company.ids)],
            'group_ids': [(6, 0, [cls.env.ref('sales_team.group_sale_salesman').id,
                                  cls.env.ref('qb_cotizador.group_autoriza_bajo_piso').id])]})
        cls.cliente = cls.env['res.partner'].create({'name': 'CONTITECH PRUEBA', 'is_company': True, 'lang': 'en_US'})
        m = cls.env.ref('uom.product_uom_meter')
        cls.poly = cls.env['product.product'].create({
            'name': 'Polycotton', 'default_code': 'POLY001', 'type': 'consu', 'uom_id': m.id, 'list_price': 2.5})
        cls.crep = cls.env['product.product'].create({
            'name': 'Crep 90', 'default_code': 'CREP090', 'type': 'consu', 'uom_id': m.id, 'list_price': 1.5})
        cls.tarifa = cls.env['product.pricelist'].create({
            'name': 'TARIFA CONTITECH PRUEBA', 'currency_id': cls.moneda.id, 'company_id': cls.company.id})
        Item = cls.env['product.pricelist.item']
        cls.item_poly = Item.create({
            'pricelist_id': cls.tarifa.id, 'applied_on': '1_product', 'product_tmpl_id': cls.poly.product_tmpl_id.id,
            'compute_price': 'fixed', 'fixed_price': 1.88, 'date_start': fields.Datetime.to_datetime('2026-01-01')})
        cls.item_crep = Item.create({
            'pricelist_id': cls.tarifa.id, 'applied_on': '1_product', 'product_tmpl_id': cls.crep.product_tmpl_id.id,
            'compute_price': 'fixed', 'fixed_price': 1.08, 'date_start': fields.Datetime.to_datetime('2026-01-01')})
        cls.cliente.with_company(cls.company).property_product_pricelist = cls.tarifa
        periodo = cls.env['qb.periodo'].create({'period': date(2026, 9, 1), 'company_id': cls.company.id})
        periodo.write({'state': 'cerrado', 'op_pct': 0.10})
        Costo = cls.env['qb.costo.unitario']
        for prod, ocioso, lleno in ((cls.poly, 1.10, 1.45), (cls.crep, 0.70, 0.95)):
            Costo.create({'periodo_id': periodo.id, 'product_id': prod.id, 'company_id': cls.company.id,
                          'mp_unit': ocioso, 'costo_variable': ocioso, 'costo_produccion': lleno * 0.9,
                          'costo_vendible': lleno * 0.9, 'rendimiento': 1.0, 'op_pct': 0.10,
                          'piso_ocioso': ocioso, 'piso_lleno': lleno, 'calidad': 'alta'})

    # ------------------------------------------------------------------
    def _negociacion(self, **vals):
        base = {'partner_id': self.cliente.id, 'currency_id': self.moneda.id,
                'fecha_inicio': date(2027, 1, 1), 'fecha_fin': date(2027, 12, 31)}
        base.update(vals)
        return self.env['qb.cotizador.negociacion'].create(base)

    def _linea_poly(self, neg, **vals):
        base = {'negociacion_id': neg.id, 'product_id': self.poly.id, 'tipo': 'escalera_incremental',
                'precio_base': 1.88, 'periodo': 'trimestral', 'volumen_pronostico': 1055000,
                'tramo_ids': [Command.create({'desde': d, 'hasta': h, 'descuento_pct': p}) for d, h, p in TRAMOS_CONTI]}
        base.update(vals)
        return self.env['qb.cotizador.negociacion.linea'].create(base)

    def _factura(self, fecha, qty, product=None, vence=None):
        inv = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': self.cliente.id, 'invoice_date': fecha,
            'invoice_date_due': vence or fecha, 'currency_id': self.moneda.id,
            'invoice_line_ids': [Command.create({'product_id': (product or self.poly).id, 'quantity': qty,
                                                 'price_unit': 1.88, 'tax_ids': [Command.clear()]})]})
        inv.action_post()
        return inv

    def _acuerdo(self, **vals):
        base = {'partner_id': self.cliente.id, 'product_id': self.poly.id, 'currency_id': self.moneda.id,
                'company_id': self.company.id, 'fecha_inicio': date(2026, 1, 1), 'fecha_fin': date(2026, 12, 31),
                'precio_base': 1.88, 'periodo': 'trimestral',
                'tramo_ids': [Command.create({'desde': d, 'hasta': h, 'descuento_pct': p}) for d, h, p in TRAMOS_CONTI]}
        base.update(vals)
        return self.env['qb.cotizador.acuerdo.volumen'].create(base)

    # ------------------------------------------------------------------
    def test_01_escalera_contitech(self):
        """Los números del programa Blockbuster, en la función de tramos."""
        trimestre, detalle = descuento_tramos(263750, TRAMOS_CONTI, 1.88, meses=3)
        self.assertAlmostEqual(trimestre, 22842.0, places=2)
        self.assertEqual([round(d[0]) for d in detalle], [0, 150000, 225000, 285000],
                         'Por trimestre los umbrales son × 3: 150k / 225k / 285k')
        self.assertAlmostEqual(4 * trimestre, 91368.0, places=2)
        self.assertAlmostEqual(round(100 * 4 * trimestre / (1055000 * 1.88), 1), 4.6)
        self.assertAlmostEqual(4 * descuento_tramos(59000 * 3, TRAMOS_CONTI, 1.88, 3)[0], 20304.0, places=2)
        self.assertAlmostEqual(4 * descuento_tramos(95000 * 3, TRAMOS_CONTI, 1.88, 3)[0], 110544.0, places=2)
        # Medido por mes da lo mismo con el volumen parejo.
        self.assertAlmostEqual(12 * descuento_tramos(1055000 / 12, TRAMOS_CONTI, 1.88, 1)[0], 91368.0, places=2)

    def test_02_simulador_y_prellenado(self):
        """Precio vigente de la tarifa, costo del período y el simulador con el pronóstico de 1,055,000 m/año."""
        neg = self._negociacion(contra_rebaja_pct=5.0)
        linea = self._linea_poly(neg)
        self.assertAlmostEqual(linea.precio_actual, 1.88, places=4)
        self.assertIn('TARIFA CONTITECH PRUEBA', linea.precio_actual_fuente)
        self.assertAlmostEqual(linea.costo_completo, 1.45, places=4)
        self.assertAlmostEqual(linea.costo_variable, 1.10, places=4)
        self.assertEqual(linea.semaforo, 'verde', 'El último tramo (1.598) queda arriba del costo completo')
        neg.action_simular()
        prono = neg.simulacion_ids.filtered(lambda s: s.escenario == 'pronostico')
        self.assertAlmostEqual(prono.descuento, 91368.0, places=2)
        self.assertAlmostEqual(round(prono.pct_efectivo, 1), 4.6)
        self.assertAlmostEqual(prono.venta_neta, 1055000 * 1.88 - 91368.0, places=2)
        self.assertAlmostEqual(prono.contrib_completa, prono.venta_neta - 1055000 * 1.45, places=2)
        # Contrapropuesta: rebaja pareja de 5 % sobre el precio actual.
        self.assertAlmostEqual(prono.cliente_descuento, 1055000 * 1.88 * 0.05, places=2)
        self.assertAlmostEqual(prono.diferencia_cliente, 1055000 * 1.88 * 0.05 - 91368.0, places=2)
        self.assertEqual(set(neg.simulacion_ids.mapped('escenario')), {'actual', 'pronostico', 'menos', 'mas'})
        total = neg.simulacion_global_ids.filtered(lambda s: s.escenario == 'pronostico')
        self.assertAlmostEqual(total.descuento, 91368.0, places=2)
        self.assertAlmostEqual(neg.descuento_pronostico, 91368.0, places=2)
        # Un tramo debajo del costo completo pinta ámbar.
        linea.tramo_ids.filtered(lambda t: t.descuento_pct == 15.0).descuento_pct = 25.0
        self.assertEqual(linea.semaforo, 'ambar')
        self.assertEqual(neg.semaforo, 'ambar')

    def test_03_tramos_invalidos(self):
        neg = self._negociacion()
        with self.assertRaises(ValidationError):
            self._linea_poly(neg, tramo_ids=[Command.create({'desde': 0, 'hasta': 0, 'descuento_pct': 0}),
                                             Command.create({'desde': 50000, 'hasta': 0, 'descuento_pct': 10})])

    def test_04_ganar_pone_precios_en_tarifa(self):
        """Crep 90 de 1.080 con rebaja de 1 % ⇒ 1.0692; el precio anterior se cierra el 2026-12-31 y el nuevo
        empieza el 2027-01-01. La escalera deja su acuerdo de volumen."""
        neg = self._negociacion()
        crep = self.env['qb.cotizador.negociacion.linea'].create({
            'negociacion_id': neg.id, 'product_id': self.crep.id, 'tipo': 'rebaja_pct', 'rebaja_pct': 1.0})
        self.assertAlmostEqual(crep.precio_actual, 1.08, places=4)
        self.assertAlmostEqual(crep.precio_resultante, 1.0692, places=6)
        poly = self._linea_poly(neg, solo_pagadas_en_plazo=True)
        neg.action_enviar_aprobacion()
        self.assertEqual(neg.state, 'por_aprobar')
        with self.assertRaises(UserError, msg='Quien pidió la aprobación no aprueba'):
            neg.action_aprobar()
        neg.with_user(self.ceo).action_aprobar()
        self.assertEqual(neg.state, 'presentada')
        Report = self.env['ir.actions.report']
        carta = Report._render_qweb_html('qb_cotizador.report_negociacion_cliente', neg.ids)[0].decode()
        self.assertIn('PRICE PROPOSAL', carta, 'El contacto está en inglés')
        self.assertIn('1.0692', carta)
        interna = Report._render_qweb_html('qb_cotizador.report_negociacion_interna', neg.ids)[0].decode()
        self.assertIn('91,368.00', interna)
        neg.action_ganar()
        self.assertEqual(neg.state, 'ganada')
        self.assertFalse(crep.pricelist_item_id, 'Sin la aprobación del cliente no se toca la tarifa')
        neg.write({'cliente_aprobo': True, 'cliente_medio': 'correo', 'cliente_fecha': date(2026, 11, 19)})

        self.assertEqual(self.item_crep.date_end.date(), date(2026, 12, 31))
        nuevo = crep.pricelist_item_id
        self.assertEqual(nuevo.pricelist_id, self.tarifa)
        self.assertAlmostEqual(nuevo.fixed_price, 1.0692, places=6)
        self.assertEqual(nuevo.date_start.date(), date(2027, 1, 1))
        self.assertEqual(nuevo.date_end.date(), date(2027, 12, 31))
        self.assertTrue(self.item_crep.exists(), 'El precio anterior no se borra: queda el historial')
        self.assertEqual(crep.item_cerrado_ids, self.item_crep)

        self.assertAlmostEqual(poly.pricelist_item_id.fixed_price, 1.88, places=4)
        self.assertEqual(self.item_poly.date_end.date(), date(2026, 12, 31))
        acuerdo = poly.acuerdo_id
        self.assertTrue(acuerdo)
        self.assertEqual(acuerdo.negociacion_id, neg)
        self.assertEqual(acuerdo.periodo, 'trimestral')
        self.assertTrue(acuerdo.solo_pagadas_en_plazo)
        self.assertEqual(len(acuerdo.tramo_ids), 4)
        self.assertEqual(acuerdo.fecha_inicio, date(2027, 1, 1))
        # Volver a aplicar no duplica.
        neg._aplicar_en_tarifa()
        self.assertEqual(self.env['product.pricelist.item'].search_count(
            [('pricelist_id', '=', self.tarifa.id), ('product_tmpl_id', '=', self.crep.product_tmpl_id.id)]), 2)

    def test_05_liquidacion_trimestre(self):
        """Un trimestre con 263,750 m ⇒ US$22,842 en UNA nota de crédito en borrador que no se duplica."""
        acuerdo = self._acuerdo()
        facturas = self._factura('2026-01-20', 87917) | self._factura('2026-02-20', 87917) \
            | self._factura('2026-03-20', 87916)
        # Refacturación: una factura cancelada con su nota de crédito no suma.
        mala = self._factura('2026-03-25', 10000)
        mala._reverse_moves(cancel=True)
        self._factura('2026-03-25', 5000, product=self.crep)  # otro producto: no cuenta
        acuerdo.action_calcular_periodo()
        q1 = acuerdo.liquidacion_ids.filtered(lambda l: l.periodo_inicio == date(2026, 1, 1))
        self.assertEqual(q1.periodo_fin, date(2026, 3, 31))
        self.assertAlmostEqual(q1.unidades, 263750, places=2)
        self.assertAlmostEqual(q1.descuento, 22842.0, places=2)
        nota = q1.nota_id
        self.assertEqual(nota.move_type, 'out_refund')
        self.assertEqual(nota.state, 'draft', 'La nota de crédito nunca se publica aquí')
        self.assertAlmostEqual(nota.amount_untaxed, 22842.0, places=2)
        self.assertEqual(nota.qb_liquidacion_id, q1)
        self.assertTrue(facturas <= q1.factura_ids)
        for f in facturas:
            self.assertIn(f.name, nota.invoice_origin)
        # Q2 y Q3 cerrados sin ventas: liquidación en cero, sin nota.
        self.assertFalse(acuerdo.liquidacion_ids.filtered(lambda l: l.periodo_inicio != date(2026, 1, 1)).nota_id)

        # Otra vez: misma línea, misma nota, nada duplicado (y la nota no cuenta como venta del producto).
        acuerdo.action_calcular_periodo()
        self.env['qb.cotizador.acuerdo.volumen']._cron_liquidar()
        q1_otra = acuerdo.liquidacion_ids.filtered(lambda l: l.periodo_inicio == date(2026, 1, 1))
        self.assertEqual(q1_otra, q1)
        self.assertAlmostEqual(q1.unidades, 263750, places=2)
        self.assertEqual(q1.nota_id, nota)
        self.assertEqual(self.env['account.move'].search_count([('qb_liquidacion_id', '!=', False)]), 1)

        # Con la nota publicada ya no se recalcula.
        nota.action_post()
        self._factura('2026-03-30', 1000)
        acuerdo.action_calcular_periodo()
        self.assertAlmostEqual(q1.unidades, 263750, places=2)

    def test_06_solo_pagadas_en_plazo(self):
        acuerdo = self._acuerdo(solo_pagadas_en_plazo=True)
        pagada = self._factura('2026-04-10', 200000, vence=date(2026, 5, 10))
        tarde = self._factura('2026-05-10', 60000, vence=date(2026, 6, 9))
        self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=pagada.ids).create(
            {'payment_date': date(2026, 5, 1)})._create_payments()
        self.assertIn(pagada.payment_state, ('paid', 'in_payment'))
        self.assertEqual(tarde.payment_state, 'not_paid')
        acuerdo.action_calcular_periodo()
        q2 = acuerdo.liquidacion_ids.filtered(lambda l: l.periodo_inicio == date(2026, 4, 1))
        self.assertAlmostEqual(q2.unidades, 200000, places=2)
        self.assertAlmostEqual(q2.unidades_excluidas, 60000, places=2)
        self.assertEqual(q2.marcadas, 1)
        self.assertIn(tarde.name, q2.detalle_pagos)
        # 200,000 en el trimestre: (200k − 150k) × 1.88 × 10 %.
        self.assertAlmostEqual(q2.descuento, 9400.0, places=2)
        # «Solo marcar»: cuenta todo y deja la marca.
        acuerdo.tratamiento_no_pagadas = 'marcar'
        q2.action_recalcular()
        self.assertAlmostEqual(q2.unidades, 260000, places=2)
        self.assertEqual(q2.marcadas, 1)

    def test_07_regreso_y_perdida_con_motivo(self):
        neg = self._negociacion()
        self._linea_poly(neg)
        neg.action_enviar_aprobacion()
        accion = neg.with_user(self.ceo).action_regresar()
        wiz = self.env['qb.cotizador.decision.wizard'].with_user(self.ceo).with_context(**accion['context']).create(
            {'motivo_id': self.env.ref('qb_cotizador.motivo_regreso_margen').id})
        self.assertEqual(wiz.negociacion_id, neg)
        wiz.action_confirmar()
        self.assertEqual(neg.state, 'borrador')
        self.assertEqual(neg.regreso_count, 1)

    def test_08_divisa_con_lineas_en_el_mismo_create(self):
        """1.8.1: el TC se fija antes de crear las líneas que vienen en el mismo create; el costo (en pesos)
        llega convertido a la moneda de la negociación."""
        eur = self.env.ref('base.EUR')
        eur.active = True
        self.env['res.currency.rate'].create({'currency_id': eur.id, 'company_id': self.company.id,
                                              'name': date(2026, 1, 1), 'rate': 0.05})
        neg = self._negociacion(currency_id=eur.id, linea_ids=[Command.create({
            'product_id': self.crep.id, 'tipo': 'rebaja_pct', 'rebaja_pct': 1.0})])
        self.assertAlmostEqual(neg.fx_rate, 20.0, places=4)
        self.assertAlmostEqual(neg.linea_ids.costo_completo, 0.95 / 20.0, places=4)
        self.assertAlmostEqual(neg.linea_ids.costo_variable, 0.70 / 20.0, places=4)
