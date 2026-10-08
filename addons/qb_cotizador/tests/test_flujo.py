# -*- coding: utf-8 -*-
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import tagged

from ..models.settings import PARAM_ARCHIVAR_DIAS, PARAM_ESCALERA_PCT, PARAM_MARGEN_MINIMO, PARAM_SEGUIMIENTO_DIAS
from .common import CotizadorCase


@tagged('post_install', '-at_install')
class TestFlujo(CotizadorCase):

    def test_01_aprobacion_por_puesto_y_pdf(self):
        cot = self._cot()
        self.assertTrue(cot.folio.startswith('COT-'))
        cot.action_calcular()
        self.assertAlmostEqual(cot.piso_ocioso, 7.0 / 0.9, places=4)
        self.assertAlmostEqual(cot.piso_lleno, (9.0 / 0.9) / 0.9, places=4)
        self.assertEqual(cot.semaforo, 'verde')
        self.assertEqual(cot.calidad, 'alta')
        self.assertEqual(cot.precio_mercado, 14.0)
        with self.assertRaises(UserError, msg='Sin aprobación no se imprime el PDF comercial'):
            cot.action_print_cliente()
        cot.action_enviar_aprobacion()
        self.assertEqual(cot.state, 'por_aprobar')
        self.assertTrue(cot.activity_ids.filtered(lambda a: a.user_id == self.finanzas))
        with self.assertRaises(UserError, msg='El vendedor no tiene el puesto que aprueba'):
            cot.with_user(self.vendedor).action_aprobar()
        self.assertFalse(cot.with_user(self.vendedor).puede_aprobar)
        self.assertTrue(cot.with_user(self.finanzas).puede_aprobar)
        cot.with_user(self.finanzas).action_aprobar()
        self.assertEqual(cot.state, 'presentada')
        self.assertEqual(cot.approved_by_id, self.finanzas)
        hoy = fields.Date.context_today(cot)
        self.assertEqual(cot.validez_hasta, hoy + timedelta(days=15))
        self.assertTrue(cot.seguimiento_fecha and cot.seguimiento_fecha > hoy)
        self.assertEqual(cot.action_print_cliente()['type'], 'ir.actions.report')

    def test_02_regreso_con_motivo(self):
        cot = self._cot()
        cot.action_calcular()
        cot.action_enviar_aprobacion()
        motivo = self.env.ref('qb_cotizador.motivo_regreso_margen')
        wiz = self.env['qb.cotizador.decision.wizard'].with_user(self.finanzas).create({
            'cotizacion_id': cot.id, 'tipo': 'regreso', 'motivo_id': motivo.id, 'nota': 'sube 5 %'})
        wiz.action_confirmar()
        self.assertEqual(cot.state, 'borrador')
        self.assertEqual(cot.regreso_motivo_id, motivo)
        self.assertEqual(cot.regreso_count, 1)
        self.assertTrue(cot.activity_ids.filtered(lambda a: a.user_id == self.vendedor))
        with self.assertRaises(UserError, msg='Un motivo de pérdida no sirve para regresar'):
            self.env['qb.cotizador.decision.wizard'].create({
                'cotizacion_id': cot.id, 'tipo': 'regreso',
                'motivo_id': self.env.ref('qb_cotizador.motivo_perdida_precio').id}).action_confirmar()

    def test_03_rojo_solo_ceo(self):
        cot = self._cot(precio_objetivo=5.0)
        cot.action_calcular()
        self.assertEqual(cot.semaforo, 'rojo')
        cot.action_enviar_aprobacion()
        with self.assertRaises(UserError):
            cot.with_user(self.finanzas).action_aprobar()
        cot.with_user(self.ceo).action_aprobar()
        self.assertEqual(cot.state, 'presentada')

    def test_04_vencimiento_seguimiento_renovar_perder(self):
        cot = self._presentada()
        hoy = fields.Date.context_today(cot)
        cot.write({'validez_hasta': hoy - timedelta(days=1)})
        cot.sudo().write({'seguimiento_fecha': hoy - timedelta(days=1)})
        self.env['qb.cotizador.cotizacion']._cron_diario()
        self.assertEqual(cot.state, 'vencida')
        self.assertTrue(cot.activity_ids.filtered(lambda a: a.user_id == self.ventas),
                        'La actividad de decidir va al puesto de Ventas')
        nueva = self.env['qb.cotizador.cotizacion'].browse(cot.action_renovar()['res_id'])
        self.assertEqual(nueva.state, 'borrador')
        self.assertEqual(nueva.revision, 2)
        self.assertEqual(nueva.revision_anterior_id, cot)
        self.assertEqual(cot.state, 'reemplazada')
        self.assertNotEqual(nueva.folio, cot.folio)
        otra = self._presentada()
        self.env['qb.cotizador.decision.wizard'].create({
            'cotizacion_id': otra.id, 'tipo': 'perdida',
            'motivo_id': self.env.ref('qb_cotizador.motivo_perdida_precio').id}).action_confirmar()
        self.assertEqual(otra.state, 'perdida')
        self.assertEqual(otra.perdida_motivo_id.name, 'Precio')

    def test_05_seguimiento_cron(self):
        cot = self._presentada()
        hoy = fields.Date.context_today(cot)
        cot.sudo().write({'seguimiento_fecha': hoy})
        self.env['qb.cotizador.cotizacion']._cron_diario()
        self.assertTrue(cot.seguimiento_hecho)
        self.assertEqual(cot.state, 'presentada')
        self.assertTrue(cot.activity_ids.filtered(lambda a: a.user_id == self.ventas and 'Seguimiento' in a.summary))

    def test_06_ganada_pone_precio_en_tarifa(self):
        cot = self._presentada()
        cot.write({'cliente_aprobo': True, 'cliente_medio': 'oc', 'cliente_fecha': fields.Date.today()})
        self.assertFalse(cot.pricelist_item_id, 'Sin ganar no hay precio en tarifa')
        cot.action_ganar()
        self.assertEqual(cot.state, 'ganada')
        item = cot.pricelist_item_id
        self.assertTrue(item)
        self.assertEqual(item.product_tmpl_id, self.tela.product_tmpl_id)
        self.assertEqual(item.fixed_price, 14.0)
        self.assertEqual(item.pricelist_id.currency_id, cot.currency_id)
        self.assertTrue(item.pricelist_id.name.startswith('TARIFA '))
        self.assertEqual(self.partner.property_product_pricelist, item.pricelist_id)
        self.assertEqual(item.date_end.date(), cot.validez_hasta)
        self.assertTrue(cot.tarifa_fecha, '1.2.0: queda cuándo se puso el precio (medición de C1.17)')
        self.assertEqual(cot.tarifa_user_id, self.vendedor, 'Quien ganó la cotización (su usuario) puso el precio')
        with self.assertRaises(UserError, msg='La aprobación del cliente lleva medio y fecha'):
            self._presentada().write({'cliente_aprobo': True})

    def test_07_ganada_automatica_por_pedido(self):
        cot = self._presentada()
        order = self.env['sale.order'].create({
            'partner_id': self.partner.id,
            'order_line': [(0, 0, {'product_id': self.tela.id, 'product_uom_qty': 10, 'price_unit': 14})]})
        order.action_confirm()
        self.assertEqual(cot.state, 'ganada')
        self.assertEqual(cot.sale_order_id, order)

    def test_08_borradores_viejos_se_archivan(self):
        self.env['ir.config_parameter'].sudo().set_param(PARAM_ARCHIVAR_DIAS, '30')  # 1.1.0: vacío = apagado
        cot = self._cot()
        self.env.cr.execute('UPDATE qb_cotizador_cotizacion SET write_date = %s WHERE id = %s',
                            (fields.Datetime.now() - timedelta(days=40), cot.id))
        cot.invalidate_recordset(['write_date'])
        self.env['qb.cotizador.cotizacion']._cron_diario()
        self.assertFalse(cot.active)

    def test_09_parametros_vacios_sin_efecto(self):
        cot = self._cot(precio_objetivo=11.5)
        cot.action_calcular()
        self.assertFalse(cot.bajo_margen_minimo, 'Sin margen mínimo no hay aviso')
        self.assertFalse(cot.tramo_ids, 'Sin descuento no hay escalera')
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_MARGEN_MINIMO, '10')
        Param.set_param(PARAM_ESCALERA_PCT, '3')
        cot.invalidate_recordset()
        cot.action_calcular()
        self.assertTrue(cot.bajo_margen_minimo)
        self.assertEqual(len(cot.tramo_ids), 4)
        contribs = [t.contrib_total_mes for t in cot.tramo_ids.sorted('volumen')]
        self.assertEqual(contribs, sorted(contribs), 'La contribución mensual no baja al crecer el tramo')
        self.assertTrue(all(t.precio_mxn >= cot.piso_lleno - 1e-6 for t in cot.tramo_ids))
        self.assertTrue(cot.tramo_ids.filtered('es_base'))

    def test_10_validaciones_antes_de_aprobar(self):
        cot = self._cot(volumen=0)
        with self.assertRaises(UserError):
            cot.action_enviar_aprobacion()
        cot.write({'volumen': 100})
        with self.assertRaises(UserError, msg='Sin cálculo del costo no se pide aprobación'):
            cot.action_enviar_aprobacion()

    def test_10_sin_parametros_no_hay_seguimiento_ni_archivo(self):
        """1.1.0 (Jose 2026-10-08, punto 2): seguimiento y archivo de borradores vacíos = apagados."""
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_SEGUIMIENTO_DIAS, False)
        Param.set_param(PARAM_ARCHIVAR_DIAS, False)
        cot = self._presentada()
        self.assertFalse(cot.seguimiento_fecha, 'Sin días no hay fecha de seguimiento')
        self.assertTrue(cot.seguimiento_hecho, 'Y el cron no lo busca')
        borrador = self._cot()
        self.env.cr.execute('UPDATE qb_cotizador_cotizacion SET write_date = %s WHERE id = %s',
                            (fields.Datetime.now() - timedelta(days=400), borrador.id))
        borrador.invalidate_recordset(['write_date'])
        self.env['qb.cotizador.cotizacion']._cron_diario()
        self.assertTrue(borrador.active, 'Sin días no se archivan borradores')
        self.assertFalse(cot.activity_ids.filtered(lambda a: 'Seguimiento' in (a.summary or '')))

    def test_11_calculadora_guarda_en_el_cotizador_nuevo(self):
        """1.1.0 (Jose 2026-10-08, punto 3): la calculadora viva entra aquí como borrador."""
        d = {'name': 'CALC-1', 'partner_id': self.partner.id, 'product_id': self.tela.id,
             'spec_galga': '18', 'volumen': 1000.0, 'uom_name': 'm', 'currency_id': False, 'fx_rate': 1.0,
             'mp_unit': 6.0, 'energia_unit': 1.0, 'fab_unit': 2.0, 'conv_unit': 0.5, 'conv_var_unit': 0.2,
             'rendimiento': 0.9, 'op_pct': 10.0, 'costo_variable': 7.0, 'costo_absorbido_sin_op': 8.0,
             'precio_objetivo': 15.0, 'precio_mercado': 14.0, 'supuestos': 'prueba',
             'tramo_ids': [{'multiplo': 1.0, 'volumen': 1000.0, 'es_base': True, 'precio_mxn': 15.0,
                            'precio_divisa': 15.0, 'margen_neto_pct': 0.2, 'contrib_total_mes': 8000.0,
                            'semaforo': 'verde'}]}
        cot = self.env['qb.cotizador.cotizacion'].with_user(self.vendedor).crear_desde_calculadora(d)
        self.assertEqual(cot.state, 'borrador', 'Entra sin aprobar: la aprueba el puesto')
        self.assertTrue(cot.folio)
        self.assertEqual(cot.costo_fuente, 'legado')
        self.assertEqual(cot.spec_galga, '18')
        self.assertEqual(cot.currency_id, self.company.currency_id)
        self.assertAlmostEqual(cot.energia_unit, 1.2)
        self.assertAlmostEqual(cot.fabricacion_unit, 3.5)
        self.assertAlmostEqual(cot.costo_variable, 6.3)
        self.assertAlmostEqual(cot.op_pct, 0.10)
        self.assertAlmostEqual(cot.precio_objetivo, 15.0)
        self.assertEqual(len(cot.tramo_ids), 1)
        self.assertEqual(cot.user_id, self.vendedor)
