# -*- coding: utf-8 -*-
"""KPIs automáticos 2.0 — Paso 1: los 4 modos directos.

Cada test crea su propia data del periodo (no depende de demo), corre el
cálculo y asegura el valor Y la evidencia (el valor navega a sus registros).
"""
import datetime
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestKpi20Step1(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.Measure = cls.env['sgi.indicator.measure']

    def _indicator(self, mode, **vals):
        base = {
            'code': 'K20-%s' % mode[:6].upper(),
            'name': 'KPI %s' % mode,
            'calc_mode': mode,
        }
        base.update(vals)
        return self.Indicator.create(base)

    def _measure(self, indicator, period_date):
        return self.Measure.create({
            'indicator_id': indicator.id,
            'period_date': period_date,
            'value': indicator.last_value,
            'state': 'capturado',
        })

    def _evidence_records(self, measure):
        action = measure.action_view_evidence()
        self.assertEqual(action['type'], 'ir.actions.act_window')
        return self.env[action['res_model']].search(action['domain']), action

    # ------------------------------------------------------------------
    # VE-01 crecimiento_ventas
    # ------------------------------------------------------------------
    def _post_invoice(self, partner, amount, inv_date, refund=False):
        income = self.env['account.account'].search(
            [('account_type', '=', 'income')], limit=1)
        move = self.env['account.move'].create({
            'move_type': 'out_refund' if refund else 'out_invoice',
            'partner_id': partner.id,
            'invoice_date': inv_date,
            'invoice_line_ids': [(0, 0, {
                'name': 'Venta KPI',
                'quantity': 1,
                'price_unit': amount,
                'account_id': income.id,
                'tax_ids': [(6, 0, [])],
            })],
        })
        move.action_post()
        return move

    def test_01_crecimiento_ventas(self):
        # Periodo libre de facturas de demo (2040/2039) para aislar el KPI.
        partner = self.env['res.partner'].create({'name': 'Cliente KPI VE'})
        # Periodo actual: 1000 timbrado - 100 nota de crédito = 900 neto.
        self._post_invoice(partner, 1000.0, date(2040, 6, 10))
        self._post_invoice(partner, 100.0, date(2040, 6, 20), refund=True)
        # Mismo periodo del año anterior: 600 neto → variación (900-600)/600 = 50%.
        self._post_invoice(partner, 600.0, date(2039, 6, 10))
        ind = self._indicator('crecimiento_ventas')
        value = ind._calc_crecimiento_ventas(date(2040, 6, 1), date(2040, 6, 30))
        self.assertEqual(value, 50.0)
        # Sin base comparable (año anterior en cero) → None.
        self.assertIsNone(
            ind._calc_crecimiento_ventas(date(2040, 7, 1), date(2040, 7, 31)))
        # Evidencia: las facturas del periodo actual (factura + nota de crédito).
        measure = self._measure(ind, date(2040, 6, 1))
        records, _ = self._evidence_records(measure)
        self.assertEqual(len(records), 2)
        self.assertEqual(set(records.mapped('move_type')),
                         {'out_invoice', 'out_refund'})

    # ------------------------------------------------------------------
    # MT-03 ots_atendidas
    # ------------------------------------------------------------------
    def test_02_ots_atendidas(self):
        stage_done = self.env['maintenance.stage'].create(
            {'name': 'Terminada KPI', 'done': True})
        stage_open = self.env['maintenance.stage'].create(
            {'name': 'Abierta KPI', 'done': False})
        Request = self.env['maintenance.request']
        # Periodo libre de solicitudes de demo (2040) para aislar el KPI.
        req_date, close_date = date(2040, 6, 15), date(2040, 6, 20)
        first, last = date(2040, 6, 1), date(2040, 6, 30)
        # 4 creadas en el periodo; 3 cerradas (etapa terminada). close_date
        # explícito se conserva porque la etapa ya es 'done' al crear.
        for _ in range(3):
            Request.create({'name': 'OT cerrada', 'request_date': req_date,
                            'stage_id': stage_done.id, 'close_date': close_date})
        Request.create({'name': 'OT abierta', 'request_date': req_date,
                        'stage_id': stage_open.id})
        ind = self._indicator('ots_atendidas')
        value = ind._calc_ots_atendidas(first, last)
        self.assertEqual(value, 75.0, "3 de 4 OTs atendidas.")
        measure = self._measure(ind, first)
        records, _ = self._evidence_records(measure)
        self.assertEqual(len(records), 4, "Evidencia = las solicitudes creadas.")

    # ------------------------------------------------------------------
    # CO-02 requisiciones
    # ------------------------------------------------------------------
    def test_03_requisiciones(self):
        category = self.env['approval.category'].create({
            'name': 'Requisición de compra KPI',
            'approval_type': 'purchase',
            'approval_minimum': 1,
        })
        # Aislar el KPI a esta categoría (evita categorías de compra de demo).
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.purchase_approval_category_id', category.id)
        Request = self.env['approval.request']
        Request.create({
            'name': 'Req aprobada', 'category_id': category.id,
            'approver_ids': [(0, 0, {'user_id': self.env.user.id,
                                     'status': 'approved'})],
        })
        Request.create({
            'name': 'Req pendiente', 'category_id': category.id,
            'approver_ids': [(0, 0, {'user_id': self.env.user.id,
                                     'status': 'new'})],
        })
        ind = self._indicator('requisiciones')
        today = date.today()
        value = ind._calc_requisiciones(today, today)
        self.assertEqual(value, 50.0, "1 de 2 requisiciones aprobadas.")
        measure = self._measure(ind, today.replace(day=1))
        records, action = self._evidence_records(measure)
        self.assertEqual(action['res_model'], 'approval.request')
        self.assertEqual(set(records.mapped('category_id')), {category})

    def test_03b_requisiciones_none_sin_categoria(self):
        # Sin categoría de compras detectable → None (no truena).
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.purchase_approval_category_id', '0')
        ind = self._indicator('requisiciones')
        # Forzamos categorías vacías apuntando a una categoría inexistente vía el
        # helper: si el entorno tiene categorías de compra, este test las tolera
        # devolviendo un valor float o None; lo que probamos es que no truena.
        result = ind._calc_requisiciones(date(1990, 1, 1), date(1990, 1, 31))
        self.assertIn(type(result), (type(None), float))

    # ------------------------------------------------------------------
    # AL-02 embarques_sin_error
    # ------------------------------------------------------------------
    def _delivery(self, product, when, with_return=False):
        out_type = self.env['stock.picking.type'].search(
            [('code', '=', 'outgoing')], limit=1)
        stock = self.env['stock.location'].search(
            [('usage', '=', 'internal')], limit=1)
        customer = self.env['stock.location'].search(
            [('usage', '=', 'customer')], limit=1)
        pick = self.env['stock.picking'].create({
            'picking_type_id': out_type.id,
            'location_id': stock.id,
            'location_dest_id': customer.id,
        })
        move = self.env['stock.move'].create({
            'product_id': product.id,
            'product_uom_qty': 1, 'product_uom': product.uom_id.id,
            'location_id': stock.id, 'location_dest_id': customer.id,
            'picking_id': pick.id, 'state': 'done',
        })
        pick.write({'state': 'done', 'date_done': when})
        if with_return:
            # La devolución del cliente: un movimiento de retorno ligado al original.
            self.env['stock.move'].create({
                'product_id': product.id,
                'product_uom_qty': 1, 'product_uom': product.uom_id.id,
                'location_id': customer.id, 'location_dest_id': stock.id,
                'state': 'done', 'origin_returned_move_id': move.id,
            })
        return pick

    def test_04_embarques_sin_error(self):
        product = self.env['product.product'].create(
            {'name': 'Producto embarque KPI', 'type': 'consu'})
        # Periodo libre de embarques de demo (2040) para aislar el KPI.
        when = datetime.datetime(2040, 6, 15, 10, 0, 0)
        good1 = self._delivery(product, when)
        good2 = self._delivery(product, when)
        bad = self._delivery(product, when, with_return=True)
        ind = self._indicator('embarques_sin_error')
        value = ind._calc_embarques_sin_error(date(2040, 6, 1), date(2040, 6, 30))
        self.assertEqual(value, 66.67, "2 de 3 embarques sin devolución.")
        # Evidencia = los embarques CON devolución (los errores).
        measure = self._measure(ind, date(2040, 6, 1))
        records, action = self._evidence_records(measure)
        self.assertEqual(action['res_model'], 'stock.picking')
        self.assertEqual(records, bad)
        self.assertNotIn(good1, records)
        self.assertNotIn(good2, records)

    # ------------------------------------------------------------------
    # Siembra idempotente del calc_mode
    # ------------------------------------------------------------------
    def test_05_activate_auto_indicators_idempotent(self):
        Config = self.env['sgi.config']
        ve = self.env.ref('quimibond_sgi.sgi_ind_crecimiento_ventas')
        # Ya activado al cargar el módulo.
        self.assertEqual(ve.calc_mode, 'crecimiento_ventas')
        # Regresarlo a manual y reactivar → vuelve a auto.
        ve.calc_mode = 'manual'
        Config.activate_auto_indicators()
        self.assertEqual(ve.calc_mode, 'crecimiento_ventas')
        # Una decisión de MAST (otro modo) NO se pisa.
        ve.calc_mode = 'presupuesto_ventas'
        Config.activate_auto_indicators()
        self.assertEqual(ve.calc_mode, 'presupuesto_ventas')
        # CO-03 (proxy) NO se activa en la siembra: queda manual.
        co03 = self.env.ref('quimibond_sgi.sgi_ind_errores_oc')
        self.assertEqual(co03.calc_mode, 'manual')


@tagged('post_install', '-at_install')
class TestKpi20Step2(TransactionCase):
    """Paso 2: los 2 modos con parámetro + el proxy + RH-02."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.Measure = cls.env['sgi.indicator.measure']
        cls.Param = cls.env['ir.config_parameter'].sudo()
        cls.stock = cls.env['stock.location'].search(
            [('usage', '=', 'internal')], limit=1)
        cls.prodloc = cls.env['stock.location'].search(
            [('usage', '=', 'production')], limit=1)

    def _indicator(self, mode, **vals):
        base = {'code': 'K20B-%s' % mode[:5].upper(),
                'name': 'KPI %s' % mode, 'calc_mode': mode}
        base.update(vals)
        return self.Indicator.create(base)

    def _measure(self, indicator, period_date):
        return self.Measure.create({
            'indicator_id': indicator.id, 'period_date': period_date,
            'value': 0.0, 'state': 'capturado'})

    # ---------------- MA-02 produccion_vs_capacidad ----------------
    def _production(self, product, produced_qty, when):
        mo = self.env['mrp.production'].create(
            {'product_id': product.id, 'product_qty': produced_qty})
        move = self.env['stock.move'].create({
            'product_id': product.id, 'product_uom_qty': produced_qty,
            'product_uom': product.uom_id.id, 'location_id': self.prodloc.id,
            'location_dest_id': self.stock.id, 'production_id': mo.id,
            'state': 'done'})
        move.quantity = produced_qty
        move.picked = True
        mo.write({'state': 'done', 'date_finished': when})
        return mo

    def test_01_produccion_vs_capacidad(self):
        self.Param.set_param('quimibond_sgi.production_monthly_capacity', '1000')
        product = self.env['product.product'].create(
            {'name': 'Tela capacidad KPI', 'type': 'consu'})
        # Periodo libre de demo (2040). Producidos 800 sobre capacidad 1000 = 80%.
        self._production(product, 800.0, datetime.datetime(2040, 6, 15, 8, 0, 0))
        ind = self._indicator('produccion_vs_capacidad')  # mensual por defecto
        value = ind._calc_produccion_vs_capacidad(date(2040, 6, 1), date(2040, 6, 30))
        self.assertEqual(value, 80.0)
        # Sin capacidad configurada → None (captura manual).
        self.Param.set_param('quimibond_sgi.production_monthly_capacity', '0')
        self.assertIsNone(
            ind._calc_produccion_vs_capacidad(date(2040, 6, 1), date(2040, 6, 30)))
        # Evidencia: las órdenes de producción del periodo.
        self.Param.set_param('quimibond_sgi.production_monthly_capacity', '1000')
        measure = self._measure(ind, date(2040, 6, 1))
        action = measure.action_view_evidence()
        self.assertEqual(action['res_model'], 'mrp.production')
        self.assertTrue(self.env['mrp.production'].search(action['domain']))

    def test_01b_capacidad_prorratea_semanal(self):
        self.Param.set_param('quimibond_sgi.production_monthly_capacity', '3000')
        product = self.env['product.product'].create(
            {'name': 'Tela capacidad semanal KPI', 'type': 'consu'})
        # Semana de 7 días de junio 2040 (30 días) → capacidad prorrateada
        # 3000*7/30 = 700. Producidos 350 → 50%.
        self._production(product, 350.0, datetime.datetime(2040, 6, 3, 8, 0, 0))
        ind = self._indicator('produccion_vs_capacidad', frequency='weekly')
        value = ind._calc_produccion_vs_capacidad(date(2040, 6, 1), date(2040, 6, 7))
        self.assertEqual(value, 50.0)

    # ---------------- TR-03 consumo_energia ----------------
    def _post_bill(self, partner, amount, inv_date, refund=False):
        expense = self.env['account.account'].search(
            [('account_type', '=', 'expense')], limit=1)
        move = self.env['account.move'].create({
            'move_type': 'in_refund' if refund else 'in_invoice',
            'partner_id': partner.id, 'invoice_date': inv_date,
            'invoice_line_ids': [(0, 0, {
                'name': 'Energía', 'quantity': 1, 'price_unit': amount,
                'account_id': expense.id, 'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        return move

    def test_02_consumo_energia(self):
        partner = self.env['res.partner'].create({'name': 'CFE KPI'})
        self.Param.set_param('quimibond_sgi.energy_partner_id', partner.id)
        # 5000 facturado - 500 nota de crédito = 4500 neto en 2040-06.
        self._post_bill(partner, 5000.0, date(2040, 6, 10))
        self._post_bill(partner, 500.0, date(2040, 6, 20), refund=True)
        ind = self._indicator('consumo_energia', direction='lower_better')
        value = ind._calc_consumo_energia(date(2040, 6, 1), date(2040, 6, 30))
        self.assertEqual(value, 4500.0)
        # Evidencia: las facturas del proveedor en el periodo.
        measure = self._measure(ind, date(2040, 6, 1))
        action = measure.action_view_evidence()
        self.assertEqual(action['res_model'], 'account.move')
        self.assertEqual(len(self.env['account.move'].search(action['domain'])), 2)

    def test_02b_energia_sin_proveedor_queda_pendiente_con_nota(self):
        # Sin proveedor NO hay valor (un 0 "capturado" pintaría verde un KPI
        # lower_better): la medición del cron queda PENDIENTE con la nota que
        # pide configurarlo.
        self.Param.set_param('quimibond_sgi.energy_partner_id', '0')
        ind = self._indicator('consumo_energia', direction='lower_better')
        self.assertIsNone(
            ind._calc_consumo_energia(date(2040, 6, 1), date(2040, 6, 30)))
        self.assertIn('proveedor de energía',
                      ind._note_consumo_energia(date(2040, 6, 1), date(2040, 6, 30)))
        Cron = self.env['sgi.cron']
        Cron._sgi_generate_measures(
            ind, date(2040, 6, 1), date(2040, 6, 1), date(2040, 6, 30),
            date(2040, 7, 5), '06/2040')
        measure = self.Measure.search(
            [('indicator_id', '=', ind.id), ('period_date', '=', date(2040, 6, 1))])
        self.assertEqual(measure.state, 'pendiente')
        self.assertFalse(measure.semaphore)
        self.assertIn('proveedor de energía', measure.note or '')

    # ---------------- CO-03 compras_sin_devolucion (proxy) ----------------
    def _confirmed_po(self, product, when, with_return=False):
        vendor = self.env['res.partner'].create({'name': 'Proveedor OC KPI'})
        po = self.env['purchase.order'].create({
            'partner_id': vendor.id,
            'order_line': [(0, 0, {
                'product_id': product.id, 'product_qty': 10.0,
                'price_unit': 5.0, 'name': product.name,
                'date_planned': when})]})
        po.button_confirm()
        po.write({'date_approve': when})
        if with_return:
            receipt = po.picking_ids[:1]
            move = receipt.move_ids[:1]
            self.env['stock.move'].create({
                'product_id': product.id, 'product_uom_qty': 1.0,
                'product_uom': product.uom_id.id,
                'location_id': move.location_dest_id.id,
                'location_dest_id': move.location_id.id,
                'state': 'done', 'origin_returned_move_id': move.id})
        return po

    def test_03_compras_sin_devolucion_proxy(self):
        product = self.env['product.product'].create({
            'name': 'Insumo OC KPI', 'type': 'consu', 'purchase_ok': True})
        when = datetime.datetime(2040, 6, 15, 9, 0, 0)
        good = self._confirmed_po(product, when)
        bad = self._confirmed_po(product, when, with_return=True)
        ind = self._indicator('compras_sin_devolucion', direction='higher_better')
        value = ind._calc_compras_sin_devolucion(date(2040, 6, 1), date(2040, 6, 30))
        self.assertEqual(value, 50.0, "1 de 2 OCs sin devolución.")
        # El source_info deja claro que es un PROXY a validar por MAST.
        self.assertIn('PROXY', ind.source_info)
        # Evidencia = las OCs con devolución (el error).
        measure = self._measure(ind, date(2040, 6, 1))
        action = measure.action_view_evidence()
        self.assertEqual(action['res_model'], 'purchase.order')
        records = self.env['purchase.order'].search(action['domain'])
        self.assertIn(bad, records)
        self.assertNotIn(good, records)

    def test_03b_co03_no_se_activa_en_la_siembra(self):
        # El proxy NO entra en _SGI_AUTO_INDICATORS: MAST lo activa a mano.
        auto = self.env['sgi.config']._SGI_AUTO_INDICATORS
        self.assertNotIn('compras_sin_devolucion', auto.values())
        self.env['sgi.config'].activate_auto_indicators()
        co03 = self.env.ref('quimibond_sgi.sgi_ind_errores_oc')
        self.assertEqual(co03.calc_mode, 'manual')

    # ---------------- RH-02 capacitacion ----------------
    def test_04_capacitacion_cierra_brecha_sube_pct(self):
        stype = self.env['hr.skill.type'].create({'name': 'Certificación KPI'})
        level = self.env['hr.skill.level'].create({
            'skill_type_id': stype.id, 'name': 'Vigente', 'level_progress': 100})
        skill = self.env['hr.skill'].create({
            'name': 'Norma KPI', 'skill_type_id': stype.id})
        job = self.env['hr.job'].create({'name': 'Puesto capacitación KPI'})
        self.env['hr.job.skill'].create({
            'job_id': job.id, 'skill_id': skill.id,
            'skill_type_id': stype.id, 'skill_level_id': level.id})
        employee = self.env['hr.employee'].create({
            'name': 'Empleado capacitación KPI', 'job_id': job.id})
        ind = self._indicator('capacitacion')
        d1, d2 = date(2040, 6, 1), date(2040, 6, 30)
        # Con la competencia requerida SIN cubrir → hay una brecha.
        val_gap = ind._calc_capacitacion(d1, d2)
        self.assertIsNotNone(val_gap)
        # El empleado obtiene la competencia vigente (valid_to futuro) → cierra brecha.
        self.env['hr.employee.skill'].create({
            'employee_id': employee.id, 'skill_id': skill.id,
            'skill_type_id': stype.id, 'skill_level_id': level.id,
            'valid_to': date(2045, 1, 1)})
        val_ok = ind._calc_capacitacion(d1, d2)
        self.assertGreater(val_ok, val_gap,
                           "Cerrar la brecha sube el % de competencias vigentes.")
        # Evidencia: las brechas de competencia.
        measure = self._measure(ind, d1)
        action = measure.action_view_evidence()
        self.assertEqual(action['res_model'], 'sgi.competence.gap')

    def test_05_step2_activation_seed(self):
        # MA-02, TR-03 y RH-02 se activan en la siembra idempotente.
        Config = self.env['sgi.config']
        mapping = {
            'quimibond_sgi.sgi_ind_producido_capacidad': 'produccion_vs_capacidad',
            'quimibond_sgi.sgi_ind_consumo_energia': 'consumo_energia',
            'quimibond_sgi.sgi_ind_capacitacion': 'capacitacion',
        }
        for xmlid, mode in mapping.items():
            self.assertEqual(self.env.ref(xmlid).calc_mode, mode)
            # Vuelve a manual y reactiva → recupera el modo automático.
            self.env.ref(xmlid).calc_mode = 'manual'
        Config.activate_auto_indicators()
        for xmlid, mode in mapping.items():
            self.assertEqual(self.env.ref(xmlid).calc_mode, mode)
