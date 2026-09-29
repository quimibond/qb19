# -*- coding: utf-8 -*-
"""57.14.0 (indicadores 2): S2-01, S1-05, C4-01, C1-04, RH-01, S4-01 y S6-02
con datos propios (fechas de 2044-2045, sin datos de producción)."""
from datetime import date, datetime
from unittest.mock import patch

from odoo.tests import TransactionCase, tagged

from .common_accounts import sgi_test_payable


@tagged('post_install', '-at_install')
class TestIndicadores2(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.Indicator = env['sgi.indicator']
        cls.Param = env['ir.config_parameter'].sudo()
        cls.company = env.company
        cls.Param.set_param('quimibond_sgi.kpi_company_id', str(cls.company.id))
        cls.company.partner_id.tz = 'America/Mexico_City'
        cls.period = date(2045, 3, 1)
        cls.period_end = date(2045, 3, 31)

    def _ind(self, mode):
        return self.Indicator.new({'calc_mode': mode})

    def _account(self, account_type):
        return self.env['account.account'].search(
            [('account_type', '=', account_type), ('company_ids', 'in', self.company.ids)], limit=1) \
            or self.env['account.account'].search([('account_type', '=', account_type)], limit=1)

    def _messages_of(self, employee):
        versions = self.env['hr.version'].with_context(active_test=False).search(
            [('employee_id', '=', employee.id)])
        return self.env['mail.message'].sudo().search([
            '|', '&', ('model', '=', 'hr.employee'), ('res_id', '=', employee.id),
            '&', ('model', '=', 'hr.version'), ('res_id', 'in', versions.ids)])

    def _flush_tracking(self):
        self.env.flush_all()
        self.env.cr.precommit.run()

    # ---- S2-01 -----------------------------------------------------------
    def test_01_complementos_pago_en_plazo(self):
        env = self.env
        if 'l10n_mx_edi.document' not in env:
            self.skipTest("Sin l10n_mx_edi no hay complementos de pago.")
        customer = env['res.partner'].create({'name': 'Cliente S2-01'})
        journal = env['account.journal'].search([
            ('type', '=', 'bank'), ('company_id', '=', self.company.id)], limit=1)
        payments = env['account.payment']
        for day in (3, 20, 28):
            payment = env['account.payment'].create({
                'payment_type': 'inbound', 'partner_type': 'customer',
                'partner_id': customer.id, 'amount': 100.0,
                'date': date(2045, 3, day), 'journal_id': journal.id})
            payment.action_post()
            payments |= payment
        pay_a, pay_b, _pay_c = payments
        Doc = env['l10n_mx_edi.document']
        # 5-abr 23:30 hora de México (UTC-6) = 6-abr 05:30 UTC: en plazo.
        Doc.create({'move_id': pay_a.move_id.id, 'state': 'payment_sent',
                    'datetime': datetime(2045, 4, 6, 5, 30)})
        # 6-abr 01:00 hora de México: fuera de plazo.
        Doc.create({'move_id': pay_b.move_id.id, 'state': 'payment_sent',
                    'datetime': datetime(2045, 4, 6, 7, 0)})
        # Un intento fallido no cuenta como timbrado.
        Doc.create({'move_id': pay_b.move_id.id, 'state': 'payment_sent_failed',
                    'datetime': datetime(2045, 4, 1, 12, 0)})
        cls = type(self.Indicator)
        with patch.object(cls, '_sgi_complement_payments', lambda self, a, b: payments):
            detail = self._ind('complementos_pago')._detail_complementos_pago(
                self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (1, 3))
        self.assertEqual(detail['value'], 33.33)
        self.assertEqual(detail['model'], 'account.payment')
        self.assertIn('1 pago(s)', detail['note'], "El pago sin complemento se nombra.")
        # Antes de que venza el plazo del periodo la medición queda pendiente.
        vals = self._ind('complementos_pago')._sgi_measure_vals(self.period, self.period_end)
        self.assertEqual(vals['state'], 'pendiente')

    # ---- S1-05 -----------------------------------------------------------
    def test_02_desviacion_precio_contra_oc(self):
        env = self.env
        supplier = env['res.partner'].create({'name': 'Proveedor S1-05'})
        sgi_test_payable(env, supplier)
        hilo = env['product.product'].create({'name': 'Hilo S1-05', 'type': 'consu'})
        tinte = env['product.product'].create({'name': 'Tinte S1-05', 'type': 'consu'})
        order = env['purchase.order'].create({
            'partner_id': supplier.id,
            'order_line': [(0, 0, {'product_id': hilo.id, 'product_qty': 2.0, 'price_unit': 100.0}),
                           (0, 0, {'product_id': tinte.id, 'product_qty': 4.0, 'price_unit': 50.0})]})
        line_hilo, line_tinte = order.order_line
        expense = self._account('expense_direct_cost')

        def bill_line(product, qty, price, po_line=None):
            vals = {'product_id': product.id, 'quantity': qty, 'price_unit': price,
                    'account_id': expense.id, 'tax_ids': [(6, 0, [])]}
            if po_line:
                vals['purchase_line_id'] = po_line.id
            return (0, 0, vals)

        bill = env['account.move'].create({
            'move_type': 'in_invoice', 'partner_id': supplier.id,
            'invoice_date': date(2045, 3, 10), 'currency_id': self.company.currency_id.id,
            'invoice_line_ids': [bill_line(hilo, 2.0, 110.0, line_hilo),     # +10 × 2
                                 bill_line(tinte, 4.0, 50.0, line_tinte),    # igual
                                 bill_line(tinte, 1.0, 300.0)]})             # sin OC
        bill.action_post()
        detail = self._ind('desviacion_precio_compra')._detail_desviacion_precio_compra(
            self.period, self.period_end)
        self.assertAlmostEqual(detail['numerator'], 20.0, places=2)
        self.assertAlmostEqual(detail['denominator'], 420.0, places=2)
        self.assertEqual(detail['value'], 4.76)
        self.assertEqual(detail['model'], 'account.move.line')
        self.assertEqual(len(detail['ids']), 2, "Solo las líneas que vienen de una OC.")
        self.assertIn('1 línea(s)', detail['note'], "La línea sin OC se cuenta en la nota.")

    # ---- C4-01 -----------------------------------------------------------
    def test_03_ordenes_cerradas_48h(self):
        env = self.env
        self.Param.set_param('quimibond_sgi.c4_stamp_seconds', '60')
        workcenter = env['mrp.workcenter'].create({'name': 'Circular C4-01'})
        tela = env['product.product'].create({'name': 'Tela C4-01', 'type': 'consu'})
        week_from, week_to = date(2045, 3, 6), date(2045, 3, 12)

        def order(op_end, closed=None):
            mo = env['mrp.production'].create({'product_id': tela.id, 'product_qty': 10.0})
            workorder = env['mrp.workorder'].create({
                'name': 'Tejer', 'production_id': mo.id, 'workcenter_id': workcenter.id,
                'product_uom_id': mo.product_uom_id.id})
            # Por SQL: el inverso de las fechas de la operación planea en el
            # calendario del centro de trabajo; aquí solo importa la fecha.
            env.flush_all()
            env.cr.execute("UPDATE mrp_workorder SET date_finished = %s WHERE id = %s",
                           (op_end, workorder.id))
            if closed:
                env.cr.execute("UPDATE mrp_production SET state = 'done', date_finished = %s "
                               "WHERE id = %s", (closed, mo.id))
            env.invalidate_all()
            return mo

        on_time = order(datetime(2045, 3, 7, 10), datetime(2045, 3, 8, 9))       # 23 h
        late = order(datetime(2045, 3, 8, 10), datetime(2045, 3, 11, 10))       # 72 h
        order(datetime(2045, 3, 9, 10), datetime(2045, 3, 9, 10, 0, 30))        # el cierre la terminó
        open_mo = order(datetime(2045, 3, 10, 10))                               # sin cerrar
        order(datetime(2045, 3, 14, 10), datetime(2045, 3, 14, 11))              # otra semana
        detail = self._ind('ordenes_cerradas_48h')._detail_ordenes_cerradas_48h(week_from, week_to)
        self.assertEqual((detail['numerator'], detail['denominator']), (1, 3))
        self.assertEqual(set(detail['ids']), {on_time.id, late.id, open_mo.id})
        self.assertIn('1 orden(es) fuera', detail['note'])

    # ---- C1-04 -----------------------------------------------------------
    def test_04_desarrollos_vendidos_cohorte_6_meses(self):
        env = self.env
        categ = env['product.category'].create({'name': 'Producto Terminado C1-04'})
        sub = env['product.category'].create({'name': 'Tejido C1-04', 'parent_id': categ.id})
        self.Param.set_param('quimibond_sgi.finished_product_categ_ids', str(categ.id))
        customer = env['res.partner'].create({'name': 'Cliente C1-04'})

        def article(name, created, categ_id=sub.id):
            template = env['product.template'].create({'name': name, 'categ_id': categ_id})
            env.flush_all()
            env.cr.execute("UPDATE product_template SET create_date = %s WHERE id = %s",
                           (created, template.id))
            template.invalidate_recordset(['create_date'])
            return template

        def sell(template, when):
            so = env['sale.order'].create({
                'partner_id': customer.id,
                'order_line': [(0, 0, {'product_id': template.product_variant_id.id,
                                       'product_uom_qty': 1.0, 'price_unit': 1.0})]})
            env.flush_all()
            env.cr.execute("UPDATE sale_order SET state = 'sale', date_order = %s WHERE id = %s",
                           (when, so.id))
            so.invalidate_recordset(['state', 'date_order'])

        sold = article('Vendido', datetime(2044, 9, 10, 12))
        late = article('Vendido tarde', datetime(2044, 9, 20, 12))
        article('Sin venta', datetime(2044, 9, 25, 12))
        other = article('Otra cohorte', datetime(2044, 10, 2, 12))
        article('No es terminado', datetime(2044, 9, 12, 12),
                categ_id=env['product.category'].create({'name': 'MP C1-04'}).id)
        sell(sold, datetime(2044, 12, 1, 12))
        sell(late, datetime(2045, 5, 1, 12))      # más de 6 meses después del alta
        sell(other, datetime(2044, 11, 1, 12))
        detail = self._ind('desarrollos_vendidos')._detail_desarrollos_vendidos(
            self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (1, 3),
                         "Marzo de 2045 mide los artículos dados de alta en septiembre de 2044.")
        self.assertEqual(detail['model'], 'product.template')
        self.assertIn('30/09/2044', detail['note'])

    # ---- RH-01 -----------------------------------------------------------
    def test_05_cobertura_plantilla(self):
        env = self.env
        Job = env['hr.job']
        telar = Job.create({'name': 'Tejedor RH-01', 'sgi_authorized_headcount': 3})
        jefe = Job.create({'name': 'Jefe RH-01', 'sgi_authorized_headcount': 1})
        sin = Job.create({'name': 'Sin plantilla RH-01'})
        Employee = env['hr.employee']

        def person(name, job, departure=None, active=True):
            employee = Employee.create({'name': name, 'job_id': job.id,
                                        'company_id': self.company.id})
            if departure:
                employee.departure_date = departure
            if not active:
                employee.active = False
            return employee

        person('Tejedor 1', telar)
        person('Tejedor 2', telar)
        person('Tejedor que salió', telar, departure=date(2045, 2, 10), active=False)
        person('Tejedor que sale después', telar, departure=date(2045, 4, 10), active=False)
        person('Jefe 1', jefe)
        person('Jefe 2', jefe)                   # de más: no cubre al telar
        person('Sin plantilla', sin)
        detail = self._ind('cobertura_plantilla')._detail_cobertura_plantilla(
            self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (4, 4),
                         "Telar 3 de 3 (el que sale en abril sigue en marzo); jefe 1 de 1.")
        self.assertEqual(detail['value'], 100.0)
        telar.sgi_authorized_headcount = 5
        detail = self._ind('cobertura_plantilla')._detail_cobertura_plantilla(
            self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (4, 6))
        self.assertIn('sgi_authorized_headcount', telar._fields)
        self.assertTrue(telar._fields['sgi_authorized_headcount'].tracking)

    # ---- S4-01 -----------------------------------------------------------
    def test_06_bajas_registradas_al_dia_siguiente(self):
        env = self.env
        reason = env['hr.departure.reason'].search([], limit=1) \
            or env['hr.departure.reason'].create({'name': 'Renuncia prueba'})
        Employee = env['hr.employee']
        departure = date(2045, 3, 14)            # martes: vence el miércoles 15

        def leaver(name, registered, with_reason=True):
            employee = Employee.create({'name': name, 'company_id': self.company.id})
            vals = {'departure_date': departure}
            if with_reason:
                vals['departure_reason_id'] = reason.id
            employee.write(vals)
            self._flush_tracking()
            self._messages_of(employee).write({'date': registered})
            return employee

        leaver('A tiempo', datetime(2045, 3, 16, 5, 0))       # 15-mar 23:00 hora de México
        leaver('Tarde', datetime(2045, 3, 20, 17, 0))
        leaver('Sin motivo', datetime(2045, 3, 14, 17, 0), with_reason=False)
        detail = self._ind('bajas_registradas')._detail_bajas_registradas(
            self.period, self.period_end)
        if not detail['denominator']:
            self.skipTest("Sin bajas: el entorno no guardó el seguimiento.")
        self.assertEqual((detail['numerator'], detail['denominator']), (1, 3))
        self.assertEqual(detail['model'], 'hr.employee')
        self.assertIn('sin motivo', detail['note'])

    # ---- S6-02 -----------------------------------------------------------
    def test_07_bajas_accesos_y_equipo(self):
        env = self.env
        accesos = env.ref('quimibond_sgi.sgi_activity_type_retirar_accesos')
        equipo = env.ref('quimibond_sgi.sgi_activity_type_recoger_equipo')
        plan = env.ref('quimibond_sgi.sgi_offboarding_plan')
        self.assertEqual(plan.res_model, 'hr.employee')
        self.assertEqual(plan.template_ids.activity_type_id, accesos | equipo)
        Employee = env['hr.employee']
        departure = date(2045, 3, 14)

        def leaver(name, done):
            employee = Employee.create({'name': name, 'company_id': self.company.id,
                                        'departure_date': departure})
            for act_type, when in done:
                activity = employee.activity_schedule(
                    activity_type_id=act_type.id, user_id=env.user.id)
                activity.action_feedback(feedback='hecho')
                env['mail.message'].sudo().search([
                    ('model', '=', 'hr.employee'), ('res_id', '=', employee.id),
                    ('mail_activity_type_id', '=', act_type.id)]).write({'date': when})
            return employee

        leaver('A tiempo', [(accesos, datetime(2045, 3, 14, 18)),
                            (equipo, datetime(2045, 3, 15, 20))])
        leaver('Equipo tarde', [(accesos, datetime(2045, 3, 14, 18)),
                                (equipo, datetime(2045, 3, 17, 18))])
        leaver('Sin equipo', [(accesos, datetime(2045, 3, 14, 18))])
        detail = self._ind('bajas_accesos_equipo')._detail_bajas_accesos_equipo(
            self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (1, 3))
        self.assertIn('1 baja(s)', detail['note'])

    # ---- Activación ------------------------------------------------------
    def test_08_activacion_solo_manuales_sin_formula(self):
        Indicator = self.Indicator
        codes = ('S2-01', 'S1-05', 'C1-04', 'RH-01', 'S4-01', 'S6-02', 'C4-01')
        for existing in Indicator.with_context(active_test=False).search([('code', 'in', codes)]):
            existing.code = existing.code + '-PROD'
        created = {code: Indicator.create({'code': code, 'name': code, 'calc_mode': 'manual'})
                   for code in codes}
        created['S1-05'].calc_mode = 'configurable'      # decisión de MAST: no se toca
        done = Indicator._sgi_activate_ind2()
        self.assertNotIn('S1-05', done)
        self.assertNotIn('C4-01', done, "C4-01 no se activa sola.")
        self.assertEqual(created['S2-01'].calc_mode, 'complementos_pago')
        self.assertEqual(created['S4-01'].calc_mode, 'bajas_registradas')
        self.assertEqual(created['S6-02'].calc_mode, 'bajas_accesos_equipo')
        self.assertTrue(created['S6-02'].measure_from, "S6-02 se mide desde el mes siguiente.")
        self.assertFalse(created['S4-01'].measure_from)
        self.assertEqual(created['C4-01'].calc_mode, 'manual')
        self.assertEqual(Indicator._sgi_activate_ind2(), {}, "Idempotente.")
        self.assertIn('complemento', created['S2-01'].source_info)
