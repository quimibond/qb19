# -*- coding: utf-8 -*-
from datetime import date, datetime

from odoo.tests import TransactionCase


class CosteoCase(TransactionCase):
    """Base con una compañía, un plan mínimo, dos centros (uno directo con un
    centro de trabajo, uno indirecto) y asientos de un mes."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        cls.periodo_date = date(2026, 9, 1)
        Account = cls.env['account.account']

        def cuenta(code, name, atype):
            acc = Account.search([('code', '=', code),
                                  ('company_ids', 'in', cls.company.id)],
                                 limit=1)
            return acc or Account.create({
                'code': code, 'name': name, 'account_type': atype})

        cls.acc_ventas = cuenta('QB401', 'Ventas prueba', 'income')
        cls.acc_renta = cuenta('QB504R', 'Renta prueba', 'expense_direct_cost')
        cls.acc_luz = cuenta('QB504L', 'Luz prueba', 'expense_direct_cost')
        cls.acc_nomina = cuenta('QB501N', 'Nómina prueba',
                                'expense_direct_cost')
        cls.acc_admin = cuenta('QB602', 'Admin prueba', 'expense')
        cls.acc_mant = cuenta('QB504M', 'Mantenimiento prueba',
                              'expense_direct_cost')
        cls.acc_sin = cuenta('QB599', 'Sin clasificar prueba', 'expense')
        cls.acc_contra = cuenta('QB999', 'Contrapartida', 'asset_current')

        cls.journal = cls.env['account.journal'].search(
            [('type', '=', 'general'), ('company_id', '=', cls.company.id)],
            limit=1) or cls.env['account.journal'].create({
                'name': 'Misc prueba', 'code': 'QBM', 'type': 'general'})

        cal = cls.env['resource.calendar'].create({
            'name': 'Calendario 8h', 'tz': 'UTC',
            'attendance_ids': [(0, 0, {
                'name': d, 'dayofweek': str(i), 'hour_from': 8,
                'hour_to': 16}) for i, d in enumerate(
                    ['lu', 'ma', 'mi', 'ju', 'vi'])],
        })
        cls.wc_tin = cls.env['mrp.workcenter'].create({
            'name': 'Tintorería 1', 'resource_calendar_id': cal.id,
            'time_efficiency': 100, 'costs_hour': 0})
        cls.wc_aca = cls.env['mrp.workcenter'].create({
            'name': 'Rama 1', 'resource_calendar_id': cal.id,
            'time_efficiency': 50, 'costs_hour': 5})

        Centro = cls.env['qb.centro']
        cls.c_tin = Centro.create({
            'code': 'TIN', 'name': 'Tintorería', 'nature': 'directo',
            'driver': 'kg_ciclo', 'workcenter_ids': [(6, 0, [cls.wc_tin.id])],
            'publica_tarifa': True})
        cls.c_aca = Centro.create({
            'code': 'ACA', 'name': 'Acabado', 'nature': 'directo',
            'driver': 'm_velocidad',
            'workcenter_ids': [(6, 0, [cls.wc_aca.id])],
            'publica_tarifa': False})
        cls.c_mant = Centro.create({
            'code': 'MANT', 'name': 'Mantenimiento', 'nature': 'indirecto',
            'reparto_llave': 'horas'})

        Clase = cls.env['qb.cuenta.clase']
        Clase.create({'account_id': cls.acc_ventas.id, 'bucket': 'ventas'})
        Clase.create({'account_id': cls.acc_renta.id, 'bucket': 'fijo',
                      'centro_id': cls.c_tin.id, 'pct': 60})
        Clase.create({'account_id': cls.acc_renta.id, 'bucket': 'fijo',
                      'centro_id': cls.c_aca.id, 'pct': 40})
        Clase.create({'account_id': cls.acc_luz.id, 'bucket': 'energia',
                      'centro_id': cls.c_tin.id})
        Clase.create({'account_id': cls.acc_nomina.id, 'bucket': 'nomina'})
        Clase.create({'account_id': cls.acc_admin.id, 'bucket': 'operacion'})
        Clase.create({'account_id': cls.acc_mant.id, 'bucket': 'fijo',
                      'centro_id': cls.c_mant.id})

        # Suavizado de un mes para que los montos sean los del asiento.
        P = cls.env['qb.parametro']
        P.set_float('suavizado_fijo_meses', 1)
        P.set_float('suavizado_operacion_meses', 1)

        cls.periodo = cls.env['qb.periodo'].create({
            'period': cls.periodo_date, 'suavizado_fijo_meses': 1})

    def asiento(self, lineas, fecha=None, ref=None):
        """lineas: [(account, debit, credit)]."""
        move = self.env['account.move'].create({
            'journal_id': self.journal.id,
            'date': fecha or date(2026, 9, 15),
            'ref': ref,
            'line_ids': [(0, 0, {'account_id': a.id, 'debit': d,
                                 'credit': c}) for a, d, c in lineas],
        })
        move.action_post()
        return move

    def gasto(self, account, monto, **kw):
        return self.asiento([(account, monto, 0), (self.acc_contra, 0, monto)],
                            **kw)

    def venta(self, monto, **kw):
        return self.asiento([(self.acc_ventas, 0, monto),
                             (self.acc_contra, monto, 0)], **kw)

    def workorder_done(self, workcenter, horas, product=None, fecha=None):
        """Orden de fabricación con una orden de trabajo terminada."""
        product = product or self.env['product.product'].create({
            'name': 'Tela prueba', 'type': 'consu', 'is_storable': True})
        mo = self.env['mrp.production'].create({
            'product_id': product.id, 'product_qty': 100,
            'product_uom_id': product.uom_id.id})
        wo = self.env['mrp.workorder'].create({
            'name': 'WO', 'production_id': mo.id,
            'workcenter_id': workcenter.id, 'product_uom_id': product.uom_id.id})
        fin = fecha or datetime(2026, 9, 10, 12)
        wo.write({'state': 'done', 'duration': horas * 60,
                  'date_finished': fin})
        return mo, wo
