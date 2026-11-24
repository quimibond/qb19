# -*- coding: utf-8 -*-
"""Cálculo del flujo de efectivo a 13 semanas (método directo).

Todo se lleva a pesos al tipo de cambio de la fecha del reporte: una
proyección de caja se hace con el tipo de cambio de hoy, no con el histórico
de cada factura.

Renglones (``clave``):

* Comprometido (sale de documentos registrados):
  ``cobro_vencido``, ``cobro`` (facturas de cliente),
  ``pago_vencido``, ``pago`` (facturas de proveedor).
* Estimado (supuestos):
  ``pedido_venta``, ``orden_compra`` (confirmado por facturar),
  ``comp_<id>`` (compromisos recurrentes), ``ajuste_entrada``, ``ajuste_salida``.
* Informativo, no suma: ``incobrable``, ``revisar``, ``cobro_despues``,
  ``pago_despues``.
"""
from collections import defaultdict
from datetime import timedelta

from odoo import api, fields, models

from . import calendario


class QbFlujoMotor(models.AbstractModel):
    _name = 'qb.flujo.motor'
    _description = 'Cálculo del flujo de efectivo a 13 semanas'

    @api.model
    def calcular(self, config, hoy):
        sems = calendario.semanas(hoy)
        res = {
            'semanas': sems,
            'caja': [],
            'filas': defaultdict(lambda: {'semanas': [0.0] * len(sems), 'detalle': {}}),
            'info': defaultdict(lambda: {'total': 0.0, 'detalle': {}}),
        }
        self._caja(config, hoy, res)
        self._facturas(config, hoy, res)
        if config.incluir_pedidos:
            self._pedidos_venta(config, hoy, res)
            self._ordenes_compra(config, hoy, res)
        self._compromisos(config, hoy, res)
        self._ajustes(config, hoy, res)
        res['caja_total'] = sum(c['importe'] for c in res['caja'])
        return res

    # ------------------------------------------------------------------
    def _a_pesos(self, company, currency, importe, hoy):
        if not currency or currency == company.currency_id:
            return importe
        return currency._convert(importe, company.currency_id, company, hoy, round=False)

    @staticmethod
    def _sumar(fila, idx, clave_detalle, nombre, importe, model=None, res_id=None, ids=()):
        fila['semanas'][idx] += importe
        det = fila['detalle'].setdefault(clave_detalle, {
            'nombre': nombre, 'model': model, 'res_id': res_id, 'semanas': [0.0] * len(fila['semanas']),
            'ids': set()})
        det['semanas'][idx] += importe
        det['ids'].update(ids)

    @staticmethod
    def _sumar_info(info, clave_detalle, nombre, importe, model=None, res_id=None, ids=()):
        info['total'] += importe
        det = info['detalle'].setdefault(clave_detalle, {
            'nombre': nombre, 'model': model, 'res_id': res_id, 'total': 0.0, 'ids': set()})
        det['total'] += importe
        det['ids'].update(ids)

    # ------------------------------------------------------------------
    def _caja(self, config, hoy, res):
        company = config.company_id
        for caja in config.caja_ids:
            journal = caja.journal_id
            currency = journal.currency_id or company.currency_id
            if caja.fuente == 'extracto':
                stmt = self.env['account.bank.statement'].search(
                    [('journal_id', '=', journal.id), ('date', '<=', hoy)], order='date desc, id desc', limit=1)
                importe_moneda = stmt.balance_end_real if stmt else 0.0
                al = stmt.date if stmt else False
            else:
                account = journal.default_account_id
                groups = self.env['account.move.line']._read_group(
                    [('account_id', '=', account.id), ('parent_state', '=', 'posted'), ('date', '<=', hoy),
                     ('company_id', '=', company.id)],
                    aggregates=['balance:sum', 'amount_currency:sum'])
                balance, amount_currency = groups[0] if groups else (0.0, 0.0)
                importe_moneda = (amount_currency if currency != company.currency_id else balance) or 0.0
                al = hoy
            res['caja'].append({
                'journal_id': journal.id, 'nombre': journal.name, 'moneda': currency.name, 'al': al,
                'fuente': caja.fuente, 'importe_moneda': importe_moneda,
                'importe': self._a_pesos(company, currency, importe_moneda, hoy),
            })

    def _facturas(self, config, hoy, res):
        company = config.company_id
        sems = res['semanas']
        excluir_pagos = set(config.compromiso_ids.mapped('partner_id').ids)
        revisar = set(config.partner_revisar_ids.ids)
        limite_incobrable = hoy - timedelta(days=config.dias_incobrable or 0)
        lines = self.env['account.move.line'].search([
            ('company_id', '=', company.id),
            ('parent_state', '=', 'posted'),
            ('account_id.account_type', 'in', ('asset_receivable', 'liability_payable')),
            ('reconciled', '=', False),
            ('move_id.move_type', 'in', ('out_invoice', 'out_refund', 'in_invoice', 'in_refund')),
        ])
        for line in lines:
            if line.currency_id and line.currency_id != company.currency_id:
                importe = self._a_pesos(company, line.currency_id, line.amount_residual_currency, hoy)
            else:
                importe = line.amount_residual
            if company.currency_id.is_zero(importe):
                continue
            move = line.move_id
            partner = line.partner_id.commercial_partner_id
            vence = line.date_maturity or move.invoice_date_due or move.date
            nombre = partner.display_name or move.name
            clave_det = ('res.partner', partner.id)
            es_cliente = line.account_id.account_type == 'asset_receivable'
            if es_cliente:
                fecha_factura = move.invoice_date or move.date
                if (config.cartera_desde and fecha_factura < config.cartera_desde) or vence < limite_incobrable:
                    self._sumar_info(res['info']['incobrable'], clave_det, nombre, importe, 'res.partner',
                                     partner.id, [('account.move.line', line.id)])
                    continue
            else:
                if partner.id in excluir_pagos:
                    continue  # ya va como compromiso recurrente (nómina, IMSS, SAT)
                if partner.id in revisar:
                    self._sumar_info(res['info']['revisar'], clave_det, nombre, importe, 'res.partner',
                                     partner.id, [('account.move.line', line.id)])
                    continue
            idx = calendario.indice_semana(vence, sems)
            if idx is None:
                clave = 'cobro_despues' if es_cliente else 'pago_despues'
                self._sumar_info(res['info'][clave], clave_det, nombre, importe, 'res.partner', partner.id, [('account.move.line', line.id)])
                continue
            if es_cliente:
                clave = 'cobro_vencido' if vence < hoy else 'cobro'
            else:
                clave = 'pago_vencido' if vence < hoy else 'pago'
            self._sumar(res['filas'][clave], idx, clave_det, nombre, importe, 'res.partner', partner.id, [('account.move.line', line.id)])

    @staticmethod
    def _plazo_dias(term):
        if not term:
            return 0
        return max(term.line_ids.mapped('nb_days') or [0])

    def _pedidos_venta(self, config, hoy, res):
        if 'sale.order' not in self.env:
            return
        company = config.company_id
        desde = hoy - timedelta(days=config.dias_pedido_vigente or 0)
        orders = self.env['sale.order'].search([
            ('company_id', '=', company.id), ('state', '=', 'sale'),
            ('invoice_status', 'in', ('to invoice', 'no')),
            ('date_order', '>=', fields.Datetime.to_datetime(desde)),
        ])
        for order in orders:
            pendiente = order.amount_to_invoice
            if order.currency_id.is_zero(pendiente) or pendiente < 0:
                continue
            entrega = (order.commitment_date or order.expected_date or order.date_order).date()
            fecha = max(entrega, hoy) + timedelta(days=self._plazo_dias(order.payment_term_id))
            idx = calendario.indice_semana(fecha, res['semanas'])
            if idx is None:
                continue
            partner = order.partner_id.commercial_partner_id
            self._sumar(res['filas']['pedido_venta'], idx, ('res.partner', partner.id), partner.display_name,
                        self._a_pesos(company, order.currency_id, pendiente, hoy), 'sale.order', None,
                        [('sale.order', order.id)])

    def _ordenes_compra(self, config, hoy, res):
        if 'purchase.order' not in self.env:
            return
        company = config.company_id
        desde = hoy - timedelta(days=config.dias_pedido_vigente or 0)
        excluir = set(config.compromiso_ids.mapped('partner_id').ids) | set(config.partner_revisar_ids.ids)
        orders = self.env['purchase.order'].search([
            ('company_id', '=', company.id), ('state', '=', 'purchase'),
            ('invoice_status', '!=', 'invoiced'), ('date_order', '>=', fields.Datetime.to_datetime(desde)),
        ])
        for order in orders:
            partner = order.partner_id.commercial_partner_id
            if partner.id in excluir:
                continue
            pendiente = 0.0
            for line in order.order_line.filtered(lambda l: not l.display_type and l.product_qty):
                por_facturar = line.product_qty - line.qty_invoiced
                if por_facturar > 0:
                    pendiente += line.price_total * por_facturar / line.product_qty
            if order.currency_id.is_zero(pendiente):
                continue
            entrega = (order.date_planned or order.date_order).date()
            fecha = max(entrega, hoy) + timedelta(days=self._plazo_dias(order.payment_term_id))
            idx = calendario.indice_semana(fecha, res['semanas'])
            if idx is None:
                continue
            self._sumar(res['filas']['orden_compra'], idx, ('res.partner', partner.id), partner.display_name,
                        -self._a_pesos(company, order.currency_id, pendiente, hoy), 'purchase.order', None,
                        [('purchase.order', order.id)])

    def _compromisos(self, config, hoy, res):
        sems = res['semanas']
        for comp in config.compromiso_ids.filtered('active'):
            signo = 1 if comp.tipo == 'entrada' else -1
            fila = res['filas']['comp_%d' % comp.id]
            fila['nombre'] = comp.name
            fila['tipo'] = comp.tipo
            for fecha in comp.fechas_entre(hoy, sems[-1][1]):
                idx = calendario.indice_semana(fecha, sems)
                if idx is not None:
                    self._sumar(fila, idx, ('fecha', fecha), fields.Date.to_string(fecha), signo * comp.importe)

    def _ajustes(self, config, hoy, res):
        sems = res['semanas']
        for aj in config.ajuste_ids.filtered(lambda a: hoy <= a.fecha <= sems[-1][1]):
            idx = calendario.indice_semana(aj.fecha, sems)
            clave = 'ajuste_entrada' if aj.tipo == 'entrada' else 'ajuste_salida'
            signo = 1 if aj.tipo == 'entrada' else -1
            self._sumar(res['filas'][clave], idx, ('qb.flujo.ajuste', aj.id), aj.name, signo * aj.importe,
                        'qb.flujo.ajuste', aj.id, [('qb.flujo.ajuste', aj.id)])
