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
  ``venta_estimada`` (lo que falta facturar del promedio o del presupuesto),
  ``compra_estimada`` (proporción histórica de compras sobre esas ventas),
  ``comp_<id>`` (compromisos recurrentes), ``ajuste_entrada``, ``ajuste_salida``.

Fechas de las facturas abiertas: la «Fecha esperada en el flujo» de la factura
si está; si no, el vencimiento más el atraso histórico del contacto (lo
vencido que ya pasó ese atraso se reparte en las primeras semanas).
* Informativo, no suma: ``incobrable``, ``revisar``, ``cobro_despues``,
  ``pago_despues``.
"""
from collections import defaultdict
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

from odoo import _, api, fields, models

from . import calendario

ENTRADAS = ('cobro_vencido', 'cobro', 'pedido_venta', 'venta_estimada', 'ajuste_entrada')


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
            'meta': {},
            # auxiliares para no contar dos veces
            '_facturado_mes': defaultdict(float),   # (partner, (año, mes)) -> pagos ya facturados que vencen ese mes
            '_pedidos_mes': defaultdict(float),     # (partner, (año, mes)) -> pedidos por facturar sin IVA
            '_ordenes_mes': defaultdict(float),     # (año, mes) -> órdenes de compra por facturar
            '_ventas_dia': defaultdict(float),      # fecha de factura estimada -> venta estimada con IVA
        }
        res['_excluir_pagos'], res['_compromiso_facturas'] = self._partners_compromisos(config)
        res['_fuera'] = set(config.partner_traspaso_ids.commercial_partner_id.ids)
        res['_hist_cli'] = self._historial(config, hoy, True)
        res['_hist_prov'] = self._historial(config, hoy, False, excluir=res['_excluir_pagos'] | res['_fuera'])
        self._caja(config, hoy, res)
        self._facturas(config, hoy, res)
        if config.incluir_pedidos:
            self._pedidos_venta(config, hoy, res)
            self._ordenes_compra(config, hoy, res)
        if config.fuente_ventas != 'ninguna':
            self._ventas_estimadas(config, hoy, res)
            self._compras_estimadas(config, hoy, res)
        self._compromisos(config, hoy, res)
        self._ajustes(config, hoy, res)
        res['caja_total'] = sum(c['importe'] for c in res['caja'])
        for k in [k for k in res if k.startswith('_')]:
            del res[k]
        return res

    @staticmethod
    def es_entrada(clave, fila):
        if clave.startswith('comp_'):
            return fila.get('tipo') == 'entrada'
        return clave in ENTRADAS

    @api.model
    def totales(self, res):
        """Entradas, salidas, neto y saldo final por semana de un resultado de ``calcular``."""
        n = len(res['semanas'])
        entradas, salidas = [0.0] * n, [0.0] * n
        for clave, fila in res['filas'].items():
            destino = entradas if self.es_entrada(clave, fila) else salidas
            for i, v in enumerate(fila['semanas']):
                destino[i] += v
        saldo, final = res['caja_total'], []
        for i in range(n):
            saldo += entradas[i] + salidas[i]
            final.append(saldo)
        return {'entradas': entradas, 'salidas': salidas,
                'neto': [entradas[i] + salidas[i] for i in range(n)], 'final': final}

    # ------------------------------------------------------------------
    def _partners_compromisos(self, config):
        excluir, con_facturas = set(), {}
        for comp in config.compromiso_ids.filtered(lambda c: c.active and c.partner_id):
            ids = set(comp._partners().ids)
            if comp.excluir_facturas:
                excluir |= ids
            else:
                con_facturas[comp.id] = ids
        return excluir, con_facturas

    def _historial(self, config, hoy, clientes, excluir=frozenset()):
        """Días que tarda cada contacto en pagar, con lo conciliado en los últimos meses, ponderado por
        importe: ``atraso`` (pago − vencimiento) y ``dias`` (pago − factura)."""
        company = config.company_id
        desde = hoy - timedelta(days=30 * (config.meses_historial or 12))
        campo = 'debit_move_id' if clientes else 'credit_move_id'
        partials = self.env['account.partial.reconcile'].search([
            (campo + '.company_id', '=', company.id),
            ('max_date', '>=', desde), ('max_date', '<', hoy),
            (campo + '.move_id.move_type', '=', 'out_invoice' if clientes else 'in_invoice'),
            (campo + '.account_id.account_type', '=', 'asset_receivable' if clientes else 'liability_payable'),
        ])
        por_partner = defaultdict(lambda: {'atraso': [], 'dias': []})
        todos = {'atraso': [], 'dias': []}
        for part in partials:
            line = part[campo]
            move = line.move_id
            vence = line.date_maturity or move.invoice_date_due
            fecha = move.invoice_date or move.date
            if not vence or not fecha or part.amount <= 0:
                continue
            pid = line.partner_id.commercial_partner_id.id
            if pid in excluir:
                continue
            atraso = min(max((part.max_date - vence).days, -30), 180)
            dias = min(max((part.max_date - fecha).days, 0), 365)
            for destino in (por_partner[pid], todos):
                destino['atraso'].append((atraso, part.amount))
                destino['dias'].append((dias, part.amount))
        out = {pid: {k: calendario.promedio_ponderado(v) for k, v in d.items()} for pid, d in por_partner.items()}
        out[None] = {k: calendario.promedio_ponderado(v) for k, v in todos.items()}
        return out

    @staticmethod
    def _dias(hist, pid, clave, defecto=0.0):
        valor = (hist.get(pid) or {}).get(clave)
        if valor is None:
            valor = (hist.get(None) or {}).get(clave)
        return defecto if valor is None else valor

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
        excluir_pagos = res['_excluir_pagos'] | res['_fuera']
        con_facturas = set().union(*res['_compromiso_facturas'].values()) if res['_compromiso_facturas'] else set()
        revisar = set(config.partner_revisar_ids.commercial_partner_id.ids)
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
            esperada = move.qb_fecha_flujo
            nombre = partner.display_name or move.name
            clave_det = ('res.partner', partner.id)
            ids = [('account.move.line', line.id)]
            es_cliente = line.account_id.account_type == 'asset_receivable'
            if es_cliente:
                fecha_factura = move.invoice_date or move.date
                if not esperada and ((config.cartera_desde and fecha_factura < config.cartera_desde)
                                     or vence < limite_incobrable):
                    self._sumar_info(res['info']['incobrable'], clave_det, nombre, importe, 'res.partner',
                                     partner.id, ids)
                    continue
                hist, usar_atraso, reparto = res['_hist_cli'], config.atraso_clientes, config.semanas_reparto_clientes
            else:
                if partner.id in excluir_pagos:
                    continue  # ya va como compromiso recurrente (nómina, IMSS, SAT) o no es flujo
                if partner.id in revisar:
                    self._sumar_info(res['info']['revisar'], clave_det, nombre, importe, 'res.partner',
                                     partner.id, ids)
                    continue
                if partner.id in con_facturas:
                    res['_facturado_mes'][(partner.id, (vence.year, vence.month))] += -importe
                hist, usar_atraso = res['_hist_prov'], config.atraso_proveedores
                reparto = config.semanas_reparto_proveedores
            if esperada:
                partes = [(max(esperada, hoy), 1.0)]
            else:
                atraso = round(self._dias(hist, partner.id, 'atraso')) if usar_atraso else 0
                partes = calendario.repartir(vence, hoy, atraso, sems, reparto)
            if es_cliente:
                clave = 'cobro_vencido' if vence < hoy else 'cobro'
            else:
                clave = 'pago_vencido' if vence < hoy else 'pago'
            for fecha, parte in partes:
                idx = calendario.indice_semana(fecha, sems)
                if idx is None:
                    despues = 'cobro_despues' if es_cliente else 'pago_despues'
                    self._sumar_info(res['info'][despues], clave_det, nombre, importe * parte, 'res.partner',
                                     partner.id, ids)
                    continue
                self._sumar(res['filas'][clave], idx, clave_det, nombre, importe * parte, 'res.partner', partner.id,
                            ids)

    @staticmethod
    def _plazo_dias(term):
        if not term:
            return 0
        return max(term.line_ids.mapped('nb_days') or [0])

    def _dias_cobro(self, config, res, partner, term=None):
        """Días de la factura al cobro: los que el cliente ha tardado o, sin historial (o con el atraso
        apagado), su plazo de pago."""
        plazo = self._plazo_dias(term or partner.property_payment_term_id)
        if not config.atraso_clientes:
            return plazo
        valor = (res['_hist_cli'].get(partner.id) or {}).get('dias')
        return round(valor) if valor is not None else plazo

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
            partner = order.partner_id.commercial_partner_id
            mes = max(entrega, hoy)
            fecha = mes + timedelta(days=self._dias_cobro(config, res, partner, order.payment_term_id))
            idx = calendario.indice_semana(fecha, res['semanas'])
            sin_iva = pendiente * (order.amount_untaxed / order.amount_total if order.amount_total else 1.0)
            res['_pedidos_mes'][(partner.id, (mes.year, mes.month))] += self._a_pesos(
                company, order.currency_id, sin_iva, hoy)
            if idx is None:
                continue
            self._sumar(res['filas']['pedido_venta'], idx, ('res.partner', partner.id), partner.display_name,
                        self._a_pesos(company, order.currency_id, pendiente, hoy), 'sale.order', None,
                        [('sale.order', order.id)])

    def _ordenes_compra(self, config, hoy, res):
        if 'purchase.order' not in self.env:
            return
        company = config.company_id
        desde = hoy - timedelta(days=config.dias_pedido_vigente or 0)
        excluir = res['_excluir_pagos'] | res['_fuera'] | set(config.partner_revisar_ids.commercial_partner_id.ids)
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
            mes = max(entrega, hoy)
            pesos = self._a_pesos(company, order.currency_id, pendiente, hoy)
            res['_ordenes_mes'][(mes.year, mes.month)] += pesos
            fecha = mes + timedelta(days=self._plazo_dias(order.payment_term_id))
            idx = calendario.indice_semana(fecha, res['semanas'])
            if idx is None:
                continue
            self._sumar(res['filas']['orden_compra'], idx, ('res.partner', partner.id), partner.display_name,
                        -pesos, 'purchase.order', None,
                        [('purchase.order', order.id)])

    # ------------------------------------------------------------------
    # Ventas y compras que todavía no tienen documento
    # ------------------------------------------------------------------
    def _facturado_por_cliente(self, company, desde, hasta):
        """``{partner: (sin IVA, con IVA)}`` facturado a clientes entre dos fechas (``hasta`` excluida)."""
        groups = self.env['account.move']._read_group(
            [('company_id', '=', company.id), ('state', '=', 'posted'),
             ('move_type', 'in', ('out_invoice', 'out_refund')),
             ('invoice_date', '>=', desde), ('invoice_date', '<', hasta)],
            groupby=['commercial_partner_id'], aggregates=['amount_untaxed_signed:sum', 'amount_total_signed:sum'])
        return {p.id: (u or 0.0, t or 0.0) for p, u, t in groups if p}

    def _presupuesto(self, config, anio):
        Budget = self.env['sgi.sales.budget']
        base = [('company_id', '=', config.company_id.id), ('kind', '=', 'presupuesto'), ('year', '=', anio)]
        return (Budget.search(base + [('state', '=', 'aprobado')], order='id desc', limit=1)
                or Budget.search(base + [('state', 'in', ('revisado', 'borrador'))], order='id desc', limit=1))

    def _ventas_estimadas(self, config, hoy, res):
        """Ventas que aún no tienen factura ni pedido: por cliente y mes, lo que falta para llegar al promedio
        facturado (o al presupuesto), facturado parejo en los días que quedan del mes y cobrado con los días que
        ese cliente tarda en pagar."""
        company = config.company_id
        sems = res['semanas']
        fin = sems[-1][1]
        inicio_mes = hoy.replace(day=1)
        meses = calendario.meses(inicio_mes, fin)
        base = defaultdict(float)  # (partner, (año, mes)) -> sin IVA
        historia = self._facturado_por_cliente(company, inicio_mes - relativedelta(months=12), inicio_mes)
        etiqueta = ''
        if config.fuente_ventas == 'historial':
            n = config.meses_ventas or 3
            reciente = self._facturado_por_cliente(company, inicio_mes - relativedelta(months=n), inicio_mes)
            for pid, (sin_iva, _con_iva) in reciente.items():
                if sin_iva > 0:
                    for mes in meses:
                        base[(pid, mes)] = sin_iva / n
            etiqueta = _('promedio de %s meses', n)
        elif 'sgi.sales.budget.line' in self.env:
            usados = []
            for anio in sorted({a for a, _m in meses}):
                budget = self._presupuesto(config, anio)
                if not budget:
                    continue
                usados.append(budget.name + ('' if budget.state == 'aprobado' else _(' (sin aprobar)')))
                groups = self.env['sgi.sales.budget.line']._read_group(
                    [('budget_id', '=', budget.id), ('potencial', '=', False),
                     ('date', '>=', date(anio, 1, 1)), ('date', '<=', date(anio, 12, 31))],
                    groupby=['partner_id', 'date:month'], aggregates=['amount_budget:sum'])
                moneda = budget.currency_id or company.currency_id
                for partner, mes, importe in groups:
                    clave = (mes.year, mes.month)
                    if clave in meses and importe:
                        pid = partner.commercial_partner_id.id if partner else False
                        base[(pid, clave)] += self._a_pesos(company, moneda, importe, hoy)
            etiqueta = ', '.join(usados) or _('sin presupuesto')
        res['meta']['ventas'] = etiqueta
        if not base:
            return
        este_mes = self._facturado_por_cliente(company, inicio_mes, hoy + timedelta(days=1))
        clave_hoy = (hoy.year, hoy.month)
        partners = self.env['res.partner'].browse([pid for pid, _m in base if pid])
        nombres = {p.id: p.display_name for p in partners}
        for (pid, mes), sin_iva in base.items():
            falta = sin_iva - res['_pedidos_mes'].get((pid, mes), 0.0)
            if mes == clave_hoy:
                falta -= este_mes.get(pid, (0.0, 0.0))[0]
            if falta <= 0:
                continue
            u, t = historia.get(pid, (0.0, 0.0))
            factor_iva = t / u if u > 0 and t > 0 else 1.16
            dias = calendario.dias_restantes(mes[0], mes[1], hoy)
            if not dias:
                continue
            por_dia = falta * factor_iva / len(dias)
            partner = partners.browse(pid) if pid else partners.browse()
            dias_cobro = self._dias_cobro(config, res, partner) if pid else round(
                self._dias(res['_hist_cli'], None, 'dias'))
            nombre = nombres.get(pid) or _('Sin cliente')
            for d in dias:
                res['_ventas_dia'][d] += por_dia
                idx = calendario.indice_semana(d + timedelta(days=dias_cobro), sems)
                if idx is not None:
                    self._sumar(res['filas']['venta_estimada'], idx, ('res.partner', pid or 0), nombre, por_dia,
                                'res.partner', pid or None)

    def _proporcion_compras(self, config, hoy, res):
        company = config.company_id
        inicio_mes = hoy.replace(day=1)
        desde = inicio_mes - relativedelta(months=config.meses_proporcion or 6)
        dominio = [('company_id', '=', company.id), ('state', '=', 'posted'),
                   ('invoice_date', '>=', desde), ('invoice_date', '<', inicio_mes)]
        fuera = res['_excluir_pagos'] | res['_fuera'] | set().union(*res['_compromiso_facturas'].values() or [set()])
        fuera |= set(config.partner_revisar_ids.commercial_partner_id.ids)
        compras = self.env['account.move']._read_group(
            dominio + [('move_type', 'in', ('in_invoice', 'in_refund'))], groupby=['commercial_partner_id'],
            aggregates=['amount_total_signed:sum'])
        total_compras = -sum(t or 0.0 for p, t in compras if p.id not in fuera)
        ventas = self.env['account.move']._read_group(
            dominio + [('move_type', 'in', ('out_invoice', 'out_refund'))], groupby=[],
            aggregates=['amount_total_signed:sum'])
        total_ventas = ventas[0][0] if ventas and ventas[0][0] else 0.0
        return total_compras / total_ventas if total_ventas > 0 else 0.0

    def _compras_estimadas(self, config, hoy, res):
        """Compras de las ventas estimadas: la proporción (histórica o la que se capture) de cada día de venta,
        menos lo que ya está en órdenes de compra ese mes, pagada con los días que se tarda en pagar."""
        if not res['_ventas_dia']:
            return
        proporcion = (config.proporcion_compras / 100.0 if config.proporcion_compras
                      else self._proporcion_compras(config, hoy, res))
        dias_pago = round(self._dias(res['_hist_prov'], None, 'dias', 30.0))
        res['meta']['proporcion_compras'] = proporcion
        res['meta']['dias_pago'] = dias_pago
        if proporcion <= 0:
            return
        por_mes = defaultdict(float)
        for d, venta in res['_ventas_dia'].items():
            por_mes[(d.year, d.month)] += venta * proporcion
        escala = {m: max(0.0, total - res['_ordenes_mes'].get(m, 0.0)) / total for m, total in por_mes.items() if total}
        nombre = _('%(p)s %% de las ventas estimadas, pagado a %(d)s días',
                   p='{:,.0f}'.format(proporcion * 100), d=dias_pago)
        for d, venta in res['_ventas_dia'].items():
            importe = venta * proporcion * escala.get((d.year, d.month), 0.0)
            idx = calendario.indice_semana(d + timedelta(days=dias_pago), res['semanas'])
            if importe and idx is not None:
                self._sumar(res['filas']['compra_estimada'], idx, ('qb', 'compras'), nombre, -importe)

    def _compromisos(self, config, hoy, res):
        sems = res['semanas']
        for comp in config.compromiso_ids.filtered('active'):
            signo = 1 if comp.tipo == 'entrada' else -1
            fila = res['filas']['comp_%d' % comp.id]
            fila['nombre'] = comp.name
            fila['tipo'] = comp.tipo
            partners = res['_compromiso_facturas'].get(comp.id) or set()
            facturado = defaultdict(float)
            for (pid, mes), importe in res['_facturado_mes'].items():
                if pid in partners:
                    facturado[mes] += importe
            for fecha in comp.fechas_entre(hoy, sems[-1][1]):
                idx = calendario.indice_semana(fecha, sems)
                if idx is None:
                    continue
                importe = comp.importe
                mes = (fecha.year, fecha.month)
                if facturado.get(mes):
                    # lo ya facturado que vence ese mes va en «Proveedores»: aquí solo lo que falta
                    usado = min(importe, facturado[mes])
                    facturado[mes] -= usado
                    importe -= usado
                if importe:
                    self._sumar(fila, idx, ('fecha', fecha), fields.Date.to_string(fecha), signo * importe)

    def _ajustes(self, config, hoy, res):
        sems = res['semanas']
        for aj in config.ajuste_ids.filtered(lambda a: hoy <= a.fecha <= sems[-1][1]):
            idx = calendario.indice_semana(aj.fecha, sems)
            clave = 'ajuste_entrada' if aj.tipo == 'entrada' else 'ajuste_salida'
            signo = 1 if aj.tipo == 'entrada' else -1
            self._sumar(res['filas'][clave], idx, ('qb.flujo.ajuste', aj.id), aj.name, signo * aj.importe,
                        'qb.flujo.ajuste', aj.id, [('qb.flujo.ajuste', aj.id)])
