# -*- coding: utf-8 -*-
"""Presupuesto maestro de ventas (F-P-A28-18 / F-P-A28-17).

Matriz tipo MPS: por mercado (equipo de ventas) y año, filas = producto y
columnas = enero…diciembre en cantidad y pesos. El REAL se calcula solo desde lo
facturado (la cifra dura) y, complementariamente, desde lo pedido.

Dos invariantes del negocio, tratadas con cuidado:
  · UNIDADES — vendemos en metros, kg, rollos y piezas. Cada línea lleva su
    unidad; las cantidades NUNCA se suman entre unidades distintas (los totales
    de cantidad son POR unidad). El único total global es el de dinero.
  · DIVISAS — facturamos MXN y USD. Todos los montos van en moneda de la
    compañía; el real en dinero sale de account.move.line.balance (contabilidad
    ya convirtió cada factura a su tipo de cambio de la fecha — no reconvertimos).

Este archivo tiene la CABECERA (sgi.sales.budget) y los helpers de módulo
compartidos (_REAL_MOVE_TYPES, _convert_qty). La LÍNEA (sgi.sales.budget.line)
vive en sgi_sales_budget_line.py, solo separada por tamaño.
"""
from collections import defaultdict
from datetime import date, timedelta

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

_REAL_MOVE_TYPES = ('out_invoice', 'out_refund')


def _convert_qty(qty, from_uom, to_uom):
    """Convierte `qty` de `from_uom` a `to_uom` si son de la misma categoría de
    unidad; devuelve None si son incompatibles (no se pueden sumar entre sí).

    Centraliza el patrón _has_common_reference + _compute_quantity repetido en
    todos los agregados de cantidad (facturado, comprometido, pedido, demanda)."""
    if from_uom and to_uom and from_uom._has_common_reference(to_uom):
        return from_uom._compute_quantity(qty, to_uom, round=False)
    return None


class SgiSalesBudget(models.Model):
    _name = 'sgi.sales.budget'
    _description = "Presupuesto de ventas (F-P-A28-17/18)"
    _inherit = ['sgi.base.mixin', 'sgi.format.mixin']
    _order = 'year desc, team_id, revision desc'
    _sgi_sequence_code = 'sgi.sales.budget'
    _sgi_locked_states = ('aprobado',)

    _folio_uniq = models.Constraint(
        'unique(folio)', "Ya existe un presupuesto con ese folio.")

    year = fields.Integer(string="Año", required=True, tracking=True,
                          default=lambda self: fields.Date.context_today(self).year)
    team_id = fields.Many2one('crm.team', string="Mercado (equipo de ventas)",
                              required=True, tracking=True,
                              help="Cada hoja del F-P-A28-18 es un mercado: "
                                   "industrial, confección, especiales…")
    revision = fields.Integer(string="Revisión", default=1, required=True,
                              tracking=True)
    kind = fields.Selection([
        ('presupuesto', "Presupuesto mensual (por mercado)"),
        ('pronostico', "Pronóstico semanal (por cliente)"),
    ], string="Tipo", default='presupuesto', required=True, tracking=True,
        help="Presupuesto: matriz mensual por mercado (F-P-A28-18). "
             "Pronóstico: matriz semanal por cliente (F-P-A28-13); un registro "
             "por cliente y año, como cada hoja del forecast.")
    partner_id = fields.Many2one(
        'res.partner', string="Cliente del pronóstico",
        domain="[('is_company', '=', True)]",
        help="Cliente de este pronóstico (obligatorio en modo pronóstico; una "
             "hoja del forecast = un cliente-año).")
    name = fields.Char(string="Nombre", compute='_compute_name', store=True)
    company_id = fields.Many2one('res.company', string="Compañía",
                                 default=lambda self: self.env.company, required=True)
    currency_id = fields.Many2one('res.currency', related='company_id.currency_id',
                                  string="Moneda", readonly=True)
    # Ciclo de vida diferenciado por kind (P-A28 Rev.15, 4.2.1 / 4.2.2):
    #   · presupuesto: borrador → revisado (Admin de ventas, 4.2.2.3) →
    #     aprobado (alta dirección, 4.2.2.4). Se congela en 'aprobado'.
    #   · pronóstico: borrador → revisado (Admin de ventas). Documento VIVO:
    #     nunca llega a 'aprobado', sin candado de líneas; editar una línea de un
    #     pronóstico 'revisado' lo regresa a borrador (4.2.2.7).
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('revisado', "Revisado"),
        ('aprobado', "Aprobado"),
        ('obsoleto', "Obsoleto"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    line_ids = fields.One2many('sgi.sales.budget.line', 'budget_id',
                               string="Líneas del presupuesto", copy=True)
    line_count = fields.Integer(string="# Líneas", compute='_compute_line_count')

    # Totales — SOLO de dinero (moneda compañía). Las cantidades no se totalizan
    # globalmente porque mezclarían unidades distintas.
    amount_budget_total = fields.Monetary(string="Presupuesto (total)",
                                          compute='_compute_amount_totals')
    amount_real_total = fields.Monetary(string="Facturado (total)",
                                        compute='_compute_amount_totals')
    amount_ordered_total = fields.Monetary(string="Pedido (total)",
                                           compute='_compute_amount_totals')
    fulfillment_pct = fields.Float(string="% Cumplimiento (importe)",
                                   compute='_compute_amount_totals')
    amount_real_unbudgeted = fields.Monetary(
        string="Facturado NO presupuestado",
        compute='_compute_unbudgeted',
        help="Facturación neta del equipo en el año que no matchea ninguna línea "
             "(ni global ni por cliente): total real del equipo menos el real "
             "capturado por las líneas. Producto/cliente vendido sin presupuestar.")
    qty_budget_text = fields.Char(string="Cantidad presupuestada (por unidad)",
                                  compute='_compute_qty_texts')
    qty_real_text = fields.Char(string="Cantidad facturada (por unidad)",
                                compute='_compute_qty_texts')
    unconverted_count = fields.Integer(
        string="Facturas sin convertir (otra unidad)",
        compute='_compute_unconverted_count',
        help="Líneas de factura del periodo cuya unidad es de otra categoría que "
             "la presupuestada: se cuentan en importe pero NO en cantidad. El "
             "hueco queda visible aquí para corregir la unidad.")
    amount_currency_text = fields.Char(
        string="Totales por divisa", compute='_compute_amount_currency_text',
        help="Totales POR divisa de las líneas cuya lista no es la moneda de la "
             "compañía. Nunca se suman entre sí: el único total global es en pesos.")
    no_price_count = fields.Integer(
        string="Productos sin precio de lista", compute='_compute_no_price_count',
        help="Líneas cuyo producto no tiene precio en la lista aplicable. Corrige "
             "LA LISTA de precios (no el presupuesto).")
    price_deviation_count = fields.Integer(
        string="Desviaciones de precio", compute='_compute_price_deviation_count',
        help="Líneas con desviación de precio leve o grave (facturado vs lista).")
    uncovered_count = fields.Integer(
        string="Semanas descubiertas", compute='_compute_uncovered_count',
        help="Líneas de pronóstico en horizonte sin pedido o con pedido parcial.")
    # KPI y justificación (P-A28 4.3.6.1): no bloquea nada; es evidencia del
    # análisis del incumplimiento, no un candado.
    nonfulfillment_note = fields.Text(
        string="Justificación de incumplimiento (F-P-A28 4.3.6.1)",
        help="Análisis del porqué del incumplimiento del presupuesto (bajo el "
             "umbral de cumplimiento). No bloquea: es justificación, no candado.")
    needs_justification = fields.Boolean(
        string="Requiere justificación", compute='_compute_needs_justification',
        help="El presupuesto aprobado va por debajo del umbral de cumplimiento "
             "(Ajustes → 'Cumplimiento mínimo del presupuesto') y aún no tiene "
             "justificación capturada.")

    @api.depends('fulfillment_pct', 'state', 'kind', 'amount_budget_total',
                 'nonfulfillment_note')
    def _compute_needs_justification(self):
        threshold = float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_fulfillment_min', 80) or 0)
        for budget in self:
            budget.needs_justification = bool(
                budget.kind == 'presupuesto' and budget.state == 'aprobado'
                and budget.amount_budget_total > 0
                and budget.fulfillment_pct < threshold
                and not (budget.nonfulfillment_note or '').strip())

    @api.depends('team_id', 'year', 'revision', 'kind', 'partner_id')
    def _compute_name(self):
        for budget in self:
            if budget.kind == 'pronostico':
                who = budget.partner_id.name or budget.team_id.name or "?"
                budget.name = "Pronóstico %s %s Rev.%s" % (
                    who, budget.year or "?", budget.revision or 1)
            else:
                team = budget.team_id.name or "?"
                budget.name = "Presupuesto %s %s Rev.%s" % (
                    team, budget.year or "?", budget.revision or 1)

    @api.depends('line_ids')
    def _compute_line_count(self):
        for budget in self:
            budget.line_count = len(budget.line_ids)

    @api.depends('line_ids.amount_budget', 'line_ids.amount_real',
                 'line_ids.amount_ordered')
    def _compute_amount_totals(self):
        for budget in self:
            lines = budget.line_ids
            budget.amount_budget_total = sum(lines.mapped('amount_budget'))
            budget.amount_real_total = sum(lines.mapped('amount_real'))
            budget.amount_ordered_total = sum(lines.mapped('amount_ordered'))
            budget.fulfillment_pct = (
                round(budget.amount_real_total / budget.amount_budget_total * 100.0, 2)
                if budget.amount_budget_total else 0.0)

    @api.depends('line_ids.qty_budget', 'line_ids.qty_real', 'line_ids.uom_id')
    def _compute_qty_texts(self):
        for budget in self:
            budget.qty_budget_text = budget._sgi_qty_text('qty_budget')
            budget.qty_real_text = budget._sgi_qty_text('qty_real')

    def _sgi_qty_text(self, field_name):
        """Totaliza una cantidad POR unidad (nunca mezcla): '12,500 m · 3,200 kg'."""
        self.ensure_one()
        by_uom = defaultdict(float)
        for line in self.line_ids:
            if line.uom_id:
                by_uom[line.uom_id] += line[field_name]
        parts = []
        for uom, qty in sorted(by_uom.items(), key=lambda kv: kv[0].name or ''):
            parts.append("%s %s" % ('{:,.2f}'.format(qty).rstrip('0').rstrip('.'),
                                    uom.name))
        return " · ".join(parts)

    @api.depends('line_ids.unconverted_count')
    def _compute_unconverted_count(self):
        for budget in self:
            budget.unconverted_count = sum(budget.line_ids.mapped('unconverted_count'))

    @api.depends('line_ids.amount_currency', 'line_ids.list_currency_id',
                 'amount_budget_total', 'currency_id')
    def _compute_amount_currency_text(self):
        for budget in self:
            company_ccy = budget.currency_id
            by_ccy = defaultdict(float)
            for line in budget.line_ids:
                if line.list_currency_id and line.list_currency_id != company_ccy:
                    by_ccy[line.list_currency_id] += line.amount_currency
            parts = ["%s %s" % (ccy.name, '{:,.0f}'.format(amt))
                     for ccy, amt in sorted(by_ccy.items(), key=lambda kv: kv[0].name)]
            parts.append("Total compañía %s %s" % (
                company_ccy.name or '', '{:,.0f}'.format(budget.amount_budget_total)))
            budget.amount_currency_text = " · ".join(parts)

    @api.depends('line_ids.has_list_price')
    def _compute_no_price_count(self):
        for budget in self:
            budget.no_price_count = len(
                budget.line_ids.filtered(lambda l: not l.has_list_price))

    def action_view_no_price(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Líneas sin precio de lista — corrige LA LISTA",
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('budget_id', '=', self.id), ('has_list_price', '=', False)],
        }

    @api.depends('line_ids.price_gap_alert')
    def _compute_price_deviation_count(self):
        for budget in self:
            budget.price_deviation_count = len(budget.line_ids.filtered(
                lambda l: l.price_gap_alert in ('leve', 'grave')))

    def action_view_price_deviations(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Desviaciones de precio — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('budget_id', '=', self.id),
                       ('price_gap_alert', 'in', ('leve', 'grave'))],
        }

    def action_price_coverage_report(self):
        """Auditoría de cobertura (en cualquier estado): las líneas sin precio de
        lista, para revisar ANTES de intentar aprobar."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Cobertura de precios — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('budget_id', '=', self.id), ('has_list_price', '=', False)],
        }

    # --- Cobertura del pronóstico (P-A28 4.2.2.7) ----------------------------
    @api.depends('line_ids.coverage_state')
    def _compute_uncovered_count(self):
        for budget in self:
            budget.uncovered_count = len(budget.line_ids.filtered(
                lambda l: l.coverage_state in ('sin_pedido', 'parcial')))

    def action_view_uncovered(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Semanas descubiertas — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('budget_id', '=', self.id),
                       ('coverage_state', 'in', ('sin_pedido', 'parcial'))],
        }

    def _sgi_horizon_mondays(self):
        """Lista de lunes del horizonte de captura (semana actual + N-1)."""
        self.ensure_one()
        horizon = int(float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.forecast_capture_horizon_weeks', 3) or 1))
        today = fields.Date.context_today(self)
        monday = today - timedelta(days=today.weekday())
        return [monday + timedelta(weeks=w) for w in range(horizon)]

    def _sgi_orders_without_forecast(self):
        """Control inverso (4.2.2.7): líneas de pedido confirmadas del cliente en
        semanas del horizonte SIN línea de pronóstico para ese producto+semana."""
        self.ensure_one()
        partner = self.partner_id.commercial_partner_id
        if self.kind != 'pronostico' or not partner:
            return self.env['sale.order.line']
        mondays = set(self._sgi_horizon_mondays())
        Line = self.env['sgi.sales.budget.line']
        forecast_keys = {(l.product_id.id, l.date) for l in self.line_ids}
        # Cota de fecha: sin ella se traía TODO el histórico de pedidos del
        # cliente para filtrar en Python. Un pedido con compromiso en el
        # horizonte de hoy no viene de órdenes de hace más de un año.
        date_floor = fields.Datetime.to_datetime(min(mondays)) - timedelta(days=365)
        sols = self.env['sale.order.line'].search([
            ('order_id.state', 'in', ('sale', 'done')),
            ('order_id.partner_id.commercial_partner_id', '=', partner.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('order_id.date_order', '>=', date_floor),
        ])
        result = self.env['sale.order.line']
        for sol in sols:
            monday = Line._sgi_effective_monday(sol.order_id)
            if monday in mondays and (sol.product_id.id, monday) not in forecast_keys:
                result |= sol
        return result

    def action_view_orders_without_forecast(self):
        self.ensure_one()
        orders = self._sgi_orders_without_forecast().mapped('order_id')
        return {
            'type': 'ir.actions.act_window',
            'name': "Pedidos fuera de pronóstico — %s" % self.name,
            'res_model': 'sale.order',
            'view_mode': 'list,form',
            'domain': [('id', 'in', orders.ids)],
        }

    def _sgi_team_year_real(self):
        """Facturación neta del equipo en todo el año del presupuesto (moneda
        compañía). Base para detectar lo vendido sin presupuestar."""
        self.ensure_one()
        if not self.team_id or not self.year:
            return 0.0
        moves = self.env['account.move'].sudo().search([
            ('move_type', 'in', ('out_invoice', 'out_refund')),
            ('state', '=', 'posted'),
            ('team_id', '=', self.team_id.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('invoice_date', '>=', date(self.year, 1, 1)),
            ('invoice_date', '<=', date(self.year, 12, 31)),
        ])
        return sum(moves.mapped('amount_untaxed_signed'))

    @api.depends('line_ids.amount_real', 'team_id', 'year')
    def _compute_unbudgeted(self):
        for budget in self:
            captured = sum(budget.line_ids.mapped('amount_real'))
            budget.amount_real_unbudgeted = budget._sgi_team_year_real() - captured

    # --- Conciliación facturado vs contabilidad (cuadre del presupuesto) ------
    def _sgi_reconcile_data(self):
        """Descompone el facturado contable del equipo en el año contra lo que
        capturaron las líneas del presupuesto, para explicar la brecha:

          A  = facturado contable del equipo (out_invoice/out_refund posted del
               año, amount_untaxed_signed);
          B  = suma de amount_real de las líneas del presupuesto (lo capturado);
          partidas que explican A − B (cada una con monto):
            · unbudgeted        productos facturados del equipo NO presupuestados;
            · no_product        líneas de factura sin producto (fletes/servicios);
            · by_client_others  facturado de productos presupuestados POR CLIENTE a
                                OTROS clientes distintos del presupuestado;
          residual = A − (B + partidas) → 0 si la foto real está fresca y cuadra;
          no_team  = informativo (FUERA de A): facturas del año SIN equipo de
                     ventas (la fuga principal del cuadre).

        Objetivo verificable: A = B + unbudgeted + no_product + by_client_others +
        residual. Trabaja sobre la foto real vigente (amount_real); refresca antes
        (action_refresh_actuals) para que el residuo sea solo redondeo."""
        self.ensure_one()
        result = {
            'A': 0.0, 'B': 0.0, 'unbudgeted': 0.0, 'no_product': 0.0,
            'by_client_others': 0.0, 'residual': 0.0, 'captured_aml': 0.0,
            'no_team': 0.0, 'currency': self.currency_id,
        }
        if self.kind != 'presupuesto':
            return result
        result['B'] = sum(self.line_ids.mapped('amount_real'))
        # Esquema presupuestado por producto (global vs por cliente).
        global_products = set()
        by_client = defaultdict(set)
        for line in self.line_ids:
            if line.partner_id:
                by_client[line.product_id.id].add(
                    line.partner_id.commercial_partner_id.id)
            else:
                global_products.add(line.product_id.id)
        moves = self.env['account.move'].sudo().search([
            ('move_type', 'in', _REAL_MOVE_TYPES),
            ('state', '=', 'posted'),
            ('team_id', '=', self.team_id.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('invoice_date', '>=', date(self.year, 1, 1)),
            ('invoice_date', '<=', date(self.year, 12, 31)),
        ])
        captured = 0.0
        for move in moves:
            result['A'] += move.amount_untaxed_signed
            cp = move.commercial_partner_id
            for aml in move.invoice_line_ids.filtered(
                    lambda l: l.display_type == 'product'):
                contrib = -aml.balance  # base sin impuestos, moneda compañía, signada
                product = aml.product_id
                if not product:
                    result['no_product'] += contrib
                elif product.id in global_products:
                    captured += contrib
                elif product.id in by_client:
                    if cp.id in by_client[product.id]:
                        captured += contrib
                    else:
                        result['by_client_others'] += contrib
                else:
                    result['unbudgeted'] += contrib
        result['captured_aml'] = captured
        result['residual'] = (
            result['A'] - result['B'] - result['unbudgeted']
            - result['no_product'] - result['by_client_others'])
        no_team_moves = self.env['account.move'].sudo().search([
            ('move_type', 'in', _REAL_MOVE_TYPES),
            ('state', '=', 'posted'),
            ('team_id', '=', False),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('invoice_date', '>=', date(self.year, 1, 1)),
            ('invoice_date', '<=', date(self.year, 12, 31)),
        ])
        result['no_team'] = sum(no_team_moves.mapped('amount_untaxed_signed'))
        return result

    def action_reconcile_invoiced(self):
        """Refresca la foto y publica en el chatter la tabla de conciliación del
        facturado contable vs el presupuesto (solo presupuesto)."""
        self.ensure_one()
        if self.kind != 'presupuesto':
            raise UserError(
                "La conciliación de facturado aplica al presupuesto (el pronóstico "
                "mide compromiso, no facturación).")
        self.action_refresh_actuals()
        d = self._sgi_reconcile_data()
        ccy = (d['currency'].name or '') + ' '

        def _m(v):
            return "%s%s" % (ccy, '{:,.2f}'.format(v))
        cuadra = abs(d['residual']) < 0.01
        body = (
            "<b>Conciliación facturado vs presupuesto — %s</b>"
            "<table class='table table-sm'>"
            "<tr><td><b>A. Facturado contable del equipo</b></td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td>B. Capturado por el presupuesto (facturado de las líneas)</td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td>+ Productos facturados NO presupuestados</td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td>+ Líneas de factura sin producto (fletes/servicios)</td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td>+ Presupuestado por cliente, facturado a OTROS clientes</td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td><b>= Residuo (A − B − partidas)</b></td>"
            "<td style='text-align:right'><b>%s</b></td></tr>"
            "</table>"
            "%s"
            "<i>Informativo (fuera de A): facturas del año SIN equipo de ventas: "
            "%s.</i>" % (
                self.year, _m(d['A']), _m(d['B']), _m(d['unbudgeted']),
                _m(d['no_product']), _m(d['by_client_others']), _m(d['residual']),
                ("<span class='text-success'>Cuadra exacto (residuo 0).</span><br/>"
                 if cuadra else
                 "<span class='text-danger'>No cuadra: revisa el residuo "
                 "(foto real desactualizada o redondeos).</span><br/>"),
                _m(d['no_team'])))
        self.message_post(body=body)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': "Conciliación de facturado",
                'message': "A=%s · B=%s · residuo=%s. Detalle en el chatter." % (
                    _m(d['A']), _m(d['B']), _m(d['residual'])),
                'type': 'success' if cuadra else 'warning',
                'sticky': False,
            },
        }

    # --- Constraint: un solo no-obsoleto por año+equipo(+cliente)+kind+compañía -
    @api.constrains('year', 'team_id', 'state', 'kind', 'partner_id', 'company_id')
    def _check_unique_active(self):
        for budget in self:
            if budget.state == 'obsoleto':
                continue
            dup = self.search([
                ('id', '!=', budget.id),
                ('year', '=', budget.year),
                ('team_id', '=', budget.team_id.id),
                ('kind', '=', budget.kind),
                ('partner_id', '=', budget.partner_id.id),
                ('company_id', '=', budget.company_id.id),  # por compañía del grupo
                ('state', '!=', 'obsoleto'),
            ], limit=1)
            if dup:
                raise ValidationError(
                    "Ya existe un %s no obsoleto de %s para %s (%s). Revísalo "
                    "(crea una nueva Rev.) en vez de duplicarlo." % (
                        dict(self._fields['kind'].selection)[budget.kind],
                        budget.partner_id.name or budget.team_id.name,
                        budget.year, dup.folio or dup.name))

    @api.constrains('kind', 'partner_id')
    def _check_forecast_partner(self):
        for budget in self:
            if budget.kind == 'pronostico' and not budget.partner_id:
                raise ValidationError(
                    "Un pronóstico semanal necesita un cliente (cada hoja del "
                    "forecast es un cliente-año).")

    def unlink(self):
        # Borra las líneas por ORM antes del DELETE en cascada de BD: así se
        # descartan sus cómputos pendientes (los Monetary con currency_field
        # calculado, p.ej. amount_currency/list_currency_id) y no se intenta hacer
        # flush sobre registros ya eliminados (evita MissingError al borrar).
        self.with_context(sgi_bypass_lock=True).mapped('line_ids').unlink()
        return super().unlink()

    def _sgi_locked_records(self):
        """El candado de inmutabilidad (estado 'aprobado') aplica SOLO al
        presupuesto: el pronóstico es un documento vivo (P-A28 4.2.2.7) y nunca
        se congela, así que se excluye aunque hubiera quedado en 'aprobado'
        (pre-migración)."""
        return super()._sgi_locked_records().filtered(
            lambda b: b.kind != 'pronostico')

    # --- Flujo de estados -----------------------------------------------------
    def _sgi_no_price_pairs(self, limit=10):
        """(texto de hasta `limit` pares 'producto — cliente' sin precio, total)."""
        self.ensure_one()
        lines = self.line_ids.filtered(lambda l: not l.has_list_price)
        pairs = []
        for line in lines[:limit]:
            product = line.product_id.default_code or line.product_id.name or ''
            client = line.partner_id.name or "Sin cliente"
            pairs.append("%s — %s" % (product, client))
        return pairs, len(lines)

    def _sgi_can_review(self):
        """Admin de ventas (o Jefe MAST/SGI como fallback de sistema): manda a
        revisión el presupuesto y marca revisado el pronóstico (P-A28 4.2.2.3)."""
        return (self.env.user.has_group('sales_team.group_sale_manager')
                or self.env.user.has_group('quimibond_sgi.group_sgi_manager'))

    def action_send_to_review(self):
        """borrador → revisado. Presupuesto: 'Enviar a revisión' (Admin de ventas,
        4.2.2.3). Pronóstico: 'Marcar revisado' (mismo permiso). El gate de precios
        de la 5.4 corre aquí como AVISO (no bloquea): el bloqueo es al aprobar."""
        if not self._sgi_can_review():
            raise UserError(
                "Solo el Administrador de ventas (o el Jefe de MAST/SGI) puede "
                "enviar a revisión.")
        for budget in self:
            if budget.state != 'borrador':
                raise UserError(
                    "Solo se envía a revisión desde borrador. El %s está en '%s'." % (
                        budget.folio or budget.name,
                        dict(self._fields['state'].selection)[budget.state]))
            if not budget.line_ids:
                raise UserError(
                    "El %s no tiene líneas: captura la matriz antes de enviarlo a "
                    "revisión." % (budget.folio or budget.name))
            # Aviso (no bloqueo) de cobertura de precios en el presupuesto.
            if budget.kind == 'presupuesto' and budget.no_price_count > 0:
                pairs, total = budget._sgi_no_price_pairs()
                budget.message_post(
                    body="AVISO de cobertura de precios al enviar a revisión: %d "
                         "producto(s) sin precio en la lista. Corrígelos ANTES de "
                         "aprobar (ahí sí bloquea).<br/>- %s" % (
                             total, "<br/>- ".join(pairs)))
            budget.state = 'revisado'
        return True

    def action_approve(self):
        """revisado → aprobado (solo presupuesto). Aprueba la ALTA DIRECCIÓN
        (P-A28 4.2.2.4): grupo Dirección o Jefe MAST/SGI (fallback de sistema).
        Exige haber pasado por 'revisado'. Bloquea si hay productos sin precio de
        lista (gate 5.4), salvo contexto sgi_bypass_price_check, que deja constancia
        en el chatter."""
        can_approve = self.env.user.has_group('quimibond_sgi.group_sgi_director') \
            or self.env.user.has_group('quimibond_sgi.group_sgi_manager')
        bypass_price = self.env.context.get('sgi_bypass_price_check')
        for budget in self:
            if budget.kind != 'presupuesto':
                raise UserError(
                    "El pronóstico es un documento vivo: no se aprueba (P-A28 "
                    "4.2.2.7). Solo se marca revisado.")
            if not can_approve:
                raise UserError(
                    "Solo la alta dirección (grupo Dirección) o el Jefe de MAST/SGI "
                    "puede aprobar un presupuesto de ventas.")
            if budget.state != 'revisado':
                raise UserError(
                    "Un presupuesto debe estar REVISADO antes de aprobarse "
                    "(P-A28 4.2.2.3 → 4.2.2.4). El %s está en '%s'." % (
                        budget.folio or budget.name,
                        dict(self._fields['state'].selection)[budget.state]))
            if not budget.line_ids:
                raise UserError(
                    "El presupuesto %s no tiene líneas: captura la matriz antes de "
                    "aprobarlo." % (budget.folio or budget.name))
            if budget.no_price_count > 0:
                pairs, total = budget._sgi_no_price_pairs()
                if not bypass_price:
                    more = "\n…y %d más." % (total - len(pairs)) if total > len(pairs) else ""
                    raise UserError(
                        "No se puede aprobar %s: %d producto(s) sin precio en la "
                        "lista.\n\n- %s%s\n\nDa de alta el precio en la lista del "
                        "cliente o aprueba con excepción (constancia en chatter)." % (
                            budget.name, total, "\n- ".join(pairs), more))
                budget.message_post(
                    body="Aprobado con EXCEPCIÓN de cobertura de precios: %d "
                         "producto(s) sin precio en la lista.<br/>- %s" % (
                             total, "<br/>- ".join(pairs)))
            budget.state = 'aprobado'
        return True

    def action_set_borrador(self):
        self.write({'state': 'borrador'})
        return True

    def action_set_obsoleto(self):
        self.write({'state': 'obsoleto'})
        return True

    def action_revise(self):
        """Revisión de junio del P-A28: crea la Rev. siguiente en borrador
        copiando las líneas y obsoleta la anterior. La historia se conserva:
        NUNCA se pisa lo aprobado. Solo MAST."""
        self.ensure_one()
        if self.kind == 'pronostico':
            raise UserError(
                "El pronóstico es un documento vivo (P-A28 4.2.2.7): no se generan "
                "revisiones congeladas. Edita sus líneas directamente; un pronóstico "
                "revisado vuelve a borrador al tocarlo.")
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe de MAST y SGI puede revisar un presupuesto.")
        if self.state != 'aprobado':
            raise UserError(
                "Solo se revisa un presupuesto aprobado. El %s está en '%s'." % (
                    self.folio or self.name, dict(self._fields['state'].selection)[self.state]))
        # Obsoletar la vigente primero para no chocar con el constraint de unicidad.
        self.state = 'obsoleto'
        new = self.copy({
            'state': 'borrador',
            'revision': self.revision + 1,
            'folio': False,
        })
        return {
            'type': 'ir.actions.act_window',
            'name': new.name,
            'res_model': 'sgi.sales.budget',
            'res_id': new.id,
            'view_mode': 'form',
        }

    def action_open_lines(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Líneas — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('budget_id', '=', self.id)],
            'context': {'default_budget_id': self.id},
        }

    def _action_grid(self, view_xmlid, name):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — %s" % (name, self.name),
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'grid,list,form',
            'views': [
                (self.env.ref(view_xmlid).id, 'grid'),
                (False, 'list'), (False, 'form')],
            'domain': [('budget_id', '=', self.id)],
            'context': {'default_budget_id': self.id,
                        'grid_anchor': fields.Date.to_string(
                            fields.Date.to_date('%s-01-01' % self.year))},
        }

    def action_open_grid_qty(self):
        return self._action_grid(
            'quimibond_sgi.sgi_sales_budget_line_grid_qty', "Cantidades")

    def action_open_comparison(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Comparación (ppto vs facturado vs pedido) — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'pivot,graph,list',
            'domain': [('budget_id', '=', self.id)],
            'context': {'default_budget_id': self.id,
                        'search_default_group_uom': 1},
        }

    def action_open_analysis(self):
        """Botón inteligente: abre el análisis por cliente filtrado a ESTE
        presupuesto (sin el filtro de vigente: ya estás en una revisión concreta)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Análisis por cliente — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'pivot,list',
            'views': [
                (self.env.ref(
                    'quimibond_sgi.sgi_sales_analysis_pivot_cliente').id, 'pivot'),
                (self.env.ref(
                    'quimibond_sgi.sgi_sales_budget_line_view_list').id, 'list')],
            'domain': [('budget_id', '=', self.id)],
            'context': {'search_default_group_partner': 1},
        }

    def action_open_cumulative(self):
        """Curva acumulada mes a mes (presupuesto vs facturado YTD) — la gráfica
        de la Revisión por la Dirección."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Curva acumulada — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'graph',
            'views': [(self.env.ref(
                'quimibond_sgi.sgi_sales_budget_line_view_graph_curve').id, 'graph')],
            'domain': [('budget_id', '=', self.id)],
        }

    # --- Matriz para el reporte F-P-A28-18 -----------------------------------
    def _report_matrix(self):
        """Estructura producto × 12 meses para el QWeb, agrupada por producto. Un
        producto presupuestado por cliente se desglosa en una fila por cliente con
        subtotal de producto; uno global lleva una sola fila. Las cantidades no se
        totalizan globalmente (unidades distintas); el total global es el de pesos."""
        self.ensure_one()
        months = list(range(1, 13))
        groups = {}
        for line in self.line_ids.sorted(lambda l: (
                l.product_id.default_code or '', l.product_id.name or '',
                l.partner_id.name or '', l.date)):
            grp = groups.get(line.product_id.id)
            if not grp:
                grp = {
                    'product': line.product_id.display_name,
                    'by_client': False,
                    'rows': {},
                    'subtotal_amount': 0.0,
                }
                groups[line.product_id.id] = grp
            if line.partner_id:
                grp['by_client'] = True
            rkey = (line.partner_id.id, line.uom_id.id)
            row = grp['rows'].get(rkey)
            if not row:
                row = {
                    'client': line.partner_id.name or '',
                    'uom': line.uom_id.name or '',
                    'cells': {m: 0.0 for m in months},
                    'qty_total': 0.0,
                    'amount_total': 0.0,
                    'qty_real': 0.0,
                    'amount_real': 0.0,
                    'amount_currency': 0.0,
                    'list_currency': line.list_currency_id,
                }
                grp['rows'][rkey] = row
            row['cells'][line.date.month] += line.qty_budget
            row['qty_total'] += line.qty_budget
            row['amount_total'] += line.amount_budget
            row['qty_real'] += line.qty_real
            row['amount_real'] += line.amount_real
            row['amount_currency'] += line.amount_currency
            if line.list_currency_id:
                row['list_currency'] = line.list_currency_id
            grp['subtotal_amount'] += line.amount_budget
        ordered = []
        for grp in groups.values():
            rows = list(grp['rows'].values())
            for row in rows:
                row['price_budget'] = (
                    row['amount_total'] / row['qty_total'] if row['qty_total'] else 0.0)
                row['price_real'] = (
                    row['amount_real'] / row['qty_real'] if row['qty_real'] else 0.0)
            grp['rows'] = rows
            ordered.append(grp)
        return {
            'months': months,
            'groups': ordered,
            'qty_by_uom': self.qty_budget_text,
            'amount_total': self.amount_budget_total,
        }

    def _report_forecast_matrix(self):
        """Estructura del pronóstico F-P-A28-13 para el QWeb: producto+código de
        cliente × semanas (las presentes en las líneas, con su mes), con la
        cantidad presupuestada y la comprometida (real) por semana."""
        self.ensure_one()
        jan1 = date(self.year, 1, 1)
        first_monday = jan1 + timedelta(days=(7 - jan1.weekday()) % 7)
        weeks = {}
        rows = {}
        for line in self.line_ids.sorted(lambda l: (
                l.product_id.default_code or '', l.customer_code or '', l.date)):
            week = ((line.date - first_monday).days // 7) + 1
            weeks[week] = line.date.month
            key = (line.product_id.id, line.customer_code or '', line.uom_id.id)
            row = rows.get(key)
            if not row:
                row = {
                    'product': line.product_id.display_name,
                    'code': line.customer_code or '',
                    'uom': line.uom_id.name or '',
                    'budget': {}, 'real': {}, 'net': {},
                }
                rows[key] = row
            row['budget'][week] = row['budget'].get(week, 0.0) + line.qty_budget
            row['real'][week] = row['real'].get(week, 0.0) + line.qty_real
            row['net'][week] = row['net'].get(week, 0.0) + line.qty_net_demand
        weeks_sorted = sorted(weeks)
        return {
            'weeks': [{'num': w, 'month': weeks[w]} for w in weeks_sorted],
            'rows': list(rows.values()),
            'partner': self.partner_id.name or '',
        }

    # Banner de formato según el tipo: pronóstico = F-P-A28-13, presupuesto = 18.
    def _sgi_format_code(self, fmap):
        self.ensure_one()
        if self.kind == 'pronostico':
            return fmap.sgi_code_alt or 'F-P-A28-13'
        return fmap.sgi_code

    def action_print_budget(self):
        self.ensure_one()
        return self.env.ref(
            'quimibond_sgi.action_report_sales_budget').report_action(self)

    # --- Plantilla descargable (filas ya puestas) ----------------------------
    _SGI_TEMPLATE_MONTHS = [
        'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO',
        'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']

    def _sgi_historical_products(self):
        """Productos vendidos a ESTE cliente en 24 meses (pronóstico) — para
        pre-llenar las filas del forecast."""
        self.ensure_one()
        from dateutil.relativedelta import relativedelta as _rd
        date_from = fields.Date.context_today(self) - _rd(months=24)
        partner = self.partner_id.commercial_partner_id
        if not partner:
            return self.env['product.product']
        products = self.env['sale.order.line'].search([
            ('order_id.state', 'in', ('sale', 'done')),
            ('order_id.date_order', '>=', date_from),
            ('order_id.partner_id.commercial_partner_id', '=', partner.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
        ]).mapped('product_id')
        # sudo puntual: un producto de otra compañía (mal configurado) pudo colarse
        # en pedidos de esta compañía; leer default_code/name para ordenar reventaría
        # el prefetch multicompañía (AccessError). Solo se leen etiquetas, no datos
        # sensibles.
        return products.sudo().sorted(lambda p: p.default_code or p.name or '')

    def _sgi_historical_pairs(self):
        """[(cliente comercial, producto)] FACTURADOS por el equipo en los últimos
        24 meses, ordenado por cliente y luego producto — filas del presupuesto."""
        self.ensure_one()
        from dateutil.relativedelta import relativedelta as _rd
        date_from = fields.Date.context_today(self) - _rd(months=24)
        amls = self.env['account.move.line'].search([
            ('parent_state', '=', 'posted'),
            ('move_id.move_type', 'in', _REAL_MOVE_TYPES),
            ('move_id.team_id', '=', self.team_id.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('move_id.invoice_date', '>=', date_from),
            ('product_id', '!=', False),
        ])
        pairs = {(aml.move_id.commercial_partner_id, aml.product_id) for aml in amls}
        # sudo al ordenar: un producto de otra compañía en facturas de esta compañía
        # reventaría el prefetch (AccessError); solo se leen etiquetas para ordenar.
        return sorted(pairs, key=lambda cp: (
            cp[0].name or '', cp[1].sudo().default_code or cp[1].sudo().name or ''))

    def action_download_template(self):
        """Genera con openpyxl el Excel VACÍO con las FILAS ya puestas (productos
        históricos) para llenar cantidades y subir con el importador."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("La plantilla se descarga sobre un presupuesto en borrador.")
        import base64
        import io
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        if self.kind == 'pronostico':
            ws.title = (self.partner_id.name or 'Pronostico')[:31]
            ws.append(['PRODUCTO', 'CODIGO CLIENTE', 'SEMANA']
                      + list(range(1, 53)))
            for product in self._sgi_historical_products():
                product = product.sudo()  # prefetch multicompañía: solo etiqueta
                ws.append([product.default_code or product.name, '', ''] + [''] * 52)
        else:
            ws.title = (self.team_id.name or 'Presupuesto')[:31]
            months = ['%s m' % m for m in self._SGI_TEMPLATE_MONTHS]
            # Instrucción discreta (fila sin producto: el importador la ignora).
            ws.append(['', "Instrucciones: deja CLIENTE vacío para presupuestar "
                       "global; un mismo material no puede ir con cliente y global "
                       "a la vez."])
            ws.append(['PRODUCTO', 'CLIENTE', 'UNIDAD'] + months)
            for partner, product in self._sgi_historical_pairs():
                product = product.sudo()  # prefetch multicompañía: uom/código
                ws.append([product.default_code or product.name, partner.name,
                           product.uom_id.name] + [''] * 12)
            # Bloque de filas libres para clientes/productos nuevos (producto vacío
            # → el importador las ignora si quedan sin llenar).
            ws.append(['', '— Filas libres para clientes/productos nuevos —'])
            for _dummy in range(10):
                ws.append([''])
        buf = io.BytesIO()
        wb.save(buf)
        attachment = self.env['ir.attachment'].create({
            'name': 'Plantilla %s.xlsx' % (self.name or self.folio),
            'datas': base64.b64encode(buf.getvalue()),
            'res_model': 'sgi.sales.budget', 'res_id': self.id,
            'mimetype': 'application/vnd.openxmlformats-officedocument.'
                        'spreadsheetml.sheet',
        })
        return {
            'type': 'ir.actions.act_url',
            'url': '/web/content/%d?download=true' % attachment.id,
            'target': 'self',
        }

    def action_refresh_actuals(self):
        """Recalcula la foto de facturado/pedido Y el precio de lista de las líneas
        (los computes almacenados no se refrescan solos al cambiar facturas o la
        lista de precios). El precio solo cambia en borradores (ver _compute_price)."""
        lines = self.mapped('line_ids')
        # El precio solo se refresca en borradores: lo aprobado queda congelado.
        lines.filtered(lambda l: l.budget_id.state == 'borrador')._compute_price()
        lines._compute_real()
        lines._compute_ordered()
        return True

    # --- Consumo de pronóstico → demanda al MPS ------------------------------
    sgi_mps_available = fields.Boolean(compute='_compute_mps_available')

    def _compute_mps_available(self):
        available = 'mrp.production.schedule' in self.env
        for budget in self:
            budget.sgi_mps_available = available

    def _sgi_committed_by_week(self):
        """{(product, lunes): cantidad en la unidad DE VENTA del producto} de los
        pedidos confirmados del cliente del pronóstico, por semana comprometida."""
        self.ensure_one()
        result = defaultdict(float)
        partner = self.partner_id.commercial_partner_id
        if not partner:
            return result
        Line = self.env['sgi.sales.budget.line']
        # Cota de fecha: solo interesan los lunes del año del pronóstico; un
        # pedido ordenado antes del año previo no compromete semanas de este
        # año (antes se traía y filtraba TODO el histórico del cliente).
        sols = self.env['sale.order.line'].search([
            ('order_id.state', 'in', ('sale', 'done')),
            ('order_id.partner_id.commercial_partner_id', '=', partner.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('order_id.date_order', '>=',
             fields.Datetime.to_datetime(date(self.year - 1, 1, 1))),
        ])
        for sol in sols:
            monday = Line._sgi_effective_monday(sol.order_id)
            if not monday or monday.year != self.year:
                continue
            product = sol.product_id
            # sudo puntual: product de otra compañía en pedidos de esta compañía
            # (mal configurado) reventaría el prefetch al leer uom_id (AccessError).
            conv = _convert_qty(
                sol.product_uom_qty, sol.product_uom_id, product.sudo().uom_id)
            result[(product, monday)] += (
                conv if conv is not None else sol.product_uom_qty)
        return result

    def action_preload_from_orders(self):
        """Precarga propuesta (solo borrador), diferenciada por kind (P-A28 4.2.2.1):
          · pronóstico → semanas con pedidos confirmados y sin celda, con lo
            comprometido (el vendedor solo captura el futuro);
          · presupuesto → qty por producto(+cliente) por mes combinando los
            pronósticos vigentes del año (semana→mes) y el facturado real de los
            últimos 12 meses.
        Idempotente: nunca pisa celdas ya capturadas."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("Solo se precarga un %s en borrador." % (
                dict(self._fields['kind'].selection)[self.kind]))
        if self.kind == 'pronostico':
            return self._preload_forecast_from_orders()
        return self._preload_budget_from_forecast_and_real()

    def _preload_forecast_from_orders(self):
        """Pronóstico: crea las semanas con pedidos confirmados y sin celda de
        pronóstico, con qty_budget = lo comprometido. No toca lo ya capturado."""
        self.ensure_one()
        Line = self.env['sgi.sales.budget.line']
        existing = {(l.product_id.id, l.date) for l in self.line_ids}
        created = 0
        for (product, monday), qty in self._sgi_committed_by_week().items():
            if qty <= 0 or (product.id, monday) in existing:
                continue
            Line.create({
                'budget_id': self.id, 'product_id': product.id, 'date': monday,
                'uom_id': product.sudo().uom_id.id,  # prefetch multicompañía
                'partner_id': self.partner_id.id, 'qty_budget': qty,
            })
            created += 1
        self.message_post(
            body="Precarga desde pedidos: %d celda(s) creada(s) con lo "
                 "comprometido (el vendedor solo captura el futuro)." % created)
        return True

    def _preload_budget_proposals(self):
        """{(product, commercial_partner, month): qty en la unidad de venta del
        producto} propuesta para el presupuesto, combinando (a) los pronósticos
        vigentes del año (agregando semana→mes) y (b) el facturado real de los
        últimos 12 meses. Combinación = max(pronóstico, real) por celda: no
        planear por debajo de lo pronosticado ni de lo que la historia demuestra."""
        self.ensure_one()
        from dateutil.relativedelta import relativedelta as _rd

        def _to_sale_uom(product, uom, qty):
            # sudo puntual: el producto puede venir de facturas y ser de otra
            # compañía (mal configurado); leer uom_id sin sudo reventaría el
            # prefetch multicompañía (AccessError). Solo se lee la unidad.
            conv = _convert_qty(qty, uom, product.sudo().uom_id)
            return conv if conv is not None else qty

        forecast_qty = defaultdict(float)
        forecasts = self.env['sgi.sales.budget'].search([
            ('kind', '=', 'pronostico'), ('year', '=', self.year),
            ('state', '!=', 'obsoleto'), ('company_id', '=', self.company_id.id)])
        for line in forecasts.mapped('line_ids'):
            if not line.product_id or not line.date or line.qty_budget <= 0:
                continue
            partner = line.partner_id.commercial_partner_id
            key = (line.product_id, partner, line.date.month)
            forecast_qty[key] += _to_sale_uom(
                line.product_id, line.uom_id, line.qty_budget)

        real_qty = defaultdict(float)
        date_from = fields.Date.context_today(self) - _rd(months=12)
        amls = self.env['account.move.line'].sudo().search([
            ('parent_state', '=', 'posted'),
            ('move_id.move_type', 'in', _REAL_MOVE_TYPES),
            ('move_id.team_id', '=', self.team_id.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('move_id.invoice_date', '>=', date_from),
            ('product_id', '!=', False),
        ])
        for aml in amls:
            product = aml.product_id
            month = aml.move_id.invoice_date.month
            partner = aml.move_id.commercial_partner_id
            sign = 1.0 if aml.move_id.move_type == 'out_invoice' else -1.0
            real_qty[(product, partner, month)] += _to_sale_uom(
                product, aml.product_uom_id, sign * aml.quantity)

        proposals = {}
        for key in set(forecast_qty) | set(real_qty):
            qty = max(forecast_qty.get(key, 0.0), real_qty.get(key, 0.0))
            if qty > 0:
                proposals[key] = qty
        return proposals

    def _preload_budget_from_forecast_and_real(self):
        """Presupuesto: crea líneas por producto+cliente+mes con la cantidad
        propuesta (ver _preload_budget_proposals). Respeta el anti-doble-conteo
        (omite productos que ya están capturados como global) y nunca pisa celdas
        existentes."""
        self.ensure_one()
        Line = self.env['sgi.sales.budget.line']
        existing = {(l.product_id.id, (l.partner_id.commercial_partner_id.id
                                       if l.partner_id else False), l.date)
                    for l in self.line_ids}
        # Producto ya presente como GLOBAL (sin cliente): no se puede mezclar con
        # líneas por cliente (constraint anti-doble-conteo) → se omite.
        global_products = {l.product_id.id for l in self.line_ids if not l.partner_id}
        created = skipped_scheme = skipped_company = 0
        for (product, partner, month), qty in sorted(
                self._preload_budget_proposals().items(),
                key=lambda kv: (kv[0][0].id, (kv[0][1].id if kv[0][1] else 0), kv[0][2])):
            when = date(self.year, month, 1)
            partner_id = partner.id if partner else False
            if (product.id, partner_id, when) in existing:
                continue
            # Producto de OTRA compañía facturado en esta (mal configurado): no se
            # presupuesta (crear la línea reventaría al leer su producto foráneo).
            prod_company = product.sudo().company_id
            if prod_company and prod_company.id != self.company_id.id:
                skipped_company += 1
                continue
            if product.id in global_products:
                skipped_scheme += 1
                continue
            Line.create({
                'budget_id': self.id, 'product_id': product.id, 'date': when,
                'uom_id': product.sudo().uom_id.id,  # prefetch multicompañía
                'partner_id': partner_id, 'qty_budget': qty,
            })
            existing.add((product.id, partner_id, when))
            created += 1
        note = ("Precarga (pronóstico vigente + facturado 12 m): %d línea(s) "
                "propuesta(s) por producto/cliente/mes (combinación = máximo de "
                "pronosticado y real; el comercial ajusta)." % created)
        if skipped_scheme:
            note += (" %d omitida(s): el producto ya está capturado como global "
                     "(no se mezcla con líneas por cliente)." % skipped_scheme)
        if skipped_company:
            note += (" %d omitida(s): producto de otra compañía facturado en "
                     "esta (mal configurado)." % skipped_company)
        self.message_post(body=note)
        return True

    def _sgi_forecast_covered_products(self):
        """IDs de productos cubiertos por un pronóstico REVISADO del mismo año y
        compañía. Esos productos los manda el pronóstico al MPS con su demanda neta;
        el presupuesto NO debe volverlos a enviar (anti-doble conteo, P-A28 4.2.1 +
        4.2.2.5). Solo cuenta el pronóstico 'revisado': un borrador NO puede enviar
        demanda (action_send_to_mps exige 'revisado'), así que omitir sus productos
        del presupuesto los dejaría SIN demanda en el MPS."""
        self.ensure_one()
        forecasts = self.env['sgi.sales.budget'].search([
            ('kind', '=', 'pronostico'), ('year', '=', self.year),
            ('state', '=', 'revisado'), ('company_id', '=', self.company_id.id)])
        return set(forecasts.mapped('line_ids.product_id').ids)

    def _sgi_mps_warehouse(self):
        self.ensure_one()
        warehouse = self.env['stock.warehouse'].search(
            [('company_id', '=', self.company_id.id)], limit=1)
        if not warehouse:
            raise UserError("No hay almacén configurado para la compañía.")
        return warehouse

    def _sgi_push_forecast_cells(self, warehouse, demand):
        """Vuelca {(product, date): qty en unidad de venta} al forecast del MPS.
        Crea el schedule si falta; re-envío actualiza sin duplicar. Devuelve el
        detalle por celda (texto)."""
        Schedule = self.env['mrp.production.schedule']
        Forecast = self.env['mrp.product.forecast']
        details = []
        for (product, when), qty in sorted(
                demand.items(), key=lambda kv: (kv[0][0].id, kv[0][1])):
            sched = Schedule.search([
                ('product_id', '=', product.id),
                ('warehouse_id', '=', warehouse.id)], limit=1)
            if not sched:
                sched = Schedule.create(
                    {'product_id': product.id, 'warehouse_id': warehouse.id})
            forecast = sched.forecast_ids.filtered(lambda f: f.date == when)[:1]
            if forecast:
                forecast.forecast_qty = qty  # re-envío: actualiza sin duplicar
            else:
                Forecast.create({
                    'production_schedule_id': sched.id, 'date': when,
                    'forecast_qty': qty})
            details.append("%s · %s: %s %s" % (
                product.default_code or product.name, when,
                round(qty, 2), product.uom_id.name))
        return details

    def action_send_to_mps(self):
        """Vuelca la demanda al forecast del Programa Maestro (mrp_mps),
        diferenciada por kind:
          · pronóstico → DEMANDA NETA por producto/semana (no el bruto);
          · presupuesto → qty presupuestada por producto/mes, EXCLUYENDO los
            productos cubiertos por un pronóstico vigente del mismo periodo (esos
            los manda el pronóstico) — anti-doble conteo P-A28 4.2.1 / 4.2.2.5.
        No crea pedidos de venta; el re-envío actualiza sin duplicar."""
        self.ensure_one()
        if 'mrp.production.schedule' not in self.env:
            raise UserError("El módulo Programa Maestro (mrp_mps) no está instalado.")
        if self.kind == 'pronostico':
            return self._send_forecast_to_mps()
        return self._send_budget_to_mps()

    def _send_forecast_to_mps(self):
        self.ensure_one()
        if self.state != 'revisado':
            raise UserError(
                "Solo se envía la demanda de un pronóstico revisado (P-A28 "
                "4.2.2.3): el pronóstico no se aprueba, se marca revisado.")
        warehouse = self._sgi_mps_warehouse()
        demand = defaultdict(float)
        for line in self.line_ids:
            if line.qty_net_demand <= 0:
                continue
            conv = _convert_qty(
                line.qty_net_demand, line.uom_id, line.product_id.uom_id)
            demand[(line.product_id, line.date)] += (
                conv if conv is not None else line.qty_net_demand)
        details = self._sgi_push_forecast_cells(warehouse, demand)
        self.message_post(
            body="Demanda neta enviada al Programa Maestro (%d celdas):<br/>%s" % (
                len(details), "<br/>".join(details) or "sin demanda"))
        return True

    def _send_budget_to_mps(self):
        self.ensure_one()
        if self.state != 'aprobado':
            raise UserError("Solo se envía la demanda de un presupuesto aprobado.")
        warehouse = self._sgi_mps_warehouse()
        covered = self._sgi_forecast_covered_products()
        demand = defaultdict(float)
        omitted = self.env['product.product']
        for line in self.line_ids:
            if line.qty_budget <= 0:
                continue
            if line.product_id.id in covered:
                omitted |= line.product_id  # lo manda el pronóstico vigente
                continue
            conv = _convert_qty(
                line.qty_budget, line.uom_id, line.product_id.uom_id)
            demand[(line.product_id, line.date)] += (
                conv if conv is not None else line.qty_budget)
        details = self._sgi_push_forecast_cells(warehouse, demand)
        omitted_txt = ", ".join(
            p.default_code or p.name for p in omitted) or "ninguno"
        self.message_post(
            body="Presupuesto → Programa Maestro: enviados %d celda(s); omitidos "
                 "%d producto(s) por pronóstico vigente (los manda el pronóstico "
                 "con su demanda neta): %s.<br/>%s" % (
                     len(details), len(omitted), omitted_txt,
                     "<br/>".join(details) or "sin demanda propia del presupuesto"))
        return True

    def action_open_import(self):
        """Abre el asistente de importación del Excel F-P-A28-18 (solo borrador)."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("Solo se importa sobre un presupuesto en borrador.")
        return {
            'type': 'ir.actions.act_window',
            'name': "Importar desde Excel — %s" % self.name,
            'res_model': 'sgi.sales.budget.import',
            'view_mode': 'form',
            'target': 'new',
            'context': {'active_id': self.id, 'default_budget_id': self.id},
        }
