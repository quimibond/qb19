# -*- coding: utf-8 -*-
"""Propuesta del presupuesto del año siguiente (19.0.3.7.0).

Arma el borrador del presupuesto de un año a partir de lo vendido en los últimos
12 meses cerrados (o en el año anterior completo), con la misma regla de venta
que el VE-02 y el cumplimiento (cuentas de venta, equipos de los mercados, sin
líneas de costo):

- Industrial y Especiales: un renglón por producto, cliente y mes.
- Confección: un renglón por producto y mes, sin cliente (se presupuesta por
  promedios; no hay releases).
- Cantidad = la vendida ese mes × (1 + crecimiento del mercado).
- Precio = el de la lista del cliente (o la presupuestal); si la lista no tiene
  regla para el producto, el precio real promedio del periodo base en la moneda
  en que se facturó («Precio real del periodo base»).
- Tipo de cambio de los 12 meses: el presupuestal de Ajustes.
- Opcional: las líneas de proyecto de otro presupuesto, movidas al año nuevo.

No entran saldos ni desperdicio, notas de crédito genéricas ni servicios, ni lo
de clientes o productos por debajo del mínimo anual que se elija. El resultado
queda en el chatter del presupuesto nuevo; Jessica ajusta renglón por renglón.
"""
from collections import defaultdict
from datetime import date

from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_sales_budget import MERCADOS_CON_CLIENTE, _convert_qty, team_mercado

BASES = [('ultimos12', "Últimos 12 meses cerrados"), ('anio', "Año anterior completo")]
LABELS = {'industrial': "Industrial", 'confeccion': "Confección", 'especiales': "Especiales"}
PROJECT_FIELDS = ('mercado', 'partner_id', 'product_id', 'proyecto_descripcion', 'proyecto_gramaje',
                  'proyecto_ancho_cm', 'uom_id', 'qty_budget', 'price_input', 'price_currency_id',
                  'price_basis', 'price_origen')


class QbSalesBudgetProposal(models.TransientModel):
    _name = 'qb.sales.budget.proposal'
    _description = "Propuesta del presupuesto de ventas"

    year = fields.Integer(string="Año del presupuesto", required=True,
                          default=lambda self: fields.Date.context_today(self).year + 1)
    base = fields.Selection(BASES, string="Base", required=True, default='ultimos12',
                            help="De dónde salen las cantidades: los 12 meses cerrados antes de hoy "
                                 "(cada mes va al mismo mes del año nuevo) o el año anterior completo.")
    date_from = fields.Date(string="Desde", compute='_compute_period')
    date_to = fields.Date(string="Hasta", compute='_compute_period')
    growth_industrial = fields.Float(string="Crecimiento Industrial (%)", digits=(6, 1))
    growth_confeccion = fields.Float(string="Crecimiento Confección (%)", digits=(6, 1))
    growth_especiales = fields.Float(string="Crecimiento Especiales (%)", digits=(6, 1))
    min_amount = fields.Float(
        string="Mínimo anual por renglón (MXN)", default=0.0,
        help="No se proponen las combinaciones (producto y cliente; en Confección, producto) que "
             "vendieron menos de esto en todo el periodo base. Cero: todas.")
    project_source_id = fields.Many2one(
        'sgi.sales.budget', string="Traer proyectos de",
        domain="[('kind', '=', 'presupuesto'), ('state', '!=', 'obsoleto')]",
        help="Copia las líneas de proyecto (telas sin producto) de ese presupuesto al año nuevo, "
             "en los mismos meses.")
    rate = fields.Float(
        string="Tipo de cambio presupuestal", digits=(12, 4),
        default=lambda self: float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_planning_rate', 0) or 0),
        help="Pesos por dólar para los 12 meses (pestaña Tipo de cambio del presupuesto).")

    @api.depends('base', 'year')
    def _compute_period(self):
        today = fields.Date.context_today(self)
        for wizard in self:
            if wizard.base == 'anio':
                wizard.date_from = date(wizard.year - 1, 1, 1)
                wizard.date_to = date(wizard.year - 1, 12, 31)
            else:
                end = today.replace(day=1) - relativedelta(days=1)
                wizard.date_to = end
                wizard.date_from = end.replace(day=1) - relativedelta(months=11)

    def _growth(self, mercado):
        return {'industrial': self.growth_industrial, 'confeccion': self.growth_confeccion,
                'especiales': self.growth_especiales}.get(mercado, 0.0)

    # --- base -----------------------------------------------------------------
    def _qb_base_cells(self, budget):
        """{(mercado, product, partner|None, mes): {'qty', 'amount', 'currency': {ccy: (importe, qty)}}}
        y conteos de lo que no entra."""
        self.ensure_one()
        teams = budget._qb_market_teams()
        if not teams:
            raise UserError("Configure los equipos de los mercados en Ajustes antes de armar la propuesta.")
        domain = [
            ('move_id.move_type', 'in', ('out_invoice', 'out_refund')), ('parent_state', '=', 'posted'),
            ('company_id', '=', budget.company_id.id), ('display_type', '=', 'product'),
            ('move_id.invoice_date', '>=', self.date_from), ('move_id.invoice_date', '<=', self.date_to),
            ('move_id.team_id', 'in', teams.ids),
        ] + self.env['sgi.indicator']._sgi_sales_account_domain()
        Budget = self.env['sgi.sales.budget']
        cells = defaultdict(lambda: {'qty': 0.0, 'amount': 0.0, 'currency': defaultdict(lambda: [0.0, 0.0])})
        skipped = defaultdict(float)
        for aml in self.env['account.move.line'].sudo().search(domain):
            product = aml.product_id
            amount = -aml.balance
            if Budget._qb_is_adjustment(product):
                skipped['ajustes'] += amount
                continue
            if Budget._qb_is_saldo(product):
                skipped['saldos'] += amount
                continue
            if product.company_id and product.company_id != budget.company_id:
                skipped['otra_compania'] += amount
                continue
            mercado = team_mercado(self.env, aml.move_id.team_id)
            if not mercado:
                continue
            partner = aml.move_id.commercial_partner_id if mercado in MERCADOS_CON_CLIENTE else None
            qty = _convert_qty(aml.quantity, aml.product_uom_id, product.uom_id)
            if qty is None:
                qty = aml.quantity
            sign = -1.0 if aml.move_id.move_type == 'out_refund' else 1.0
            cell = cells[(mercado, product, partner, aml.move_id.invoice_date.month)]
            cell['qty'] += sign * qty
            cell['amount'] += amount
            ccy = aml.currency_id or budget.currency_id
            cell['currency'][ccy][0] += -aml.amount_currency if aml.currency_id else amount
            cell['currency'][ccy][1] += sign * qty
        return cells, skipped

    # --- armado ---------------------------------------------------------------
    def _qb_target_budget(self):
        Budget = self.env['sgi.sales.budget']
        company = self.env.company
        open_budget = Budget.search([
            ('kind', '=', 'presupuesto'), ('year', '=', self.year), ('company_id', '=', company.id),
            ('state', 'in', ('borrador', 'revisado'))], limit=1)
        if open_budget:
            if open_budget.line_ids:
                raise UserError(
                    "Ya hay un presupuesto %s abierto con renglones (%s). Márquelo obsoleto o "
                    "bórrelo para armar la propuesta desde cero." % (self.year, open_budget.name))
            if open_budget.state != 'borrador':
                open_budget.action_set_borrador()
            return open_budget
        approved = Budget.search([
            ('kind', '=', 'presupuesto'), ('year', '=', self.year), ('company_id', '=', company.id),
            ('state', '=', 'aprobado')], limit=1)
        vals = {'year': self.year, 'kind': 'presupuesto', 'company_id': company.id}
        if approved:
            vals['revision'] = approved.revision + 1
        return Budget.create(vals)

    def action_build(self):
        self.ensure_one()
        if self.year < 2000:
            raise UserError("Escriba el año del presupuesto.")
        budget = self._qb_target_budget()
        cells, skipped = self._qb_base_cells(budget)

        # Mínimo anual por combinación (producto y cliente, o producto en Confección).
        yearly = defaultdict(float)
        for (mercado, product, partner, _m), cell in cells.items():
            yearly[(mercado, product, partner)] += cell['amount']
        below = {k for k, amount in yearly.items() if self.min_amount and amount < self.min_amount}

        # Un renglón por (producto, cliente, mes): si el mismo producto y cliente salen en dos
        # mercados en el mismo mes (raro), queda en el de mayor venta.
        rows = {}
        for (mercado, product, partner, month), cell in cells.items():
            if (mercado, product, partner) in below or cell['qty'] <= 0 or cell['amount'] <= 0:
                continue
            key = (product.id, partner.id if partner else False, month)
            if key in rows and rows[key]['amount'] >= cell['amount']:
                continue
            rows[key] = dict(cell, mercado=mercado, product=product, partner=partner, month=month)

        Line = self.env['sgi.sales.budget.line']
        vals_list = []
        for key in sorted(rows, key=lambda k: (rows[k]['mercado'], k[0], k[1] or 0, k[2])):
            row = rows[key]
            qty = round(row['qty'] * (1.0 + self._growth(row['mercado']) / 100.0), 0)
            if qty <= 0:
                continue
            vals_list.append({
                'budget_id': budget.id, 'mercado': row['mercado'], 'product_id': row['product'].id,
                'partner_id': row['partner'].id if row['partner'] else False,
                'date': date(self.year, row['month'], 1), 'uom_id': row['product'].uom_id.id,
                'qty_budget': qty,
            })
        lines = Line.create(vals_list)

        if self.rate > 0:
            budget.rate_default = self.rate
            budget.action_qb_apply_rate_all()
        from_list = lines._qb_suggest_price()
        # Sin regla en la lista: el precio real promedio del periodo base, en la moneda facturada.
        from_history = 0
        by_key = {(l.product_id.id, l.partner_id.id or False, l.date.month): l for l in lines}
        for key, row in rows.items():
            line = by_key.get(key)
            if not line or line.price_input > 0:
                continue
            ccy, (amount, qty) = max(row['currency'].items(), key=lambda kv: abs(kv[1][0]))
            if qty > 0 and amount > 0:
                line.write({'price_input': round(amount / qty, 4), 'price_currency_id': ccy.id,
                            'price_basis': 'venta', 'price_origen': 'historico'})
                from_history += 1

        projects = 0
        if self.project_source_id:
            for src in self.project_source_id.line_ids.filtered('proyecto_descripcion'):
                vals = {f: (src[f].id if hasattr(src[f], 'id') else src[f]) for f in PROJECT_FIELDS}
                vals.update(budget_id=budget.id, date=src.date.replace(year=self.year))
                Line.create(vals)
                projects += 1

        budget.action_qb_update_base()
        self._qb_post_summary(budget, lines, skipped, len(below), from_list, from_history, projects)
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.sales.budget', 'res_id': budget.id,
                'view_mode': 'form', 'target': 'current'}

    def _qb_post_summary(self, budget, lines, skipped, below, from_list, from_history, projects):
        fmt = '{:,.0f}'.format
        per_market = []
        for base, label in LABELS.items():
            mine = lines.filtered(lambda l, b=base: l.mercado == b)
            if mine:
                per_market.append("%s: %d renglones, %s (crecimiento %s %%)" % (
                    label, len(mine), fmt(sum(mine.mapped('amount_budget'))), self._growth(base)))
        notes = [
            "Propuesta armada con la venta del %s al %s." % (
                self.date_from.strftime('%d/%m/%Y'), self.date_to.strftime('%d/%m/%Y')),
            Markup("<br/>").join(per_market) if per_market else "Sin venta en el periodo base.",
            "Precio: %d de la lista, %d del precio real del periodo base, %d sin precio." % (
                from_list, from_history, budget.no_price_count),
        ]
        out = []
        if skipped.get('saldos'):
            out.append("saldos y desperdicio %s" % fmt(skipped['saldos']))
        if skipped.get('ajustes'):
            out.append("notas de crédito genéricas y servicios %s" % fmt(skipped['ajustes']))
        if skipped.get('otra_compania'):
            out.append("productos de otra compañía %s" % fmt(skipped['otra_compania']))
        if below:
            out.append("%d combinación(es) por debajo del mínimo de %s" % (below, fmt(self.min_amount)))
        if out:
            notes.append("No entraron: %s." % "; ".join(out))
        if projects:
            notes.append("%d línea(s) de proyecto traídas de %s." % (projects, self.project_source_id.name))
        budget.message_post(body=Markup("<br/>").join(Markup("%s") % n for n in notes))


class SgiSalesBudgetProposal(models.Model):
    _inherit = 'sgi.sales.budget'

    @api.model
    def action_qb_build_proposal(self, year, growth=None, base='ultimos12', min_amount=0.0,
                                 project_source_id=False, rate=0.0):
        """Para MCP: arma la propuesta del año ``year``; ``growth`` = {'industrial': 5, …}.
        Devuelve el id del presupuesto."""
        growth = growth or {}
        vals = {'year': year, 'base': base, 'min_amount': min_amount,
                'project_source_id': project_source_id or False,
                'growth_industrial': growth.get('industrial', 0.0),
                'growth_confeccion': growth.get('confeccion', 0.0),
                'growth_especiales': growth.get('especiales', 0.0)}
        if rate:
            vals['rate'] = rate
        action = self.env['qb.sales.budget.proposal'].create(vals).action_build()
        return action['res_id']
