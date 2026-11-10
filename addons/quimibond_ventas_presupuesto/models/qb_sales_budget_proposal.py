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
- 3.9.0 (diseño v3, 2.11.3): origen **desarrollo**. Cada desarrollo abierto (de
  Cotización a Pilotaje) con consumo anual (o volumen mensual de la solicitud) da un
  renglón de Proyectos por mes desde la fecha requerida, en la unidad de venta del
  producto (``qb_convertir``; el consumo en kg ya se convierte) o, sin código, en
  metros o kilos con la descripción, gramaje y ancho del desarrollo. Precio: el de la
  última cotización abierta al cliente del producto; si no hay, el precio objetivo de
  la solicitud; si no, el de la lista. No entra si su producto ya se vendió en el
  periodo base o si sus renglones ya vienen del presupuesto de proyectos. Cada
  renglón dice su origen y su etapa; Jessica decide cuánto entra.

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
from .qb_sales_project import proyecto_mercado

BASES = [('ultimos12', "Últimos 12 meses cerrados"), ('anio', "Año anterior completo")]
LABELS = {'industrial': "Industrial", 'confeccion': "Confección", 'especiales': "Especiales"}
PROJECT_FIELDS = ('mercado', 'partner_id', 'product_id', 'proyecto_descripcion', 'proyecto_gramaje',
                  'proyecto_ancho_cm', 'uom_id', 'qty_budget', 'price_input', 'price_currency_id',
                  'price_basis', 'price_origen', 'dev_project_id')
# Etapas del desarrollo que entran a la propuesta (abiertas, antes de Liberado).
DEV_PROPOSAL_STAGES = ('cotizacion', 'aprobacion_cliente', 'muestra', 'respuesta_cliente', 'pilotaje')
YARD_M = 0.9144


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
    include_dev = fields.Boolean(
        string="Traer desarrollos", default=True,
        help="Agrega un renglón de Proyectos por mes para cada desarrollo de producto abierto (de "
             "Cotización a Pilotaje) con consumo estimado, desde su fecha requerida.")
    dev_pct = fields.Float(
        string="Del consumo del desarrollo (%)", digits=(6, 1), default=100.0,
        help="Qué parte del consumo estimado por el cliente entra a la propuesta. Después se "
             "ajusta renglón por renglón.")
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
                'qty_budget': qty, 'qty_sugerida': qty, 'qty_origen': 'venta',
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
                vals.update(budget_id=budget.id, date=src.date.replace(year=self.year),
                            qty_sugerida=src.qty_budget, qty_origen='proyecto')
                Line.create(vals)
                projects += 1

        dev_lines = Line
        dev_skipped = defaultdict(int)
        if self.include_dev:
            # 3.16.3: lo que ya se vende es por producto Y cliente (un producto de línea para un
            # cliente nuevo sí es proyecto); en Confección, sin cliente, por producto.
            sold = {(key[0], key[1]) for key in rows}
            dev_lines, dev_skipped = self._qb_dev_lines(budget, sold)

        budget.action_qb_update_base()
        self._qb_post_summary(budget, lines, skipped, len(below), from_list, from_history, projects,
                              dev_lines, dev_skipped)
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.sales.budget', 'res_id': budget.id,
                'view_mode': 'form', 'target': 'current'}

    # --- desarrollos ---------------------------------------------------------
    def _qb_dev_projects(self, budget):
        """Desarrollos abiertos de la compañía (de Cotización a Pilotaje), sin plantillas."""
        stages = [self.env.ref('quimibond_sgi.sgi_dev_stage_%s' % key, raise_if_not_found=False)
                  for key in DEV_PROPOSAL_STAGES]
        stage_ids = [stage.id for stage in stages if stage]
        if not stage_ids:
            return self.env['project.project']
        return self.env['project.project'].sudo().search([
            ('sgi_is_ft', '=', True), ('is_template', '=', False), ('stage_id', 'in', stage_ids),
            ('company_id', 'in', (budget.company_id.id, False))])

    @api.model
    def _qb_dev_monthly(self, dev):
        """(cantidad mensual, unidad 'm' o 'kg') del desarrollo: el consumo anual entre 12 (las
        yardas pasan a metros) o, si no hay, el volumen mensual de la solicitud. (0, '') sin dato."""
        if dev.sgi_dev_consumption_annual > 0:
            qty = dev.sgi_dev_consumption_annual / 12.0
            unit = dev.sgi_dev_consumption_uom or 'm'
            if unit == 'yd':
                return qty * YARD_M, 'm'
            return qty, unit
        if dev.sgi_dev_volume > 0:
            return dev.sgi_dev_volume, dev.sgi_dev_volume_uom or 'm'
        return 0.0, ''

    @api.model
    def _qb_dev_price(self, dev, product, partner):
        """{price_input, price_currency_id, price_basis, price_origen} del desarrollo: la última
        cotización abierta del cliente con ese producto (en su unidad de venta); si no hay, el
        precio objetivo de la solicitud (por metro o por kilo, la unidad de su volumen). {} si no."""
        if product and partner:
            sol = self.env['sale.order.line'].sudo().search([
                ('order_id.state', 'in', ('draft', 'sent')), ('product_id', '=', product.id),
                ('order_id.partner_id.commercial_partner_id', '=', partner.id),
                ('price_unit', '>', 0)], order='id desc', limit=1)
            if sol and (not sol.product_uom_id or sol.product_uom_id == product.uom_id):
                return {'price_input': sol.price_unit, 'price_currency_id': sol.order_id.currency_id.id,
                        'price_basis': 'venta', 'price_origen': 'desarrollo'}
        if dev.sgi_dev_target_price > 0:
            return {'price_input': dev.sgi_dev_target_price,
                    'price_currency_id': (dev.sgi_dev_currency_id or self.env.company.currency_id).id,
                    'price_basis': 'kg' if dev.sgi_dev_volume_uom == 'kg' else 'm',
                    'price_origen': 'desarrollo'}
        return {}

    def _qb_dev_lines(self, budget, sold_pairs):
        """Renglones de Proyectos de los desarrollos abiertos. Devuelve (renglones, conteos de
        lo que no entró)."""
        Line = self.env['sgi.sales.budget.line']
        uom = {'m': self.env.ref('uom.product_uom_meter'), 'kg': self.env.ref('uom.product_uom_kgm')}
        skipped = defaultdict(int)
        already = set(budget.line_ids.mapped('dev_project_id').ids)
        # Renglones que ya existen o que ya propuso otro desarrollo (mismo producto, cliente y
        # descripción): dos desarrollos del mismo cliente y artículo no chocan con la restricción
        # única ni tumban la propuesta entera.
        taken = {(l.product_id.id or False, l.partner_id.commercial_partner_id.id or False,
                  l.proyecto_descripcion or False) for l in budget.line_ids}
        vals_list = []
        for dev in self._qb_dev_projects(budget):
            if dev.id in already:
                skipped['ya_en_proyectos'] += 1
                continue
            product = dev._qb_dev_product()
            partner = dev.partner_id.commercial_partner_id
            if product and ((product.id, partner.id or False) in sold_pairs
                            or (product.id, False) in sold_pairs):
                skipped['ya_se_vende'] += 1
                continue
            key = (product.id or False, partner.id or False, False if product else dev.display_name)
            if key in taken:
                skipped['duplicado'] += 1
                continue
            taken.add(key)
            monthly, unit = self._qb_dev_monthly(dev)
            monthly *= (self.dev_pct or 0.0) / 100.0
            if monthly <= 0:
                skipped['sin_consumo'] += 1
                continue
            required = dev.sgi_dev_required_date
            if required and required.year > self.year:
                skipped['fuera_del_anio'] += 1
                continue
            first_month = required.month if required and required.year == self.year else 1
            if product:
                qty = product.qb_convertir(monthly, unit, 'venta')
                if qty is None and product.uom_id._has_common_reference(uom[unit]):
                    qty = _convert_qty(monthly, uom[unit], product.uom_id)
                if not qty:
                    skipped['sin_conversion'] += 1
                    continue
                line_uom = product.uom_id
            else:
                qty, line_uom = monthly, uom[unit]
            qty = round(qty, 0)
            if qty <= 0:
                skipped['sin_consumo'] += 1
                continue
            from .qb_forecast import team_of_partner  # import local: qb_forecast va después
            price = self._qb_dev_price(dev, product, partner)
            for month in range(first_month, 13):
                vals = {
                    'budget_id': budget.id, 'dev_project_id': dev.id,
                    'mercado': proyecto_mercado(self.env, dev.sgi_dev_team_id or team_of_partner(partner)),
                    'partner_id': partner.id or False, 'product_id': product.id or False,
                    # Sin código, el nombre del desarrollo (lleva el folio): dos desarrollos del
                    # mismo cliente con la misma tela no chocan en el mismo mes.
                    'proyecto_descripcion': False if product else dev.display_name,
                    'date': date(self.year, month, 1), 'uom_id': line_uom.id,
                    'qty_budget': qty, 'qty_sugerida': qty, 'qty_origen': 'desarrollo',
                }
                vals.update(price)
                vals_list.append(vals)
        return Line.create(vals_list), skipped

    def _qb_post_summary(self, budget, lines, skipped, below, from_list, from_history, projects,
                         dev_lines=None, dev_skipped=None):
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
        if dev_lines:
            notes.append("Desarrollos: %d con %d renglón(es) de Proyectos por %s (%s %% del consumo "
                         "estimado)." % (len(dev_lines.dev_project_id), len(dev_lines),
                                         fmt(sum(dev_lines.mapped('amount_budget'))), self.dev_pct))
        dev_labels = {'ya_se_vende': "su producto ya se vende a ese cliente",
                      'ya_en_proyectos': "ya venían en los proyectos",
                      'duplicado': "otro desarrollo del mismo cliente y artículo ya está",
                      'sin_consumo': "sin consumo estimado", 'fuera_del_anio': "fecha requerida después del año",
                      'sin_conversion': "sin conversión a la unidad de venta"}
        dev_out = ["%s: %d" % (label, (dev_skipped or {}).get(key)) for key, label in dev_labels.items()
                   if (dev_skipped or {}).get(key)]
        if dev_out:
            notes.append("Desarrollos que no entraron (%s)." % "; ".join(dev_out))
        budget.message_post(body=Markup("<br/>").join(Markup("%s") % n for n in notes))


class SgiSalesBudgetProposal(models.Model):
    _inherit = 'sgi.sales.budget'

    @api.model
    def action_qb_build_proposal(self, year, growth=None, base='ultimos12', min_amount=0.0,
                                 project_source_id=False, rate=0.0, include_dev=True, dev_pct=100.0):
        """Para MCP: arma la propuesta del año ``year``; ``growth`` = {'industrial': 5, …}.
        Devuelve el id del presupuesto."""
        growth = growth or {}
        vals = {'year': year, 'base': base, 'min_amount': min_amount,
                'project_source_id': project_source_id or False,
                'growth_industrial': growth.get('industrial', 0.0),
                'growth_confeccion': growth.get('confeccion', 0.0),
                'growth_especiales': growth.get('especiales', 0.0),
                'include_dev': include_dev, 'dev_pct': dev_pct}
        if rate:
            vals['rate'] = rate
        action = self.env['qb.sales.budget.proposal'].create(vals).action_build()
        return action['res_id']
