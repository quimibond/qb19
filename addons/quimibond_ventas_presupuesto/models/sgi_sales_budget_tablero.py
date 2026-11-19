# -*- coding: utf-8 -*-
"""Tablero de la ficha del presupuesto anual (19.0.3.22.0, Jose 19-nov: «todos»).

La ficha era una pared de textos («Industrial 115,318,254 · …») y 854 renglones abajo. Ahora:

- **A. Tablero arriba:** presupuesto del año, al corte, vendido al corte, cumplimiento con semáforo y
  cierre estimado.
- **B. Por mercado:** presupuesto, venta, % y la diferencia separada (volumen, precio, tipo de cambio,
  presupuesto del mes y venta sin renglón), con los proyectos debajo de su mercado.
- **C. Por mes:** los 12 meses con presupuesto, facturado, pedido, acumulados y cierre estimado: lo
  vendido en los meses cerrados; en los que faltan, el presupuesto del mes (foto del pronóstico vigente
  con precio) o, si no hay, el anual.
- **D. Por cliente:** los 15 principales, otros, Confección (sin cliente) y venta a clientes sin
  presupuesto.
- **F.** Los meses y mercados que el anual no trae toman el presupuesto del mes
  (``_qb_month_plan_fill``).

La venta es la del cumplimiento (``_qb_sales_lines``, regla del VE-02, sin saldos ni desperdicio), así
que las tablas cuadran con el tablero. Todo es de lectura; los botones de cada pestaña abren el análisis
agrupado igual para bajar al renglón.
"""
from collections import defaultdict
from datetime import date

from dateutil.relativedelta import relativedelta
from markupsafe import Markup, escape

from odoo import api, fields, models

from .sgi_sales_budget import MERCADO_BASE, MERCADOS, team_mercado

BASES = [('industrial', "Industrial"), ('confeccion', "Confección"), ('especiales', "Especiales")]
MONTHS = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre",
          "Octubre", "Noviembre", "Diciembre"]
TOP_CLIENTS = 15


def _money(value):
    return '{:,.0f}'.format(value or 0.0)


def _pct(num, den):
    return (num / den * 100.0) if den else None


def _pct_text(value):
    return '—' if value is None else '{:.1f} %'.format(value)


class SgiSalesBudgetTablero(models.Model):
    _inherit = 'sgi.sales.budget'

    amount_month_plan_fill = fields.Monetary(
        string="Del presupuesto del mes", compute='_compute_qb_tablero',
        help="Lo que el presupuesto del mes pone en los meses y mercados que este presupuesto no trae.")
    amount_year_full = fields.Monetary(
        string="Presupuesto del año", compute='_compute_qb_tablero',
        help="Renglones del presupuesto más lo que el presupuesto del mes pone en los meses y mercados "
             "sin renglones.")
    amount_close_estimate = fields.Monetary(
        string="Cierre estimado", compute='_compute_qb_tablero',
        help="Lo vendido en los meses cerrados más, en los que faltan, el presupuesto del mes (o el "
             "anual si no hay) o lo ya vendido si es mayor.")
    qb_kpi_html = fields.Html(string="Tablero", compute='_compute_qb_tablero', sanitize=False)
    qb_mercado_html = fields.Html(string="Tabla por mercado", compute='_compute_qb_tablero', sanitize=False)
    qb_month_html = fields.Html(string="Tabla por mes", compute='_compute_qb_tablero', sanitize=False)
    qb_partner_html = fields.Html(string="Tabla por cliente", compute='_compute_qb_tablero', sanitize=False)

    # --- Datos ------------------------------------------------------------------
    def _qb_tablero_sales(self):
        """Venta del año (regla del cumplimiento) por mes, mercado base y cliente comercial, y a tipo de
        cambio presupuestal por mercado base hasta el corte."""
        self.ensure_one()
        cutoff = self.cutoff_date
        by_month, by_base, by_base_rate = defaultdict(float), defaultdict(float), defaultdict(float)
        by_partner = defaultdict(float)  # (partner_id o False, mercado base) -> venta al corte
        company_ccy = self.currency_id
        today = fields.Date.context_today(self)
        if self.kind != 'presupuesto' or self.state == 'obsoleto' or self.year > today.year:
            return by_month, by_base, by_base_rate, by_partner
        for aml in self._qb_sales_lines():
            if aml.product_id and self._qb_is_saldo(aml.product_id):
                continue
            invoice_date = aml.move_id.invoice_date
            amount = -aml.balance
            by_month[invoice_date.month] += amount
            if not cutoff or invoice_date > cutoff:
                continue
            base = team_mercado(self.env, aml.move_id.team_id)
            by_base[base] += amount
            factor = 0.0
            if aml.currency_id and aml.currency_id != company_ccy:
                factor = self._qb_fx_factor(aml.currency_id, invoice_date.month)
            by_base_rate[base] += (-aml.amount_currency * factor) if factor else amount
            partner = aml.move_id.partner_id.commercial_partner_id
            by_partner[(partner.id or False, base)] += amount
        return by_month, by_base, by_base_rate, by_partner

    def _qb_month_plan_totals(self):
        """{mes (1-12): (total, estado)} de los presupuestos del mes del año."""
        self.ensure_one()
        if 'qb.sales.month.plan' not in self.env or not self.year:
            return {}
        plans = self.env['qb.sales.month.plan'].sudo().search([
            ('company_id', '=', self.company_id.id),
            ('month', '>=', date(self.year, 1, 1)), ('month', '<=', date(self.year, 12, 31))])
        return {plan.month.month: (plan.amount_total, plan.state) for plan in plans}

    @api.depends('line_ids.amount_budget', 'line_ids.amount_real', 'line_ids.amount_ordered',
                 'line_ids.date', 'line_ids.mercado', 'line_ids.partner_id', 'year', 'kind', 'state')
    def _compute_qb_tablero(self):
        for budget in self:
            if budget.kind != 'presupuesto':
                budget.amount_month_plan_fill = budget.amount_year_full = budget.amount_close_estimate = 0.0
                budget.qb_kpi_html = budget.qb_mercado_html = False
                budget.qb_month_html = budget.qb_partner_html = False
                continue
            budget._qb_fill_tablero()

    def _qb_fill_tablero(self):
        self.ensure_one()
        cutoff = self.cutoff_date
        fill = self._qb_month_plan_fill()
        sales_month, sales_base, sales_base_rate, sales_partner = self._qb_tablero_sales()
        plan_totals = self._qb_month_plan_totals()

        # Presupuesto por mes, por mercado y por cliente (renglones + presupuesto del mes).
        bud_month, ord_month = defaultdict(float), defaultdict(float)
        fill_month = defaultdict(float)
        bud_mercado_year, bud_mercado_cut = defaultdict(float), defaultdict(float)
        fill_base_cut = defaultdict(float)
        bud_partner_year, bud_partner_cut = defaultdict(float), defaultdict(float)
        partner_names = {}
        for line in self.line_ids.filtered('date'):
            month = line.date.month
            bud_month[month] += line.amount_budget
            ord_month[month] += line.amount_ordered
            bud_mercado_year[line.mercado] += line.amount_budget
            key = self._qb_partner_key(line.partner_id, line.mercado)
            bud_partner_year[key] += line.amount_budget
            if line.partner_id:
                partner_names[key] = line.partner_id.commercial_partner_id.display_name
            if cutoff and line.date <= cutoff:
                bud_mercado_cut[line.mercado] += line.amount_budget
                bud_partner_cut[key] += line.amount_budget
        if fill and 'qb.sales.month.plan.line' in self.env:
            # Por cliente hace falta el renglón del presupuesto del mes, no solo el total.
            covered = {(l.date.replace(day=1), l.mercado) for l in self.line_ids if l.date}
            for pline in self.env['qb.sales.month.plan.line'].sudo().search([
                    ('plan_id.company_id', '=', self.company_id.id),
                    ('plan_id.month', '>=', date(self.year, 1, 1)),
                    ('plan_id.month', '<=', date(self.year, 12, 31))]):
                month_date = pline.plan_id.month
                if pline.mercado not in MERCADO_BASE or (month_date, pline.mercado) in covered:
                    continue
                key = self._qb_partner_key(pline.partner_id, pline.mercado)
                bud_partner_year[key] += pline.amount
                if pline.partner_id:
                    partner_names[key] = pline.partner_id.commercial_partner_id.display_name
                if cutoff and month_date <= cutoff:
                    bud_partner_cut[key] += pline.amount
        for (month_date, mercado), amount in fill.items():
            fill_month[month_date.month] += amount
            bud_mercado_year[mercado] += amount
            if cutoff and month_date <= cutoff:
                bud_mercado_cut[mercado] += amount
                fill_base_cut[MERCADO_BASE[mercado]] += amount

        self.amount_month_plan_fill = sum(fill.values())
        self.amount_year_full = self.amount_budget_total + self.amount_month_plan_fill

        # C. Por mes y cierre estimado.
        month_rows, estimate = [], 0.0
        acc_bud = acc_est = 0.0
        for month in range(1, 13):
            budget_m = bud_month[month] + fill_month[month]
            sold_m = sales_month.get(month, 0.0)
            month_end = date(self.year, month, 1) + relativedelta(months=1, days=-1)
            closed = bool(cutoff and month_end <= cutoff)
            plan = plan_totals.get(month)
            if closed:
                est, source = sold_m, "Vendido"
            elif plan:
                est = max(plan[0], sold_m)
                source = "Presupuesto del mes%s" % (" (borrador)" if plan[1] == 'borrador' else "")
            else:
                est, source = max(budget_m, sold_m), "Presupuesto anual"
            if not closed and est == sold_m and sold_m:
                source = "Vendido (ya pasó lo presupuestado)"
            estimate += est
            acc_bud += budget_m
            acc_est += est
            month_rows.append({
                'month': MONTHS[month - 1], 'budget': budget_m, 'fill': fill_month[month], 'sold': sold_m,
                'ordered': ord_month[month], 'pct': _pct(sold_m, budget_m) if closed else None,
                'acc_bud': acc_bud, 'acc_est': acc_est, 'estimate': est, 'source': source,
                'closed': closed})
        self.amount_close_estimate = estimate

        self.qb_kpi_html = self._qb_kpi_html(cutoff)
        self.qb_mercado_html = self._qb_mercado_html(
            cutoff, bud_mercado_year, bud_mercado_cut, fill_base_cut, sales_base, sales_base_rate)
        self.qb_month_html = self._qb_month_html(month_rows)
        self.qb_partner_html = self._qb_partner_html(
            cutoff, bud_partner_year, bud_partner_cut, partner_names, sales_partner)

    @staticmethod
    def _qb_partner_key(partner, mercado):
        if partner:
            return partner.commercial_partner_id.id
        return 'sin_cliente_%s' % (MERCADO_BASE.get(mercado) or 'otro')

    # --- HTML -------------------------------------------------------------------
    def _qb_traffic(self, pct):
        """Clase de color del semáforo: verde desde 90 %, ámbar desde el mínimo de Ajustes (80 %)."""
        if pct is None:
            return 'secondary'
        minimum = float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_fulfillment_min', 80) or 80)
        return 'success' if pct >= 90 else ('warning' if pct >= minimum else 'danger')

    def _qb_kpi_html(self, cutoff):
        pct = self.fulfillment_pct if cutoff else None
        traffic = self._qb_traffic(pct)
        cut_text = cutoff.strftime('%d/%m/%Y') if cutoff else "sin corte"
        estimate_pct = _pct(self.amount_close_estimate, self.amount_year_full)
        fill_note = Markup("")
        if self.amount_month_plan_fill:
            fill_note = Markup('<div class="small text-muted">incluye %s del presupuesto del mes</div>') % (
                _money(self.amount_month_plan_fill))
        cards = [
            ("Presupuesto del año", _money(self.amount_year_full), fill_note, 'secondary'),
            ("Presupuesto al corte", _money(self.amount_budget_to_date),
             Markup('<div class="small text-muted">corte %s</div>') % cut_text, 'secondary'),
            ("Vendido al corte", _money(self.amount_sales_to_date),
             Markup('<div class="small text-muted">diferencia %s</div>') % _money(self.amount_difference),
             'secondary'),
            ("Cumplimiento", _pct_text(pct),
             Markup('<div class="progress mt-1" style="height: 6px;"><div class="progress-bar bg-%s" '
                    'role="progressbar" style="width: %s%%;"></div></div>') % (
                 traffic, '%.0f' % min(pct or 0.0, 100.0)), traffic),
            ("Cierre estimado", _money(self.amount_close_estimate),
             Markup('<div class="small text-muted">%s del presupuesto del año</div>') % _pct_text(estimate_pct),
             self._qb_traffic(estimate_pct)),
        ]
        cells = Markup('').join(
            Markup('<div class="col"><div class="card h-100 border-%s"><div class="card-body p-2">'
                   '<div class="small text-muted">%s</div><div class="fs-3 fw-bold text-%s">%s</div>%s'
                   '</div></div></div>') % (
                color, label, color if label in ("Cumplimiento", "Cierre estimado") else 'body', value, note)
            for label, value, note, color in cards)
        return Markup('<div class="row row-cols-2 row-cols-md-5 g-2 mb-2">%s</div>') % cells

    @staticmethod
    def _qb_table(headers, rows, foot=None):
        """Tabla de Bootstrap; la primera columna a la izquierda y las demás a la derecha."""
        def cells(values, tag, bold=False):
            out = []
            for i, value in enumerate(values):
                css = 'text-start' if i == 0 else 'text-end'
                if bold:
                    css += ' fw-bold'
                out.append(Markup('<%s class="%s">%s</%s>') % (tag, css, value, tag))
            return Markup('').join(out)
        head = Markup('<thead><tr>%s</tr></thead>') % cells(headers, 'th')
        body = Markup('').join(
            Markup('<tr class="%s">%s</tr>') % (row.get('css', ''), cells(row['values'], 'td'))
            for row in rows)
        tfoot = Markup('<tfoot><tr class="table-light">%s</tr></tfoot>') % cells(foot, 'td', True) if foot else ''
        return Markup('<div class="table-responsive"><table class="table table-sm table-hover mb-1">'
                      '%s<tbody>%s</tbody>%s</table></div>') % (head, body, tfoot)

    def _qb_mercado_html(self, cutoff, bud_year, bud_cut, fill_cut, sold, sold_rate):
        if not cutoff:
            return Markup('<p class="text-muted">El año no ha empezado o el documento es obsoleto: '
                          'todavía no hay venta que comparar.</p>')
        labels = dict(MERCADOS)
        headers = ["Mercado", "Ppto año", "Ppto al corte", "Vendido al corte", "Cumplimiento",
                   "Diferencia", "Volumen", "Precio", "Tipo de cambio", "Ppto del mes", "Sin renglón"]
        lines = self.line_ids.filtered(lambda l: l.date and l.date <= cutoff)
        rows, totals = [], defaultdict(float)
        for base, label in BASES:
            members = [m for m in MERCADO_BASE if MERCADO_BASE[m] == base]
            year = sum(bud_year.get(m, 0.0) for m in members)
            cut = sum(bud_cut.get(m, 0.0) for m in members)
            sold_b = sold.get(base, 0.0)
            if not (year or sold_b):
                continue
            mine = lines.filtered(lambda l, ms=members: l.mercado in ms)
            volume = sum(mine.mapped('effect_volume'))
            price = sum(mine.mapped('effect_price'))
            fx = sold_b - sold_rate.get(base, 0.0)
            month_plan = -fill_cut.get(base, 0.0)
            diff = sold_b - cut
            rest = diff - volume - price - fx - month_plan
            pct = _pct(sold_b, cut)
            values = [label, _money(year), _money(cut), _money(sold_b),
                      Markup('<span class="badge text-bg-%s">%s</span>') % (self._qb_traffic(pct), _pct_text(pct)),
                      _money(diff), _money(volume), _money(price), _money(fx), _money(month_plan), _money(rest)]
            rows.append({'values': values, 'css': 'fw-semibold'})
            for key, amount in (('year', year), ('cut', cut), ('sold', sold_b), ('diff', diff), ('volume', volume),
                                ('price', price), ('fx', fx), ('month_plan', month_plan), ('rest', rest)):
                totals[key] += amount
            for project in [m for m in members if m != base and (bud_year.get(m) or bud_cut.get(m))]:
                rows.append({'values': [Markup('<span class="ps-3 text-muted">de ello %s</span>') % labels[project],
                                        _money(bud_year.get(project)), _money(bud_cut.get(project)),
                                        '—', '', '', '', '', '', '', ''],
                             'css': 'text-muted small'})
        # Venta de equipos que no son de ningún mercado: no entra (como el cumplimiento).
        pct = _pct(totals['sold'], totals['cut'])
        foot = ["Total", _money(totals['year']), _money(totals['cut']), _money(totals['sold']), _pct_text(pct),
                _money(totals['diff']), _money(totals['volume']), _money(totals['price']), _money(totals['fx']),
                _money(totals['month_plan']), _money(totals['rest'])]
        note = Markup(
            '<p class="small text-muted mb-0">La diferencia (vendido − presupuesto al corte) se separa en '
            'volumen y precio de los renglones, tipo de cambio de toda la venta, lo que agrega el presupuesto '
            'del mes en los meses sin renglones y la venta que no cae en ningún renglón. Los proyectos se venden '
            'con el equipo de su mercado: su venta va en el renglón del mercado.</p>')
        return self._qb_table(headers, rows, foot) + note

    def _qb_month_html(self, rows_data):
        headers = ["Mes", "Presupuesto", "Facturado", "Cumplimiento", "Pedido (renglones)",
                   "Ppto acumulado", "Estimado", "Estimado acumulado", "Estimado con"]
        rows = []
        for row in rows_data:
            budget = _money(row['budget'])
            if row['fill']:
                budget = Markup('%s <span class="text-muted small" title="%s del presupuesto del mes">*</span>') % (
                    budget, _money(row['fill']))
            pct = row['pct']
            rows.append({
                'values': [row['month'], budget, _money(row['sold']),
                           (Markup('<span class="badge text-bg-%s">%s</span>') % (self._qb_traffic(pct), _pct_text(pct))
                            if pct is not None else '—'),
                           _money(row['ordered']), _money(row['acc_bud']), _money(row['estimate']),
                           _money(row['acc_est']),
                           Markup('<span class="small text-muted">%s</span>') % row['source']],
                'css': '' if row['closed'] else 'table-info'})
        total_bud = sum(r['budget'] for r in rows_data)
        total_sold = sum(r['sold'] for r in rows_data)
        total_est = sum(r['estimate'] for r in rows_data)
        foot = ["Año", _money(total_bud), _money(total_sold), '', _money(sum(r['ordered'] for r in rows_data)),
                _money(total_bud), _money(total_est), _money(total_est),
                _pct_text(_pct(total_est, total_bud)) + " del presupuesto"]
        note = Markup(
            '<p class="small text-muted mb-0">En azul, los meses que faltan. Estimado: lo vendido en los meses '
            'cerrados; en los que faltan, el presupuesto del mes (la foto del pronóstico vigente con precio) o, si '
            'no hay, el anual; si ya se vendió más, lo vendido. * El mes toma el presupuesto del mes en los mercados '
            'que el anual no trae.</p>')
        return self._qb_table(headers, rows, foot) + note

    def _qb_partner_html(self, cutoff, bud_year, bud_cut, names, sales):
        if not cutoff:
            return Markup('<p class="text-muted">Todavía no hay venta que comparar.</p>')
        # La venta de un cliente sin renglones va a «Confección (sin cliente)» si es de Confección (se
        # presupuesta sin cliente) o a «Clientes sin presupuesto».
        sold = defaultdict(float)
        for (partner_id, base), amount in sales.items():
            if partner_id and partner_id in bud_year:
                sold[partner_id] += amount
            elif base == 'confeccion':
                sold['sin_cliente_confeccion'] += amount
            else:
                sold['sin_presupuesto'] += amount
        clients = sorted((k for k in bud_year if isinstance(k, int)), key=lambda k: -bud_year[k])
        top, rest = clients[:TOP_CLIENTS], clients[TOP_CLIENTS:]
        buckets = [(k, names.get(k, '')) for k in top]
        if rest:
            buckets.append((tuple(rest), "Otros %d clientes" % len(rest)))
        special = {'sin_cliente_confeccion': "Confección (sin cliente)",
                   'sin_cliente_industrial': "Industrial sin cliente",
                   'sin_cliente_especiales': "Especiales sin cliente",
                   'sin_presupuesto': "Clientes sin presupuesto"}
        for key, label in special.items():
            if bud_year.get(key) or sold.get(key):
                buckets.append((key, label))
        headers = ["Cliente", "Ppto año", "Ppto al corte", "Vendido al corte", "Cumplimiento", "Diferencia"]
        rows, totals = [], defaultdict(float)
        for key, label in buckets:
            keys = key if isinstance(key, tuple) else (key,)
            year = sum(bud_year.get(k, 0.0) for k in keys)
            cut = sum(bud_cut.get(k, 0.0) for k in keys)
            sold_k = sum(sold.get(k, 0.0) for k in keys)
            pct = _pct(sold_k, cut)
            rows.append({'values': [escape(label), _money(year), _money(cut), _money(sold_k),
                                    (Markup('<span class="badge text-bg-%s">%s</span>') % (
                                        self._qb_traffic(pct), _pct_text(pct))) if pct is not None else '—',
                                    _money(sold_k - cut)],
                         'css': 'text-muted' if not isinstance(key, int) else ''})
            totals['year'] += year
            totals['cut'] += cut
            totals['sold'] += sold_k
        foot = ["Total", _money(totals['year']), _money(totals['cut']), _money(totals['sold']),
                _pct_text(_pct(totals['sold'], totals['cut'])), _money(totals['sold'] - totals['cut'])]
        note = Markup(
            '<p class="small text-muted mb-0">Vendido: la venta del cumplimiento por cliente comercial. La de un '
            'cliente sin renglones va a «Confección (sin cliente)» si es de Confección o a «Clientes sin '
            'presupuesto». «Abrir por cliente» baja a los renglones.</p>')
        return self._qb_table(headers, rows, foot) + note

    # --- Botones ------------------------------------------------------------------
    def action_qb_open_lines(self):
        """E: los renglones salen de la ficha; se abren aquí agrupados por mercado y cliente."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Renglones — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,grid,pivot,form',
            'views': [
                (self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_view_list').id, 'list'),
                (self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_grid_qty').id, 'grid'),
                (self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_view_pivot').id, 'pivot'),
                (self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_view_form').id, 'form')],
            'search_view_id': self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_view_search').id,
            'domain': [('budget_id', '=', self.id)],
            'context': {'default_budget_id': self.id, 'group_by': ['mercado', 'partner_id'],
                        'grid_anchor': fields.Date.to_string(date(self.year, 1, 1))},
        }
