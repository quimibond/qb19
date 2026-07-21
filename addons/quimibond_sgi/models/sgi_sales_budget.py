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
"""
from collections import defaultdict
from datetime import date, timedelta

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

_REAL_MOVE_TYPES = ('out_invoice', 'out_refund')


class SgiSalesBudget(models.Model):
    _name = 'sgi.sales.budget'
    _description = "Presupuesto de ventas (F-P-A28-18)"
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
    state = fields.Selection([
        ('borrador', "Borrador"),
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
            ('invoice_date', '>=', date(self.year, 1, 1)),
            ('invoice_date', '<=', date(self.year, 12, 31)),
        ])
        return sum(moves.mapped('amount_untaxed_signed'))

    @api.depends('line_ids.amount_real', 'team_id', 'year')
    def _compute_unbudgeted(self):
        for budget in self:
            captured = sum(budget.line_ids.mapped('amount_real'))
            budget.amount_real_unbudgeted = budget._sgi_team_year_real() - captured

    # --- Constraint: un solo no-obsoleto por año+equipo(+cliente)+kind --------
    @api.constrains('year', 'team_id', 'state', 'kind', 'partner_id')
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

    # --- Flujo de estados -----------------------------------------------------
    def action_approve(self):
        """Aprueba el presupuesto (solo MAST). A partir de aquí es evidencia."""
        for budget in self:
            if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
                raise UserError(
                    "Solo el Jefe de MAST y SGI puede aprobar un presupuesto de ventas.")
            if not budget.line_ids:
                raise UserError(
                    "El presupuesto %s no tiene líneas: captura la matriz antes de "
                    "aprobarlo." % (budget.folio or budget.name))
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
        """Productos que este equipo vendió (o que se vendieron a ESTE cliente,
        en pronóstico) en los últimos 24 meses — para pre-llenar las filas."""
        self.ensure_one()
        from dateutil.relativedelta import relativedelta as _rd
        date_from = fields.Date.context_today(self) - _rd(months=24)
        domain = [('order_id.state', 'in', ('sale', 'done')),
                  ('order_id.date_order', '>=', date_from)]
        if self.kind == 'pronostico':
            partner = self.partner_id.commercial_partner_id
            if not partner:
                return self.env['product.product']
            domain.append(
                ('order_id.partner_id.commercial_partner_id', '=', partner.id))
        else:
            domain.append(('order_id.team_id', '=', self.team_id.id))
        products = self.env['sale.order.line'].search(domain).mapped('product_id')
        return products.sorted(lambda p: p.default_code or p.name or '')

    def action_download_template(self):
        """Genera con openpyxl el Excel VACÍO con las FILAS ya puestas (productos
        históricos) para llenar cantidades y subir con el importador."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("La plantilla se descarga sobre un presupuesto en borrador.")
        import base64
        import io
        import openpyxl
        products = self._sgi_historical_products()
        wb = openpyxl.Workbook()
        ws = wb.active
        if self.kind == 'pronostico':
            ws.title = (self.partner_id.name or 'Pronostico')[:31]
            ws.append(['PRODUCTO', 'CODIGO CLIENTE', 'SEMANA']
                      + list(range(1, 53)))
            for product in products:
                ws.append([product.default_code or product.name, '', ''] + [''] * 52)
        else:
            ws.title = (self.team_id.name or 'Presupuesto')[:31]
            ws.append(['PRODUCTO', 'UNIDAD', 'CLIENTE']
                      + ['%s m' % m for m in self._SGI_TEMPLATE_MONTHS])
            for product in products:
                ws.append([product.default_code or product.name,
                           product.uom_id.name, ''] + [''] * 12)
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
        sols = self.env['sale.order.line'].search([
            ('order_id.state', 'in', ('sale', 'done')),
            ('order_id.partner_id.commercial_partner_id', '=', partner.id),
        ])
        for sol in sols:
            monday = Line._sgi_effective_monday(sol.order_id)
            if not monday or monday.year != self.year:
                continue
            product = sol.product_id
            qty = sol.product_uom_qty
            if sol.product_uom_id and product.uom_id and \
                    sol.product_uom_id._has_common_reference(product.uom_id):
                qty = sol.product_uom_id._compute_quantity(
                    qty, product.uom_id, round=False)
            result[(product, monday)] += qty
        return result

    def action_preload_from_orders(self):
        """Precarga las semanas del horizonte que ya tienen pedidos confirmados y
        NO tienen celda de pronóstico, con qty_budget = lo comprometido.
        Idempotente: no toca celdas ya capturadas."""
        self.ensure_one()
        if self.kind != 'pronostico':
            raise UserError("La precarga desde pedidos es solo para pronósticos.")
        if self.state != 'borrador':
            raise UserError("Solo se precarga un pronóstico en borrador.")
        Line = self.env['sgi.sales.budget.line']
        existing = {(l.product_id.id, l.date) for l in self.line_ids}
        created = 0
        for (product, monday), qty in self._sgi_committed_by_week().items():
            if qty <= 0 or (product.id, monday) in existing:
                continue
            Line.create({
                'budget_id': self.id, 'product_id': product.id, 'date': monday,
                'uom_id': product.uom_id.id, 'partner_id': self.partner_id.id,
                'qty_budget': qty,
            })
            created += 1
        self.message_post(
            body="Precarga desde pedidos: %d celda(s) creada(s) con lo "
                 "comprometido (el vendedor solo captura el futuro)." % created)
        return True

    def action_send_to_mps(self):
        """Vuelca la DEMANDA NETA (no el pronóstico bruto) por producto/semana al
        forecast del Programa Maestro (mrp_mps). Crea el schedule si falta;
        re-envío tras revisión actualiza sin duplicar. No crea pedidos de venta."""
        self.ensure_one()
        if 'mrp.production.schedule' not in self.env:
            raise UserError("El módulo Programa Maestro (mrp_mps) no está instalado.")
        if self.kind != 'pronostico':
            raise UserError("Solo se envía la demanda de un pronóstico.")
        if self.state != 'aprobado':
            raise UserError("Solo se envía la demanda de un pronóstico aprobado.")
        warehouse = self.env['stock.warehouse'].search(
            [('company_id', '=', self.company_id.id)], limit=1)
        if not warehouse:
            raise UserError("No hay almacén configurado para la compañía.")
        Schedule = self.env['mrp.production.schedule']
        Forecast = self.env['mrp.product.forecast']
        demand = defaultdict(float)
        for line in self.line_ids:
            if line.qty_net_demand <= 0:
                continue
            qty = line.qty_net_demand
            if line.uom_id and line.product_id.uom_id and \
                    line.uom_id._has_common_reference(line.product_id.uom_id):
                qty = line.uom_id._compute_quantity(
                    qty, line.product_id.uom_id, round=False)
            demand[(line.product_id, line.date)] += qty
        details = []
        for (product, monday), qty in sorted(
                demand.items(), key=lambda kv: (kv[0][0].id, kv[0][1])):
            sched = Schedule.search([
                ('product_id', '=', product.id),
                ('warehouse_id', '=', warehouse.id)], limit=1)
            if not sched:
                sched = Schedule.create(
                    {'product_id': product.id, 'warehouse_id': warehouse.id})
            forecast = sched.forecast_ids.filtered(lambda f: f.date == monday)[:1]
            if forecast:
                forecast.forecast_qty = qty  # re-envío: actualiza sin duplicar
            else:
                Forecast.create({
                    'production_schedule_id': sched.id, 'date': monday,
                    'forecast_qty': qty})
            details.append("%s · %s: %s %s" % (
                product.default_code or product.name, monday,
                round(qty, 2), product.uom_id.name))
        self.message_post(
            body="Demanda neta enviada al Programa Maestro (%d celdas):<br/>%s" % (
                len(details), "<br/>".join(details) or "sin demanda"))
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


class SgiSalesBudgetLine(models.Model):
    _name = 'sgi.sales.budget.line'
    _description = "Línea de presupuesto de ventas (producto × mes)"
    _order = 'budget_id, date, product_id'

    budget_id = fields.Many2one('sgi.sales.budget', string="Presupuesto",
                                required=True, ondelete='cascade', index=True)
    team_id = fields.Many2one(related='budget_id.team_id', store=True,
                              string="Mercado")
    company_id = fields.Many2one(related='budget_id.company_id', store=True)
    currency_id = fields.Many2one(related='budget_id.currency_id', store=True,
                                  string="Moneda")
    kind = fields.Selection(related='budget_id.kind', store=True, string="Tipo")
    product_id = fields.Many2one('product.product', string="Producto",
                                 required=True, index=True)
    customer_code = fields.Char(
        string="Código del cliente para el material",
        help="Código con que el cliente identifica el material (ej. SCR31); se "
             "imprime junto al producto en el pronóstico F-P-A28-13.")
    partner_id = fields.Many2one(
        'res.partner', string="Cliente", index=True,
        domain="[('is_company', '=', True)]",
        help="Presupuesto: vacío = producto para todo el mercado; con cliente = "
             "esa cuenta (un producto no mezcla ambos). Pronóstico: es el cliente "
             "de la cabecera (no editable).")
    date = fields.Date(string="Mes / Semana", required=True,
                       help="Presupuesto: primer día del mes. Pronóstico: lunes "
                            "de la semana.")
    uom_id = fields.Many2one(
        'uom.uom', string="Unidad", required=True,
        help="Unidad en que se captura y lee la cantidad de esta línea "
             "(vendemos en metros, kg, rollos, piezas). Editable solo dentro de "
             "la misma categoría que la unidad de venta del producto.")
    qty_budget = fields.Float(string="Cantidad presupuestada",
                              digits='Product Unit')
    # PRECIO: no se captura. Sale SIEMPRE de la lista de precios (la única fuente
    # de verdad); si un precio está mal se corrige LA LISTA. Compute almacenado
    # (foto), se refresca en borrador; al aprobar queda congelado (candado de
    # líneas). Doble moneda: la de la lista (lo que el cliente conoce) y la de la
    # compañía (convertida con el tipo presupuestal de Ajustes).
    price_unit_budget = fields.Monetary(
        string="Precio MXN", currency_field='currency_id',
        compute='_compute_price', store=True,
        help="Precio unitario en moneda de la compañía, tomado de la lista de "
             "precios del cliente (o la lista default). No se captura.")
    price_unit_currency = fields.Monetary(
        string="Precio (divisa)", currency_field='list_currency_id',
        compute='_compute_price', store=True,
        help="Precio en la moneda de la lista aplicada (lo que el cliente conoce).")
    list_currency_id = fields.Many2one(
        'res.currency', string="Moneda de la lista",
        compute='_compute_price', store=True)
    has_list_price = fields.Boolean(
        string="Con precio de lista", compute='_compute_price', store=True,
        help="Falso si el producto no tiene precio en la lista aplicable: la línea "
             "se crea igual, pero hay que corregir LA LISTA.")
    price_source = fields.Char(
        string="Origen del precio", compute='_compute_price', store=True, copy=False,
        help="Rastro del origen del precio y el tipo de cambio usado (ej. "
             "\"Lista 'Export USD': 2.15 USD × 17.50\").")
    amount_budget = fields.Monetary(
        string="Importe MXN", currency_field='currency_id',
        compute='_compute_amount_budget', store=True,
        help="Cantidad × precio de lista, en moneda de la compañía.")
    amount_currency = fields.Monetary(
        string="Importe (divisa)", currency_field='list_currency_id',
        compute='_compute_amount_budget', store=True,
        help="Cantidad × precio, en la moneda de la lista.")

    # Real automático (base = FACTURADO). Almacenados para poder agregarse en
    # pivot/graph; son una FOTO: se recalculan al tocar la línea, con el botón
    # "Actualizar facturado/pedido" del presupuesto y en el cron mensual (no se
    # refrescan solos al timbrar una factura nueva).
    qty_real = fields.Float(string="Cantidad facturada", digits='Product Unit',
                            compute='_compute_real', store=True, aggregator='sum',
                            help="Cantidad facturada del periodo convertida a la "
                                 "unidad de esta línea.")
    amount_real = fields.Monetary(
        string="Importe real", compute='_compute_real', store=True,
        help="Presupuesto: FACTURADO — suma de account.move.line.balance (con el "
             "signo de out_invoice/out_refund) de las facturas del periodo "
             "(contabilidad ya convirtió a moneda compañía; no se reconvierte). "
             "Pronóstico: COMPROMETIDO — importe de los pedidos confirmados del "
             "cliente cuya fecha comprometida cae en la semana.")
    unconverted_count = fields.Integer(
        string="Facturas sin convertir", compute='_compute_real', store=True)
    qty_ordered = fields.Float(string="Cantidad pedida", digits='Product Unit',
                               compute='_compute_ordered', store=True,
                               aggregator='sum',
                               help="Lo pedido (sale.order confirmadas) — visión "
                                    "comercial, aún no necesariamente facturado.")
    amount_ordered = fields.Monetary(string="Importe pedido",
                                     compute='_compute_ordered', store=True)
    qty_net_demand = fields.Float(
        string="Demanda neta", digits='Product Unit',
        compute='_compute_net_demand', store=True, aggregator='sum',
        help="Consumo de pronóstico (forecast consumption): max(pronosticado, "
             "comprometido). Los pedidos confirmados CONSUMEN el pronóstico de su "
             "semana; si superan lo pronosticado, manda el pedido. Es la demanda "
             "que se envía al Programa Maestro (no el pronóstico bruto).")
    avg_price_budget = fields.Monetary(string="Precio prom. presupuestado",
                                       compute='_compute_avg_prices')
    avg_price_real = fields.Monetary(string="Precio prom. real",
                                     compute='_compute_avg_prices')

    # Unicidad producto+mes+cliente. NULLS NOT DISTINCT (PG15+): un cliente nulo
    # cuenta como su propio valor, así que solo hay una línea global por prod+mes.
    _product_month_partner_uniq = models.Constraint(
        'unique nulls not distinct (budget_id, product_id, date, partner_id)',
        "Ya existe una línea para ese producto, mes y cliente en este presupuesto.")

    @api.depends('product_id', 'date', 'uom_id', 'partner_id', 'customer_code')
    def _compute_display_name(self):
        for line in self:
            product = line.product_id
            label = product.default_code or product.name or ''
            if line.customer_code:
                label = "%s [%s]" % (label, line.customer_code)
            uom = line.uom_id.name or ''
            name = "%s (%s)" % (label, uom) if uom else label
            if line.partner_id:
                name = "%s — %s" % (name, line.partner_id.name)
            line.display_name = name

    @api.depends('qty_budget', 'price_unit_budget', 'price_unit_currency')
    def _compute_amount_budget(self):
        for line in self:
            line.amount_budget = line.qty_budget * line.price_unit_budget
            line.amount_currency = line.qty_budget * line.price_unit_currency

    @api.onchange('product_id')
    def _onchange_product_id(self):
        if self.product_id and not self.uom_id:
            self.uom_id = self.product_id.uom_id

    @api.depends('product_id', 'partner_id', 'uom_id', 'qty_budget')
    def _compute_price(self):
        """El precio SIEMPRE sale de la lista (nunca se captura). Los campos de la
        línea no cambian tras aprobar (candado), así que este compute no se
        re-dispara solo en aprobados; el refresco manual/cron sí lo salta para no
        pisar lo aprobado (ver action_refresh_actuals). Congelar aquí rompería el
        primer cálculo perezoso de una línea recién aprobada."""
        for line in self:
            price_company, source, list_ccy, price_list, has_price = \
                line._sgi_pricelist_price()
            line.price_unit_budget = price_company
            line.price_unit_currency = price_list
            line.list_currency_id = list_ccy
            line.price_source = source
            line.has_list_price = has_price

    def _sgi_default_pricelist(self):
        """Lista de precios default de la compañía (cuando la línea no tiene
        cliente): la primera de la compañía / global."""
        company = self.company_id or self.env.company
        return self.env['product.pricelist'].sudo().search(
            [('company_id', 'in', (False, company.id))],
            order='company_id, id', limit=1)

    def _sgi_pricelist_price(self):
        """(precio_compañía, texto_origen, moneda_lista, precio_en_lista, hay_precio)
        de la lista aplicable a la línea. La lista es la única fuente de precios."""
        self.ensure_one()
        product = self.product_id
        company_currency = self.currency_id or self.env.company.currency_id
        if not product:
            return 0.0, '', company_currency, 0.0, False
        qty = self.qty_budget or 1.0
        uom = self.uom_id or product.uom_id
        day = fields.Date.context_today(self)
        pricelist = (self.partner_id.property_product_pricelist
                     if self.partner_id else self._sgi_default_pricelist())
        if pricelist:
            raw = pricelist._get_product_price(product, qty, uom=uom)
            list_currency = pricelist.currency_id or company_currency
            price, rate = self._sgi_to_company_price(raw, list_currency, day)
            if list_currency and list_currency != company_currency:
                source = "Lista '%s': %.4g %s × %.4g = %s %s" % (
                    pricelist.name, raw, list_currency.name, rate,
                    '{:,.2f}'.format(price), company_currency.name)
            else:
                source = "Lista '%s': %s %s" % (
                    pricelist.name, '{:,.2f}'.format(price), company_currency.name)
            return price, source, list_currency, raw, raw > 0
        price = product.list_price
        return (price, "Precio de venta del producto: %s %s" % (
            '{:,.2f}'.format(price), company_currency.name),
            company_currency, price, price > 0)

    def _sgi_to_company_price(self, price, list_currency, day):
        """Convierte un precio de la lista a moneda de la compañía usando el tipo
        de cambio PRESUPUESTAL (budget_planning_rate, USD→MXN); si es 0, usa el
        tipo del día. Devuelve (precio_compañía, tipo_usado)."""
        company = self.company_id or self.env.company
        company_currency = self.currency_id or company.currency_id
        if not list_currency or list_currency == company_currency:
            return price, 0.0
        rate = float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_planning_rate', 0) or 0)
        usd = self.env.ref('base.USD', raise_if_not_found=False)
        mxn = self.env.ref('base.MXN', raise_if_not_found=False)
        if rate > 0 and usd and mxn:
            if list_currency == usd and company_currency == mxn:
                return price * rate, rate
            if list_currency == mxn and company_currency == usd:
                return price / rate, rate
        converted = list_currency._convert(price, company_currency, company, day)
        return converted, (converted / price if price else 0.0)

    def _sgi_suggest_price(self):
        """(precio en moneda compañía, texto de origen) sugerido para la línea."""
        self.ensure_one()
        product = self.product_id
        company_currency = self.currency_id or self.env.company.currency_id
        if not product:
            return 0.0, ''
        qty = self.qty_budget or 1.0
        uom = self.uom_id or product.uom_id
        day = fields.Date.context_today(self)
        pricelist = self.partner_id.property_product_pricelist if self.partner_id else False
        if pricelist:
            raw = pricelist._get_product_price(product, qty, uom=uom)
            list_currency = pricelist.currency_id
            price, rate = self._sgi_to_company_price(raw, list_currency, day)
            if list_currency and list_currency != company_currency:
                source = "Lista '%s': %.4g %s × %.4g = %s %s" % (
                    pricelist.name, raw, list_currency.name, rate,
                    '{:,.2f}'.format(price), company_currency.name)
            else:
                source = "Lista '%s': %s %s" % (
                    pricelist.name, '{:,.2f}'.format(price), company_currency.name)
            return price, source
        price = product.list_price
        return price, "Precio de venta del producto: %s %s" % (
            '{:,.2f}'.format(price), company_currency.name)

    # --- Constraints de la línea ---------------------------------------------
    @api.constrains('date', 'budget_id')
    def _check_date_in_year(self):
        for line in self:
            if not line.date:
                continue
            if line.date.year != line.budget_id.year:
                raise ValidationError(
                    "La fecha %s no cae dentro del año del presupuesto (%s)." % (
                        line.date, line.budget_id.year))
            if line.budget_id.kind == 'pronostico':
                if line.date.weekday() != 0:
                    raise ValidationError(
                        "En un pronóstico semanal, la fecha de la línea debe ser "
                        "lunes de la semana (%s)." % line.date)
            elif line.date.day != 1:
                raise ValidationError(
                    "La fecha de la línea debe ser el primer día del mes (%s)." % line.date)

    @api.constrains('partner_id', 'budget_id')
    def _check_forecast_partner(self):
        """En pronóstico, el cliente de la línea es el de la cabecera."""
        for line in self:
            if line.budget_id.kind == 'pronostico' and line.partner_id != line.budget_id.partner_id:
                raise ValidationError(
                    "En un pronóstico, el cliente de la línea es el del pronóstico "
                    "(%s)." % (line.budget_id.partner_id.name or ''))

    @api.constrains('partner_id', 'product_id', 'budget_id')
    def _check_no_mixed_scheme(self):
        """Anti-doble-conteo: dentro de un presupuesto, un producto es global
        (sin cliente) O por cliente, nunca ambos — o el mismo importe se contaría
        dos veces contra el mismo real."""
        for line in self:
            siblings = self.search([
                ('budget_id', '=', line.budget_id.id),
                ('product_id', '=', line.product_id.id),
                ('id', '!=', line.id),
            ])
            has_global = any(not s.partner_id for s in siblings) or not line.partner_id
            has_client = any(s.partner_id for s in siblings) or bool(line.partner_id)
            if has_global and has_client:
                raise ValidationError(
                    "El producto '%s' ya está presupuestado por cliente en este "
                    "presupuesto; captura el resto como otro cliente o cambia el "
                    "esquema (no mezcles líneas con cliente y sin cliente para el "
                    "mismo producto)." % line.product_id.display_name)

    @api.constrains('uom_id', 'product_id')
    def _check_uom_category(self):
        for line in self:
            sale_uom = line.product_id.uom_id
            if line.uom_id and sale_uom and not sale_uom._has_common_reference(line.uom_id):
                raise ValidationError(
                    "La unidad '%s' no es de la misma categoría que la unidad de "
                    "venta del producto '%s' ('%s'): no se puede convertir entre "
                    "ellas. Usa una unidad compatible." % (
                        line.uom_id.name, line.product_id.display_name, sale_uom.name))

    # --- Real (facturado) y pedido, en lotes (cero N+1) ----------------------
    def _sgi_month_bounds(self, when):
        """(primer día, primer día del mes siguiente) del mes de `when`."""
        first = when.replace(day=1)
        if first.month == 12:
            nxt = first.replace(year=first.year + 1, month=1)
        else:
            nxt = first.replace(month=first.month + 1)
        return first, nxt

    def _sgi_week_bounds(self, monday):
        """(lunes, lunes siguiente) de la semana de una línea de pronóstico."""
        from datetime import timedelta
        return monday, monday + timedelta(days=7)

    @api.depends('product_id', 'date', 'uom_id', 'team_id', 'partner_id', 'kind')
    def _compute_real(self):
        """Presupuesto: real = FACTURADO (account.move.line). Pronóstico: real =
        COMPROMETIDO a entregar = pedidos confirmados del cliente cuya fecha
        comprometida (commitment_date, fallback expected_date/date_order) cae en la
        semana. Base distinta a propósito (mide compromiso, no facturación)."""
        forecast = self.filtered(lambda l: l.kind == 'pronostico')
        (self - forecast)._sgi_compute_real_invoiced()
        forecast._sgi_compute_real_committed()

    def _sgi_compute_real_invoiced(self):
        AML = self.env['account.move.line']
        by_team = defaultdict(lambda: self.browse())
        for line in self:
            line.qty_real = 0.0
            line.amount_real = 0.0
            line.unconverted_count = 0
            if line.team_id and line.product_id and line.date:
                by_team[line.team_id.id] |= line
        for team_id, lines in by_team.items():
            products = lines.mapped('product_id')
            dates = lines.mapped('date')
            start = min(dates).replace(day=1)
            end = max(dates)
            _, end_next = lines[0]._sgi_month_bounds(end)
            amls = AML.search([
                ('parent_state', '=', 'posted'),
                ('move_id.move_type', 'in', _REAL_MOVE_TYPES),
                ('move_id.team_id', '=', team_id),
                ('product_id', 'in', products.ids),
                ('move_id.invoice_date', '>=', start),
                ('move_id.invoice_date', '<', end_next),
            ])
            # Índice por (producto, (año, mes)) para asignar a cada línea sin N+1.
            bucket = defaultdict(lambda: self.env['account.move.line'])
            for aml in amls:
                inv_date = aml.move_id.invoice_date
                bucket[(aml.product_id.id, (inv_date.year, inv_date.month))] |= aml
            for line in lines:
                key = (line.product_id.id, (line.date.year, line.date.month))
                qty = amount = 0.0
                unconverted = 0
                partner = line.partner_id.commercial_partner_id
                for aml in bucket.get(key, self.env['account.move.line']):
                    # Línea por cliente: solo la empresa comercial del documento.
                    if partner and aml.move_id.commercial_partner_id != partner:
                        continue
                    sign = 1.0 if aml.move_id.move_type == 'out_invoice' else -1.0
                    amount += -aml.balance  # balance ya en moneda compañía
                    row_uom = aml.product_uom_id
                    if row_uom and line.uom_id and row_uom._has_common_reference(line.uom_id):
                        qty += sign * row_uom._compute_quantity(
                            aml.quantity, line.uom_id, round=False)
                    else:
                        unconverted += 1
                line.qty_real = qty
                line.amount_real = amount
                line.unconverted_count = unconverted

    def _sgi_effective_monday(self, order):
        """Lunes de la semana comprometida de un pedido: commitment_date, o
        expected_date, o date_order."""
        from datetime import timedelta
        eff = order.commitment_date or order.expected_date or order.date_order
        if not eff:
            return False
        eff_date = fields.Datetime.to_datetime(eff).date()
        return eff_date - timedelta(days=eff_date.weekday())

    def _sgi_forecast_sols(self):
        """Líneas de pedido confirmadas del cliente (comercial) para el producto
        de esta línea, cuya semana comprometida = la semana de la línea."""
        self.ensure_one()
        partner = self.partner_id.commercial_partner_id
        if not partner or not self.product_id or not self.date:
            return self.env['sale.order.line']
        sols = self.env['sale.order.line'].search([
            ('order_id.state', 'in', ('sale', 'done')),
            ('product_id', '=', self.product_id.id),
            ('order_id.partner_id.commercial_partner_id', '=', partner.id),
        ])
        return sols.filtered(
            lambda s: self._sgi_effective_monday(s.order_id) == self.date)

    def _sgi_compute_real_committed(self):
        SOL = self.env['sale.order.line']
        by_partner = defaultdict(lambda: self.browse())
        for line in self:
            line.qty_real = 0.0
            line.amount_real = 0.0
            line.unconverted_count = 0
            if line.partner_id and line.product_id and line.date:
                by_partner[line.partner_id.commercial_partner_id.id] |= line
        for partner_id, lines in by_partner.items():
            products = lines.mapped('product_id')
            sols = SOL.search([
                ('order_id.state', 'in', ('sale', 'done')),
                ('product_id', 'in', products.ids),
                ('order_id.partner_id.commercial_partner_id', '=', partner_id),
            ])
            bucket = defaultdict(lambda: self.env['sale.order.line'])
            for sol in sols:
                monday = lines[0]._sgi_effective_monday(sol.order_id)
                if monday:
                    bucket[(sol.product_id.id, monday)] |= sol
            for line in lines:
                key = (line.product_id.id, line.date)
                qty = amount = 0.0
                unconverted = 0
                company = line.company_id or self.env.company
                for sol in bucket.get(key, self.env['sale.order.line']):
                    amount += sol.currency_id._convert(
                        sol.price_subtotal, line.currency_id, company,
                        sol.order_id.date_order.date())
                    row_uom = sol.product_uom_id
                    if row_uom and line.uom_id and row_uom._has_common_reference(line.uom_id):
                        qty += row_uom._compute_quantity(
                            sol.product_uom_qty, line.uom_id, round=False)
                    else:
                        unconverted += 1
                line.qty_real = qty
                line.amount_real = amount
                line.unconverted_count = unconverted

    def action_view_week_orders(self):
        """Drill-down: los pedidos confirmados de ese producto/cliente/semana
        (sustituye a las filas de PO/fecha del Excel)."""
        self.ensure_one()
        orders = self._sgi_forecast_sols().mapped('order_id')
        return {
            'type': 'ir.actions.act_window',
            'name': "Pedidos de la semana — %s" % self.display_name,
            'res_model': 'sale.order',
            'view_mode': 'list,form',
            'domain': [('id', 'in', orders.ids)],
        }

    @api.depends('product_id', 'date', 'uom_id', 'team_id', 'partner_id')
    def _compute_ordered(self):
        SOL = self.env['sale.order.line']
        by_team = defaultdict(lambda: self.browse())
        for line in self:
            line.qty_ordered = 0.0
            line.amount_ordered = 0.0
            if line.team_id and line.product_id and line.date:
                by_team[line.team_id.id] |= line
        for team_id, lines in by_team.items():
            products = lines.mapped('product_id')
            dates = lines.mapped('date')
            start = min(dates).replace(day=1)
            _, end_next = lines[0]._sgi_month_bounds(max(dates))
            sols = SOL.search([
                ('order_id.state', 'in', ('sale', 'done')),
                ('order_id.team_id', '=', team_id),
                ('product_id', 'in', products.ids),
                ('order_id.date_order', '>=', start),
                ('order_id.date_order', '<', end_next),
            ])
            bucket = defaultdict(lambda: self.env['sale.order.line'])
            for sol in sols:
                order_date = sol.order_id.date_order.date()
                bucket[(sol.product_id.id, (order_date.year, order_date.month))] |= sol
            for line in lines:
                key = (line.product_id.id, (line.date.year, line.date.month))
                qty = amount = 0.0
                company = line.company_id or self.env.company
                partner = line.partner_id.commercial_partner_id
                for sol in bucket.get(key, self.env['sale.order.line']):
                    if partner and sol.order_id.partner_id.commercial_partner_id != partner:
                        continue
                    amount += sol.currency_id._convert(
                        sol.price_subtotal, line.currency_id, company,
                        sol.order_id.date_order.date())
                    row_uom = sol.product_uom_id
                    if row_uom and line.uom_id and row_uom._has_common_reference(line.uom_id):
                        qty += row_uom._compute_quantity(
                            sol.product_uom_qty, line.uom_id, round=False)
                line.qty_ordered = qty
                line.amount_ordered = amount

    @api.depends('qty_budget', 'qty_real')
    def _compute_net_demand(self):
        for line in self:
            line.qty_net_demand = max(line.qty_budget, line.qty_real)

    @api.depends('amount_budget', 'qty_budget', 'amount_real', 'qty_real')
    def _compute_avg_prices(self):
        for line in self:
            line.avg_price_budget = (
                line.amount_budget / line.qty_budget if line.qty_budget else 0.0)
            line.avg_price_real = (
                line.amount_real / line.qty_real if line.qty_real else 0.0)

    # --- Inmutabilidad: las líneas de un presupuesto aprobado no se tocan -----
    # (patrón Ola A: en borrador el equipo edita libre; aprobado es evidencia;
    # solo MAST puede, tras regresar el presupuesto a borrador.)
    _SGI_LOCKED_PARENT_STATES = ('aprobado',)
    _SGI_EDITABLE_FIELDS = {
        'product_id', 'date', 'uom_id', 'qty_budget', 'amount_budget', 'budget_id',
        'price_unit_budget', 'price_source', 'partner_id', 'customer_code'}

    @api.model_create_multi
    def create(self, vals_list):
        Budget = self.env['sgi.sales.budget']
        for vals in vals_list:
            if vals.get('budget_id') and not vals.get('partner_id'):
                budget = Budget.browse(vals['budget_id'])
                if budget.kind == 'pronostico' and budget.partner_id:
                    vals['partner_id'] = budget.partner_id.id
        return super().create(vals_list)

    def _sgi_locked_lines(self):
        return self.filtered(
            lambda l: l.budget_id.state in self._SGI_LOCKED_PARENT_STATES)

    def write(self, vals):
        if (not self.env.su and not self.env.context.get('sgi_bypass_lock')
                and self._SGI_EDITABLE_FIELDS & set(vals)
                and not self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            locked = self._sgi_locked_lines()
            if locked:
                raise UserError(
                    "No se puede editar la línea de un presupuesto aprobado (es "
                    "evidencia). Pide al Jefe de MAST regresarlo a borrador o "
                    "crear una nueva revisión.\n\nPresupuesto(s): %s" % (
                        ", ".join(locked.mapped('budget_id.name'))))
        return super().write(vals)

    def unlink(self):
        if (not self.env.su and not self.env.context.get('sgi_bypass_lock')
                and not self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            locked = self._sgi_locked_lines()
            if locked:
                raise UserError(
                    "No se puede borrar la línea de un presupuesto aprobado (es "
                    "evidencia). Pide al Jefe de MAST regresarlo a borrador.\n\n"
                    "Presupuesto(s): %s" % (
                        ", ".join(locked.mapped('budget_id.name'))))
        return super().unlink()

    # --- Grid de captura (matriz producto × mes) -----------------------------
    def _grid_cell_vals_from_domain(self, domain):
        """Producto, mes y presupuesto de la celda a crear, leídos del dominio del
        grid (fila producto + columna mes) y del contexto (default_budget_id)."""
        vals = {}
        for leaf in domain:
            if isinstance(leaf, (list, tuple)) and len(leaf) == 3:
                field, op, val = leaf
                if field == 'product_id' and op == '=':
                    vals['product_id'] = val
                elif field == 'date' and op in ('>=', '=', '>'):
                    vals['date'] = val
                elif field == 'budget_id' and op == '=':
                    vals['budget_id'] = val
        vals.setdefault('budget_id', self.env.context.get('default_budget_id'))
        product = self.env['product.product'].browse(vals.get('product_id'))
        vals['uom_id'] = product.uom_id.id
        return vals

    @api.model
    def grid_update_cell(self, domain, measure_field_name, value):
        """Suma `value` a la celda (producto × mes) del grid; crea la línea si no
        existe (patrón timesheet_grid). El grid gestiona SOLO el esquema por
        producto (sin cliente); el presupuesto por cliente se captura en la vista
        lista (si el producto ya tiene líneas por cliente, el constraint
        anti-doble-conteo avisará)."""
        if not value:
            return
        domain = list(domain) + [('partner_id', '=', False)]
        line = self.search(domain, limit=1)
        if line:
            line[measure_field_name] += value
            return
        vals = self._grid_cell_vals_from_domain(domain)
        vals['partner_id'] = False
        if not vals.get('budget_id') or not vals.get('product_id') or not vals.get('date'):
            raise UserError(
                "No se pudo ubicar la celda (producto/mes/presupuesto). Captura "
                "la línea desde la lista o la ficha.")
        vals[measure_field_name] = value
        self.create(vals)
