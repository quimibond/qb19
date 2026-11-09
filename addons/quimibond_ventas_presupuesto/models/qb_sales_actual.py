# -*- coding: utf-8 -*-
"""Venta real, comparativo y faltantes (19.0.3.11.0, diseño v3 secciones 2.7 y 2.8).

**Venta real** (`qb.sales.actual`): una fila por línea de factura de cliente publicada en las
cuentas de venta del VE-02 (la misma regla: ``sgi.indicator._sgi_sales_account_domain``), con
cliente comercial, producto, artículo, mercado, importe en pesos (−balance: la nota de crédito
resta sola), importe y moneda de la factura, tipo de cambio, cantidad con signo en la unidad de
venta, metros y kilos (``qb_convertir``), y las marcas ``es_saldo`` (saldos y desperdicio) y
``es_ajuste`` (notas de crédito genéricas, servicios).

Desviación consciente del diseño (2.7 decía vista SQL, como ya pasó con la Demanda): es una
**tabla que se reconstruye** (cron nocturno, botón «Actualizar» y al abrir los reportes, que
refrescan el mes en curso y el anterior), porque la regla de cuentas del VE-02 (códigos por
compañía) y la conversión a metros y kilos viven en Python.

**Mercado:** el del equipo de la factura (el mismo con que el presupuesto mide su real y con
que se arma la propuesta); si el cliente y el artículo tienen un proyecto comercial vigente
en la fecha de la factura, «Proyectos» de ese mercado. Sin equipo de mercado: «Sin mercado».

**Comparativo** (`qb.sales.compare`): vista SQL sobre tablas propias (renglones de
presupuesto, pronóstico vigente y venta real sin saldos) por compañía, cliente comercial,
artículo, mercado y mes. Sobre ella van los reportes Mensual por mercado, Anual y Cliente ×
artículo.

**Faltantes** (`qb.sales.missing`): lo que impide que las cifras cuadren (ventas sin mercado,
productos vendidos sin conversión, renglones del presupuesto que cuentan 0, partes del cliente
sin producto, proyectos perdidos con presupuesto); se reconstruye con su botón y el cron.
"""
import logging
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models, tools

from .qb_forecast import MARKETS
from .sgi_sales_budget import team_mercado

_logger = logging.getLogger(__name__)

PARAM_FROM = 'quimibond_ventas_presupuesto.actual_years'
DEFAULT_YEARS = 2  # el año en curso y el anterior (lo que piden el cierre y el anual)
BATCH = 2000


class QbSalesActual(models.Model):
    _name = 'qb.sales.actual'
    _description = "Venta real (líneas de factura de venta)"
    _order = 'date desc, id desc'
    _rec_name = 'product_id'

    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True, readonly=True)
    move_line_id = fields.Many2one('account.move.line', string="Línea de factura", readonly=True,
                                   index=True, ondelete='cascade')
    move_id = fields.Many2one('account.move', string="Factura", readonly=True, index=True)
    date = fields.Date(string="Fecha de factura", required=True, index=True, readonly=True)
    month = fields.Date(string="Mes", index=True, readonly=True)
    year = fields.Integer(string="Año", index=True, readonly=True, aggregator=None)
    partner_id = fields.Many2one('res.partner', string="Cliente", index=True, readonly=True)
    ship_to_id = fields.Many2one('res.partner', string="Dirección de entrega", readonly=True)
    product_id = fields.Many2one('product.product', string="Producto", index=True, readonly=True)
    articulo_id = fields.Many2one('product.product', string="Artículo", index=True, readonly=True)
    mercado = fields.Selection(MARKETS, string="Mercado", index=True, readonly=True)
    es_proyecto = fields.Boolean(string="Proyecto", readonly=True)
    qb_project_id = fields.Many2one('qb.sales.project', string="Proyecto comercial", readonly=True)
    team_id = fields.Many2one('crm.team', string="Equipo de la factura", readonly=True)
    es_saldo = fields.Boolean(string="Saldo o desperdicio", readonly=True,
                              help="No cuenta en el cumplimiento (decisión de Jose, 4-nov).")
    es_ajuste = fields.Boolean(string="Nota genérica o servicio", readonly=True,
                               help="Producto que no es tela: nota de crédito genérica, servicio, anticipo.")
    amount = fields.Float(string="Venta (MXN)", digits=(16, 2), readonly=True, aggregator='sum',
                          help="−balance de la línea en moneda de la compañía; la nota de crédito resta.")
    amount_currency = fields.Float(string="Importe en moneda de la factura", digits=(16, 2), readonly=True,
                                   aggregator='sum')
    currency_id = fields.Many2one('res.currency', string="Moneda de la factura", readonly=True)
    rate = fields.Float(string="Tipo de cambio", digits=(12, 4), readonly=True, aggregator='avg')
    qty_venta = fields.Float(string="Cantidad (unidad de venta)", digits='Product Unit', readonly=True,
                             aggregator='sum')
    uom_venta = fields.Char(string="Unidad de venta", readonly=True)
    qty_m = fields.Float(string="Metros", digits=(16, 2), readonly=True, aggregator='sum')
    qty_kg = fields.Float(string="Kilos", digits=(16, 2), readonly=True, aggregator='sum')
    conv_ok = fields.Boolean(string="Convertida", readonly=True)

    # --- Reconstrucción --------------------------------------------------------
    @api.model
    def _qb_period(self, date_from=None, date_to=None):
        today = fields.Date.context_today(self)
        if not date_from:
            years = int(self.env['ir.config_parameter'].sudo().get_param(PARAM_FROM, DEFAULT_YEARS) or DEFAULT_YEARS)
            date_from = date(today.year - max(years, 1) + 1, 1, 1)
        return date_from, date_to or today

    @api.model
    def _qb_projects_index(self, company):
        """{(cliente, artículo): [(desde, hasta, proyecto)]} de los proyectos comerciales con
        producto (los vigentes y los que ya pasaron a línea o se perdieron: cuentan en su fecha)."""
        index = {}
        for project in self.env['qb.sales.project'].sudo().search(
                [('company_id', '=', company.id), ('product_id', '!=', False)]):
            articulo = project.product_id.qb_articulo_id or project.product_id
            key = (project.partner_id.commercial_partner_id.id or False, articulo.id)
            index.setdefault(key, []).append((project.date_from, project.date_to, project))
        return index

    @api.model
    def _qb_row(self, aml, projects, mercados):
        Budget = self.env['sgi.sales.budget']
        move = aml.move_id
        product = aml.product_id
        articulo = product.qb_articulo_id or product
        partner = move.commercial_partner_id
        sign = -1.0 if move.move_type == 'out_refund' else 1.0
        qty = aml.quantity
        if product and aml.product_uom_id and aml.product_uom_id != product.uom_id \
                and aml.product_uom_id._has_common_reference(product.uom_id):
            qty = aml.product_uom_id._compute_quantity(qty, product.uom_id, round=False)
        qty *= sign
        qty_m = product.qb_convertir(qty, 'venta', 'm') if product else None
        qty_kg = product.qb_convertir(qty, 'venta', 'kg') if product else None
        team = move.team_id
        if team.id not in mercados:
            mercados[team.id] = team_mercado(self.env, team) if team else None
        mercado = mercados[team.id] or 'otro'
        project = False
        if product and mercado in ('industrial', 'confeccion', 'especiales'):
            for start, end, candidate in projects.get((partner.id, articulo.id), ()):
                if start and start <= move.invoice_date and (not end or move.invoice_date <= end):
                    project = candidate
                    break
            if project:
                mercado = project.mercado
        amount = -aml.balance
        amount_ccy = -aml.amount_currency if aml.currency_id else amount
        return {
            'company_id': aml.company_id.id, 'move_line_id': aml.id, 'move_id': move.id,
            'date': move.invoice_date, 'month': move.invoice_date.replace(day=1),
            'year': move.invoice_date.year,
            'partner_id': partner.id or False, 'ship_to_id': move.partner_shipping_id.id or False,
            'product_id': product.id or False, 'articulo_id': articulo.id or False,
            'mercado': mercado, 'es_proyecto': bool(project),
            'qb_project_id': project.id if project else False, 'team_id': team.id or False,
            'es_saldo': bool(product) and Budget._qb_is_saldo(product),
            'es_ajuste': Budget._qb_is_adjustment(product),
            'amount': amount, 'amount_currency': amount_ccy,
            'currency_id': (aml.currency_id or aml.company_id.currency_id).id,
            'rate': abs(amount / amount_ccy) if amount_ccy else 0.0,
            'qty_venta': qty, 'uom_venta': product.uom_id.name if product else False,
            'qty_m': qty_m or 0.0, 'qty_kg': qty_kg or 0.0,
            'conv_ok': qty_m is not None and qty_kg is not None,
        }

    @api.model
    def action_qb_rebuild(self, date_from=None, date_to=None):
        """Reconstruye la venta real del periodo (por omisión, el año en curso y el anterior)."""
        date_from, date_to = self._qb_period(date_from, date_to)
        AML = self.env['account.move.line'].sudo()
        account_domain = self.env['sgi.indicator']._sgi_sales_account_domain()
        mercados = {}
        total = 0
        for company in self.env['res.company'].sudo().search([]):
            domain = [
                ('move_id.move_type', 'in', ('out_invoice', 'out_refund')), ('parent_state', '=', 'posted'),
                ('company_id', '=', company.id), ('display_type', '=', 'product'),
                ('move_id.invoice_date', '>=', date_from), ('move_id.invoice_date', '<=', date_to),
            ] + account_domain
            self.sudo().search([('company_id', '=', company.id), ('date', '>=', date_from),
                                ('date', '<=', date_to)]).unlink()
            projects = self._qb_projects_index(company)
            amls = AML.search(domain, order='id')
            for start in range(0, len(amls), BATCH):
                chunk = amls[start:start + BATCH]
                self.sudo().create([self._qb_row(aml, projects, mercados) for aml in chunk])
                total += len(chunk)
        _logger.info("Venta real: %d línea(s) del %s al %s.", total, date_from, date_to)
        return total

    @api.model
    def _qb_refresh_recent(self):
        """El mes en curso y el anterior (lo que cambia día a día)."""
        today = fields.Date.context_today(self)
        start = today.replace(day=1) - relativedelta(months=1)
        return self.action_qb_rebuild(start, today)

    @api.model
    def cron_qb_sales_actual(self):
        self.action_qb_rebuild()
        self.env['qb.sales.missing'].action_qb_rebuild()

    def action_qb_refresh_button(self):
        self._qb_refresh_recent()
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    @api.model
    def action_qb_open_report(self, xmlid):
        """Refresca el mes en curso y el anterior y abre el reporte ``xmlid``."""
        self._qb_refresh_recent()
        return self.env['ir.actions.act_window']._for_xml_id(xmlid)


class QbSalesCompare(models.Model):
    """Presupuesto, pronóstico y venta real por compañía, cliente, artículo, mercado y mes."""
    _name = 'qb.sales.compare'
    _description = "Comparativo de ventas: presupuesto, pronóstico y real"
    _auto = False
    _order = 'month, mercado'
    _rec_name = 'articulo_id'

    fuente = fields.Selection([('presupuesto', "Presupuesto"), ('pronostico', "Pronóstico"),
                               ('real', "Venta real")], string="Fuente", readonly=True)
    company_id = fields.Many2one('res.company', string="Compañía", readonly=True)
    month = fields.Date(string="Mes", readonly=True)
    year = fields.Integer(string="Año", readonly=True, aggregator=None)
    partner_id = fields.Many2one('res.partner', string="Cliente", readonly=True)
    articulo_id = fields.Many2one('product.product', string="Artículo", readonly=True)
    product_id = fields.Many2one('product.product', string="Producto", readonly=True)
    proyecto = fields.Char(string="Proyecto sin código", readonly=True)
    mercado = fields.Selection(MARKETS, string="Mercado", readonly=True)
    budget_id = fields.Many2one('sgi.sales.budget', string="Presupuesto (revisión)", readonly=True)
    budget_state = fields.Selection([('borrador', "Borrador"), ('revisado', "Revisado"),
                                     ('aprobado', "Aprobado"), ('reemplazado', "Reemplazado"),
                                     ('obsoleto', "Obsoleto")], string="Estado del presupuesto", readonly=True)
    amount_budget = fields.Float(string="Presupuesto (MXN)", digits=(16, 2), readonly=True, aggregator='sum')
    amount_real = fields.Float(string="Venta (MXN)", digits=(16, 2), readonly=True, aggregator='sum')
    amount_diff = fields.Float(string="Venta − presupuesto (MXN)", digits=(16, 2), readonly=True,
                               aggregator='sum')
    qty_m_budget = fields.Float(string="Metros presupuestados", digits=(16, 2), readonly=True, aggregator='sum')
    qty_kg_budget = fields.Float(string="Kilos presupuestados", digits=(16, 2), readonly=True, aggregator='sum')
    qty_m_forecast = fields.Float(string="Metros pronosticados", digits=(16, 2), readonly=True, aggregator='sum')
    qty_kg_forecast = fields.Float(string="Kilos pronosticados", digits=(16, 2), readonly=True, aggregator='sum')
    qty_m_real = fields.Float(string="Metros vendidos", digits=(16, 2), readonly=True, aggregator='sum')
    qty_kg_real = fields.Float(string="Kilos vendidos", digits=(16, 2), readonly=True, aggregator='sum')

    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute("""
            CREATE OR REPLACE VIEW %s AS (
                SELECT row_number() OVER () AS id, u.* FROM (
                    SELECT 'presupuesto'::varchar AS fuente, l.company_id,
                           date_trunc('month', l.date)::date AS month,
                           EXTRACT(YEAR FROM l.date)::int AS year,
                           COALESCE(p.commercial_partner_id, l.partner_id) AS partner_id,
                           COALESCE(l.articulo_id, l.product_id) AS articulo_id, l.product_id,
                           CASE WHEN l.product_id IS NULL THEN l.proyecto_descripcion END AS proyecto,
                           l.mercado, l.budget_id, b.state AS budget_state,
                           COALESCE(l.amount_budget, 0) AS amount_budget, 0.0 AS amount_real,
                           -COALESCE(l.amount_budget, 0) AS amount_diff,
                           COALESCE(l.qty_m, 0) AS qty_m_budget, COALESCE(l.qty_kg, 0) AS qty_kg_budget,
                           0.0 AS qty_m_forecast, 0.0 AS qty_kg_forecast,
                           0.0 AS qty_m_real, 0.0 AS qty_kg_real
                      FROM sgi_sales_budget_line l
                      JOIN sgi_sales_budget b ON b.id = l.budget_id
                      LEFT JOIN res_partner p ON p.id = l.partner_id
                     WHERE b.kind = 'presupuesto' AND b.state != 'obsoleto'
                    UNION ALL
                    SELECT 'pronostico', f.company_id, f.month, EXTRACT(YEAR FROM f.month)::int,
                           COALESCE(p.commercial_partner_id, f.partner_id),
                           COALESCE(f.articulo_id, f.product_id), f.product_id, NULL,
                           f.mercado, NULL, NULL,
                           0.0, 0.0, 0.0, 0.0, 0.0,
                           COALESCE(f.qty_m, 0), COALESCE(f.qty_kg, 0), 0.0, 0.0
                      FROM qb_forecast_line f
                      LEFT JOIN res_partner p ON p.id = f.partner_id
                     WHERE f.active
                    UNION ALL
                    SELECT 'real', a.company_id, a.month, a.year, a.partner_id,
                           a.articulo_id, a.product_id, NULL, a.mercado, NULL, NULL,
                           0.0, a.amount, a.amount, 0.0, 0.0, 0.0, 0.0,
                           a.qty_m, a.qty_kg
                      FROM qb_sales_actual a
                     WHERE NOT a.es_saldo
                ) u
            )""" % self._table)


MISSING_KINDS = [
    ('venta_sin_mercado', "Venta sin mercado"),
    ('venta_sin_conversion', "Producto vendido sin conversión a metros y kilos"),
    ('renglon_sin_precio', "Renglón del presupuesto que cuenta 0"),
    ('parte_sin_producto', "Parte del cliente sin producto"),
    ('proyecto_perdido', "Proyecto perdido con presupuesto"),
    ('proyecto_sin_desarrollo', "Proyecto de producto nuevo sin desarrollo"),
]


class QbSalesMissing(models.Model):
    _name = 'qb.sales.missing'
    _description = "Faltantes del presupuesto y la venta"
    _order = 'kind, amount desc, id'

    company_id = fields.Many2one('res.company', string="Compañía", readonly=True, index=True)
    kind = fields.Selection(MISSING_KINDS, string="Qué falta", required=True, readonly=True, index=True)
    name = fields.Char(string="Detalle", readonly=True)
    partner_id = fields.Many2one('res.partner', string="Cliente", readonly=True)
    product_id = fields.Many2one('product.product', string="Producto", readonly=True)
    amount = fields.Float(string="Importe (MXN)", digits=(16, 2), readonly=True, aggregator='sum')
    count = fields.Integer(string="Casos", readonly=True, aggregator='sum')
    res_model = fields.Char(string="Modelo", readonly=True)
    res_id = fields.Integer(string="Registro", readonly=True, aggregator=None)

    @api.model
    def action_qb_rebuild(self):
        rows = []
        today = fields.Date.context_today(self)
        Actual = self.env['qb.sales.actual'].sudo()
        year_start = date(today.year, 1, 1)
        # Venta del año sin mercado y productos vendidos sin conversión (sin saldos ni notas).
        for company, partner, amount, count in Actual._read_group(
                [('date', '>=', year_start), ('mercado', '=', 'otro'), ('es_saldo', '=', False)],
                ['company_id', 'partner_id'], ['amount:sum', '__count']):
            rows.append({'company_id': company.id, 'kind': 'venta_sin_mercado', 'partner_id': partner.id,
                         'name': "Facturas del año sin equipo de mercado", 'amount': amount, 'count': count,
                         'res_model': 'res.partner', 'res_id': partner.id})
        for company, product, amount, count in Actual._read_group(
                [('date', '>=', year_start), ('conv_ok', '=', False), ('es_saldo', '=', False),
                 ('es_ajuste', '=', False), ('product_id', '!=', False)],
                ['company_id', 'product_id'], ['amount:sum', '__count']):
            rows.append({'company_id': company.id, 'kind': 'venta_sin_conversion', 'product_id': product.id,
                         'name': "Falta el rendimiento (o el ancho) en la ficha o el código",
                         'amount': amount, 'count': count,
                         'res_model': 'product.product', 'res_id': product.id})
        # Renglones de presupuestos vigentes que cuentan 0.
        lines = self.env['sgi.sales.budget.line'].sudo().search([
            ('kind', '=', 'presupuesto'), ('price_problem', '!=', False),
            ('budget_id.state', 'in', ('borrador', 'revisado', 'aprobado'))])
        for line in lines:
            rows.append({'company_id': line.company_id.id, 'kind': 'renglon_sin_precio',
                         'partner_id': line.partner_id.id, 'product_id': line.product_id.id,
                         'name': "%s · %s · %s" % (line.budget_id.name, line.date.strftime('%m/%Y'),
                                                   dict(line._fields['price_problem'].selection).get(
                                                       line.price_problem)),
                         'count': 1, 'res_model': 'sgi.sales.budget.line', 'res_id': line.id})
        for part in self.env['qb.customer.part'].sudo().search([('product_id', '=', False)]):
            rows.append({'company_id': part.company_id.id, 'kind': 'parte_sin_producto',
                         'partner_id': part.partner_id.id, 'name': part.customer_part, 'count': 1,
                         'res_model': 'qb.customer.part', 'res_id': part.id})
        for project in self.env['qb.sales.project'].sudo().search([]):
            open_lines = project.budget_line_ids.filtered(
                lambda l: l.budget_id.state in ('borrador', 'revisado', 'aprobado'))
            if project.state == 'perdido' and open_lines:
                rows.append({'company_id': project.company_id.id, 'kind': 'proyecto_perdido',
                             'partner_id': project.partner_id.id, 'product_id': project.product_id.id,
                             'name': project.name, 'amount': sum(open_lines.mapped('amount_budget')),
                             'count': len(open_lines), 'res_model': 'qb.sales.project', 'res_id': project.id})
            elif (project.state == 'vigente' and not project.dev_project_id
                  and (not project.product_id or project.product_id.sgi_dev_state)):
                # Un producto nuevo pasa por Desarrollo (decisión de Jose, 4-nov); uno de línea no.
                rows.append({'company_id': project.company_id.id, 'kind': 'proyecto_sin_desarrollo',
                             'partner_id': project.partner_id.id, 'product_id': project.product_id.id,
                             'name': project.name, 'amount': sum(open_lines.mapped('amount_budget')),
                             'count': 1, 'res_model': 'qb.sales.project', 'res_id': project.id})
        self.sudo().search([]).unlink()
        if rows:
            self.sudo().create(rows)
        return len(rows)

    def action_qb_rebuild_button(self):
        self.action_qb_rebuild()
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def action_qb_open_record(self):
        self.ensure_one()
        if not self.res_model or not self.res_id:
            return False
        return {'type': 'ir.actions.act_window', 'res_model': self.res_model, 'res_id': self.res_id,
                'view_mode': 'form', 'target': 'current'}
