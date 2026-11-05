# -*- coding: utf-8 -*-
"""Pronóstico vigente (E1, diseño v3 sección 2.3).

Una sola tabla viva, sin estados: una fila por cliente × planta × producto ×
semana (lunes de la semana de embarque). Guarda la cantidad como la mandó el
cliente (``qty_origen`` en ``uom_origen``), en la unidad de venta del producto
y en metros y kilos, con el origen de la conversión. Cada cambio deja una fila
en ``qb.forecast.change``.

Sustituye al pronóstico como documento (``sgi.sales.budget`` con
``kind='pronostico'``), que se queda como histórico hasta la migración de E1b.
"""
from collections import defaultdict
from datetime import timedelta

from odoo import api, fields, models

ORIGINS = [
    ('release', "Release del cliente"),
    ('captura', "Captura"),
    ('excel', "Forecast de Excel"),
    ('anual', "Forecast anual"),
    ('historico', "Histórico"),
]
MARKETS = [
    ('industrial', "Industrial"),
    ('proyectos_industrial', "Proyectos industrial"),
    ('confeccion', "Confección"),
    ('proyectos_confeccion', "Proyectos confección"),
    ('especiales', "Especiales"),
    ('otro', "Sin mercado"),
]
ZONES = [('firme', "Firme"), ('planeado', "Planeado"), ('pasado', "Pasado")]
UNITS = [('m', "m"), ('yd', "yd"), ('kg', "kg"), ('m2', "m²")]
PARAM_FIRM_WEEKS = 'quimibond_ventas_presupuesto.forecast_firm_weeks'
DEFAULT_FIRM_WEEKS = 3
# Nombre del equipo de ventas → mercado (los equipos de producción hoy: Industrial, Confección,
# Especiales). Los proyectos se separan en E2 con qb.sales.project.
TEAM_MARKET = {'INDUSTRIAL': 'industrial', 'CONFECCION': 'confeccion', 'CONFECCIÓN': 'confeccion',
               'ESPECIALES': 'especiales'}


def monday_of(day):
    return day - timedelta(days=day.weekday()) if day else day


def market_of_partner(partner):
    team = partner.commercial_partner_id.team_id or partner.team_id if partner else None
    return TEAM_MARKET.get((team.name or '').strip().upper(), 'otro') if team else 'otro'


class QbForecastLine(models.Model):
    _name = 'qb.forecast.line'
    _description = "Pronóstico vigente (cliente × producto × semana)"
    _order = 'week, partner_id, product_id'
    _rec_name = 'product_id'

    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True,
                                 default=lambda self: self.env.company)
    partner_id = fields.Many2one(
        'res.partner', string="Cliente", index=True, domain="[('is_company', '=', True)]",
        help="Empresa comercial del cliente. Vacío solo en Confección, que pronostica por producto.")
    ship_to_id = fields.Many2one('res.partner', string="Dirección de entrega",
                                 domain="[('id', 'child_of', partner_id)]",
                                 help="Planta del cliente cuando ya existe como dirección de entrega.")
    plant = fields.Char(string="Planta", help="Planta como la escribe el forecast (Silao, Tennessee) mientras "
                                               "no exista como dirección de entrega.")
    product_id = fields.Many2one('product.product', string="Producto", required=True, index=True)
    articulo_id = fields.Many2one('product.product', string="Artículo", related='product_id.qb_articulo_id',
                                  store=True, index=True)
    customer_part_id = fields.Many2one('qb.customer.part', string="Parte del cliente (catálogo)")
    customer_part = fields.Char(string="Parte del cliente")
    week = fields.Date(string="Semana", required=True, index=True,
                       help="Lunes de la semana de embarque.")
    month = fields.Date(string="Mes", compute='_compute_month', store=True, index=True)
    delivery_date = fields.Date(string="Fecha de entrega del cliente")
    mercado = fields.Selection(MARKETS, string="Mercado", compute='_compute_mercado', store=True,
                               readonly=False, precompute=True)
    qty_origen = fields.Float(string="Cantidad del cliente", digits='Product Unit', required=True,
                              help="Como la mandó el cliente o la capturó Ventas, en su unidad.")
    uom_origen = fields.Selection(UNITS, string="Unidad del cliente",
                                  help="Vacía = la unidad de venta del producto.")
    uom_venta = fields.Char(string="Unidad de venta", related='product_id.uom_id.name')
    qty_venta = fields.Float(string="Cantidad (unidad de venta)", digits='Product Unit',
                             compute='_compute_qty', store=True, aggregator='sum')
    qty_m = fields.Float(string="Metros", digits=(16, 2), compute='_compute_qty', store=True,
                         aggregator='sum')
    qty_kg = fields.Float(string="Kilos", digits=(16, 2), compute='_compute_qty', store=True,
                          aggregator='sum')
    conv_ok = fields.Boolean(string="Convertida", compute='_compute_qty', store=True,
                             help="Falso si falta el rendimiento o el ancho para pasar a la unidad de venta.")
    # Sin lista de opciones propia: un campo relacionado toma la del producto (con lista, Odoo 19
    # avisa «selection attribute will be ignored»; 2.0.2).
    conv_origen = fields.Selection(string="Origen de la conversión", related='product_id.qb_conv_origen')
    origen = fields.Selection(ORIGINS, string="Origen", required=True, default='captura')
    release_id = fields.Many2one('qb.release', string="Release", index=True)
    customer_po = fields.Char(string="OC del cliente")
    tag = fields.Char(string="Marca", help="Lo que el forecast escribe en lugar de unidad: Muestra, Pokayoke, "
                                            "Maquila…")
    qty_pedido = fields.Float(string="Pedido (unidad de venta)", digits='Product Unit', readonly=True,
                              aggregator='sum',
                              help="Pedidos confirmados pendientes de entregar del cliente, mismo artículo y "
                                   "semana de compromiso. Lo recalcula «Actualizar pedidos» y el cron diario.")
    qty_neta = fields.Float(string="Pronóstico sin pedido", digits='Product Unit', readonly=True,
                            aggregator='sum',
                            help="Lo que los pedidos todavía no cubren: pronóstico − pedido, no menos de 0.")
    zona = fields.Selection(ZONES, string="Zona", readonly=True,
                            help="Firme: dentro de las semanas firmes (3 por omisión). La recalcula el cron "
                                 "diario.")
    active = fields.Boolean(default=True)

    _line_uniq = models.Constraint(
        'unique nulls not distinct (company_id, partner_id, ship_to_id, plant, product_id, week)',
        "Ya hay pronóstico de ese cliente, planta, producto y semana.")

    @api.depends('week')
    def _compute_month(self):
        for line in self:
            line.month = line.week.replace(day=1) if line.week else False

    @api.depends('partner_id')
    def _compute_mercado(self):
        for line in self:
            if not line.mercado or line.mercado == 'otro':
                line.mercado = market_of_partner(line.partner_id) if line.partner_id else 'confeccion'

    @api.depends('qty_origen', 'uom_origen', 'product_id', 'product_id.uom_id', 'product_id.qb_m_por_kg',
                 'product_id.qb_ancho_m', 'product_id.qb_articulo_id.qb_m_por_kg')
    def _compute_qty(self):
        for line in self:
            product = line.product_id
            if not product:
                line.qty_venta = line.qty_m = line.qty_kg = 0.0
                line.conv_ok = False
                continue
            source = line.uom_origen or 'venta'
            venta = product.qb_convertir(line.qty_origen, source, 'venta')
            line.conv_ok = venta is not None
            line.qty_venta = venta or 0.0
            line.qty_m = product.qb_convertir(line.qty_origen, source, 'm') or 0.0
            line.qty_kg = product.qb_convertir(line.qty_origen, source, 'kg') or 0.0

    # --- Bitácora ------------------------------------------------------------
    def _qb_change_vals(self, before, after, note=False):
        self.ensure_one()
        return {
            'company_id': self.company_id.id, 'line_id': self.id, 'partner_id': self.partner_id.id,
            'plant': self.plant or self.ship_to_id.name or False, 'product_id': self.product_id.id,
            'week': self.week, 'qty_before': before, 'qty_after': after, 'origen': self.origen,
            'release_id': self.release_id.id, 'note': note,
        }

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('week'):
                vals['week'] = monday_of(fields.Date.to_date(vals['week']))
        lines = super().create(vals_list)
        self.env['qb.forecast.change'].sudo().create(
            [line._qb_change_vals(0.0, line.qty_venta) for line in lines])
        return lines

    def write(self, vals):
        if vals.get('week'):
            vals['week'] = monday_of(fields.Date.to_date(vals['week']))
        tracked = {'qty_origen', 'uom_origen', 'product_id'} & set(vals)
        before = {line.id: line.qty_venta for line in self} if tracked else {}
        res = super().write(vals)
        if tracked:
            changes = [line._qb_change_vals(before[line.id], line.qty_venta)
                       for line in self if abs(before[line.id] - line.qty_venta) > 1e-6]
            if changes:
                self.env['qb.forecast.change'].sudo().create(changes)
        return res

    def unlink(self):
        changes = [line._qb_change_vals(line.qty_venta, 0.0, note="Semana quitada") for line in self
                   if line.qty_venta]
        res = super().unlink()
        if changes:
            for vals in changes:
                vals['line_id'] = False
            self.env['qb.forecast.change'].sudo().create(changes)
        return res

    # --- Zona y pedidos ------------------------------------------------------
    @api.model
    def _qb_firm_weeks(self):
        return int(float(self.env['ir.config_parameter'].sudo().get_param(
            PARAM_FIRM_WEEKS, DEFAULT_FIRM_WEEKS) or DEFAULT_FIRM_WEEKS))

    @api.model
    def _qb_firm_by_partner(self, company):
        """{partner_id: semanas firmes} de los perfiles de release activos; el resto usa el parámetro."""
        profiles = self.env['qb.release.profile'].sudo().search([('company_id', '=', company.id)])
        return {p.partner_id.commercial_partner_id.id: p.firm_weeks for p in profiles if p.firm_weeks}

    @api.model
    def _qb_zone_for(self, week, today, firm=None):
        """Zona de una semana: firme dentro de las semanas firmes del perfil del cliente (o del
        parámetro, 3), planeado después y pasado antes de la semana en curso."""
        current = monday_of(today)
        if week < current:
            return 'pasado'
        firm = firm or self._qb_firm_weeks()
        return 'firme' if week < current + timedelta(weeks=firm) else 'planeado'

    @api.model
    def _qb_open_sale_lines(self, company, partners=None):
        """Líneas de pedido confirmadas con cantidad pendiente de entregar."""
        domain = [('order_id.state', '=', 'sale'), ('company_id', '=', company.id),
                  ('product_id', '!=', False), ('display_type', '=', False)]
        if partners:
            domain.append(('order_id.partner_id.commercial_partner_id', 'in', partners.ids))
        sols = self.env['sale.order.line'].sudo().search(domain)
        return sols.filtered(lambda s: s.product_uom_qty - s.qty_delivered > 1e-6)

    @staticmethod
    def _qb_sol_week(sol):
        order = sol.order_id
        when = order.commitment_date or order.expected_date or order.date_order
        return monday_of(fields.Datetime.to_datetime(when).date()) if when else False

    @api.model
    def _qb_sol_pending(self, sol, product):
        """Pendiente de entregar de una línea de pedido, en la unidad de venta de ``product``."""
        pending = sol.product_uom_qty - sol.qty_delivered
        qty = sol.product_uom_id._compute_quantity(pending, sol.product_id.uom_id, round=False) \
            if sol.product_uom_id and sol.product_uom_id._has_common_reference(sol.product_id.uom_id) \
            else pending
        if sol.product_id == product:
            return qty
        kg_or_m = sol.product_id.qb_convertir(qty, 'venta', product._qb_sale_unit())
        return kg_or_m

    def action_qb_refresh(self):
        """Recalcula zona, pedido y pronóstico sin pedido de estas líneas (todas si vacío)."""
        lines = self or self.search([('week', '>=', monday_of(fields.Date.context_today(self))
                                      - timedelta(weeks=8))])
        today = fields.Date.context_today(self)
        for company in lines.company_id:
            company_lines = lines.filtered(lambda l: l.company_id == company)
            partners = company_lines.partner_id
            firm_by_partner = self._qb_firm_by_partner(company)
            default_firm = self._qb_firm_weeks()
            pedidos = defaultdict(list)
            for sol in self._qb_open_sale_lines(company, partners):
                key = (sol.order_id.partner_id.commercial_partner_id.id,
                       (sol.product_id.qb_articulo_id or sol.product_id).id, self._qb_sol_week(sol))
                pedidos[key].append(sol)
            groups = defaultdict(lambda: self.browse())
            for line in company_lines:
                groups[(line.partner_id.id, (line.articulo_id or line.product_id).id, line.week)] |= line
            for key, group in groups.items():
                group = group.sorted(lambda l: (l.ship_to_id.id or 0, l.plant or '', l.id))
                available = 0.0
                for sol in pedidos.get(key, []):
                    available += self._qb_sol_pending(sol, group[0].product_id) or 0.0
                for index, line in enumerate(group):
                    take = available if index == len(group) - 1 else min(available, line.qty_venta)
                    available -= take
                    vals = {'qty_pedido': take, 'qty_neta': max(line.qty_venta - take, 0.0),
                            'zona': self._qb_zone_for(line.week, today, firm_by_partner.get(
                                line.partner_id.id, default_firm))}
                    if any(line[k] != v for k, v in vals.items()):
                        super(QbForecastLine, line).write(vals)
        return True

    @api.model
    def cron_qb_forecast_refresh(self):
        """Cron diario: zona y pedidos del pronóstico, y la Demanda de Planeación."""
        self.action_qb_refresh()
        self.env['qb.demand.line'].action_qb_rebuild()
        return True


class QbForecastChange(models.Model):
    _name = 'qb.forecast.change'
    _description = "Cambio del pronóstico vigente"
    _order = 'create_date desc, id desc'

    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True)
    line_id = fields.Many2one('qb.forecast.line', string="Renglón", ondelete='set null', index=True)
    partner_id = fields.Many2one('res.partner', string="Cliente", index=True)
    plant = fields.Char(string="Planta")
    product_id = fields.Many2one('product.product', string="Producto", index=True)
    week = fields.Date(string="Semana", index=True)
    qty_before = fields.Float(string="Antes", digits='Product Unit')
    qty_after = fields.Float(string="Después", digits='Product Unit')
    qty_delta = fields.Float(string="Cambio", digits='Product Unit', compute='_compute_delta', store=True,
                             aggregator='sum')
    origen = fields.Selection(ORIGINS, string="Origen")
    release_id = fields.Many2one('qb.release', string="Release")
    note = fields.Char(string="Nota")

    @api.depends('qty_before', 'qty_after')
    def _compute_delta(self):
        for change in self:
            change.qty_delta = change.qty_after - change.qty_before


class QbForecastSheet(models.Model):
    _name = 'qb.forecast.sheet'
    _description = "Hoja del forecast de Excel → cliente"
    _order = 'name, plant'

    name = fields.Char(string="Hoja", required=True, help="Nombre de la hoja en forecast.xlsx (TQ-1, SHAWMUT…).")
    plant = fields.Char(string="Planta del forecast",
                        help="Subbloque de planta dentro de la hoja (SILAO, TENNESSEE). Vacío = toda la hoja.")
    partner_id = fields.Many2one('res.partner', string="Cliente", domain="[('is_company', '=', True)]")
    ship_to_id = fields.Many2one('res.partner', string="Dirección de entrega",
                                 domain="[('id', 'child_of', partner_id)]")
    default_uom = fields.Selection(UNITS, string="Unidad si el forecast no la dice")
    company_id = fields.Many2one('res.company', string="Compañía", required=True,
                                 default=lambda self: self.env.company)
    active = fields.Boolean(default=True)

    _sheet_uniq = models.Constraint('unique nulls not distinct (company_id, name, plant)',
                                    "Esa hoja y planta ya están ligadas.")
