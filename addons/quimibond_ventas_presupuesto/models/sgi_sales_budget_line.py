# -*- coding: utf-8 -*-
"""Línea del presupuesto de ventas (mercado × cliente × producto × mes).

La cabecera vive en sgi_sales_budget.py; aquí la línea con su precio, la foto del
real (facturado y pedido) y las guardas de captura. 3.0.0: el pronóstico ya no
vive aquí (qb.forecast.line); los pronósticos viejos quedan como histórico de
solo lectura.

3.5.0 (diseño v3, 2.6): la línea lleva el **mercado** (uno de cinco), puede ser
un **renglón de proyecto** sin producto (descripción, gramaje y ancho), y el
**precio se captura** (``price_input`` en USD o MXN, por metro, por kilo o por
la unidad del renglón). Al crear el renglón se sugiere el de la lista del cliente
con las reglas de siempre (``_sgi_pricelist_price``); si la lista no tiene regla
o el precio es basura, queda vacío y el renglón cuenta 0 («sin precio»), nunca
el precio de venta del producto. Importe = cantidad en la base del precio ×
precio × tipo de cambio del mes del documento. El precio de lista sigue como
referencia (columna aparte y control de desviaciones contra lo facturado).
"""
from collections import defaultdict

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

from odoo.addons.quimibond_sgi.models.sgi_base import sgi_bypass_allowed
from .product_articulo import CONV_ORIGINS, _UOM_REFS
from .sgi_sales_budget import (_REAL_MOVE_TYPES, _convert_qty, MERCADOS, MERCADO_BASE,
                               PROYECTO_MERCADOS, default_mercado, mercado_team)

PRICE_BASIS = [('venta', "Unidad del renglón"), ('m', "Por metro"), ('kg', "Por kilo")]
PRICE_ORIGINS = [('lista', "Lista del cliente"), ('captura', "Captura"), ('excel', "Excel"),
                 ('proyecto', "Proyecto")]
PRICE_PROBLEMS = [('sin_precio', "Sin precio"), ('sin_tc', "Sin tipo de cambio"),
                  ('sin_conversion', "Sin conversión")]


def _uom_code(env, uom):
    """'m' o 'kg' si la unidad es de longitud o de peso; '' si no."""
    for xmlid, code in _UOM_REFS:
        ref = env.ref(xmlid, raise_if_not_found=False)
        if ref and uom and uom._has_common_reference(ref):
            return code
    return ''


class SgiSalesBudgetLine(models.Model):
    _name = 'sgi.sales.budget.line'
    _description = "Línea de presupuesto de ventas (producto × mes)"
    _order = 'budget_id, date, product_id'

    budget_id = fields.Many2one('sgi.sales.budget', string="Presupuesto",
                                required=True, ondelete='cascade', index=True)
    # 3.5.0: el equipo sale del mercado (parámetro de Ajustes); el real y lo pedido
    # se miden en ese equipo.
    team_id = fields.Many2one('crm.team', string="Equipo del mercado", compute='_compute_team_id',
                              store=True, index=True)
    mercado = fields.Selection(MERCADOS, string="Mercado", index=True,
                               help="Industrial y Especiales llevan cliente; Confección se "
                                    "presupuesta sin cliente. Proyectos: combinaciones producto–"
                                    "cliente en arranque, o renglones sin código todavía.")
    company_id = fields.Many2one(related='budget_id.company_id', store=True)
    currency_id = fields.Many2one(related='budget_id.currency_id', store=True,
                                  string="Moneda")
    kind = fields.Selection(related='budget_id.kind', store=True, string="Tipo")
    product_id = fields.Many2one('product.product', string="Producto", index=True,
                                 help="Vacío solo en un renglón de proyecto que aún no tiene código: "
                                      "cuando se dé de alta, póngale el producto y conserva su historia.")
    articulo_id = fields.Many2one(related='product_id.qb_articulo_id', store=True,
                                  string="Artículo")
    proyecto_descripcion = fields.Char(
        string="Proyecto", help="Qué es el renglón de proyecto sin código (tela, cliente, uso).")
    proyecto_gramaje = fields.Float(string="Gramaje del proyecto (g/m²)")
    proyecto_ancho_cm = fields.Float(string="Ancho del proyecto (cm)")
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
    qty_m = fields.Float(string="Metros", digits=(16, 2), compute='_compute_qty_conv', store=True,
                         help="La cantidad en metros (rendimiento del producto, o gramaje y "
                              "ancho del proyecto). Vacío si no se puede convertir.")
    qty_kg = fields.Float(string="Kilos", digits=(16, 2), compute='_compute_qty_conv', store=True)
    conv_origen = fields.Selection(CONV_ORIGINS + [('proyecto', "Datos del proyecto")],
                                   string="Origen de la conversión", compute='_compute_qty_conv',
                                   store=True)
    # PRECIO PRESUPUESTAL (3.5.0): se captura; al crear se sugiere de la lista.
    price_input = fields.Float(
        string="Precio", digits=(16, 4),
        help="Precio presupuestal en la moneda y la base indicadas. Al crear el renglón se "
             "sugiere el de la lista del cliente; vacío = cuenta 0 («sin precio»).")
    price_currency_id = fields.Many2one(
        'res.currency', string="Moneda del precio",
        default=lambda self: self.env.company.currency_id)
    price_basis = fields.Selection(PRICE_BASIS, string="Base del precio", default='venta',
                                   required=True)
    price_origen = fields.Selection(PRICE_ORIGINS, string="De dónde salió el precio", copy=True)
    rate = fields.Float(string="Tipo de cambio", digits=(12, 4), compute='_compute_amount_budget',
                        store=True, help="Tipo de cambio del mes del documento (1 si el precio "
                                         "está en la moneda de la compañía).")
    price_problem = fields.Selection(PRICE_PROBLEMS, string="Problema del importe",
                                     compute='_compute_amount_budget', store=True)
    # PRECIO: no se captura. Sale SIEMPRE de la lista de precios (la única fuente
    # de verdad); si un precio está mal se corrige LA LISTA. Compute almacenado
    # (foto), se refresca en borrador; al aprobar queda congelado (candado de
    # líneas). Doble moneda: la de la lista (lo que el cliente conoce) y la de la
    # compañía (convertida con el tipo presupuestal de Ajustes).
    price_unit_budget = fields.Monetary(
        string="Precio de lista (MXN)", currency_field='currency_id',
        compute='_compute_price', store=True,
        help="Referencia: precio de la lista del cliente (o la presupuestal) en moneda "
             "de la compañía. El importe usa el precio presupuestal.")
    price_unit_currency = fields.Monetary(
        string="Precio de lista", currency_field='list_currency_id',
        compute='_compute_price', store=True,
        help="Referencia: precio en la moneda de la lista aplicada (lo que el cliente conoce).")
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
        help="Cantidad en la base del precio × precio × tipo de cambio del mes, en moneda "
             "de la compañía. Lo lee el VE-02.")
    amount_currency = fields.Monetary(
        string="Importe (moneda del precio)", currency_field='price_currency_id',
        compute='_compute_amount_budget', store=True,
        help="Cantidad en la base del precio × precio, en la moneda del precio.")

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
        help="FACTURADO: suma de account.move.line.balance (con el signo de "
             "out_invoice/out_refund) de las facturas del periodo (contabilidad ya "
             "convirtió a moneda compañía; no se reconvierte).")
    unconverted_count = fields.Integer(
        string="Facturas sin convertir", compute='_compute_real', store=True)
    qty_ordered = fields.Float(string="Cantidad pedida", digits='Product Unit',
                               compute='_compute_ordered', store=True,
                               aggregator='sum',
                               help="Lo pedido (sale.order confirmadas) — visión "
                                    "comercial, aún no necesariamente facturado.")
    amount_ordered = fields.Monetary(string="Importe pedido",
                                     compute='_compute_ordered', store=True)
    avg_price_budget = fields.Monetary(string="Precio prom. presupuestado",
                                       compute='_compute_avg_prices')
    avg_price_real = fields.Monetary(string="Precio prom. real",
                                     compute='_compute_avg_prices')

    # Informativo (P-A28 4.2.2.1): lo PRONOSTICADO del mismo producto/cliente/mes
    # por los pronósticos vigentes del año. Compute NO almacenado (referencia viva
    # para el comercial); se muestra en la lista y la ficha del presupuesto.
    qty_forecast = fields.Float(
        string="Pronosticado (info)", digits='Product Unit',
        compute='_compute_qty_forecast',
        help="Cantidad pronosticada de este producto/cliente en el mes por los "
             "pronósticos vigentes del año (agregando semanas). Referencia: no "
             "entra en el importe ni en la demanda; solo compara.")

    # Control de precios (lista vs facturado): informativo, parte de la FOTO del
    # real (se recalcula solo en el refresh; el precio de lista congelado del
    # aprobado es la referencia contra la que se mide la desviación).
    # 3.5.6: la desviación es contra el PRECIO PRESUPUESTAL del renglón (price_input), en
    # su moneda y su base (unidad, metro o kilo), no contra la lista de hoy.
    price_real_unit_currency = fields.Monetary(
        string="Precio real", currency_field='price_currency_id',
        compute='_compute_real', store=True,
        help="Precio facturado promedio en la moneda y la base del precio presupuestal. Si "
             "la factura está en otra moneda, se convierte con el tipo de cambio del mes del "
             "presupuesto (ver «Desviación cruza divisas»).")
    price_gap_fx = fields.Boolean(
        string="Desviación cruza divisas", compute='_compute_real', store=True,
        help="La comparación de precio usó conversión (la factura no estaba en la "
             "moneda del precio presupuestal).")
    price_gap = fields.Monetary(
        string="Desviación de precio", currency_field='price_currency_id',
        compute='_compute_real', store=True,
        help="Precio real menos precio presupuestal, en la moneda del precio presupuestal.")
    # 3.6.2: análisis de la diferencia del renglón (presupuesto contra lo facturado que cae en
    # él): volumen = (cantidad real − presupuestada) × precio presupuestal × TC presupuestal;
    # precio = (precio real − presupuestal) × cantidad real × TC presupuestal; tipo de cambio =
    # lo facturado en pesos − lo facturado a TC presupuestal. Suman facturado − presupuesto.
    effect_volume = fields.Monetary(string="Efecto volumen", compute='_compute_effects', store=True,
                                    currency_field='currency_id')
    effect_price = fields.Monetary(string="Efecto precio", compute='_compute_effects', store=True,
                                   currency_field='currency_id')
    effect_fx = fields.Monetary(string="Efecto tipo de cambio", compute='_compute_effects', store=True,
                                currency_field='currency_id')
    price_gap_pct = fields.Float(
        string="Desviación de precio (%)", compute='_compute_real', store=True)
    price_gap_alert = fields.Selection([
        ('ok', "OK"), ('leve', "Leve"), ('grave', "Grave"),
    ], string="Alerta de precio", default='ok',
        compute='_compute_real', store=True)

    # Unicidad producto+mes+cliente. NULLS NOT DISTINCT (PG15+): un cliente nulo
    # cuenta como su propio valor, así que solo hay una línea global por prod+mes.
    _product_month_partner_uniq = models.Constraint(
        'unique nulls not distinct (budget_id, product_id, date, partner_id, proyecto_descripcion)',
        "Ya existe una línea para ese producto (o proyecto), mes y cliente en este presupuesto.")

    def _qb_label(self):
        self.ensure_one()
        if self.product_id:
            return self.product_id.default_code or self.product_id.name or ''
        return "Proyecto: %s" % (self.proyecto_descripcion or '?')

    @api.depends('product_id', 'date', 'uom_id', 'partner_id', 'proyecto_descripcion')
    def _compute_display_name(self):
        for line in self:
            label = line._qb_label()
            uom = line.uom_id.name or ''
            name = "%s (%s)" % (label, uom) if uom else label
            if line.partner_id:
                name = "%s — %s" % (name, line.partner_id.name)
            line.display_name = name

    @api.depends('mercado', 'budget_id.kind')
    def _compute_team_id(self):
        for line in self:
            line.team_id = mercado_team(self.env, line.mercado) if line.mercado else False

    @api.model
    def _qb_refresh_market_teams(self):
        """Equipo de cada renglón de presupuesto según los parámetros de Ajustes, y su real y
        pedido. El equipo no entra al importe, así que también se corrige en un aprobado."""
        lines = self.search([('kind', '=', 'presupuesto'), ('mercado', '!=', False)])
        changed = lines.filtered(lambda l: l.team_id != mercado_team(self.env, l.mercado))
        if changed:
            changed._compute_team_id()
            changed._compute_real()
            changed._compute_ordered()
        return len(changed)

    # Sin depends sobre el rendimiento del producto: un aprobado no cambia de importe si
    # el cron nocturno corrige un rendimiento. «Actualizar real» recalcula los abiertos.
    @api.depends('qty_budget', 'uom_id', 'product_id', 'proyecto_gramaje', 'proyecto_ancho_cm')
    def _compute_qty_conv(self):
        for line in self:
            line.qty_m, line.qty_kg, line.conv_origen = line._qb_conv_values()

    def _qb_conv_values(self):
        """(metros, kilos, origen) de la cantidad; 0 donde no se puede convertir."""
        self.ensure_one()
        code = _uom_code(self.env, self.uom_id)
        if not self.qty_budget or not code:
            return 0.0, 0.0, 'sin_dato'
        ref = self.env.ref('uom.product_uom_meter' if code == 'm' else 'uom.product_uom_kgm')
        qty = _convert_qty(self.qty_budget, self.uom_id, ref) or 0.0
        if self.product_id:
            product = self.product_id
            other = 'kg' if code == 'm' else 'm'
            converted = product.qb_convertir(qty, code, other) or 0.0
            values = {code: qty, other: converted}
            origen = (product.qb_conv_origen if product.qb_m_por_kg
                      else (product.qb_articulo_id.qb_conv_origen or 'sin_dato'))
            return values['m'], values['kg'], origen if converted else 'sin_dato'
        # Proyecto: kg = m × ancho (m) × gramaje (g/m²) / 1000.
        factor = (self.proyecto_ancho_cm / 100.0) * self.proyecto_gramaje / 1000.0
        if not factor:
            return (qty, 0.0, 'sin_dato') if code == 'm' else (0.0, qty, 'sin_dato')
        if code == 'm':
            return qty, qty * factor, 'proyecto'
        return qty / factor, qty, 'proyecto'

    @api.depends('qty_budget', 'qty_m', 'qty_kg', 'price_input', 'price_currency_id', 'price_basis',
                 'date', 'budget_id.rate_ids.rate', 'budget_id.rate_ids.month', 'currency_id')
    def _compute_amount_budget(self):
        for line in self:
            line.amount_budget, line.amount_currency, line.rate, line.price_problem = \
                line._qb_amount_values()

    def _qb_amount_values(self):
        """(importe en moneda de la compañía, importe en la moneda del precio, tipo de cambio,
        problema). Un renglón con problema cuenta 0: nunca se inventa un precio ni un factor."""
        self.ensure_one()
        if not self.qty_budget:
            return 0.0, 0.0, 0.0, False
        if self.price_input <= 0:
            return 0.0, 0.0, 0.0, 'sin_precio'
        base_qty = {'venta': self.qty_budget, 'm': self.qty_m, 'kg': self.qty_kg}[self.price_basis or 'venta']
        if not base_qty:
            return 0.0, 0.0, 0.0, 'sin_conversion'
        amount_ccy = base_qty * self.price_input
        company_ccy = self.currency_id or self.env.company.currency_id
        price_ccy = self.price_currency_id or company_ccy
        if price_ccy == company_ccy:
            return amount_ccy, amount_ccy, 1.0, False
        month_rate = self.budget_id._qb_month_rate(self.date.month) if self.date else 0.0
        factor = self._qb_rate_factor(price_ccy, company_ccy, month_rate)
        if not factor:
            return 0.0, amount_ccy, 0.0, 'sin_tc'
        return amount_ccy * factor, amount_ccy, month_rate, False

    def _qb_rate_factor(self, price_ccy, company_ccy, month_rate):
        """Factor de la moneda del precio a la de la compañía con el tipo de cambio del mes
        (pesos por dólar): × tipo si el precio es en dólares y la compañía en pesos; ÷ al revés."""
        if month_rate <= 0:
            return 0.0
        usd = self.env.ref('base.USD', raise_if_not_found=False)
        mxn = self.env.ref('base.MXN', raise_if_not_found=False)
        if price_ccy == usd and company_ccy == mxn:
            return month_rate
        if price_ccy == mxn and company_ccy == usd:
            return 1.0 / month_rate
        return 0.0

    @api.onchange('product_id')
    def _onchange_product_id(self):
        if self.product_id and not self.uom_id:
            self.uom_id = self.product_id.uom_id

    @api.onchange('partner_id')
    def _onchange_partner_mercado(self):
        if self.partner_id and not self.mercado:
            self.mercado = default_mercado(self.env, self.partner_id)

    def _qb_suggest_price(self):
        """Pone el precio de la lista del cliente (o la presupuestal) en los renglones sin
        precio, si la lista tiene una regla real del producto. Devuelve cuántos tomó."""
        done = 0
        for line in self.filtered(lambda l: l.product_id and l.price_input <= 0):
            _price, _source, list_ccy, raw, has_price = line._sgi_pricelist_price()
            if has_price and raw > 0:
                line.write({'price_input': raw, 'price_currency_id': list_ccy.id,
                            'price_basis': 'venta', 'price_origen': 'lista'})
                done += 1
        return done

    @api.depends('product_id', 'partner_id', 'uom_id', 'qty_budget')
    def _compute_price(self):
        """Precio de LISTA de referencia (3.5.0: el importe usa el precio presupuestal,
        ``price_input``; la lista lo sugiere al crear y mide la desviación contra lo
        facturado). Los campos de la línea no cambian tras aprobar (candado), así que
        este compute no se re-dispara solo en aprobados; el refresco manual/cron sí lo
        salta para no pisar lo aprobado (ver action_refresh_actuals)."""
        for line in self:
            price_company, source, list_ccy, price_list, has_price = \
                line._sgi_pricelist_price()
            line.price_unit_budget = price_company
            line.price_unit_currency = price_list
            line.list_currency_id = list_ccy
            line.price_source = source
            line.has_list_price = has_price

    def _sgi_default_pricelist(self):
        """Lista de precios PRESUPUESTAL para líneas SIN cliente (Ajustes SGI →
        'Lista de precios presupuestal'). NUNCA una lista arbitraria: si el
        parámetro no está configurado (o apunta a una lista de otra compañía),
        devuelve el recordset vacío y la línea queda has_list_price=False con un
        price_source claro. Antes se tomaba 'la primera lista por id', que en
        producción eligió la tarifa de un cliente (LEAR) para todo el global."""
        company = self.company_id or self.env.company
        pid = int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_pricelist_id', 0) or 0)
        if not pid:
            return self.env['product.pricelist']
        pricelist = self.env['product.pricelist'].sudo().browse(pid).exists()
        if pricelist and (not pricelist.company_id
                          or pricelist.company_id.id == company.id):
            return pricelist
        return self.env['product.pricelist']

    def _sgi_min_plausible(self, product=None):
        """Umbral (moneda compañía) por debajo del cual un precio resuelto se toma
        como placebo (placeholder $1) aunque venga de una regla.

        1.3.0: el mínimo propio del producto manda; si no tiene, el de su
        categoría más cercana (subcategorías heredan); si ninguno, el general de
        Ajustes. Devuelve (umbral, origen) con origen None para el general."""
        if product:
            own = product.product_tmpl_id.sudo()._sgi_budget_min_price()
            if own:
                return own
        return float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.price_min_plausible', 5.0) or 0), None

    def _sgi_pricelist_price(self):
        """(precio_compañía, texto_origen, moneda_lista, precio_en_lista, hay_precio)
        de la lista aplicable a la línea. La lista es la única fuente de precios.

        has_list_price es True SOLO si el motor matcheó una REGLA real del producto
        (rule_id) cuyo precio no sea placebo. El engine de Odoo, cuando la lista no
        tiene regla para el producto, cae al precio de venta (list_price) convertido
        a la moneda de la lista — con catálogos llenos de placeholders ($1) ese
        placebo se disfraza de precio válido. Detectamos el hoyo con tres filtros:
          · sin regla (rule_id falsy) → cayó al precio de venta: NO usar;
          · regla global 'fórmula sobre precio de venta' (applied_on '3_global' con
            base 'list_price') → es el mismo precio de venta con disfraz de regla;
          · precio resuelto (moneda compañía) por debajo de price_min_plausible →
            placebo dentro de la regla misma."""
        self.ensure_one()
        product = self.product_id
        company_currency = self.currency_id or self.env.company.currency_id
        if not product:
            return 0.0, '', company_currency, 0.0, False
        qty = self.qty_budget or 1.0
        uom = self.uom_id or product.uom_id
        day = fields.Date.context_today(self)
        if self.partner_id:
            pricelist = self.partner_id.property_product_pricelist
            if not pricelist:
                price = product.list_price
                return (price, "SIN LISTA del cliente '%s'; precio de venta del "
                        "producto: %s %s (NO usar)" % (
                            self.partner_id.name or '',
                            '{:,.2f}'.format(price), company_currency.name),
                        company_currency, price, False)
        else:
            pricelist = self._sgi_default_pricelist()
            if not pricelist:
                return (0.0, "SIN LISTA PRESUPUESTAL CONFIGURADA (Ajustes SGI → "
                        "'Lista de precios presupuestal'): configúrala para valuar "
                        "las líneas sin cliente.", company_currency, 0.0, False)
        raw, rule_id = pricelist._get_product_price_rule(
            product, qty, uom=uom, date=day)
        rule = self.env['product.pricelist.item'].browse(rule_id) if rule_id else None
        list_currency = pricelist.currency_id or company_currency
        price, rate = self._sgi_to_company_price(raw, list_currency, day)
        # ¿Regla genuina, o el engine cayó al precio de venta? Sin regla → cayó al
        # list_price. Regla global de FÓRMULA/PORCENTAJE sobre el precio de venta
        # (applied_on '3_global', base 'list_price', no fija) = el mismo precio de
        # venta con disfraz de regla. Una regla global de precio FIJO sí es real.
        fell_to_sale = (not rule_id) or (
            rule.applied_on == '3_global' and rule.base == 'list_price'
            and rule.compute_price != 'fixed')
        min_plausible, min_origin = self._sgi_min_plausible(product)
        implausible = (not fell_to_sale) and price < min_plausible
        has_price = (not fell_to_sale) and (not implausible)
        if fell_to_sale:
            source = ("SIN REGLA en lista '%s'; precio de venta del producto: "
                      "%s %s (NO usar)" % (
                          pricelist.name, '{:,.2f}'.format(price),
                          company_currency.name))
        elif implausible:
            source = ("Lista '%s': regla implausible < %s %s%s (%s %s — placeholder, "
                      "NO usar)" % (
                          pricelist.name, '{:,.2f}'.format(min_plausible),
                          company_currency.name,
                          " [mínimo propio del %s]" % min_origin if min_origin else "",
                          '{:,.2f}'.format(price), company_currency.name))
        elif list_currency and list_currency != company_currency:
            source = "Lista '%s': %.4g %s × %.4g = %s %s" % (
                pricelist.name, raw, list_currency.name, rate,
                '{:,.2f}'.format(price), company_currency.name)
        else:
            source = "Lista '%s': %s %s" % (
                pricelist.name, '{:,.2f}'.format(price), company_currency.name)
        if has_price and min_origin:
            # Constancia de la excepción: el precio pasó con el mínimo propio.
            source += " [mínimo propio del %s: %s]" % (
                min_origin, '{:,.2f}'.format(min_plausible))
        return price, source, list_currency, raw, has_price

    def _sgi_planning_factor(self, list_currency, company_currency):
        """Tipo de cambio PRESUPUESTAL (budget_planning_rate) para el par USD↔MXN:
        devuelve (factor, tipo_mostrado) para pasar de la moneda de la lista a la de
        la compañía (precio_compañía = precio_lista × factor), o None si no aplica
        (entonces se usa el tipo de cambio del día). El tipo mostrado es siempre el
        parámetro, independientemente del sentido."""
        rate = float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_planning_rate', 0) or 0)
        if rate <= 0:
            return None
        usd = self.env.ref('base.USD', raise_if_not_found=False)
        mxn = self.env.ref('base.MXN', raise_if_not_found=False)
        if not (usd and mxn):
            return None
        if list_currency == usd and company_currency == mxn:
            return rate, rate
        if list_currency == mxn and company_currency == usd:
            return 1.0 / rate, rate
        return None

    def _sgi_to_company_price(self, price, list_currency, day):
        """Convierte un precio de la lista a moneda de la compañía usando el tipo
        de cambio PRESUPUESTAL (USD↔MXN); si no aplica, usa el tipo del día.
        Devuelve (precio_compañía, tipo_usado)."""
        company = self.company_id or self.env.company
        company_currency = self.currency_id or company.currency_id
        if not list_currency or list_currency == company_currency:
            return price, 0.0
        planning = self._sgi_planning_factor(list_currency, company_currency)
        if planning is not None:
            factor, rate = planning
            return price * factor, rate
        converted = list_currency._convert(price, company_currency, company, day)
        return converted, (converted / price if price else 0.0)

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
            # Los pronósticos históricos van por semana (lunes); solo el presupuesto por mes.
            if line.budget_id.kind == 'presupuesto' and line.date.day != 1:
                raise ValidationError(
                    "La fecha de la línea debe ser el primer día del mes (%s)." % line.date)

    @api.constrains('product_id', 'proyecto_descripcion', 'mercado', 'kind')
    def _check_product_or_project(self):
        for line in self.filtered(lambda l: l.kind == 'presupuesto'):
            if not line.mercado:
                raise ValidationError("Cada renglón del presupuesto lleva su mercado.")
            if not line.product_id and not (line.proyecto_descripcion or '').strip():
                raise ValidationError(
                    "Un renglón sin producto es un proyecto: escriba qué es (descripción) o "
                    "ponga el producto.")
            if not line.product_id and line.mercado not in PROYECTO_MERCADOS:
                raise ValidationError(
                    "Solo los renglones de Proyectos van sin producto (%s)." % line.proyecto_descripcion)

    @api.constrains('partner_id', 'product_id', 'budget_id', 'mercado')
    def _check_no_mixed_scheme(self):
        """Anti-doble-conteo: dentro de un presupuesto y un mercado base (el mismo
        equipo de ventas), un producto es global (sin cliente) O por cliente, nunca
        ambos — o el mismo importe se contaría dos veces contra el mismo real.
        3.5.3: por mercado base; Confección presupuesta sin cliente un producto que
        Industrial presupuesta por cliente, y cada uno mide su equipo.

        Una search POR PRESUPUESTO, no por línea: una importación de cientos de
        líneas disparaba cientos de searches (N+1)."""
        for budget in self.budget_id:
            lines = self.filtered(lambda l: l.budget_id == budget)
            siblings = self.search([
                ('budget_id', '=', budget.id),
                ('product_id', 'in', lines.product_id.ids),
            ])
            schemes = {}   # product_id -> {True: con cliente, False: global}
            products = {}  # product_id -> record (para el mensaje)
            for sibling in siblings:
                product = sibling.product_id
                key = (product.id, MERCADO_BASE.get(sibling.mercado))
                schemes.setdefault(key, set()).add(bool(sibling.partner_id))
                products[product.id] = product
            for (product_id, _base), kinds in schemes.items():
                if len(kinds) > 1:
                    raise ValidationError(
                        "El producto '%s' ya está presupuestado por cliente en este "
                        "mercado; capture el resto como otro cliente o cambie el "
                        "esquema (no mezcle líneas con cliente y sin cliente para el "
                        "mismo producto en el mismo mercado)." % products[product_id].display_name)

    @api.constrains('uom_id', 'product_id')
    def _check_uom_category(self):
        for line in self.filtered('product_id'):
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

    @api.depends('product_id', 'date', 'uom_id', 'team_id', 'partner_id', 'kind',
                 'price_input', 'price_currency_id', 'price_basis')
    def _compute_real(self):
        """Real = FACTURADO (account.move.line) del mes. 3.0.0: el «comprometido» del
        pronóstico como documento se fue; los pedidos consumen qb.forecast.line."""
        self._sgi_compute_real_invoiced()

    def _sgi_compute_real_invoiced(self):
        AML = self.env['account.move.line']
        Param = self.env['ir.config_parameter'].sudo()
        tol = float(Param.get_param('quimibond_sgi.price_gap_tolerance_pct', 3.0) or 0)
        grave = float(Param.get_param('quimibond_sgi.price_gap_grave_pct', 10.0) or 0)
        day = fields.Date.context_today(self)
        by_team = defaultdict(lambda: self.browse())
        for line in self:
            line.qty_real = 0.0
            line.amount_real = 0.0
            line.unconverted_count = 0
            line.price_real_unit_currency = 0.0
            line.price_gap_fx = False
            line.price_gap = 0.0
            line.price_gap_pct = 0.0
            line.price_gap_alert = 'ok'
            # Agrupa por (equipo, compañía): un mismo equipo puede existir en
            # varias compañías del grupo; el real solo mide la compañía del ppto.
            if line.team_id and line.product_id and line.date:
                by_team[(line.team_id.id, line.company_id.id)] |= line
        for (team_id, company_id), lines in by_team.items():
            products = lines.mapped('product_id')
            dates = lines.mapped('date')
            start = min(dates).replace(day=1)
            end = max(dates)
            _, end_next = lines[0]._sgi_month_bounds(end)
            amls = AML.search([
                ('parent_state', '=', 'posted'),
                ('display_type', '=', 'product'),  # 3.5.2: sin las líneas de costo (cogs)
                ('move_id.move_type', 'in', _REAL_MOVE_TYPES),
                ('move_id.team_id', '=', team_id),
                ('company_id', '=', company_id),  # solo la compañía del ppto
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
                amt_list = qty_list = 0.0  # importe/cantidad en la moneda del precio presupuestal
                any_non_list = False
                partner = line.partner_id.commercial_partner_id
                list_ccy = line.price_currency_id or line.currency_id
                for aml in bucket.get(key, self.env['account.move.line']):
                    # Línea por cliente: solo la empresa comercial del documento.
                    if partner and aml.move_id.commercial_partner_id != partner:
                        continue
                    sign = 1.0 if aml.move_id.move_type == 'out_invoice' else -1.0
                    amount += -aml.balance  # balance ya en moneda compañía
                    conv = _convert_qty(
                        sign * aml.quantity, aml.product_uom_id, line.uom_id)
                    if conv is None:
                        unconverted += 1
                        continue
                    qty += conv
                    if list_ccy and aml.currency_id == list_ccy:
                        amt_list += -aml.amount_currency  # en la divisa de la lista
                        qty_list += conv
                    elif list_ccy:
                        any_non_list = True
                line.qty_real = qty
                line.amount_real = amount
                line.unconverted_count = unconverted
                line._sgi_set_price_gap(qty, amount, amt_list, qty_list,
                                        any_non_list, day, tol, grave)

    @api.depends('amount_budget', 'amount_real', 'qty_real', 'price_real_unit_currency', 'price_input',
                 'rate', 'qty_budget', 'qty_m', 'qty_kg', 'price_basis')
    def _compute_effects(self):
        for line in self:
            factor = line._qb_basis_factor()
            rate = line.rate or 1.0
            qty_real = line.qty_real * factor
            qty_budget = line.qty_budget * factor
            if line.price_input > 0 and factor and qty_real > 0 and line.price_real_unit_currency:
                volume = (qty_real - qty_budget) * line.price_input * rate
                price = (line.price_real_unit_currency - line.price_input) * qty_real * rate
                fx = line.amount_real - line.price_real_unit_currency * qty_real * rate
            else:
                # Sin venta (o sin precio o conversión): todo es volumen.
                volume, price, fx = line.amount_real - line.amount_budget, 0.0, 0.0
            line.effect_volume, line.effect_price, line.effect_fx = volume, price, fx

    def _qb_basis_factor(self):
        """Cuánto vale una unidad del renglón en la base del precio (1 para la unidad del
        renglón; metros o kilos por unidad si el precio es por metro o por kilo). 0 si no
        se puede convertir."""
        self.ensure_one()
        if (self.price_basis or 'venta') == 'venta':
            return 1.0
        if self.qty_budget:
            base = self.qty_m if self.price_basis == 'm' else self.qty_kg
            return base / self.qty_budget if base else 0.0
        return 0.0

    def _sgi_set_price_gap(self, qty, amount, amt_list, qty_list, any_non_list,
                           day, tol, grave):
        """Desviación del precio facturado contra el precio presupuestal (3.5.6), en la
        moneda y la base del precio presupuestal."""
        self.ensure_one()
        ref_ccy = self.price_currency_id or self.currency_id
        factor = self._qb_basis_factor()
        if not ref_ccy or qty <= 0 or self.price_input <= 0 or not factor:
            return  # ya está en 0/ok
        if qty_list and not any_non_list:
            # Todas las facturas en la moneda del precio presupuestal: precio directo.
            price_real = amt_list / (qty_list * factor)
            self.price_gap_fx = False
        else:
            price_real = self._qb_company_to_price_ccy(amount, ref_ccy, day) / (qty * factor)
            self.price_gap_fx = ref_ccy != self.currency_id
        self.price_real_unit_currency = price_real
        gap = price_real - self.price_input
        self.price_gap = gap
        pct = gap / self.price_input * 100.0
        self.price_gap_pct = pct
        magnitude = abs(pct)
        if magnitude <= tol:
            self.price_gap_alert = 'ok'
        elif magnitude <= grave:
            self.price_gap_alert = 'leve'
        else:
            self.price_gap_alert = 'grave'

    def _qb_company_to_price_ccy(self, amount, ref_ccy, day):
        """Importe en moneda de la compañía → moneda del precio presupuestal, con el tipo de
        cambio del mes del presupuesto; sin él, el del día."""
        company = self.company_id or self.env.company
        company_ccy = self.currency_id or company.currency_id
        if ref_ccy == company_ccy:
            return amount
        month_rate = self.budget_id._qb_month_rate(self.date.month) if self.date else 0.0
        factor = self._qb_rate_factor(ref_ccy, company_ccy, month_rate)
        if factor:
            return amount / factor
        return company_ccy._convert(amount, ref_ccy, company, day)

    def action_view_month_invoices(self):
        """Drill-down del presupuesto: las facturas del producto/equipo(/cliente)
        cuyo mes es el de la línea — análogo a "Ver pedidos de la semana"."""
        self.ensure_one()
        if not self.product_id:
            raise UserError("Un renglón de proyecto sin producto todavía no tiene facturas.")
        first, nxt = self._sgi_month_bounds(self.date)
        domain = [
            ('move_type', 'in', _REAL_MOVE_TYPES),
            ('state', '=', 'posted'),
            ('team_id', '=', self.team_id.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('invoice_line_ids.product_id', '=', self.product_id.id),
            ('invoice_date', '>=', first), ('invoice_date', '<', nxt),
        ]
        if self.partner_id:
            domain.append(('commercial_partner_id', '=',
                           self.partner_id.commercial_partner_id.id))
        return {
            'type': 'ir.actions.act_window',
            'name': "Facturas del mes — %s" % self.display_name,
            'res_model': 'account.move',
            'view_mode': 'list,form',
            'domain': domain,
        }

    @api.depends('product_id', 'date', 'uom_id', 'team_id', 'partner_id')
    def _compute_ordered(self):
        SOL = self.env['sale.order.line']
        by_team = defaultdict(lambda: self.browse())
        for line in self:
            line.qty_ordered = 0.0
            line.amount_ordered = 0.0
            if line.team_id and line.product_id and line.date:
                by_team[(line.team_id.id, line.company_id.id)] |= line
        for (team_id, company_id), lines in by_team.items():
            products = lines.mapped('product_id')
            dates = lines.mapped('date')
            start = min(dates).replace(day=1)
            _, end_next = lines[0]._sgi_month_bounds(max(dates))
            sols = SOL.search([
                ('order_id.state', 'in', ('sale', 'done')),
                ('order_id.team_id', '=', team_id),
                ('company_id', '=', company_id),  # solo la compañía del ppto
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
                    conv = _convert_qty(
                        sol.product_uom_qty, sol.product_uom_id, line.uom_id)
                    if conv is not None:
                        qty += conv
                line.qty_ordered = qty
                line.amount_ordered = amount

    @api.depends('amount_budget', 'qty_budget', 'amount_real', 'qty_real')
    def _compute_avg_prices(self):
        for line in self:
            line.avg_price_budget = (
                line.amount_budget / line.qty_budget if line.qty_budget else 0.0)
            line.avg_price_real = (
                line.amount_real / line.qty_real if line.qty_real else 0.0)

    @api.depends('product_id', 'partner_id', 'date', 'kind', 'uom_id')
    def _compute_qty_forecast(self):
        """Lo pronosticado del mismo artículo/cliente/mes en el pronóstico vigente
        (qb.forecast.line), en la unidad de esta línea. 3.0.0: antes leía los
        pronósticos como documento. Un solo search por lote."""
        budget_lines = self.filtered(
            lambda l: l.kind == 'presupuesto' and l.product_id and l.date)
        for line in (self - budget_lines):
            line.qty_forecast = 0.0
        if not budget_lines:
            return
        start = min(budget_lines.mapped('date')).replace(day=1)
        _first, end = budget_lines[0]._sgi_month_bounds(max(budget_lines.mapped('date')))
        articulos = budget_lines.product_id.mapped(lambda p: p.qb_articulo_id or p)
        forecast = self.env['qb.forecast.line'].search([
            ('company_id', 'in', budget_lines.company_id.ids), ('week', '>=', start), ('week', '<', end),
            ('articulo_id', 'in', articulos.ids)])
        index = defaultdict(list)
        for fl in forecast:
            index[(fl.company_id.id, fl.articulo_id.id, fl.week.year, fl.week.month)].append(fl)
        for line in budget_lines:
            articulo = line.product_id.qb_articulo_id or line.product_id
            partner = line.partner_id.commercial_partner_id
            total = 0.0
            for fl in index.get((line.company_id.id, articulo.id, line.date.year, line.date.month), []):
                if partner and fl.partner_id.commercial_partner_id != partner:
                    continue
                conv = _convert_qty(fl.qty_venta, fl.product_id.uom_id, line.uom_id)
                if conv is None and fl.product_id != line.product_id:
                    conv = fl.product_id.qb_convertir(fl.qty_venta, 'venta', line.product_id._qb_sale_unit())
                total += conv or 0.0
            line.qty_forecast = total

    # --- Inmutabilidad: las líneas de un presupuesto aprobado no se tocan -----
    # (patrón Ola A: en borrador el equipo edita libre; aprobado es evidencia;
    # solo MAST puede, tras regresar el presupuesto a borrador.)
    _SGI_LOCKED_PARENT_STATES = ('aprobado',)
    _SGI_EDITABLE_FIELDS = {
        'product_id', 'date', 'uom_id', 'qty_budget', 'amount_budget', 'budget_id',
        'price_unit_budget', 'price_source', 'partner_id', 'mercado', 'price_input',
        'price_currency_id', 'price_basis', 'price_origen', 'proyecto_descripcion',
        'proyecto_gramaje', 'proyecto_ancho_cm'}

    @api.model_create_multi
    def create(self, vals_list):
        Partner = self.env['res.partner']
        for vals in vals_list:
            if not vals.get('mercado'):
                vals['mercado'] = default_mercado(self.env, Partner.browse(vals.get('partner_id')))
            if not vals.get('product_id') and vals.get('price_input') and not vals.get('price_origen'):
                vals['price_origen'] = 'proyecto'
            elif vals.get('price_input') and not vals.get('price_origen'):
                vals['price_origen'] = 'captura'
        lines = super().create(vals_list)
        # 3.5.0: precio sugerido de la lista donde no vino precio (presupuesto, no histórico).
        lines.filtered(lambda l: l.kind == 'presupuesto')._qb_suggest_price()
        # Agregar líneas a un presupuesto 'revisado' lo regresa a borrador, para que
        # Dirección no apruebe contenido distinto al que revisó el Admin.
        lines._sgi_reopen_reviewed_parents(lines.budget_id)
        return lines

    def _sgi_reopen_reviewed_parents(self, budgets):
        """Regresa a 'borrador' los documentos 'revisado' de `budgets` tras editar
        sus líneas de captura, con constancia en el chatter. Aplica al pronóstico
        (documento vivo) y al presupuesto (gobernanza del revisado). Se salta bajo
        sgi_bypass_lock (refresco de la foto real, borrado en cascada) — solo si
        el contexto viene de sistema o de MAST (el cliente RPC lo puede forjar)."""
        if self.env.context.get('sgi_bypass_lock') and sgi_bypass_allowed(self.env):
            return
        for budget in budgets.filtered(lambda b: b.state == 'revisado'):
            budget.with_context(sgi_bypass_lock=True).state = 'borrador'
            budget.message_post(body="Actualizado por %s tras la revisión, requiere "
                                     "re-revisión." % self.env.user.name)

    def _sgi_locked_lines(self):
        # Solo el presupuesto aprobado congela sus líneas.
        return self.filtered(lambda l: l.budget_id.state in self._SGI_LOCKED_PARENT_STATES)

    def write(self, vals):
        # El bypass por contexto solo cuenta desde sistema o MAST (el contexto
        # lo controla el cliente RPC); para no-MAST equivale a no traerlo.
        if (not self.env.su
                and not (self.env.context.get('sgi_bypass_lock')
                         and sgi_bypass_allowed(self.env))
                and self._SGI_EDITABLE_FIELDS & set(vals)
                and not self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            locked = self._sgi_locked_lines()
            if locked:
                raise UserError(
                    "No se puede editar la línea de un presupuesto aprobado (es "
                    "evidencia). Pide al Jefe de MAST regresarlo a borrador o "
                    "crear una nueva revisión.\n\nPresupuesto(s): %s" % (
                        ", ".join(locked.mapped('budget_id.name'))))
        if 'price_input' in vals and 'price_origen' not in vals:
            vals = dict(vals, price_origen='captura' if vals['price_input'] else False)
        res = super().write(vals)
        # Tocar campos de captura de un presupuesto 'revisado' lo regresa a borrador
        # (gobernanza del revisado). Solo la edición de captura cuenta, no el refresh de la foto.
        if self._SGI_EDITABLE_FIELDS & set(vals):
            self._sgi_reopen_reviewed_parents(self.budget_id)
        return res

    def unlink(self):
        if (not self.env.su
                and not (self.env.context.get('sgi_bypass_lock')
                         and sgi_bypass_allowed(self.env))
                and not self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            locked = self._sgi_locked_lines()
            if locked:
                raise UserError(
                    "No se puede borrar la línea de un presupuesto aprobado (es "
                    "evidencia). Pide al Jefe de MAST regresarlo a borrador.\n\n"
                    "Presupuesto(s): %s" % (
                        ", ".join(locked.mapped('budget_id.name'))))
        # Capturar los documentos padre antes del DELETE: borrar líneas de captura
        # de un documento 'revisado' también lo regresa a borrador (gobernanza).
        budgets = self.budget_id
        res = super().unlink()
        self.browse()._sgi_reopen_reviewed_parents(budgets)
        return res

    # --- Grid de captura (matriz producto × mes) -----------------------------
    def _grid_cell_vals_from_domain(self, domain):
        """Producto, mes y presupuesto de la celda a crear, leídos del dominio del
        grid (fila producto + columna mes) y del contexto (default_budget_id)."""
        vals = {}
        for leaf in domain:
            if isinstance(leaf, (list, tuple)) and len(leaf) == 3:
                field, op, val = leaf
                if field in ('product_id', 'partner_id', 'mercado') and op == '=':
                    vals[field] = val[0] if isinstance(val, (list, tuple)) else val
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
        """Suma `value` a la celda (mercado × cliente × producto × mes) del grid; crea la
        línea si no existe (patrón timesheet_grid). Sin cliente en la fila, la celda es
        la del producto sin cliente (si el producto ya tiene líneas por cliente, el
        constraint anti-doble-conteo avisará). Los proyectos sin producto se capturan
        en la lista."""
        if not value:
            return
        domain = list(domain)
        fields_in = {leaf[0] for leaf in domain if isinstance(leaf, (list, tuple)) and len(leaf) == 3}
        if 'partner_id' not in fields_in:
            domain.append(('partner_id', '=', False))
        line = self.search(domain, limit=1)
        if line:
            line[measure_field_name] += value
            return
        vals = self._grid_cell_vals_from_domain(domain)
        vals.setdefault('partner_id', False)
        if not vals.get('budget_id') or not vals.get('product_id') or not vals.get('date'):
            raise UserError(
                "No se pudo ubicar la celda (producto/mes/presupuesto). Captura "
                "la línea desde la lista o la ficha.")
        vals[measure_field_name] = value
        self.create(vals)
