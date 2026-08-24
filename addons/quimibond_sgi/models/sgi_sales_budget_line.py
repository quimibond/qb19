# -*- coding: utf-8 -*-
"""Línea del presupuesto de ventas (producto × mes/semana).

La cabecera vive en sgi_sales_budget.py; aquí la línea con su precio de lista,
la foto del real (facturado/comprometido/pedido), la cobertura del pronóstico
y las guardas de captura. Se separó del archivo de la cabecera solo por tamaño
(mismo modelo lógico); comparte los helpers _REAL_MOVE_TYPES y _convert_qty.
"""
from collections import defaultdict
from datetime import timedelta

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

from .sgi_base import sgi_bypass_allowed
from .sgi_sales_budget import _REAL_MOVE_TYPES, _convert_qty


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

    # Cobertura del pronóstico (P-A28 4.2.2.7): comprometido (qty_real, que en el
    # pronóstico son los pedidos confirmados por producto+cliente+semana) vs lo
    # pronosticado. Foto: se refresca con el mismo mecanismo del real. Solo aplica
    # a kind=pronostico; en presupuesto queda 'fuera_horizonte'/0.
    coverage_pct = fields.Float(
        string="Cobertura del pronóstico", compute='_compute_real', store=True,
        help="Comprometido / pronosticado (pedidos confirmados de la semana entre "
             "lo pronosticado). Solo pronóstico.")
    coverage_state = fields.Selection([
        ('cubierto', "Cubierto"),
        ('parcial', "Parcial"),
        ('sin_pedido', "Sin pedido"),
        ('excedido', "Excedido"),
        ('fuera_horizonte', "Fuera de horizonte"),
    ], string="Estado de cobertura", default='fuera_horizonte',
        compute='_compute_real', store=True)
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
    price_real_unit_currency = fields.Monetary(
        string="Precio real (divisa)", currency_field='list_currency_id',
        compute='_compute_real', store=True,
        help="Precio unitario facturado promedio, en la moneda de la lista. Si la "
             "factura está en otra moneda, se convierte el promedio contable con "
             "el tipo presupuestal (ver Desviación cruza divisas).")
    price_gap_fx = fields.Boolean(
        string="Desviación cruza divisas", compute='_compute_real', store=True,
        help="La comparación de precio usó conversión (la factura no estaba en la "
             "moneda de la lista); tómala como referencia.")
    price_gap = fields.Monetary(
        string="Desviación de precio (divisa)", currency_field='list_currency_id',
        compute='_compute_real', store=True)
    price_gap_pct = fields.Float(
        string="Desviación de precio (%)", compute='_compute_real', store=True)
    price_gap_alert = fields.Selection([
        ('ok', "OK"), ('leve', "Leve"), ('grave', "Grave"),
    ], string="Alerta de precio", default='ok',
        compute='_compute_real', store=True)

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

    def _sgi_min_plausible(self):
        """Umbral (moneda compañía) por debajo del cual un precio resuelto se toma
        como placebo (placeholder $1) aunque venga de una regla."""
        return float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.price_min_plausible', 5.0) or 0)

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
        min_plausible = self._sgi_min_plausible()
        implausible = (not fell_to_sale) and price < min_plausible
        has_price = (not fell_to_sale) and (not implausible)
        if fell_to_sale:
            source = ("SIN REGLA en lista '%s'; precio de venta del producto: "
                      "%s %s (NO usar)" % (
                          pricelist.name, '{:,.2f}'.format(price),
                          company_currency.name))
        elif implausible:
            source = ("Lista '%s': regla implausible < %s %s (%s %s — placeholder, "
                      "NO usar)" % (
                          pricelist.name, '{:,.2f}'.format(min_plausible),
                          company_currency.name, '{:,.2f}'.format(price),
                          company_currency.name))
        elif list_currency and list_currency != company_currency:
            source = "Lista '%s': %.4g %s × %.4g = %s %s" % (
                pricelist.name, raw, list_currency.name, rate,
                '{:,.2f}'.format(price), company_currency.name)
        else:
            source = "Lista '%s': %s %s" % (
                pricelist.name, '{:,.2f}'.format(price), company_currency.name)
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
        dos veces contra el mismo real.

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
                schemes.setdefault(product.id, set()).add(bool(sibling.partner_id))
                products[product.id] = product
            for product_id, kinds in schemes.items():
                if len(kinds) > 1:
                    raise ValidationError(
                        "El producto '%s' ya está presupuestado por cliente en este "
                        "presupuesto; captura el resto como otro cliente o cambia el "
                        "esquema (no mezcles líneas con cliente y sin cliente para el "
                        "mismo producto)." % products[product_id].display_name)

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
            # La cobertura solo aplica al pronóstico; el presupuesto queda neutral.
            line.coverage_pct = 0.0
            line.coverage_state = 'fuera_horizonte'
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
                amt_list = qty_list = 0.0  # importe/cantidad en la divisa de la lista
                any_non_list = False
                partner = line.partner_id.commercial_partner_id
                list_ccy = line.list_currency_id
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

    def _sgi_set_price_gap(self, qty, amount, amt_list, qty_list, any_non_list,
                           day, tol, grave):
        """Desviación de precio facturado vs lista (en la divisa de la lista)."""
        self.ensure_one()
        list_ccy = self.list_currency_id
        if not list_ccy or qty <= 0 or not self.has_list_price:
            return  # ya está en 0/ok
        if qty_list and not any_non_list:
            # Todas las facturas en la divisa de la lista: precio directo.
            price_real = amt_list / qty_list
            self.price_gap_fx = False
        else:
            # Convierte el promedio contable (MXN) a la divisa de la lista.
            price_real = self._sgi_company_to_list_price(amount / qty, list_ccy, day)
            self.price_gap_fx = True
        self.price_real_unit_currency = price_real
        gap = price_real - self.price_unit_currency
        self.price_gap = gap
        pct = (gap / self.price_unit_currency * 100.0) if self.price_unit_currency else 0.0
        self.price_gap_pct = pct
        magnitude = abs(pct)
        if magnitude <= tol:
            self.price_gap_alert = 'ok'
        elif magnitude <= grave:
            self.price_gap_alert = 'leve'
        else:
            self.price_gap_alert = 'grave'

    def _sgi_company_to_list_price(self, price, list_currency, day):
        """Convierte un precio en moneda de la compañía a la moneda de la lista con
        el tipo presupuestal (inverso de _sgi_to_company_price: ÷ factor)."""
        company = self.company_id or self.env.company
        company_currency = self.currency_id or company.currency_id
        if not list_currency or list_currency == company_currency:
            return price
        planning = self._sgi_planning_factor(list_currency, company_currency)
        if planning is not None:
            factor, _rate = planning
            return price / factor
        return company_currency._convert(price, list_currency, company, day)

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
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
        ])
        return sols.filtered(
            lambda s: self._sgi_effective_monday(s.order_id) == self.date)

    def _sgi_compute_real_committed(self):
        SOL = self.env['sale.order.line']
        Param = self.env['ir.config_parameter'].sudo()
        over_tol = float(Param.get_param(
            'quimibond_sgi.forecast_over_tolerance_pct', 10.0) or 0)
        horizon = int(float(Param.get_param(
            'quimibond_sgi.forecast_capture_horizon_weeks', 3) or 1))
        today = fields.Date.context_today(self)
        current_monday = today - timedelta(days=today.weekday())
        by_partner = defaultdict(lambda: self.browse())
        for line in self:
            line.qty_real = 0.0
            line.amount_real = 0.0
            line.unconverted_count = 0
            # El control de precios es sobre lo facturado; el pronóstico no lo usa.
            line.price_real_unit_currency = 0.0
            line.price_gap_fx = False
            line.price_gap = 0.0
            line.price_gap_pct = 0.0
            line.price_gap_alert = 'ok'
            line.coverage_pct = 0.0
            line.coverage_state = 'fuera_horizonte'
            if line.partner_id and line.product_id and line.date:
                by_partner[(line.partner_id.commercial_partner_id.id,
                            line.company_id.id)] |= line
        for (partner_id, company_id), lines in by_partner.items():
            products = lines.mapped('product_id')
            sols = SOL.search([
                ('order_id.state', 'in', ('sale', 'done')),
                ('product_id', 'in', products.ids),
                ('order_id.partner_id.commercial_partner_id', '=', partner_id),
                ('company_id', '=', company_id),  # solo la compañía del ppto
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
                    conv = _convert_qty(
                        sol.product_uom_qty, sol.product_uom_id, line.uom_id)
                    if conv is None:
                        unconverted += 1
                    else:
                        qty += conv
                line.qty_real = qty
                line.amount_real = amount
                line.unconverted_count = unconverted
                line._sgi_set_coverage(qty, current_monday, horizon, over_tol)

    def _sgi_set_coverage(self, committed, current_monday, horizon, over_tol):
        """Estado de cobertura de una línea de pronóstico (P-A28 4.2.2.7)."""
        self.ensure_one()
        self.coverage_pct = (committed / self.qty_budget) if self.qty_budget else 0.0
        last_monday = current_monday + timedelta(weeks=horizon - 1)
        if not (current_monday <= self.date <= last_monday):
            self.coverage_state = 'fuera_horizonte'
        elif committed <= 0:
            self.coverage_state = 'sin_pedido'
        elif self.coverage_pct > 1.0 + over_tol / 100.0:
            self.coverage_state = 'excedido'
        elif self.coverage_pct >= 1.0:
            self.coverage_state = 'cubierto'
        else:
            self.coverage_state = 'parcial'

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

    def action_create_draft_quotation(self):
        """Crea una cotización BORRADOR por el faltante (qty_budget − comprometido)
        para cerrar la semana descubierta. NO la confirma (demanda real la genera
        el cliente); si ya existe un borrador con ese origin para el producto/
        semana, la reabre en vez de duplicar."""
        self.ensure_one()
        if self.budget_id.kind != 'pronostico':
            raise UserError("La cotización se crea desde una línea de pronóstico.")
        partner = self.partner_id or self.budget_id.partner_id
        if not partner:
            raise UserError("El pronóstico no tiene cliente.")
        shortfall = self.qty_budget - self.qty_real
        if shortfall <= 0:
            raise UserError(
                "No hay faltante en %s: los pedidos ya cubren el pronóstico." % (
                    self.display_name))
        from datetime import datetime, time as _time
        origin = self.budget_id.folio or self.budget_id.name
        SO = self.env['sale.order']
        existing = SO.search([
            ('state', '=', 'draft'), ('origin', '=', origin),
            ('partner_id', '=', partner.id),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('order_line.product_id', '=', self.product_id.id),
        ]).filtered(
            lambda o: o.commitment_date and o.commitment_date.date() == self.date)
        order = existing[:1] or SO.create({
            'partner_id': partner.id,
            'origin': origin,
            'commitment_date': datetime.combine(self.date, _time()),
            'team_id': self.team_id.id,
            'company_id': self.company_id.id,
            'order_line': [(0, 0, {
                'product_id': self.product_id.id,
                'product_uom_qty': shortfall,
                'product_uom_id': self.uom_id.id,
            })],
        })
        return {
            'type': 'ir.actions.act_window',
            'name': "Cotización — %s" % self.display_name,
            'res_model': 'sale.order',
            'view_mode': 'form',
            'res_id': order.id,
        }

    def action_view_month_invoices(self):
        """Drill-down del presupuesto: las facturas del producto/equipo(/cliente)
        cuyo mes es el de la línea — análogo a "Ver pedidos de la semana"."""
        self.ensure_one()
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

    @api.depends('product_id', 'partner_id', 'date', 'kind', 'uom_id')
    def _compute_qty_forecast(self):
        """Suma lo pronosticado del mismo producto/cliente/mes por los pronósticos
        vigentes (no obsoletos) del año, convertido a la unidad de esta línea."""
        for line in self:
            line.qty_forecast = 0.0
            if line.kind != 'presupuesto' or not line.product_id or not line.date:
                continue
            forecasts = self.env['sgi.sales.budget'].search([
                ('kind', '=', 'pronostico'),
                ('year', '=', line.budget_id.year),
                ('state', '!=', 'obsoleto'),
                ('company_id', '=', line.company_id.id)])
            partner = line.partner_id.commercial_partner_id
            total = 0.0
            for fl in forecasts.mapped('line_ids'):
                if fl.product_id != line.product_id or not fl.date:
                    continue
                if fl.date.month != line.date.month:
                    continue
                if partner and fl.partner_id.commercial_partner_id != partner:
                    continue
                conv = _convert_qty(fl.qty_budget, fl.uom_id, line.uom_id)
                total += conv if conv is not None else fl.qty_budget
            line.qty_forecast = total

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
        lines = super().create(vals_list)
        # Agregar líneas a un documento 'revisado' lo regresa a borrador: el
        # pronóstico porque es documento vivo (P-A28 4.2.2.7); el presupuesto para
        # que Dirección no apruebe contenido distinto al que revisó el Admin.
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
            if budget.kind == 'pronostico':
                body = "Actualizado por %s, requiere revisión." % self.env.user.name
            else:
                body = ("Actualizado por %s tras la revisión, requiere "
                        "re-revisión." % self.env.user.name)
            budget.message_post(body=body)

    def _sgi_locked_lines(self):
        # El pronóstico es documento vivo: SIN candado de líneas (P-A28 4.2.2.7);
        # solo el presupuesto aprobado congela sus líneas.
        return self.filtered(
            lambda l: l.kind != 'pronostico'
            and l.budget_id.state in self._SGI_LOCKED_PARENT_STATES)

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
        res = super().write(vals)
        # Tocar campos de captura de un documento 'revisado' lo regresa a borrador
        # (pronóstico documento vivo P-A28 4.2.2.7; presupuesto: gobernanza del
        # revisado). Solo la edición de captura cuenta, no el refresh de la foto.
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
