# -*- coding: utf-8 -*-
"""Pronóstico vigente (E1, diseño v3 sección 2.3).

Una sola tabla viva, sin estados: una fila por cliente × planta × producto ×
semana (lunes de la semana de embarque). Guarda la cantidad como la mandó el
cliente (``qty_origen`` en ``uom_origen``), en la unidad de venta del producto
y en metros y kilos, con el origen de la conversión. Cada cambio deja una fila
en ``qb.forecast.change``.

Sustituye al pronóstico como documento (``sgi.sales.budget`` con
``kind='pronostico'``), que desde 3.0.0 es histórico: obsoleto y de solo lectura.
"""
from collections import defaultdict
from datetime import date, datetime, timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from odoo.addons.quimibond_sgi.models.sgi_dev_product import DEV_PRODUCT_STATES

ORIGINS = [
    ('release', "Release del cliente"),
    ('captura', "Captura"),
    ('excel', "Forecast de Excel"),
    ('anual', "Forecast anual"),
    ('historico', "Histórico"),
    ('pedido', "Pedido del cliente"),
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


# 3.10.0 (diseño v3, 2.11.4): estados del artículo que el pronóstico marca. En desarrollo
# o archivado no se vende (el pedido no se confirma); en pilotaje se vende con aviso.
DEV_NO_SALE = ('desarrollo', 'archivado')
DEV_MARKED = DEV_NO_SALE + ('pilotaje',)


def dev_state_of(product):
    """Estado de desarrollo del producto o, si no lo tiene, el de su artículo (el código en
    kilos de un desarrollo nuevo puede no llevarlo). False si no nació de un desarrollo."""
    if not product:
        return False
    return product.sgi_dev_state or product.qb_articulo_id.sgi_dev_state or False


def _qb_fmt(qty):
    return ("%.2f" % (qty or 0.0)).rstrip('0').rstrip('.')


# 3.28.0: un pedido se liga al pronóstico de su misma semana de embarque. Ligar a la semana más cercana se
# probó y Jose lo rechazó (19-nov): el pedido se va a generar desde el pronóstico y moverlo mueve la semana.
LINK_WEEKS = 0


def monday_of(day):
    return day - timedelta(days=day.weekday()) if day else day


def team_of_partner(partner):
    """Equipo de ventas del cliente. En Odoo 19 ``res.partner`` ya no tiene ``team_id`` (2.0.3): sale
    del último pedido confirmado del cliente, si no de su última factura publicada y si no del equipo
    de su vendedor. Vacío si no hay ninguno."""
    if not partner:
        return None
    commercial = partner.commercial_partner_id
    env = partner.env
    if 'team_id' in commercial._fields and commercial.team_id:
        return commercial.team_id
    order = env['sale.order'].sudo().search([
        ('partner_id', 'child_of', commercial.id), ('state', '=', 'sale'), ('team_id', '!=', False)],
        order='date_order desc', limit=1)
    if order:
        return order.team_id
    move = env['account.move'].sudo().search([
        ('commercial_partner_id', '=', commercial.id), ('move_type', 'in', ('out_invoice', 'out_refund')),
        ('state', '=', 'posted'), ('team_id', '!=', False)], order='invoice_date desc', limit=1)
    if move:
        return move.team_id
    user = commercial.user_id
    return user.sale_team_id if user and 'sale_team_id' in user._fields and user.sale_team_id else None


def market_of_partner(partner, cache=None):
    """Mercado del cliente por el nombre de su equipo de ventas; 'otro' si no tiene. ``cache``: dict
    {partner_id: mercado} para no repetir la búsqueda en un lote."""
    if not partner:
        return 'otro'
    key = partner.commercial_partner_id.id
    if cache is not None and key in cache:
        return cache[key]
    team = team_of_partner(partner)
    market = TEAM_MARKET.get((team.name or '').strip().upper(), 'otro') if team else 'otro'
    if cache is not None:
        cache[key] = market
    return market


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
    # 3.2.0: columna de la matriz semanal (catálogo de lunes; la fecha no da columnas por semana en el grid).
    week_id = fields.Many2one('qb.forecast.week', string="Semana (matriz)", index=True, readonly=True,
                              group_expand='_read_group_week_id')
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
    # 3.17.0 (Jose, 18-nov): lo que pide el cliente en el release va aparte de lo que Ventas decide
    # enviar, como Planeación con el plan maestro. Una semana del release que Ventas corrige queda
    # «Ajustada» y el siguiente release ya no la pisa: solo actualiza «Pide el cliente» y la marca.
    qty_release = fields.Float(
        string="Pide el cliente", digits='Product Unit', readonly=True, aggregator='sum',
        help="Lo que pide el cliente en el último release, en la unidad de venta. La cantidad del "
             "pronóstico es la que Ventas decide enviar; si la corrige, queda «Ajustada».")
    ajustado = fields.Boolean(
        string="Ajustada por Ventas", readonly=True, index=True,
        help="Ventas corrigió la semana que vino de un release. El siguiente release conserva el "
             "ajuste: solo actualiza «Pide el cliente» y avisa si cambió.")
    release_cambio = fields.Boolean(
        string="El cliente cambió", readonly=True, index=True,
        help="Un release posterior al ajuste pide otra cantidad (o ya no trae la semana). Revise el "
             "ajuste: corríjalo, consérvelo o vuelva al release.")
    regla_nota = fields.Char(
        string="Reglas del release", readonly=True,
        help="Lo que hicieron las reglas del perfil al aplicar el release: semana congelada, recibe lo "
             "de las congeladas, junta semanas en un embarque, redondeo a rollos.")
    qty_diferencia = fields.Float(
        string="Contra lo que pide el cliente", digits='Product Unit', compute='_compute_qty_diferencia',
        help="Cantidad del pronóstico − lo que pide el cliente, en la unidad de venta (por un ajuste de "
             "Ventas o por las reglas del release).")
    active = fields.Boolean(default=True)
    dev_state = fields.Selection(
        DEV_PRODUCT_STATES, string="Estado del artículo", compute='_compute_dev_state', store=True,
        index=True,
        help="Estado de desarrollo del artículo (Desarrollo de producto). En desarrollo o archivado: "
             "se pronostica para que Planeación lo vea venir, pero el pedido no se confirma. En "
             "pilotaje: se vende con aviso. Vacío o liberado: de línea.")

    _line_uniq = models.Constraint(
        'unique nulls not distinct (company_id, partner_id, ship_to_id, plant, product_id, week)',
        "Ya hay pronóstico de ese cliente, planta, producto y semana.")

    @api.depends('product_id.sgi_dev_state', 'product_id.qb_articulo_id.sgi_dev_state')
    def _compute_dev_state(self):
        for line in self:
            line.dev_state = dev_state_of(line.product_id)

    @api.depends('qty_venta', 'qty_release', 'release_id')
    def _compute_qty_diferencia(self):
        for line in self:
            line.qty_diferencia = line.qty_venta - line.qty_release if line.release_id else 0.0

    @api.depends('week')
    def _compute_month(self):
        for line in self:
            line.month = line.week.replace(day=1) if line.week else False

    @api.depends('partner_id')
    def _compute_mercado(self):
        cache = {}
        for line in self:
            if not line.mercado or line.mercado == 'otro':
                line.mercado = market_of_partner(line.partner_id, cache) if line.partner_id else 'confeccion'

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
        weeks = self.env['qb.forecast.week']._qb_get([vals.get('week') for vals in vals_list])
        for vals in vals_list:
            if vals.get('week'):
                vals['week_id'] = weeks[vals['week']].id
        lines = super().create(vals_list)
        self.env['qb.forecast.change'].sudo().create(
            [line._qb_change_vals(0.0, line.qty_venta) for line in lines])
        return lines

    @api.model
    def _qb_en_unidad(self, product, qty_venta, unit):
        """(qty_origen, uom_origen) para escribir ``qty_venta`` (unidad de venta) en ``unit``. Si no hay
        unidad, falta el rendimiento o la vuelta a la unidad de venta no da lo mismo, se queda en la unidad
        de venta (3.31.0)."""
        if not unit or not product:
            return qty_venta, False
        qty = product.qb_convertir(qty_venta, 'venta', unit)
        back = product.qb_convertir(qty, unit, 'venta') if qty is not None else None
        if back is None or abs(back - qty_venta) > 1e-6 * max(1.0, abs(qty_venta)):
            return qty_venta, False
        return qty, unit

    def write(self, vals):
        # 3.31.0: cambiar solo la unidad del cliente cambia cómo se ve la cantidad, no la cantidad. Antes
        # el número se quedaba y se leía en la unidad nueva: el 20-nov, 37 semanas de Lear en kilos pasaron
        # a «metros» con el mismo número y bajaron 12 veces.
        if ('uom_origen' in vals and 'qty_origen' not in vals
                and not self.env.context.get('qb_release_apply')):
            for line in self:
                new_unit = vals['uom_origen'] or False
                if (line.uom_origen or False) == new_unit:
                    super(QbForecastLine, line).write(vals)
                    continue
                qty = line.product_id.qb_convertir(line.qty_origen, line.uom_origen or 'venta',
                                                   new_unit or 'venta')
                if qty is None:
                    raise UserError(
                        "No se puede pasar %s de %s a %s: falta el rendimiento m/kg o el ancho en la ficha. "
                        "Capture la cantidad junto con la unidad." % (
                            line.product_id.display_name, line.uom_origen or line.uom_venta,
                            new_unit or line.uom_venta))
                line.with_context(qb_release_apply=True).write(dict(vals, qty_origen=qty))
            return True
        if vals.get('week'):
            vals['week'] = monday_of(fields.Date.to_date(vals['week']))
            vals['week_id'] = self.env['qb.forecast.week']._qb_get([vals['week']])[vals['week']].id
        tracked = {'qty_origen', 'uom_origen', 'product_id'} & set(vals)
        before = {line.id: line.qty_venta for line in self} if tracked else {}
        # 3.17.0: un cambio de cantidad que no viene de aplicar un release es un ajuste de Ventas.
        manual = bool({'qty_origen', 'uom_origen'} & set(vals)) and not self.env.context.get('qb_release_apply')
        res = super().write(vals)
        if manual and not self.env.context.get('qb_pedido_sync'):
            # 3.21.0: Ventas corrige una semana que venía de un pedido: ya es suya y el pedido no la pisa.
            from_order = self.filtered(lambda l: l.origen == 'pedido')
            if from_order and 'origen' not in vals:
                super(QbForecastLine, from_order).write({'origen': 'captura'})
        if manual:
            for line in self.filtered('release_id'):
                ajustado = abs(line.qty_venta - line.qty_release) > 1e-6
                flags = {'ajustado': ajustado, 'release_cambio': False}
                if any(line[k] != v for k, v in flags.items()):
                    super(QbForecastLine, line).write(flags)
        if tracked:
            changes = [line._qb_change_vals(
                before[line.id], line.qty_venta,
                note="Ajuste de Ventas (pide el cliente: %s)" % _qb_fmt(line.qty_release)
                if manual and line.ajustado else False)
                for line in self if abs(before[line.id] - line.qty_venta) > 1e-6]
            if changes:
                self.env['qb.forecast.change'].sudo().create(changes)
        return res

    # --- Ajustes de Ventas sobre el release (3.17.0) ------------------------------
    def action_qb_open_paste(self):
        """Botón de la lista (3.20.0; antes menú «Pegar semanas desde Excel»)."""
        return self.env['ir.actions.act_window']._for_xml_id('quimibond_ventas_presupuesto.qb_forecast_paste_action')

    def action_qb_open_print(self):
        """Botón de la lista (3.20.0; antes menú «Imprimir o descargar»)."""
        return self.env['ir.actions.act_window']._for_xml_id('quimibond_ventas_presupuesto.qb_forecast_print_action')

    def action_qb_volver_release(self):
        """Deshace el ajuste: la semana vuelve a lo que pide el cliente. Si el release ya no la trae
        (pide 0), se quita."""
        lines = self.filtered('ajustado')
        gone = lines.filtered(lambda l: l.qty_release <= 1e-9)
        keep = lines - gone
        for line in keep:
            qty, unit = self._qb_en_unidad(line.product_id, line.qty_release, line.uom_origen)
            line.with_context(qb_release_apply=True).write({'qty_origen': qty, 'uom_origen': unit})
        keep.write({'ajustado': False, 'release_cambio': False})
        gone.unlink()
        keep.action_qb_refresh()
        return True

    def action_qb_conservar_ajuste(self):
        """Ventas revisó el cambio del cliente y se queda con su ajuste: quita la marca."""
        self.filtered('release_cambio').write({'release_cambio': False})
        return True

    def unlink(self):
        changes = [line._qb_change_vals(line.qty_venta, 0.0, note="Semana quitada") for line in self
                   if line.qty_venta]
        res = super().unlink()
        if changes:
            for vals in changes:
                vals['line_id'] = False
            self.env['qb.forecast.change'].sudo().create(changes)
        return res

    # --- Captura en matriz (web_grid, 3.1.0; columna week_id desde 3.2.0) -------
    GRID_KEYS = ('partner_id', 'ship_to_id', 'plant', 'product_id', 'uom_origen', 'company_id')

    @staticmethod
    def _qb_domain_leaves(domain):
        """Condiciones (campo, operador, valor) de un dominio, sea lista o ``Domain`` de Odoo 19."""
        try:
            from odoo.fields import Domain
            return [(c.field_expr, c.operator, c.value) for c in Domain(domain).iter_conditions()]
        except Exception:  # noqa: BLE001 - dominio en forma de lista
            return [tuple(leaf) for leaf in (domain or []) if isinstance(leaf, (list, tuple)) and len(leaf) == 3]

    @api.model
    def _read_group_week_id(self, weeks, domain):
        """Columnas de la matriz: las semanas del periodo del filtro («Esta semana», «Este mes»…, aunque
        estén vacías); sin periodo, las próximas 13 (3.3.0)."""
        start = end = None
        for name, op, value in self._qb_domain_leaves(domain):
            if name != 'week' or not value or op not in ('>=', '>', '<', '<='):
                continue
            if isinstance(value, datetime):
                day = value.date()
            elif isinstance(value, date):
                day = value
            else:
                day = fields.Date.to_date(str(value)[:10])
            if op in ('>=', '>'):
                day = day + timedelta(days=1) if op == '>' else day
                start = max(start, day) if start else day
            else:
                day = day - timedelta(days=1) if op == '<' else day
                end = min(end, day) if end else day
        Week = self.env['qb.forecast.week']
        if start and end and start <= end and (end - start).days <= 7 * 60:
            first = start + timedelta(days=(7 - start.weekday()) % 7)
            mondays = []
            while first <= end:
                mondays.append(first)
                first += timedelta(weeks=1)
            found = Week._qb_get(mondays)
            period = Week.browse([found[m].id for m in mondays])
        else:
            period = Week._qb_next()
        return (weeks | period).sorted('monday')

    @api.model
    def _qb_grid_cell(self, domain):
        """Valores de la celda (cliente, planta, producto, unidad) y el lunes de su semana, leídos
        del dominio del grid. La columna es un rango de fechas; si la semana del idioma empieza en
        domingo, el lunes es el siguiente día dentro del rango."""
        vals, start, end = {}, None, None
        for leaf in domain:
            if not (isinstance(leaf, (list, tuple)) and len(leaf) == 3):
                continue
            name, op, value = leaf
            if name in self.GRID_KEYS and op == '=':
                vals[name] = value
            elif name == 'week_id' and op == '=' and value:
                start = self.env['qb.forecast.week'].browse(value).monday
            elif name == 'week' and op in ('>=', '='):
                start = fields.Date.to_date(value)
            elif name == 'week' and op == '<':
                end = fields.Date.to_date(value)
        monday = start
        while monday and monday.weekday() != 0 and (not end or monday < end):
            monday += timedelta(days=1)
        if monday and monday.weekday() != 0:
            monday = monday_of(start)
        return vals, monday

    @api.model
    def grid_update_cell(self, domain, measure_field_name, value):
        """Suma ``value`` a la celda (cliente × producto × semana) en la unidad del cliente. Crea la
        semana si no existe y la quita si queda en cero; cada cambio queda en la bitácora. Una
        semana que escribió un release se puede corregir aquí: pasa a «Captura»."""
        if not value:
            return
        if measure_field_name != 'qty_origen':
            raise UserError("En la matriz solo se captura la cantidad del cliente.")
        lines = self.search(domain)
        if lines:
            line = lines[0]
            new = line.qty_origen + value
            if line.release_id:
                # 3.17.0: una semana del release se ajusta, no se quita: en cero queda «no enviamos»
                # y el siguiente release no la vuelve a poner.
                line.write({'qty_origen': max(new, 0.0)})
                (lines - line).unlink()
                line.action_qb_refresh()
            elif new <= 1e-9:
                lines.unlink()
            else:
                line.write({'qty_origen': new, 'origen': 'captura'})
                (lines - line).unlink()
                line.action_qb_refresh()
            return
        vals, monday = self._qb_grid_cell(domain)
        if not vals.get('product_id') or not monday:
            raise UserError("No se pudo ubicar la celda (producto y semana). Capture la semana desde la lista.")
        if not vals.get('partner_id'):
            vals['partner_id'] = self.env.context.get('default_partner_id')
        if not vals.get('partner_id'):
            raise UserError("Agrupe la matriz por cliente (o ábrala desde un cliente) para capturar una semana nueva.")
        if value < 0:
            return
        vals.update(week=monday, qty_origen=value, origen='captura')
        vals.setdefault('company_id', self.env.company.id)
        self.create(vals).action_qb_refresh()

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
        """Líneas de pedido confirmadas con cantidad pendiente de entregar **y una entrega abierta**.

        3.0.0: sin entrega abierta (todo hecho o cancelado) la línea ya no es demanda aunque
        ``product_uom_qty − qty_delivered`` sea positivo: es un faltante que se cerró sin
        backorder o un producto que se cambió al surtir. En producción eran 273 de 324
        «atrasados» (2026-11-05)."""
        # 3.0.1: «bloqueado» no filtra: producción bloquea solo cada pedido al confirmarlo
        # (venta → bloquear pedidos confirmados), así que un pedido vigente puede estar bloqueado.
        domain = [('order_id.state', '=', 'sale'), ('company_id', '=', company.id),
                  ('product_id', '!=', False), ('display_type', '=', False)]
        if partners:
            domain.append(('order_id.partner_id.commercial_partner_id', 'in', partners.ids))
        sols = self.env['sale.order.line'].sudo().search(domain)
        return sols.filtered(lambda s: s.product_uom_qty - s.qty_delivered > 1e-6 and any(
            move.state not in ('done', 'cancel') for move in s.move_ids))

    @staticmethod
    def _qb_sol_week(sol):
        order = sol.order_id
        when = order.commitment_date or order.expected_date or order.date_order
        return monday_of(fields.Datetime.to_datetime(when).date()) if when else False

    @staticmethod
    def _qb_sol_forecast_week(sol):
        """3.28.0: semana del pronóstico de una línea de pedido: la de embarque con que nació de «Proponer
        pedidos» (``qb_forecast_week``) o, si se capturó a mano, el lunes de su fecha compromiso."""
        week = sol.order_id.qb_forecast_week
        return week or QbForecastLine._qb_sol_week(sol)

    @staticmethod
    def _qb_nearest_week(week, weeks, window=LINK_WEEKS):
        """3.28.0: la semana con pronóstico a la que se liga un pedido: la suya, o la más cercana hasta
        ``window`` semanas (0 = solo la suya). None si no hay."""
        if not week:
            return None
        if week in weeks:
            return week
        near = [w for w in weeks if abs((w - week).days) <= window * 7]
        return min(near, key=lambda w: (abs((w - week).days), w)) if near else None

    @api.model
    def _qb_ordered_by_week(self, company, partner, articulo, ship_to=None, weeks=()):
        """3.28.0: lo pedido (confirmado, completo: lo ya entregado también cuenta) del artículo para el
        cliente, por semana del pronóstico, en la unidad de venta de ``articulo``. Con dirección de entrega,
        solo los pedidos de esa dirección."""
        domain = [('order_id.state', '=', 'sale'), ('company_id', '=', company.id),
                  ('order_id.partner_id', 'child_of', partner.commercial_partner_id.id),
                  ('display_type', '=', False),
                  '|', ('product_id', '=', articulo.id), ('product_id.qb_articulo_id', '=', articulo.id)]
        if ship_to:
            domain.append(('order_id.partner_shipping_id', '=', ship_to.id))
        out = defaultdict(float)
        for sol in self.env['sale.order.line'].sudo().search(domain):
            week = self._qb_sol_forecast_week(sol)
            if not week or (weeks and week not in weeks):
                continue
            qty = sol.product_uom_qty
            if sol.product_uom_id and sol.product_uom_id._has_common_reference(sol.product_id.uom_id):
                qty = sol.product_uom_id._compute_quantity(qty, sol.product_id.uom_id, round=False)
            if sol.product_id != articulo:
                qty = sol.product_id.qb_convertir(qty, 'venta', articulo._qb_sale_unit())
            out[week] += qty or 0.0
        return dict(out)

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

    @api.model
    def _qb_sync_pedidos(self, company, partners=None):
        """3.21.0 (Jose, 19-nov): los pedidos confirmados de un producto que el cliente no pronostica
        (producto de línea de otro cliente, proyecto, muestra) entran al pronóstico, porque también van
        al presupuesto. Una semana por cliente, producto y semana de compromiso, con origen «Pedido del
        cliente» y la cantidad pedida (no la pendiente: lo ya entregado sigue en el mes).

        Solo desde la semana en curso: las semanas pasadas no se tocan. Si el cliente ya pronostica ese
        artículo (release, captura, Excel), el pronóstico manda y el pedido no se agrega; si Ventas corrige
        la semana, pasa a «Captura» y el pedido ya no la cambia. Confección pronostica por producto, sin
        cliente: sus pedidos no se agregan. Devuelve las líneas creadas o cambiadas."""
        current = monday_of(fields.Date.context_today(self))
        # Los pedidos que nacieron de «Proponer pedidos» ya son el pronóstico: no se vuelven a agregar.
        domain = [('order_id.state', '=', 'sale'), ('company_id', '=', company.id),
                  ('order_id.qb_from_forecast', '=', False),
                  ('product_id', '!=', False), ('display_type', '=', False)]
        if partners is not None:
            domain.append(('order_id.partner_id.commercial_partner_id', 'in', partners.ids))
        wanted = defaultdict(float)
        info = {}
        markets = {}
        for sol in self.env['sale.order.line'].sudo().search(domain):
            week = self._qb_sol_forecast_week(sol)
            if not week or week < current:
                continue
            partner = sol.order_id.partner_id.commercial_partner_id
            if market_of_partner(partner, markets) in ('confeccion', 'proyectos_confeccion'):
                continue
            qty = sol.product_uom_qty
            if sol.product_uom_id and sol.product_uom_id._has_common_reference(sol.product_id.uom_id):
                qty = sol.product_uom_id._compute_quantity(qty, sol.product_id.uom_id, round=False)
            key = (partner.id, sol.product_id.id, week)
            wanted[key] += qty
            order = sol.order_id
            ship = order.partner_shipping_id
            info.setdefault(key, {
                'ship_to_id': ship.id if ship and ship != partner else False,
                'customer_po': order.client_order_ref or order.name,
                'delivery_date': fields.Datetime.to_datetime(order.commitment_date).date()
                if order.commitment_date else False})
        line_domain = [('company_id', '=', company.id), ('week', '>=', current), ('partner_id', '!=', False)]
        if partners is not None:
            line_domain.append(('partner_id', 'in', partners.ids))
        else:
            line_domain.append(('partner_id', 'in', list({k[0] for k in wanted})))
        lines = self.sudo().search(line_domain)
        own = lines.filtered(lambda l: l.origen != 'pedido')
        # El pedido no se agrega si el cliente pronostica ese artículo (3.21.0).
        forecasted = defaultdict(set)
        for l in own:
            forecasted[(l.partner_id.id, (l.articulo_id or l.product_id).id)].add(l.week)
        current_lines = {(l.partner_id.id, l.product_id.id, l.week): l for l in lines - own}
        Product = self.env['product.product'].sudo()
        Sync = self.sudo().with_context(qb_pedido_sync=True)
        touched = self.browse()
        to_create = []
        for key, qty in wanted.items():
            partner_id, product_id, week = key
            product = Product.browse(product_id)
            if (partner_id, (product.qb_articulo_id or product).id) in forecasted or qty <= 1e-6:
                continue
            line = current_lines.pop(key, None)
            vals = dict(info[key], qty_origen=round(qty, 4), uom_origen=False)
            if line:
                if abs(line.qty_origen - vals['qty_origen']) > 1e-6 or line.uom_origen:
                    Sync.browse(line.id).write(vals)
                    touched |= line
            else:
                vals.update(company_id=company.id, partner_id=partner_id, product_id=product_id, week=week,
                            origen='pedido')
                to_create.append(vals)
        if to_create:
            touched |= Sync.create(to_create)
        # Semanas de pedido que ya no tienen pedido (se canceló o cambió de semana) o que ahora pronostica
        # el cliente.
        stale = self.browse([l.id for l in current_lines.values()])
        if stale:
            stale.sudo().unlink()
        return touched

    def action_qb_refresh(self):
        """Recalcula zona, pedido y pronóstico sin pedido de estas líneas (todas si vacío). Antes, los
        pedidos de productos que el cliente no pronostica entran al pronóstico (3.21.0)."""
        if self:
            lines = self
            for company in self.company_id:
                partners = self.filtered(lambda l: l.company_id == company).partner_id
                if partners:
                    lines |= self._qb_sync_pedidos(company, partners)
            lines = lines.exists()
        else:
            for company in self.env['res.company'].sudo().search([]):
                self._qb_sync_pedidos(company)
            lines = self.search([('week', '>=', monday_of(fields.Date.context_today(self))
                                  - timedelta(weeks=8))])
        today = fields.Date.context_today(self)
        for company in lines.company_id:
            company_lines = lines.filtered(lambda l: l.company_id == company)
            partners = company_lines.partner_id
            firm_by_partner = self._qb_firm_by_partner(company)
            default_firm = self._qb_firm_weeks()
            groups = defaultdict(lambda: self.browse())
            for line in company_lines:
                groups[(line.partner_id.id, (line.articulo_id or line.product_id).id, line.week)] |= line
            weeks_of = defaultdict(set)
            for partner_id, articulo_id, week in groups:
                weeks_of[(partner_id, articulo_id)].add(week)
            pedidos = defaultdict(list)
            for sol in self._qb_open_sale_lines(company, partners):
                fl = sol.qb_forecast_line_id
                if fl and fl in company_lines:
                    # 3.29.0: un renglón ligado cuenta en su semana del pronóstico.
                    pedidos[(fl.partner_id.id, (fl.articulo_id or fl.product_id).id, fl.week)].append(sol)
                    continue
                pa = (sol.order_id.partner_id.commercial_partner_id.id,
                      (sol.product_id.qb_articulo_id or sol.product_id).id)
                # 3.28.0: semana de embarque del pedido (la de «Proponer pedidos» o la de su compromiso).
                week = self._qb_nearest_week(self._qb_sol_forecast_week(sol), weeks_of.get(pa, ()))
                if week:
                    pedidos[pa + (week,)].append(sol)
            for key, group in groups.items():
                # 3.21.0: la semana que vino de un pedido va al final (recibe lo que sobre del pedido).
                group = group.sorted(
                    lambda l: (l.origen == 'pedido', l.ship_to_id.id or 0, l.plant or '', l.id))
                available = 0.0
                for sol in pedidos.get(key, []):
                    available += self._qb_sol_pending(sol, group[0].product_id) or 0.0
                for index, line in enumerate(group):
                    take = available if index == len(group) - 1 else min(available, line.qty_venta)
                    available -= take
                    # 3.21.0: una semana que vino de un pedido no es pronóstico sin pedido (lo ya
                    # entregado no vuelve a la Demanda).
                    neta = 0.0 if line.origen == 'pedido' else max(line.qty_venta - take, 0.0)
                    vals = {'qty_pedido': take, 'qty_neta': neta,
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
