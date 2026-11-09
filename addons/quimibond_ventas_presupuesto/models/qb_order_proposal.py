# -*- coding: utf-8 -*-
"""Pedidos desde el pronóstico (19.0.3.15.0, E3; decisiones de Jose del 9-nov).

**Regla única:** una semana del pronóstico se vuelve pedido cuando está en la **zona a pedido**
(las «Semanas a pedido» de la regla del cliente, desde la semana en curso) **y tiene OC**. Lo
demás sigue como pronóstico (la visión de los periodos siguientes).

**Un pedido por OC y semana de embarque** (cotización), con la OC en «Referencia del cliente» y
la semana como fecha compromiso: así cada entrega tiene su fecha, que es lo que mide el OTIF, y
la Demanda y el consumo del pronóstico ya leen los pedidos por esa fecha.

**De dónde sale la OC** (`qb.order.rule.po_mode`, una regla por cliente y, si hace falta, planta):

- ``release``: la OC de cada semana del pronóstico (TQ-1, Contitech, FXI…); sin OC, no se pide.
- ``abierta``: la OC abierta de la regla, mientras esté vigente (Lear, Shawmut, Saltillo…).
- ``release_sin_oc``: el release es la autorización; la referencia es ``REL-<release>``.
- ``manual``: no se propone; los pedidos se capturan a mano.

La OC del pronóstico se limpia con ``release/oc.py`` (viene del Excel como «6700/PV17173»,
«PO/14792» o con el encabezado «PO»).

**Qué propone por OC, semana y producto:** la cantidad del pronóstico contra la de las
cotizaciones y pedidos abiertos de esa OC y semana: **nuevo**, **aumenta**, **baja** o **sin
cambio**. Un pedido confirmado (producción lo bloquea al confirmar) lo ajusta el **administrador
de ventas** como se hace a mano: se desbloquea, se cambia la cantidad, se vuelve a bloquear y queda
nota en el chatter (3.15.2). Los aumentos vienen marcados; las bajas en confirmados, desmarcadas.
Para otro usuario quedan **por autorizar**. Nunca baja de lo entregado. No se propone: sin precio en la lista del cliente, artículo en desarrollo
o archivado, ni lo que no tiene OC (cada caso queda en la lista con su motivo).

**Facturar sin OC:** con «Exige OC para facturar» en la regla, un pedido cuya referencia es un
release (``REL-…``) no se factura hasta capturar la OC.
"""
from collections import OrderedDict
from datetime import datetime, time, timedelta

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

from ..release.oc import separa_oc
from .qb_forecast import DEV_NO_SALE, monday_of

PO_MODES = [
    ('release', "OC en el release"),
    ('abierta', "OC abierta + release"),
    ('release_sin_oc', "Release sin OC"),
    ('manual', "Sin release (OC manual)"),
]
RELEASE_REF_PREFIX = 'REL-'
PROPOSAL_ACTIONS = [
    ('nuevo', "Pedido nuevo"), ('aumenta', "Aumenta"), ('baja', "Baja"),
    ('por_autorizar', "Baja por autorizar"), ('sin_cambio', "Sin cambio"), ('no_entra', "No entra"),
]


class QbOrderRule(models.Model):
    _name = 'qb.order.rule'
    _description = "Cómo pide el cliente (pedidos desde el pronóstico)"
    _inherit = ['mail.thread']
    _order = 'partner_id, plant'

    name = fields.Char(string="Regla", compute='_compute_name', store=True)
    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True,
                                 default=lambda self: self.env.company)
    partner_id = fields.Many2one('res.partner', string="Cliente", required=True, index=True, tracking=True,
                                 domain="[('is_company', '=', True)]")
    plant = fields.Char(string="Planta", tracking=True,
                        help="Vacío: vale para todas las plantas del cliente. Con planta (como la escribe el "
                             "pronóstico), solo para esa; gana sobre la regla sin planta.")
    po_mode = fields.Selection(PO_MODES, string="Cómo se pide", required=True, default='release', tracking=True)
    open_po = fields.Char(string="OC abierta", tracking=True,
                          help="Solo en «OC abierta + release»: la OC que cubre las entregas del release.")
    open_po_valid_to = fields.Date(string="Vigencia de la OC abierta", tracking=True,
                                   help="Vacío: sin fecha de fin. Vencida, las semanas ya no se proponen.")
    so_weeks = fields.Integer(string="Semanas a pedido", default=4, tracking=True,
                              help="Semanas del pronóstico, desde la semana en curso, que se proponen como pedido.")
    po_required_invoice = fields.Boolean(
        string="Exige OC para facturar", tracking=True,
        help="Un pedido cuya referencia es un release (REL-…) no se factura hasta capturar la OC.")
    note = fields.Text(string="Nota")
    active = fields.Boolean(default=True)

    _partner_plant_uniq = models.Constraint(
        'unique nulls not distinct (company_id, partner_id, plant)',
        "Ya hay una regla de pedido para ese cliente y planta.")

    @api.depends('partner_id', 'plant')
    def _compute_name(self):
        for rule in self:
            rule.name = "%s — %s" % (rule.partner_id.name, rule.plant) if rule.plant else rule.partner_id.name

    @api.constrains('po_mode', 'open_po')
    def _check_open_po(self):
        for rule in self:
            if rule.po_mode == 'abierta' and not (rule.open_po or '').strip():
                raise ValidationError("«OC abierta + release» lleva el número de la OC abierta.")

    @api.model
    def _qb_rule_for(self, company, partner, plant):
        """La regla de la planta si existe; si no, la del cliente sin planta."""
        rules = self.search([('company_id', '=', company.id),
                             ('partner_id', '=', partner.commercial_partner_id.id)])
        return rules.filtered(lambda r: plant and (r.plant or '').strip().lower() == plant.strip().lower())[:1] \
            or rules.filtered(lambda r: not r.plant)[:1]


class SaleOrderForecast(models.Model):
    _inherit = 'sale.order'

    qb_from_forecast = fields.Boolean(string="Desde el pronóstico", readonly=True, copy=False)
    qb_forecast_week = fields.Date(string="Semana de embarque (pronóstico)", readonly=True, copy=False)

    def _create_invoices(self, *args, **kwargs):
        """Un pedido cuya referencia es un release (sin OC) no se factura si la regla del cliente
        exige la OC para facturar."""
        Rule = self.env['qb.order.rule'].sudo()
        for order in self:
            ref = (order.client_order_ref or '').strip()
            if not ref.upper().startswith(RELEASE_REF_PREFIX):
                continue
            rule = Rule._qb_rule_for(order.company_id, order.partner_id, False)
            if rule.po_required_invoice:
                raise UserError(
                    "El pedido %s se creó con el release %s y %s exige la OC para facturar: capture la OC "
                    "en «Referencia del cliente» antes de facturar." % (order.name, ref, order.partner_id.name))
        return super()._create_invoices(*args, **kwargs)


class SaleOrderLineForecast(models.Model):
    _inherit = 'sale.order.line'

    qb_forecast_week = fields.Date(string="Semana del pronóstico", readonly=True, copy=False)
    qb_release_id = fields.Many2one('qb.release', string="Release", readonly=True, copy=False, index=True)
    qb_customer_part = fields.Char(string="Parte del cliente", readonly=True, copy=False)


class QbOrderProposal(models.TransientModel):
    _name = 'qb.order.proposal'
    _description = "Proponer pedidos desde el pronóstico"

    partner_ids = fields.Many2many('res.partner', string="Clientes", domain="[('is_company', '=', True)]",
                                   help="Vacío: todos los clientes con regla de pedido.")
    date_from = fields.Date(string="Desde la semana", required=True,
                            default=lambda self: monday_of(fields.Date.context_today(self)))
    line_ids = fields.One2many('qb.order.proposal.line', 'proposal_id', string="Propuesta")
    count_apply = fields.Integer(string="A aplicar", compute='_compute_counts')
    count_skip = fields.Integer(string="No entran", compute='_compute_counts')

    @api.depends('line_ids.apply', 'line_ids.action')
    def _compute_counts(self):
        for proposal in self:
            proposal.count_apply = len(proposal.line_ids.filtered('apply'))
            proposal.count_skip = len(proposal.line_ids.filtered(lambda l: l.action == 'no_entra'))

    # --- Armado ---------------------------------------------------------------
    def _qb_reference(self, rule, fl):
        """(referencia, motivo si no hay) de la semana ``fl`` según el modo de la regla."""
        oc, _pv = separa_oc(fl.customer_po)
        if rule.po_mode == 'abierta':
            if rule.open_po_valid_to and fl.week > rule.open_po_valid_to:
                return '', "La OC abierta %s venció el %s" % (rule.open_po, rule.open_po_valid_to)
            return rule.open_po.strip(), ''
        if rule.po_mode == 'release':
            return (oc, '') if oc else ('', "La semana no trae OC")
        if rule.po_mode == 'release_sin_oc':
            if oc:
                return oc, ''
            release = fl.release_id.release_ref or fl.release_id.name
            if release:
                return "%s%s" % (RELEASE_REF_PREFIX, release), ''
            return "%s%s" % (RELEASE_REF_PREFIX, fl.week.isoformat()), ''
        return '', "El cliente pide a mano (sin release)"

    @api.model
    def _qb_week_bounds(self, week):
        start = datetime.combine(week, time.min)
        return start, start + timedelta(days=7)

    def _qb_existing(self, company, partner, ref, week, product):
        """Líneas de cotizaciones y pedidos abiertos del cliente con esa OC y semana, y del producto."""
        start, end = self._qb_week_bounds(week)
        return self.env['sale.order.line'].sudo().search([
            ('order_id.company_id', '=', company.id),
            ('order_id.partner_id', 'child_of', partner.commercial_partner_id.id),
            ('order_id.state', 'in', ('draft', 'sent', 'sale')),
            ('order_id.client_order_ref', '=', ref),
            ('order_id.commitment_date', '>=', start), ('order_id.commitment_date', '<', end),
            ('product_id', '=', product.id), ('display_type', '=', False)])

    def _qb_price(self, partner, product, qty):
        """(precio de la lista del cliente en la moneda de la lista, hay precio)."""
        probe = self.env['sgi.sales.budget.line'].new({
            'partner_id': partner.id, 'product_id': product.id, 'uom_id': product.uom_id.id,
            'qty_budget': qty or 1.0})
        _company_price, _src, _ccy, raw, has_price = probe._sgi_pricelist_price()
        return raw, bool(has_price and raw > 0)

    def action_build(self):
        """Arma la propuesta: por cliente, planta, OC, semana y producto, del pronóstico vigente de las
        semanas a pedido de cada regla."""
        self.ensure_one()
        Rule = self.env['qb.order.rule']
        Forecast = self.env['qb.forecast.line'].sudo()
        company = self.env.company
        rules = Rule.search([('company_id', '=', company.id)]
                            + ([('partner_id', 'in', self.partner_ids.commercial_partner_id.ids)]
                               if self.partner_ids else []))
        self.line_ids.unlink()
        start = monday_of(self.date_from)
        groups = OrderedDict()
        for partner in rules.partner_id:
            horizon = max(rules.filtered(lambda r: r.partner_id == partner).mapped('so_weeks') or [4])
            lines = Forecast.search([
                ('company_id', '=', company.id), ('partner_id.commercial_partner_id', '=', partner.id),
                ('week', '>=', start), ('week', '<', start + timedelta(weeks=horizon))],
                order='week, product_id')
            for fl in lines:
                plant = fl.plant or fl.ship_to_id.name or False
                rule = Rule._qb_rule_for(company, partner, plant)
                if not rule or fl.week >= start + timedelta(weeks=rule.so_weeks or 0):
                    continue
                ref, why = self._qb_reference(rule, fl)
                key = (partner.id, plant, ref or why, fl.week, fl.product_id.id)
                group = groups.setdefault(key, {
                    'rule': rule, 'partner': partner, 'plant': plant, 'ref': ref, 'why': why,
                    'week': fl.week, 'product': fl.product_id, 'qty': 0.0, 'ship_to': fl.ship_to_id,
                    'release': fl.release_id, 'part': fl.customer_part, 'delivery': fl.delivery_date,
                    'dev_state': fl.dev_state})
                group['qty'] += fl.qty_venta
                group['release'] = group['release'] or fl.release_id
                group['part'] = group['part'] or fl.customer_part
        vals_list = []
        is_manager = self.env.user.has_group('sales_team.group_sale_manager')
        for group in groups.values():
            product, partner = group['product'], group['partner']
            existing = self._qb_existing(company, partner, group['ref'], group['week'], product) \
                if group['ref'] else self.env['sale.order.line']
            qty_existing = sum(existing.mapped('product_uom_qty'))
            delivered = sum(existing.mapped('qty_delivered'))
            target = round(group['qty'], 2)
            price, has_price = self._qb_price(partner, product, target)
            vals = {'proposal_id': self.id, 'rule_id': group['rule'].id, 'partner_id': partner.id,
                    'plant': group['plant'], 'ship_to_id': group['ship_to'].id or False,
                    'customer_po': group['ref'], 'week': group['week'], 'product_id': product.id,
                    'qty_forecast': target, 'qty_existing': qty_existing, 'qty_delivered': delivered,
                    'price_unit': price, 'release_id': group['release'].id or False,
                    'customer_part': group['part'] or False, 'delivery_date': group['delivery'],
                    'sale_line_ids': [(6, 0, existing.ids)]}
            reason = group['why']
            if not reason and group['dev_state'] in DEV_NO_SALE:
                reason = "Artículo en desarrollo o archivado: el pedido no se confirmaría"
            if not reason and not has_price and not existing:
                reason = "Sin precio en la lista del cliente"
            if reason:
                vals.update(action='no_entra', reason=reason, apply=False)
            elif not existing:
                vals.update(action='nuevo', apply=target > 0)
            elif abs(target - qty_existing) < 1e-6:
                vals.update(action='sin_cambio', apply=False)
            else:
                # 3.15.2 (opción B de Jose, 9-nov): un pedido confirmado se ajusta como lo hace Ventas a
                # mano (desbloquear, cambiar, volver a bloquear), solo el administrador de ventas. Los
                # aumentos vienen marcados y las bajas desmarcadas (puede quedar material sin pedido).
                confirmed = existing.filtered(lambda s: s.order_id.state == 'sale')
                up = target > qty_existing
                if confirmed and not is_manager:
                    vals.update(action='por_autorizar', apply=False, adjust_confirmed=True,
                                reason="Pedido confirmado: lo ajusta el administrador de ventas")
                elif confirmed:
                    vals.update(action='aumenta' if up else 'baja', apply=up, adjust_confirmed=True,
                                reason=False if up else "Baja en un pedido confirmado: márquela si procede")
                else:
                    vals.update(action='aumenta' if up else 'baja', apply=True)
            vals_list.append(vals)
        self.env['qb.order.proposal.line'].create(vals_list)
        return self._qb_reopen()

    def _qb_reopen(self):
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new', 'name': "Proponer pedidos"}

    # --- Aplicar ----------------------------------------------------------------
    def action_apply(self):
        """Crea o actualiza los pedidos de los renglones marcados (uno por OC y semana): una cotización
        se cambia directo; un pedido confirmado lo ajusta el administrador de ventas (se desbloquea,
        se cambia y se vuelve a bloquear, con nota en el chatter)."""
        self.ensure_one()
        lines = self.line_ids.filtered(lambda l: l.apply and l.action in ('nuevo', 'aumenta', 'baja'))
        if not lines:
            raise UserError("No hay renglones marcados para aplicar.")
        if lines.filtered('adjust_confirmed') and not self.env.user.has_group('sales_team.group_sale_manager'):
            raise UserError("Solo el administrador de ventas ajusta pedidos confirmados.")
        touched = self.env['sale.order']
        for line in lines:
            touched |= line._qb_apply()
        for order in touched:
            order.message_post(body=Markup("Actualizado desde el pronóstico por %s.") % self.env.user.name)
        return {'type': 'ir.actions.act_window', 'name': "Pedidos desde el pronóstico",
                'res_model': 'sale.order', 'view_mode': 'list,form', 'domain': [('id', 'in', touched.ids)]}

    def _qb_find_order(self, line):
        """Una cotización o pedido abierto del cliente con esa OC y semana (aunque no tenga el producto)."""
        start, end = self._qb_week_bounds(line.week)
        return self.env['sale.order'].search([
            ('company_id', '=', self.env.company.id),
            ('partner_id', 'child_of', line.partner_id.commercial_partner_id.id),
            ('state', 'in', ('draft', 'sent')), ('client_order_ref', '=', line.customer_po),
            ('commitment_date', '>=', start), ('commitment_date', '<', end)], limit=1)

    def _qb_order_vals(self, line):
        partner = line.partner_id.commercial_partner_id
        when = line.delivery_date or line.week
        vals = {'partner_id': partner.id, 'client_order_ref': line.customer_po,
                'commitment_date': datetime.combine(when, time(18, 0)),
                'origin': "Pronóstico %s" % (line.release_id.name or line.week.isoformat()),
                'qb_from_forecast': True, 'qb_forecast_week': line.week}
        if line.ship_to_id:
            vals['partner_shipping_id'] = line.ship_to_id.id
        if partner.property_product_pricelist:
            vals['pricelist_id'] = partner.property_product_pricelist.id
        return vals


class QbOrderProposalLine(models.TransientModel):
    _name = 'qb.order.proposal.line'
    _description = "Renglón de la propuesta de pedidos"
    _order = 'partner_id, week, customer_po, product_id'

    proposal_id = fields.Many2one('qb.order.proposal', required=True, ondelete='cascade')
    rule_id = fields.Many2one('qb.order.rule', string="Regla", readonly=True)
    po_mode = fields.Selection(related='rule_id.po_mode', string="Cómo se pide")
    partner_id = fields.Many2one('res.partner', string="Cliente", readonly=True)
    plant = fields.Char(string="Planta", readonly=True)
    ship_to_id = fields.Many2one('res.partner', string="Dirección de entrega", readonly=True)
    customer_po = fields.Char(string="OC / referencia", readonly=True)
    week = fields.Date(string="Semana", readonly=True)
    delivery_date = fields.Date(string="Entrega del cliente", readonly=True)
    product_id = fields.Many2one('product.product', string="Producto", readonly=True)
    customer_part = fields.Char(string="Parte del cliente", readonly=True)
    release_id = fields.Many2one('qb.release', string="Release", readonly=True)
    qty_forecast = fields.Float(string="Pronóstico", digits='Product Unit', readonly=True)
    qty_existing = fields.Float(string="Ya pedido", digits='Product Unit', readonly=True)
    qty_delivered = fields.Float(string="Entregado", digits='Product Unit', readonly=True)
    price_unit = fields.Float(string="Precio de lista", digits=(16, 4), readonly=True)
    action = fields.Selection(PROPOSAL_ACTIONS, string="Qué pasa", readonly=True)
    reason = fields.Char(string="Motivo", readonly=True)
    apply = fields.Boolean(string="Aplicar")
    adjust_confirmed = fields.Boolean(string="Ajusta un pedido confirmado", readonly=True,
                                      help="El pedido de esa OC y semana ya está confirmado: se desbloquea, se "
                                           "cambia y se vuelve a bloquear (solo el administrador de ventas).")
    sale_line_ids = fields.Many2many('sale.order.line', string="Líneas de pedido", readonly=True)

    def _qb_order_for(self, allow_confirmed=True):
        """El pedido de la OC y semana del renglón donde va lo nuevo: una cotización si hay; si no, el
        pedido confirmado (si el renglón ajusta confirmados); si no, una cotización nueva."""
        self.ensure_one()
        proposal = self.proposal_id
        orders = self.sale_line_ids.order_id
        order = orders.filtered(lambda o: o.state in ('draft', 'sent'))[:1] or proposal._qb_find_order(self)
        if not order and self.adjust_confirmed and allow_confirmed:
            order = orders.filtered(lambda o: o.state == 'sale')[:1]
        return order or self.env['sale.order'].create(proposal._qb_order_vals(self))

    @staticmethod
    def _qb_open_qty(sol):
        """Lo que aún se puede cambiar de una línea: si su entrega ya se surtió completa, nada."""
        return any(move.state not in ('done', 'cancel') for move in sol.move_ids) or sol.order_id.state != 'sale'

    def _qb_write_qty(self, sol, qty, note):
        """Cambia la cantidad de ``sol``; en un pedido confirmado y bloqueado, lo desbloquea, la cambia,
        lo vuelve a bloquear y deja nota (lo que Ventas hace a mano)."""
        order = sol.order_id
        was_locked = bool(order._fields.get('locked') and order.locked)
        before = sol.product_uom_qty
        if was_locked:
            order.action_unlock()
        sol.write({'product_uom_qty': qty})
        if was_locked:
            order.action_lock()
        if order.state == 'sale':
            order.message_post(body=Markup("Cantidad de %s: %s → %s (%s).") % (
                sol.product_id.display_name, '{:,.2f}'.format(before), '{:,.2f}'.format(qty), note))

    def _qb_apply(self):
        """Deja en los pedidos de la OC y semana la cantidad del pronóstico del producto, nunca menos de
        lo entregado. Devuelve los pedidos que tocó."""
        self.ensure_one()
        note = "release %s" % self.release_id.name if self.release_id else "pronóstico de la semana %s" % self.week
        target = max(self.qty_forecast, self.qty_delivered)
        sols = self.sale_line_ids.filtered(
            lambda s: s.order_id.state in ('draft', 'sent') or (self.adjust_confirmed and s.order_id.state == 'sale'))
        fixed = self.sale_line_ids - sols
        editable = sols.filtered(self._qb_open_qty)
        frozen = (sols - editable) | fixed
        need = target - sum(frozen.mapped('product_uom_qty'))
        touched = self.env['sale.order']
        if editable:
            first, rest = editable[0], editable[1:]
            qty = max(need - sum(rest.mapped('product_uom_qty')), first.qty_delivered, 0.0)
            if qty <= 0 and first.order_id.state in ('draft', 'sent'):
                touched |= first.order_id
                first.unlink()
            else:
                self._qb_write_qty(first, qty, note)
                touched |= first.order_id
            return touched
        if need > 0:
            # Sin línea que se pueda cambiar (nueva, o la entrega ya se surtió): va a la cotización de la
            # OC y semana, o al pedido confirmado si el renglón lo ajusta.
            # Si la entrega del confirmado ya se surtió completa, lo que se pide de más va a un pedido
            # nuevo (cotización), no al surtido.
            order = self._qb_order_for(allow_confirmed=not (sols - editable))
            was_locked = bool(order._fields.get('locked') and order.locked)
            if was_locked:
                order.action_unlock()
            self.env['sale.order.line'].create({
                'order_id': order.id, 'product_id': self.product_id.id, 'product_uom_qty': need,
                'qb_forecast_week': self.week, 'qb_release_id': self.release_id.id or False,
                'qb_customer_part': self.customer_part or False})
            if was_locked:
                order.action_lock()
            touched |= order
        return touched
