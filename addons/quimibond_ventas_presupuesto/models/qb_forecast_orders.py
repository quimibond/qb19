# -*- coding: utf-8 -*-
"""Pedido y pronóstico ligados (19.0.3.29.0, Jose 19-nov-2026).

«Quisiera poder generar el pedido desde ahí […] y si hago un cambio de fecha que me mueva la captura semanal
a la fecha que moví el pedido […] que no haya doble captura en pedido y en el forecast».

- Cada renglón de pedido puede quedar **ligado** a su semana del pronóstico (``qb_forecast_line_id``).
- **Crear pedido** (Pronóstico vigente, lista o ficha): una cotización por cliente, dirección, OC y semana con
  lo que falta por pedir de cada semana, la OC de Condiciones de entrega / regla / release y la fecha en el día
  de recolección. Se abre la cotización nativa para revisarla y confirmarla.
- Si cambia la fecha de entrega del pedido, lo ligado se mueve a la semana nueva del pronóstico; si cambia la
  cantidad, cambia la semana. Todo queda en la bitácora del pronóstico.
- Un pedido capturado a mano se liga al confirmarse a la semana del cliente con ese artículo (la suya o la más
  cercana con pronóstico sin pedir, hasta ``MOVE_WEEKS``) y la mueve a la semana del pedido.
- «Proponer pedidos» también deja la liga; sus cambios no se devuelven al pronóstico (ya salen de él).
"""
from collections import OrderedDict
from datetime import datetime, time, timedelta

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from .qb_forecast import _qb_fmt, monday_of

# Semanas alrededor de la del pedido en que se busca el pronóstico al confirmar un pedido capturado a mano.
MOVE_WEEKS = 4
# Contexto: el cambio viene del pronóstico (Proponer pedidos, Crear pedido): no se devuelve al pronóstico.
NO_SYNC = 'qb_no_forecast_sync'


def _order_week(order):
    """Lunes de la semana de la fecha de entrega del pedido, en la hora local del usuario."""
    when = order.commitment_date
    if not when:
        return False
    return monday_of(fields.Datetime.context_timestamp(order, when).date())


class QbForecastLineOrders(models.Model):
    _inherit = 'qb.forecast.line'

    sale_line_ids = fields.One2many('sale.order.line', 'qb_forecast_line_id', string="Pedidos ligados",
                                    readonly=True)
    qty_ligada = fields.Float(
        string="Pedido ligado", digits='Product Unit', compute='_compute_qty_ligada',
        help="Lo pedido en los renglones de pedido ligados a esta semana (cotizaciones y pedidos, sin los "
             "cancelados), en la unidad de venta.")
    qty_por_pedir = fields.Float(string="Por pedir", digits='Product Unit', compute='_compute_qty_ligada',
                                 help="Cantidad de la semana − pedido ligado, no menos de 0.")
    order_count = fields.Integer(string="Pedidos", compute='_compute_qty_ligada')

    @api.depends('sale_line_ids.product_uom_qty', 'sale_line_ids.state', 'qty_venta')
    def _compute_qty_ligada(self):
        for line in self:
            sols = line._qb_live_sale_lines()
            line.qty_ligada = sum(line._qb_sol_qty(sol) for sol in sols)
            line.qty_por_pedir = max(line.qty_venta - line.qty_ligada, 0.0)
            line.order_count = len(sols.order_id)

    def _qb_live_sale_lines(self):
        return self.sale_line_ids.filtered(lambda s: s.order_id.state != 'cancel' and not s.display_type)

    def _qb_sol_qty(self, sol):
        """Cantidad del renglón de pedido en la unidad de venta del producto de esta semana."""
        self.ensure_one()
        qty = sol.product_uom_qty
        if sol.product_uom_id and sol.product_uom_id._has_common_reference(sol.product_id.uom_id):
            qty = sol.product_uom_id._compute_quantity(qty, sol.product_id.uom_id, round=False)
        if sol.product_id != self.product_id:
            qty = sol.product_id.qb_convertir(qty, 'venta', self.product_id._qb_sale_unit()) or 0.0
        return qty

    def _qb_add_venta(self, delta, note):
        """Suma ``delta`` (unidad de venta) a la semana, en la unidad en que se capturó; nunca abajo de 0."""
        self.ensure_one()
        if abs(delta) <= 1e-9:
            return
        unit = self.uom_origen or 'venta'
        delta_origen = delta if unit == 'venta' else self.product_id.qb_convertir(delta, 'venta', unit)
        if delta_origen is None:
            raise UserError("No se puede pasar %s de %s a la unidad del pronóstico (%s): falta el rendimiento "
                            "o el ancho en la ficha." % (_qb_fmt(delta), self.product_id.display_name, unit))
        last = self.env['qb.forecast.change'].sudo().search([('line_id', '=', self.id)], limit=1,
                                                            order='id desc').id or 0
        self.write({'qty_origen': max(self.qty_origen + delta_origen, 0.0)})
        self.env['qb.forecast.change'].sudo().search(
            [('line_id', '=', self.id), ('id', '>', last)]).write({'note': note})

    def _qb_week_line(self, week):
        """La semana ``week`` del mismo cliente, planta y producto; se crea (en cero) si no existe."""
        self.ensure_one()
        week = monday_of(week)
        line = self.search([
            ('company_id', '=', self.company_id.id), ('partner_id', '=', self.partner_id.id),
            ('ship_to_id', '=', self.ship_to_id.id or False), ('plant', '=', self.plant or False),
            ('product_id', '=', self.product_id.id), ('week', '=', week)], limit=1)
        if line:
            return line
        return self.create({
            'company_id': self.company_id.id, 'partner_id': self.partner_id.id,
            'ship_to_id': self.ship_to_id.id or False, 'plant': self.plant or False,
            'product_id': self.product_id.id, 'week': week, 'qty_origen': 0.0,
            'uom_origen': self.uom_origen or False, 'origen': 'captura',
            'customer_part': self.customer_part or False, 'customer_part_id': self.customer_part_id.id,
            'customer_po': self.customer_po or False})

    def _qb_move_sale_lines(self, sols, week, why):
        """Mueve lo pedido en ``sols`` (ligados a esta semana) a la semana ``week`` del pronóstico y los
        liga a ella. La semana que se queda sin cantidad ni pedidos se quita, salvo que venga de un release
        (queda en cero y ajustada, para que el siguiente release no la vuelva a llenar)."""
        self.ensure_one()
        target = self._qb_week_line(week)
        if target == self:
            return target
        qty = sum(self._qb_sol_qty(sol) for sol in sols)
        note = "%s: %s se mueven de la semana del %s a la del %s" % (
            why, _qb_fmt(qty), self.week.strftime('%d-%m'), target.week.strftime('%d-%m'))
        take = min(qty, self.qty_venta)
        self._qb_add_venta(-take, note)
        target._qb_add_venta(qty, note)
        sols.with_context(**{NO_SYNC: True}).write({'qb_forecast_line_id': target.id})
        if self.qty_venta <= 1e-9 and not self._qb_live_sale_lines() and not self.release_id:
            self.unlink()
        return target

    # --- Crear pedido ------------------------------------------------------------
    def action_qb_create_order(self):
        """Crea una cotización por cliente, dirección de entrega, OC y semana con lo que falta por pedir de
        cada semana seleccionada y la abre para revisarla y confirmarla."""
        lines = self.filtered(lambda l: l.partner_id and l.qty_por_pedir > 1e-6)
        if not lines:
            raise UserError("Las semanas seleccionadas ya tienen pedido por toda su cantidad (o no tienen "
                            "cliente).")
        Proposal = self.env['qb.order.proposal']
        Rule = self.env['qb.order.rule'].sudo()
        Terms = self.env['qb.delivery.terms'].sudo()
        groups = OrderedDict()
        for fl in lines.sorted(lambda l: (l.partner_id.id, l.week, l.id)):
            partner = fl.partner_id.commercial_partner_id
            plant = fl.plant or fl.ship_to_id.name or False
            rule = Rule._qb_rule_for(fl.company_id, partner, plant)
            terms = Terms._qb_for(partner, fl.ship_to_id, plant, fl.company_id)
            ref = ''
            if rule:
                ref, _why = Proposal._qb_reference(rule, fl, terms)
            if not ref:
                ref, _expired = terms._qb_open_po_for(fl.product_id, fl.week)
            ref = ref or fl.customer_po or ''
            key = (fl.company_id.id, partner.id, fl.ship_to_id.id, ref, fl.week)
            groups.setdefault(key, {'lines': self.browse(), 'terms': terms, 'ref': ref, 'partner': partner})
            groups[key]['lines'] |= fl
        orders = self.env['sale.order']
        for (_company, _partner, _ship, ref, week), group in groups.items():
            first = group['lines'][0]
            partner = group['partner']
            delivery = group['terms']._qb_delivery_date(first.delivery_date, week)
            delivery = max(delivery or week, fields.Date.context_today(self))
            vals = {'partner_id': partner.id, 'client_order_ref': ref or False,
                    'company_id': first.company_id.id,
                    'commitment_date': datetime.combine(delivery, time(18, 0)),
                    'origin': "Pronóstico %s" % (first.release_id.name or week.isoformat()),
                    'qb_from_forecast': True, 'qb_forecast_week': week,
                    'order_line': [(0, 0, {
                        'product_id': fl.product_id.id, 'product_uom_qty': round(fl.qty_por_pedir, 4),
                        'qb_forecast_week': week, 'qb_forecast_line_id': fl.id,
                        'qb_release_id': fl.release_id.id or False,
                        'qb_customer_part': fl.customer_part or False}) for fl in group['lines']]}
            if first.ship_to_id:
                vals['partner_shipping_id'] = first.ship_to_id.id
            if partner.property_product_pricelist:
                vals['pricelist_id'] = partner.property_product_pricelist.id
            orders |= self.env['sale.order'].with_context(**{NO_SYNC: True}).create(vals)
        for order in orders:
            order.message_post(body=Markup("Creado desde el pronóstico por %s.") % self.env.user.name)
        lines.sudo().action_qb_refresh()
        action = {'type': 'ir.actions.act_window', 'name': "Pedidos desde el pronóstico",
                  'res_model': 'sale.order'}
        if len(orders) == 1:
            return dict(action, view_mode='form', res_id=orders.id)
        return dict(action, view_mode='list,form', domain=[('id', 'in', orders.ids)])

    def action_qb_view_orders(self):
        orders = self._qb_live_sale_lines().order_id
        action = {'type': 'ir.actions.act_window', 'name': "Pedidos de la semana", 'res_model': 'sale.order'}
        if len(orders) == 1:
            return dict(action, view_mode='form', res_id=orders.id)
        return dict(action, view_mode='list,form', domain=[('id', 'in', orders.ids)])

    # --- Pedido capturado a mano ----------------------------------------------------------
    @api.model
    def _qb_link_manual(self, sol):
        """Liga un renglón de pedido sin liga a la semana del pronóstico del cliente y artículo: la de la
        semana del pedido o, si no hay, la más cercana (hasta ``MOVE_WEEKS``) con cantidad sin pedir, que se
        mueve a la semana del pedido. Sin pronóstico cerca no se liga (entra como «Pedido del cliente»)."""
        order = sol.order_id
        week = order.qb_forecast_week or _order_week(order)
        if not week or not sol.product_id:
            return self.browse()
        partner = order.partner_id.commercial_partner_id
        articulo = sol.product_id.qb_articulo_id or sol.product_id
        domain = [('company_id', '=', order.company_id.id), ('partner_id', '=', partner.id),
                  ('origen', '!=', 'pedido'),
                  '|', ('product_id', '=', articulo.id), ('articulo_id', '=', articulo.id),
                  ('week', '>=', week - timedelta(weeks=MOVE_WEEKS)),
                  ('week', '<=', week + timedelta(weeks=MOVE_WEEKS))]
        shipping = order.partner_shipping_id
        if shipping and shipping != partner:
            domain += ['|', ('ship_to_id', '=', shipping.id), ('ship_to_id', '=', False)]
        candidates = self.sudo().search(domain)
        if not candidates:
            return self.browse()
        same = candidates.filtered(lambda l: l.week == week)
        open_ = (candidates - same).filtered(lambda l: l.qty_por_pedir > 1e-6)
        if same:
            line = same.sorted(lambda l: (l.product_id != sol.product_id, -l.qty_por_pedir))[0]
            sol.with_context(**{NO_SYNC: True}).write({'qb_forecast_line_id': line.id})
            return line
        if not open_:
            return self.browse()
        line = min(open_, key=lambda l: (abs((l.week - week).days), l.week))
        sol.with_context(**{NO_SYNC: True}).write({'qb_forecast_line_id': line.id})
        return line._qb_move_sale_lines(sol, week, "Pedido %s" % order.name)


class SaleOrderForecastLink(models.Model):
    _inherit = 'sale.order'

    def action_confirm(self):
        res = super().action_confirm()
        if not self.env.context.get(NO_SYNC):
            Forecast = self.env['qb.forecast.line'].sudo()
            for sol in self.order_line.filtered(
                    lambda s: s.product_id and not s.display_type and not s.qb_forecast_line_id):
                Forecast._qb_link_manual(sol)
        return res

    def write(self, vals):
        moved = {}
        if 'commitment_date' in vals and not self.env.context.get(NO_SYNC):
            moved = {order.id: _order_week(order) for order in self}
        res = super().write(vals)
        for order in self.filtered(lambda o: o.id in moved and o.state != 'cancel'):
            week = _order_week(order)
            if not week or week == moved[order.id]:
                continue
            if order.qb_forecast_week and order.qb_forecast_week != week:
                super(SaleOrderForecastLink, order).write({'qb_forecast_week': week})
            linked = order.order_line.filtered('qb_forecast_line_id')
            for fl in linked.qb_forecast_line_id:
                if fl.week != week:
                    fl.sudo()._qb_move_sale_lines(
                        linked.filtered(lambda s: s.qb_forecast_line_id == fl), week,
                        "Pedido %s cambió de fecha" % order.name)
        return res


class SaleOrderLineForecastLink(models.Model):
    _inherit = 'sale.order.line'

    qb_forecast_line_id = fields.Many2one(
        'qb.forecast.line', string="Semana del pronóstico (liga)", index=True, copy=False, readonly=True,
        ondelete='set null',
        help="La semana del pronóstico que cubre este renglón. Si cambia la fecha de entrega del pedido, lo "
             "pedido se mueve a la semana nueva; si cambia la cantidad, cambia la semana.")

    def write(self, vals):
        before = {}
        if 'product_uom_qty' in vals and not self.env.context.get(NO_SYNC):
            before = {sol.id: sol.product_uom_qty for sol in self if sol.qb_forecast_line_id}
        res = super().write(vals)
        for sol in self.filtered(lambda s: s.id in before and s.order_id.state != 'cancel'):
            fl = sol.qb_forecast_line_id.sudo()
            delta_sol = sol.product_uom_qty - before[sol.id]
            if abs(delta_sol) <= 1e-9:
                continue
            probe = sol.product_uom_id._compute_quantity(delta_sol, sol.product_id.uom_id, round=False) \
                if sol.product_uom_id and sol.product_uom_id._has_common_reference(sol.product_id.uom_id) \
                else delta_sol
            if sol.product_id != fl.product_id:
                probe = sol.product_id.qb_convertir(probe, 'venta', fl.product_id._qb_sale_unit()) or 0.0
            # Si el pedido cubría toda la semana, la semana sigue al pedido; si la cubría en parte, solo
            # cambia cuando el pedido se pasa de la semana (no hay doble captura en ningún caso).
            after = fl.qty_ligada
            if after - probe >= fl.qty_venta - 1e-6:
                delta = after - fl.qty_venta
            else:
                delta = max(after - fl.qty_venta, 0.0)
            if abs(delta) > 1e-9:
                fl._qb_add_venta(delta, "Pedido %s cambió la cantidad" % sol.order_id.name)
        return res

    def unlink(self):
        lines = self.qb_forecast_line_id
        res = super().unlink()
        if lines:
            lines.exists().sudo().action_qb_refresh()
        return res
