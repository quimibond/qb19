# -*- coding: utf-8 -*-
"""Presupuesto del mes (19.0.3.14.0, diseño v3 2.5; formato F-P-A28-14).

Un documento por compañía y mes, que sustituye el F-P-A28-14 de Excel:

- **Armar presupuesto de <mes>:** las líneas son una **foto del pronóstico vigente** del mes
  (cliente comercial, planta, producto y mercado; cantidad en la unidad de venta, metros y
  kilos), con el **precio presupuestal**: el del renglón del presupuesto anual aprobado del mismo
  cliente, producto y mes; si no hay, el de la lista del cliente (la misma regla del presupuesto
  anual); si tampoco, queda sin precio y cuenta 0. **Tipo de cambio del mes:** el del
  presupuesto anual aprobado (o el presupuestal de Ajustes), editable.
- El mercado es el del pronóstico; si el cliente y el artículo tienen un proyecto comercial
  vigente, «Proyectos» de ese mercado.
- Jessica ajusta cantidades y precios **antes de enviar**; no tocan el pronóstico vivo. La
  cantidad del pronóstico al armar queda en «Pronóstico al armar».
- **Confección:** los vendedores capturan su parte (renglones sin cliente) en el mismo
  documento; una regla de registro solo les deja crear, cambiar o borrar renglones de
  Confección y Proyectos confección.
- Totales por mercado y por **línea / proyectos** (los dos números del correo a Dirección).
- **Enviar a Dirección:** PDF F-P-A28-14 y redactor de correo con el PDF y los totales; queda
  **enviado** y congelado (solo el administrador del SGI lo regresa a borrador).
"""
import base64
from collections import OrderedDict, defaultdict

from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

from .qb_forecast import MARKETS
from .qb_sales_close import MONTH_NAMES
from .sgi_sales_budget_line import PRICE_BASIS, PRICE_PROBLEMS

PLAN_STATES = [('borrador', "Borrador"), ('enviado', "Enviado a Dirección")]
PLAN_PRICE_ORIGINS = [('presupuesto', "Presupuesto anual"), ('lista', "Lista del cliente"),
                      ('captura', "Captura"), ('factura', "Última factura")]
PROYECTOS = ('proyectos_industrial', 'proyectos_confeccion')
PLAN_REPORT = 'quimibond_ventas_presupuesto.action_report_sales_month_plan'


class QbSalesMonthPlan(models.Model):
    _name = 'qb.sales.month.plan'
    _description = "Presupuesto de ventas del mes (F-P-A28-14)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'month desc, id desc'

    name = fields.Char(string="Presupuesto del mes", compute='_compute_name', store=True)
    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True,
                                 default=lambda self: self.env.company)
    currency_id = fields.Many2one(related='company_id.currency_id', string="Moneda")
    month = fields.Date(string="Mes", required=True, index=True, tracking=True,
                        help="Primer día del mes presupuestado.")
    rate = fields.Float(string="Tipo de cambio del mes", digits=(12, 4), tracking=True,
                        help="Pesos por dólar con que se valúan los renglones con precio en dólares.")
    state = fields.Selection(PLAN_STATES, string="Estado", required=True, default='borrador',
                             index=True, tracking=True)
    sent_date = fields.Datetime(string="Enviado", readonly=True)
    sent_by_id = fields.Many2one('res.users', string="Envió", readonly=True)
    budget_id = fields.Many2one('sgi.sales.budget', string="Presupuesto anual", readonly=True,
                                help="El aprobado del año con que se tomaron los precios.")
    line_ids = fields.One2many('qb.sales.month.plan.line', 'plan_id', string="Renglones")
    amount_total = fields.Monetary(string="Total", compute='_compute_totals', store=True)
    amount_linea = fields.Monetary(string="Productos de línea", compute='_compute_totals', store=True)
    amount_proyectos = fields.Monetary(string="Proyectos", compute='_compute_totals', store=True)
    amount_by_mercado_text = fields.Char(string="Por mercado", compute='_compute_totals', store=True)
    no_price_count = fields.Integer(string="Renglones que cuentan 0", compute='_compute_totals', store=True)

    _company_month_uniq = models.Constraint(
        'unique (company_id, month)', "Ya hay un presupuesto de ese mes en la compañía.")

    @api.depends('month')
    def _compute_name(self):
        for plan in self:
            plan.name = ("Presupuesto de %s %s" % (MONTH_NAMES[plan.month.month - 1], plan.month.year)
                         if plan.month else "Presupuesto del mes")

    @api.depends('line_ids.amount', 'line_ids.mercado', 'line_ids.price_problem')
    def _compute_totals(self):
        labels = dict(MARKETS)
        for plan in self:
            by_mercado = defaultdict(float)
            for line in plan.line_ids:
                by_mercado[line.mercado or 'otro'] += line.amount
            plan.amount_total = sum(by_mercado.values())
            plan.amount_proyectos = sum(v for k, v in by_mercado.items() if k in PROYECTOS)
            plan.amount_linea = plan.amount_total - plan.amount_proyectos
            plan.amount_by_mercado_text = " · ".join(
                "%s %s" % (labels.get(k, k), '{:,.0f}'.format(v))
                for k, v in sorted(by_mercado.items(), key=lambda kv: [m for m, _l in MARKETS].index(kv[0])
                                   if kv[0] in labels else 99) if v)
            plan.no_price_count = len(plan.line_ids.filtered('price_problem'))

    @api.constrains('month')
    def _check_month(self):
        for plan in self:
            if plan.month and plan.month.day != 1:
                raise ValidationError("El mes del presupuesto va en su primer día.")

    def write(self, vals):
        # Los vendedores guardan sus renglones de Confección desde la ficha (escriben line_ids);
        # la cabecera (mes, tipo de cambio, estado) es del administrador de ventas o del SGI.
        managers = self.env.user.has_group('sales_team.group_sale_manager') or \
            self.env.user.has_group('quimibond_sgi.group_sgi_manager')
        if not self.env.su and not managers and set(vals) - {'line_ids', 'message_main_attachment_id'}:
            raise UserError("Solo el administrador de ventas cambia la cabecera del presupuesto del mes.")
        if (self.filtered(lambda p: p.state == 'enviado') and set(vals) - {'state', 'message_main_attachment_id'}
                and not self.env.su and not self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise UserError("El presupuesto del mes ya se envió a Dirección: está congelado.")
        return super().write(vals)

    # --- Armado ---------------------------------------------------------------
    @api.model
    def _qb_annual_budget(self, company, month):
        return self.env['sgi.sales.budget'].search([
            ('kind', '=', 'presupuesto'), ('year', '=', month.year), ('state', '=', 'aprobado'),
            ('company_id', '=', company.id)], limit=1)

    @api.model
    def _qb_default_rate(self, company, month):
        budget = self._qb_annual_budget(company, month)
        if budget:
            return budget._qb_month_rate(month.month)
        return float(self.env['ir.config_parameter'].sudo().get_param('quimibond_sgi.budget_planning_rate', 0) or 0)

    def _qb_projects(self):
        """{(cliente, artículo): mercado} de los proyectos comerciales vigentes en el mes."""
        self.ensure_one()
        month_end = self.month + relativedelta(months=1, days=-1)
        out = {}
        for project in self.env['qb.sales.project'].sudo().search([
                ('company_id', '=', self.company_id.id), ('state', '=', 'vigente'),
                ('product_id', '!=', False), ('date_from', '<=', month_end)]):
            articulo = project.product_id.qb_articulo_id or project.product_id
            out[(project.partner_id.commercial_partner_id.id or False, articulo.id)] = project.mercado
        return out

    def _qb_budget_prices(self):
        """{(cliente, producto): (precio, moneda, base)} de los renglones del presupuesto anual
        aprobado: el del mes; si el renglón no está ese mes (3.16.2), el del mes más cercano. La
        REV2 de 2026 casi no trae noviembre y diciembre, y el precio de un cliente y producto es el
        mismo todo el año."""
        self.ensure_one()
        out, distance = {}, {}
        if not self.budget_id:
            return out
        for line in self.budget_id.line_ids.filtered(lambda l: l.date and l.product_id and l.price_input > 0):
            key = (line.partner_id.commercial_partner_id.id or False, line.product_id.id)
            months = abs((line.date.year - self.month.year) * 12 + line.date.month - self.month.month)
            if key not in distance or months < distance[key]:
                distance[key] = months
                out[key] = (line.price_input, line.price_currency_id or self.currency_id, line.price_basis)
        return out

    def _qb_last_invoice_price(self, partner, product):
        """(precio, moneda, base) de la última factura a ese cliente en los 12 meses antes del mes:
        del mismo producto (base «venta», en su unidad); si no hay, de la misma tela en otro código
        (3.16.4: FXI, Lear y Shawmut se facturan en kilos, IWD038…, y el pronóstico viene en metros,
        WD038…), con la base de la unidad en que se facturó («kg» o «m»). None si no hay."""
        self.ensure_one()
        if not partner or not product:
            return None
        AML = self.env['account.move.line'].sudo()
        base_domain = [
            ('company_id', '=', self.company_id.id), ('parent_state', '=', 'posted'),
            ('move_id.move_type', '=', 'out_invoice'), ('display_type', '=', 'product'),
            ('partner_id', 'child_of', partner.commercial_partner_id.id),
            ('move_id.invoice_date', '>=', self.month - relativedelta(months=12)), ('price_unit', '>', 0)]
        order = 'invoice_date desc, id desc'
        aml = AML.search(base_domain + [('product_id', '=', product.id)], order=order, limit=1)
        if aml:
            price = aml.price_unit
            if aml.product_uom_id and aml.product_uom_id != product.uom_id \
                    and aml.product_uom_id._has_common_reference(product.uom_id):
                price = aml.product_uom_id._compute_price(price, product.uom_id)
            return price, aml.currency_id or self.currency_id, 'venta', aml
        articulo = product.qb_articulo_id or product
        units = {self.env.ref('uom.product_uom_kgm'): 'kg', self.env.ref('uom.product_uom_meter'): 'm'}
        for aml in AML.search(base_domain + [
                ('product_id', '!=', product.id),
                '|', ('product_id.qb_articulo_id', '=', articulo.id), ('product_id', '=', articulo.id)],
                order=order, limit=20):
            basis = units.get(aml.product_uom_id)
            if basis:
                return aml.price_unit, aml.currency_id or self.currency_id, basis, aml
        return None

    def _qb_list_price(self, partner, product, qty):
        """(precio, moneda) de la regla de la lista del cliente para el producto; None si la lista
        no tiene regla (o el cliente no tiene lista)."""
        probe = self.env['sgi.sales.budget.line'].new({
            'partner_id': partner.id or False, 'product_id': product.id, 'uom_id': product.uom_id.id,
            'qty_budget': qty or 1.0, 'date': self.month})
        _company_price, _src, list_ccy, raw, has_price = probe._sgi_pricelist_price()
        return (raw, list_ccy) if has_price and raw > 0 else None

    def _qb_price_vals(self, partner, product, qty, prices):
        """Precio de un renglón: el del presupuesto anual aprobado (cualquier mes) o el de la lista
        del cliente. 3.16.5 (Jose, 10-nov): **la lista del cliente es la fuente**; lo que no está ahí
        no se toma de facturas: se le pide a Ventas que lo dé de alta (``_qb_request_list_prices``).
        {} si no hay."""
        price = prices.get((partner.id or False, product.id)) or prices.get((False, product.id))
        if price:
            return {'price_input': price[0], 'price_currency_id': price[1].id, 'price_basis': price[2],
                    'price_origen': 'presupuesto'}
        listed = self._qb_list_price(partner, product, qty)
        if listed:
            return {'price_input': listed[0], 'price_currency_id': listed[1].id, 'price_basis': 'venta',
                    'price_origen': 'lista'}
        return {}

    def _qb_request_list_prices(self):
        """Una actividad por cliente para su vendedor, en la lista de precios del cliente, con los
        productos del mes que la lista no tiene y, como referencia, el último precio facturado (del
        mismo código o de la misma tela en otro código). Si ya hay una abierta de este presupuesto
        del mes, se actualiza. Devuelve cuántos productos se pidieron."""
        self.ensure_one()
        todo = self.env.ref('mail.mail_activity_data_todo', raise_if_not_found=False)
        missing = {}
        for line in self.line_ids.filtered(lambda l: l.product_id and l.partner_id):
            partner = line.partner_id.commercial_partner_id
            if self._qb_list_price(partner, line.product_id, line.qty):
                continue
            missing.setdefault(partner, {}).setdefault(line.product_id, 0.0)
            missing[partner][line.product_id] += line.qty
        summary = "Precios que faltan en la lista (%s)" % self.name
        fmt = '{:,.2f}'.format
        count = 0
        for partner, products in missing.items():
            rows = []
            for product, qty in products.items():
                last = self._qb_last_invoice_price(partner, product)
                ref = "sin factura en 12 meses"
                if last:
                    aml = last[3]
                    ref = "%s %s por %s (%s, %s)" % (
                        fmt(last[0]), last[1].name, {'venta': product.uom_id.name, 'kg': 'kg', 'm': 'm'}[last[2]],
                        aml.product_id.default_code or aml.product_id.name, aml.move_id.invoice_date)
                rows.append(Markup("<li>%s — %s %s en el mes · último facturado: %s</li>") % (
                    product.display_name, fmt(qty), product.uom_id.name, ref))
                count += 1
            pricelist = partner.property_product_pricelist
            record = pricelist if pricelist and 'activity_ids' in pricelist._fields else partner
            note = Markup("<p>Dé de alta en la lista de precios de %s el precio de:</p><ul>%s</ul>"
                          "<p>Después, en %s, pulse «Completar precios».</p>") % (
                partner.name, Markup('').join(rows), self.name)
            user = partner.user_id or self.env.user
            existing = self.env['mail.activity'].sudo().search([
                ('res_model', '=', record._name), ('res_id', '=', record.id), ('summary', '=', summary)], limit=1)
            if existing:
                existing.write({'note': note, 'user_id': user.id})
            else:
                record.sudo().activity_schedule(
                    activity_type_id=todo.id if todo else False, summary=summary, note=note,
                    user_id=user.id, date_deadline=fields.Date.context_today(self) + relativedelta(days=3))
        if missing:
            self.message_post(body="Se pidió a Ventas dar de alta %d precio(s) en la lista de %s." % (
                count, ", ".join(p.name for p in missing)))
        return count

    def action_qb_fill_prices(self):
        """Pone precio a los renglones que no tienen (sin tocar cantidades ni los que ya tienen) y pide
        a Ventas lo que falta en la lista de cada cliente. Un precio que salió de una factura (3.16.2 a
        3.16.4) se vuelve a evaluar: si la lista ya lo tiene se toma; si no, queda sin precio."""
        for plan in self:
            if plan.state != 'borrador':
                raise UserError("Los precios se completan en un presupuesto del mes en borrador.")
            prices = plan._qb_budget_prices()
            done = 0
            for line in plan.line_ids.filtered(
                    lambda l: l.product_id and (l.price_input <= 0 or l.price_origen == 'factura')):
                vals = plan._qb_price_vals(line.partner_id, line.product_id, line.qty, prices)
                if not vals and line.price_origen == 'factura':
                    vals = {'price_input': 0.0, 'price_origen': False}
                if vals:
                    line.write(vals)
                    done += bool(vals.get('price_input'))
            plan.message_post(body="Precios completados: %d renglón(es); %d siguen sin precio." % (
                done, len(plan.line_ids.filtered(lambda l: l.product_id and l.price_input <= 0))))
            plan._qb_request_list_prices()
        return True

    def action_qb_build(self):
        """Arma (o vuelve a armar, en borrador) los renglones con la foto del pronóstico del mes.
        Los renglones capturados a mano (sin pronóstico, como Confección) se conservan."""
        for plan in self:
            if plan.state != 'borrador':
                raise UserError("Solo se arma un presupuesto del mes en borrador.")
            plan.budget_id = plan._qb_annual_budget(plan.company_id, plan.month)
            if not plan.rate:
                plan.rate = plan._qb_default_rate(plan.company_id, plan.month)
            plan.line_ids.filtered('from_forecast').unlink()
            forecast = self.env['qb.forecast.line'].sudo().search([
                ('company_id', '=', plan.company_id.id), ('month', '=', plan.month)])
            groups = OrderedDict()
            for fl in forecast.sorted(lambda l: (l.partner_id.name or '', l.product_id.default_code or '')):
                partner = fl.partner_id.commercial_partner_id
                plant = fl.plant or fl.ship_to_id.name or False
                key = (partner.id or False, plant, fl.product_id.id)
                group = groups.setdefault(key, {'partner': partner, 'plant': plant, 'product': fl.product_id,
                                                'mercado': fl.mercado, 'qty': 0.0})
                group['qty'] += fl.qty_venta
            projects = plan._qb_projects()
            prices = plan._qb_budget_prices()
            Line = self.env['qb.sales.month.plan.line']
            vals_list = []
            for group in groups.values():
                if group['qty'] <= 0:
                    continue
                product, partner = group['product'], group['partner']
                articulo = product.qb_articulo_id or product
                mercado = projects.get((partner.id or False, articulo.id)) or group['mercado'] or 'otro'
                vals = {'plan_id': plan.id, 'partner_id': partner.id or False, 'plant': group['plant'],
                        'product_id': product.id, 'mercado': mercado, 'from_forecast': True,
                        'qty_forecast': group['qty'], 'qty': round(group['qty'], 2)}
                vals.update(plan._qb_price_vals(partner, product, group['qty'], prices))
                vals_list.append(vals)
            Line.create(vals_list)
            plan.message_post(body="Armado con %d renglón(es) del pronóstico vigente; precios del %s."
                              % (len(vals_list), plan.budget_id.name or "presupuesto: sin aprobado del año, "
                                                                        "solo de la lista"))
            plan._qb_request_list_prices()
        return True

    # --- Envío ------------------------------------------------------------------
    def _qb_pdf_attachment(self):
        self.ensure_one()
        pdf, _ext = self.env['ir.actions.report']._render_qweb_pdf(PLAN_REPORT, self.ids)
        return self.env['ir.attachment'].create({
            'name': "%s.pdf" % self.name, 'datas': base64.b64encode(pdf), 'res_model': self._name,
            'res_id': self.id, 'mimetype': 'application/pdf'})

    def action_qb_send(self):
        """Congela el documento y abre el correo a Dirección con el PDF y los totales."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("El presupuesto del mes ya se envió.")
        if not self.line_ids:
            raise UserError("El presupuesto del mes no tiene renglones.")
        attachment = self._qb_pdf_attachment()
        self.write({'state': 'enviado', 'sent_date': fields.Datetime.now(), 'sent_by_id': self.env.uid})
        fmt = '{:,.0f}'.format
        body = Markup("<p>%s: total %s %s (productos de línea %s; proyectos %s).</p><p>%s</p>"
                      "<p>Detalle en el PDF adjunto (F-P-A28-14).</p>") % (
            self.name, fmt(self.amount_total), self.currency_id.name or '', fmt(self.amount_linea),
            fmt(self.amount_proyectos), self.amount_by_mercado_text or '')
        return {
            'type': 'ir.actions.act_window', 'res_model': 'mail.compose.message', 'view_mode': 'form',
            'target': 'new',
            'context': {'default_model': self._name, 'default_res_ids': self.ids,
                        'default_composition_mode': 'comment', 'default_subject': self.name,
                        'default_body': body, 'default_attachment_ids': [attachment.id]},
        }

    def action_qb_set_draft(self):
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el administrador del SGI regresa a borrador un presupuesto enviado.")
        self.write({'state': 'borrador', 'sent_date': False, 'sent_by_id': False})
        self.message_post(body="Regresado a borrador por %s." % self.env.user.name)
        return True

    def _qb_report_sections(self):
        """[(etiqueta del mercado, renglones, total)] en el orden de los mercados."""
        self.ensure_one()
        out = []
        for key, label in MARKETS:
            lines = self.line_ids.filtered(lambda l, k=key: (l.mercado or 'otro') == k)
            if lines:
                out.append((label, lines.sorted(lambda l: (l.partner_id.name or '', l.product_id.default_code or '')),
                            sum(lines.mapped('amount'))))
        return out


class QbSalesMonthPlanLine(models.Model):
    _name = 'qb.sales.month.plan.line'
    _description = "Renglón del presupuesto del mes"
    _order = 'plan_id, mercado, partner_id, product_id'

    plan_id = fields.Many2one('qb.sales.month.plan', string="Presupuesto del mes", required=True,
                              ondelete='cascade', index=True)
    company_id = fields.Many2one(related='plan_id.company_id', store=True)
    currency_id = fields.Many2one(related='plan_id.currency_id', string="Moneda")
    month = fields.Date(related='plan_id.month', store=True)
    state = fields.Selection(related='plan_id.state', string="Estado")
    partner_id = fields.Many2one('res.partner', string="Cliente", index=True,
                                 domain="[('is_company', '=', True)]",
                                 help="Vacío en Confección (se presupuesta por producto).")
    plant = fields.Char(string="Planta")
    product_id = fields.Many2one('product.product', string="Producto", required=True, index=True)
    articulo_id = fields.Many2one(related='product_id.qb_articulo_id', store=True, string="Artículo")
    mercado = fields.Selection(MARKETS, string="Mercado", required=True, default='confeccion', index=True)
    from_forecast = fields.Boolean(string="Del pronóstico", readonly=True,
                                   help="Renglón armado del pronóstico; los capturados a mano se conservan al "
                                        "volver a armar.")
    qty_forecast = fields.Float(string="Pronóstico al armar", digits='Product Unit', readonly=True,
                                help="Cantidad del pronóstico vigente cuando se armó (unidad de venta).")
    qty = fields.Float(string="Cantidad", digits='Product Unit', required=True, default=0.0,
                       help="En la unidad de venta del producto. Se puede ajustar antes de enviar; no cambia el "
                            "pronóstico.")
    uom_name = fields.Char(related='product_id.uom_id.name', string="Unidad")
    qty_m = fields.Float(string="Metros", digits=(16, 2), compute='_compute_amount', store=True)
    qty_kg = fields.Float(string="Kilos", digits=(16, 2), compute='_compute_amount', store=True)
    price_input = fields.Float(string="Precio", digits=(16, 4))
    price_currency_id = fields.Many2one('res.currency', string="Moneda del precio",
                                        default=lambda self: self.env.company.currency_id)
    price_basis = fields.Selection(PRICE_BASIS, string="Base del precio", default='venta', required=True)
    price_origen = fields.Selection(PLAN_PRICE_ORIGINS, string="De dónde salió el precio")
    amount = fields.Monetary(string="Importe", compute='_compute_amount', store=True)
    price_problem = fields.Selection(PRICE_PROBLEMS, string="Problema del importe", compute='_compute_amount',
                                     store=True)

    @api.depends('qty', 'product_id', 'price_input', 'price_currency_id', 'price_basis', 'plan_id.rate',
                 'plan_id.company_id')
    def _compute_amount(self):
        usd, mxn = self.env.ref('base.USD', raise_if_not_found=False), self.env.ref('base.MXN', raise_if_not_found=False)
        for line in self:
            product = line.product_id
            qty_m = product.qb_convertir(line.qty, 'venta', 'm') if product else None
            qty_kg = product.qb_convertir(line.qty, 'venta', 'kg') if product else None
            line.qty_m, line.qty_kg = qty_m or 0.0, qty_kg or 0.0
            line.amount, line.price_problem = 0.0, False
            if not line.qty:
                continue
            if line.price_input <= 0:
                line.price_problem = 'sin_precio'
                continue
            base = {'venta': line.qty, 'm': qty_m, 'kg': qty_kg}[line.price_basis or 'venta']
            if not base:
                line.price_problem = 'sin_conversion'
                continue
            amount = base * line.price_input
            company_ccy = line.plan_id.currency_id or self.env.company.currency_id
            price_ccy = line.price_currency_id or company_ccy
            if price_ccy != company_ccy:
                rate = line.plan_id.rate
                if rate <= 0 or {price_ccy, company_ccy} != {usd, mxn}:
                    line.price_problem = 'sin_tc'
                    continue
                amount = amount * rate if price_ccy == usd else amount / rate
            line.amount = amount

    @api.onchange('product_id')
    def _onchange_product_mercado(self):
        if self.product_id and not self.partner_id and not self.mercado:
            self.mercado = 'confeccion'

    def _qb_check_open(self):
        if self.filtered(lambda l: l.plan_id.state == 'enviado') and not self.env.su \
                and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("El presupuesto del mes ya se envió a Dirección: está congelado.")

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines._qb_check_open()
        return lines

    def write(self, vals):
        self._qb_check_open()
        if 'price_input' in vals and 'price_origen' not in vals:
            vals = dict(vals, price_origen='captura')
        return super().write(vals)

    def unlink(self):
        self._qb_check_open()
        return super().unlink()


class QbSalesMonthPlanBuild(models.TransientModel):
    _name = 'qb.sales.month.plan.build'
    _description = "Armar el presupuesto del mes"

    @api.model
    def _default_month(self):
        return fields.Date.context_today(self).replace(day=1) + relativedelta(months=1)

    month = fields.Date(string="Mes", required=True, default=_default_month,
                        help="Cualquier día del mes a presupuestar (por omisión, el siguiente).")
    rate = fields.Float(string="Tipo de cambio del mes", digits=(12, 4),
                        help="Vacío: el del presupuesto anual aprobado del mes (o el presupuestal de Ajustes).")

    def action_build(self):
        self.ensure_one()
        month = self.month.replace(day=1)
        Plan = self.env['qb.sales.month.plan']
        plan = Plan.search([('company_id', '=', self.env.company.id), ('month', '=', month)], limit=1)
        if plan and plan.state != 'borrador':
            raise UserError("%s ya se envió a Dirección." % plan.name)
        if not plan:
            plan = Plan.create({'month': month, 'company_id': self.env.company.id,
                                'rate': self.rate or Plan._qb_default_rate(self.env.company, month)})
        elif self.rate:
            plan.rate = self.rate
        plan.action_qb_build()
        return {'type': 'ir.actions.act_window', 'res_model': Plan._name, 'res_id': plan.id,
                'view_mode': 'form', 'target': 'current'}
