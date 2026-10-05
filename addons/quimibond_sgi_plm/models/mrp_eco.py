# -*- coding: utf-8 -*-
"""Puente ECO ↔ PPAP del SGI.

3.2.0 (SGI 57.100.0, N-14): «Requiere PPAP» se marca solo cuando el producto
del ECO se vendió en los últimos meses (parámetro
``quimibond_sgi.ppap_sales_window_months``, 12) o ya tiene PPAP con un cliente
marcado «Exige PPAP ante cambios» en la compañía del ECO. Solo en ECO sin
aplicar y sin la casilla; nunca la desmarca. Al aplicar se genera un PPAP por
cliente (P14), sin repetir.
"""
from dateutil.relativedelta import relativedelta
from markupsafe import Markup, escape

from odoo import api, fields, models

PPAP_WINDOW_PARAM = 'quimibond_sgi.ppap_sales_window_months'


def _eco_depends(model):
    return ['product_tmpl_id'] + (['company_id'] if 'company_id' in model._fields else [])


class MrpEco(models.Model):
    _inherit = 'mrp.eco'

    sgi_requires_ppap = fields.Boolean(
        string="Requiere PPAP",
        help="Si está marcado, al aplicar el cambio se generará un expediente PPAP. El SGI lo "
             "marca solo cuando el producto se vende a clientes que exigen PPAP ante cambios.")
    sgi_customer_notice = fields.Boolean(
        string="Requiere aviso al cliente",
        help="El cambio afecta a partes ya aprobadas: notificar al cliente antes de aplicar.")
    sgi_customer_id = fields.Many2one('res.partner', string="Cliente del PPAP",
                                      domain="[('is_company', '=', True)]")
    sgi_ppap_id = fields.Many2one('sgi.ppap', string="PPAP generado", readonly=True, copy=False)
    sgi_fmea_ids = fields.Many2many('sgi.fmea', string="AMEF impactados")
    sgi_control_plan_ids = fields.Many2many('sgi.control.plan', string="Planes de control impactados")
    # 3.2.0 (N-14)
    sgi_ppap_customer_ids = fields.Many2many(
        'res.partner', string="Clientes que exigen PPAP",
        compute='_compute_sgi_ppap_customer_ids',
        help="Clientes marcados «Exige PPAP ante cambios» que compraron el producto en los "
             "últimos meses o ya tienen un PPAP de él.")
    sgi_ppap_auto = fields.Boolean(
        string="Marcado por el SGI", readonly=True, copy=False,
        help="«Requiere PPAP» lo marcó el SGI por los clientes del producto.")
    sgi_ppap_ids = fields.Many2many(
        'sgi.ppap', 'sgi_mrp_eco_ppap_rel', 'eco_id', 'ppap_id',
        string="PPAP generados", readonly=True, copy=False,
        help="Un PPAP por cliente, generados al aplicar el cambio.")
    sgi_ppap_count = fields.Integer(string="PPAP", compute='_compute_sgi_ppap_count')

    @api.depends(_eco_depends)
    def _compute_sgi_ppap_customer_ids(self):
        raw = self.env['ir.config_parameter'].sudo().get_param(PPAP_WINDOW_PARAM, '12')
        months = int(raw) if str(raw).strip().isdigit() else 12
        since = fields.Datetime.now() - relativedelta(months=months)
        Line = self.env['sale.order.line'].sudo()
        Ppap = self.env['sgi.ppap'].sudo()
        Partner = self.env['res.partner'].sudo()
        for eco in self:
            tmpl = eco.product_tmpl_id
            if not tmpl:
                eco.sgi_ppap_customer_ids = False
                continue
            company = (eco.company_id if 'company_id' in eco._fields else False) \
                or self.env.company
            buyers = Line._read_group(
                [('product_id.product_tmpl_id', '=', tmpl.id), ('state', '=', 'sale'),
                 ('order_id.date_order', '>=', since), ('company_id', '=', company.id)],
                ['order_partner_id'])
            partners = Partner.browse([partner.id for (partner,) in buyers if partner])
            # PPAP: el expediente no lleva compañía; el filtro de compañía es la
            # casilla del cliente leída con la compañía del ECO.
            partners |= Ppap.search([('product_tmpl_id', '=', tmpl.id)]).partner_id
            eco.sgi_ppap_customer_ids = partners.commercial_partner_id.filtered(
                lambda p: p.with_company(company).sgi_requires_ppap)

    @api.depends('sgi_ppap_ids', 'sgi_ppap_id')
    def _compute_sgi_ppap_count(self):
        for eco in self:
            eco.sgi_ppap_count = len(eco.sgi_ppap_ids | eco.sgi_ppap_id)

    def _sgi_apply_ppap_rule(self):
        """Marca «Requiere PPAP» si el producto se vende a un cliente que lo
        exige. Solo ECO sin aplicar y sin la casilla; nunca desmarca."""
        for eco in self.filtered(lambda e: e.state != 'done' and not e.sgi_requires_ppap):
            customers = eco.sgi_ppap_customer_ids
            if not customers:
                continue
            vals = {'sgi_requires_ppap': True, 'sgi_ppap_auto': True}
            if len(customers) == 1 and not eco.sgi_customer_id:
                vals['sgi_customer_id'] = customers.id
            eco.with_context(sgi_ppap_rule=True).write(vals)
            eco.message_post(body=Markup(
                "Requiere PPAP: el producto se vende a %s, que lo exige(n) ante cambios. Al "
                "aplicar el cambio se genera un PPAP por cliente.") % escape(
                    ", ".join(customers.mapped('name'))))
        return True

    @api.model_create_multi
    def create(self, vals_list):
        ecos = super().create(vals_list)
        ecos._sgi_apply_ppap_rule()
        return ecos

    def write(self, vals):
        res = super().write(vals)
        if 'product_tmpl_id' in vals and not self.env.context.get('sgi_ppap_rule'):
            self._sgi_apply_ppap_rule()
        return res

    def action_view_sgi_ppap(self):
        self.ensure_one()
        ppaps = self.sgi_ppap_ids | self.sgi_ppap_id
        if len(ppaps) == 1:
            return {
                'type': 'ir.actions.act_window',
                'name': "PPAP",
                'res_model': 'sgi.ppap',
                'view_mode': 'form',
                'res_id': ppaps.id,
            }
        return {
            'type': 'ir.actions.act_window',
            'name': "PPAP",
            'res_model': 'sgi.ppap',
            'view_mode': 'list,form',
            'domain': [('id', 'in', ppaps.ids)],
        }

    def action_apply(self):
        # 3.2.0: la regla corre antes de aplicar (el producto o los clientes
        # pudieron cambiar desde que se creó el ECO).
        self._sgi_apply_ppap_rule()
        res = super().action_apply()
        for eco in self:
            if eco.state == 'done':
                eco._sgi_handle_ppap()
        return res

    def _sgi_handle_ppap(self):
        """Genera un PPAP por cliente (idempotente) y agenda el aviso al
        cliente si aplica."""
        self.ensure_one()
        Cron = self.env['sgi.cron']
        if self.sgi_requires_ppap and self.product_tmpl_id:
            customers = self.sgi_customer_id
            if self.sgi_ppap_auto:
                customers |= self.sgi_ppap_customer_ids
            for customer in customers:
                done = self.sgi_ppap_ids | self.sgi_ppap_id
                if done.filtered(lambda p: p.partner_id == customer):
                    continue
                ppap = self.env['sgi.ppap'].create({
                    'partner_id': customer.id,
                    'product_tmpl_id': self.product_tmpl_id.id,
                    'reason': 'cambio_ingenieria',
                    'notes': "Generado automáticamente por el cambio de ingeniería %s." % (
                        self.name or ''),
                })
                if self.sgi_fmea_ids:
                    self._sgi_link_ppap_element(ppap, 'fmea_id', self.sgi_fmea_ids[:1].id)
                if self.sgi_control_plan_ids:
                    self._sgi_link_ppap_element(
                        ppap, 'control_plan_id', self.sgi_control_plan_ids[:1].id)
                self.sgi_ppap_ids = [(4, ppap.id)]
                # K-07: el folio va en negritas con Markup (antes se veía «<b>»).
                self.message_post(body=Markup(
                    "Se generó el PPAP <b>%s</b> para %s por el cambio de ingeniería.") % (
                        ppap.folio or '', customer.name or ''))
            if self.sgi_ppap_ids and not self.sgi_ppap_id:
                self.sgi_ppap_id = self.sgi_ppap_ids.sorted('id')[:1]
        if self.sgi_requires_ppap and not (self.sgi_ppap_ids or self.sgi_ppap_id):
            manager_id = Cron._sgi_manager_user_id()
            Cron._sgi_schedule(
                self,
                "Crear PPAP por cambio de ingeniería: %s" % (self.name or ''),
                "El ECO requiere PPAP pero falta el cliente o el producto. "
                "Cree el expediente PPAP manualmente.",
                manager_id)
        if self.sgi_customer_notice:
            sales_user_id = self._sgi_sales_user_id()
            Cron._sgi_schedule(
                self,
                "Aviso al cliente por cambio de ingeniería: %s" % (self.name or ''),
                "El cambio afecta partes aprobadas. Notifique formalmente al cliente.",
                sales_user_id)
        return True

    def _sgi_link_ppap_element(self, ppap, field_name, record_id):
        """Liga el AMEF/plan de control al elemento correspondiente del PPAP."""
        template_seq = 6 if field_name == 'fmea_id' else 7  # 6=PFMEA, 7=Plan de control
        element = ppap.element_ids.filtered(
            lambda e: e.template_id.sequence == template_seq)[:1]
        if element:
            element.write({field_name: record_id, 'state': 'listo'})

    def _sgi_sales_user_id(self):
        group = self.env.ref('sales_team.group_sale_salesman', raise_if_not_found=False)
        if group and group.all_user_ids:
            return group.all_user_ids[:1].id
        return self.env['sgi.cron']._sgi_manager_user_id()
