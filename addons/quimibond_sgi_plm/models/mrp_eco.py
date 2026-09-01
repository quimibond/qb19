# -*- coding: utf-8 -*-
from odoo import models, fields


class MrpEco(models.Model):
    _inherit = 'mrp.eco'

    sgi_requires_ppap = fields.Boolean(
        string="Requiere PPAP",
        help="Si está marcado, al aplicar el cambio se generará un expediente PPAP.")
    sgi_customer_notice = fields.Boolean(
        string="Requiere aviso al cliente",
        help="El cambio afecta a partes ya aprobadas: notificar al cliente antes de aplicar.")
    sgi_customer_id = fields.Many2one('res.partner', string="Cliente del PPAP",
                                      domain="[('is_company', '=', True)]")
    sgi_ppap_id = fields.Many2one('sgi.ppap', string="PPAP generado", readonly=True, copy=False)
    sgi_fmea_ids = fields.Many2many('sgi.fmea', string="AMEF impactados")
    sgi_control_plan_ids = fields.Many2many('sgi.control.plan', string="Planes de control impactados")

    def action_view_sgi_ppap(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "PPAP",
            'res_model': 'sgi.ppap',
            'view_mode': 'form',
            'res_id': self.sgi_ppap_id.id,
        }

    def action_apply(self):
        res = super().action_apply()
        for eco in self:
            if eco.state == 'done':
                eco._sgi_handle_ppap()
        return res

    def _sgi_handle_ppap(self):
        """Genera el PPAP (idempotente) y agenda el aviso al cliente si aplica."""
        self.ensure_one()
        Cron = self.env['sgi.cron']
        if self.sgi_requires_ppap and not self.sgi_ppap_id:
            if self.sgi_customer_id and self.product_tmpl_id:
                ppap = self.env['sgi.ppap'].create({
                    'partner_id': self.sgi_customer_id.id,
                    'product_tmpl_id': self.product_tmpl_id.id,
                    'reason': 'cambio_ingenieria',
                    'notes': "Generado automáticamente por el cambio de ingeniería %s." % (
                        self.name or ''),
                })
                self.sgi_ppap_id = ppap.id
                if self.sgi_fmea_ids:
                    self._sgi_link_ppap_element(ppap, 'fmea_id', self.sgi_fmea_ids[:1].id)
                if self.sgi_control_plan_ids:
                    self._sgi_link_ppap_element(
                        ppap, 'control_plan_id', self.sgi_control_plan_ids[:1].id)
                self.message_post(
                    body="Se generó el PPAP <b>%s</b> por el cambio de ingeniería." % ppap.folio)
            else:
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
