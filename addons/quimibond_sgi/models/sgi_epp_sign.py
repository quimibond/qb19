# -*- coding: utf-8 -*-
"""EPP por empleado con firma en Sign (PER-2, 56.0.0).

La responsiva F-P-S03-02 deja de ser texto libre: cada entrega lleva sus
renglones (cantidad, artículo, talla, observaciones) y puede firmarse con la
app Firma. MAST liga una plantilla de Sign (el PDF de la responsiva con su
campo de firma) en Ajustes → SGI; el botón «Enviar a firmar (Sign)» crea la
solicitud para el empleado con `reference_doc` apuntando a la responsiva y
el cron diario la sella como firmada cuando Sign termina. La firma interna
(«Firmar: recibí el EPP») sigue disponible como respaldo.
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class SgiEppDeliveryLine(models.Model):
    _name = 'sgi.epp.delivery.line'
    _description = "Renglón de la responsiva de EPP"
    _order = 'sequence, id'

    delivery_id = fields.Many2one('sgi.epp.delivery', required=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    quantity = fields.Float(string="Cantidad", default=1.0, required=True)
    uom = fields.Selection([('pz', "pz"), ('par', "par"), ('juego', "juego")], string="Unidad", default='pz', required=True)
    name = fields.Char(string="Artículo", required=True)
    size = fields.Char(string="Talla")
    note = fields.Char(string="Observaciones")


class SgiEppDeliverySign(models.Model):
    _inherit = 'sgi.epp.delivery'

    line_ids = fields.One2many('sgi.epp.delivery.line', 'delivery_id', string="Renglones de EPP")
    items = fields.Text(compute='_compute_items', store=True, readonly=False, required=False)
    sign_request_id = fields.Many2one('sign.request', string="Solicitud de firma (Sign)", readonly=True, copy=False,
                                      ondelete='set null')
    sign_state = fields.Selection(related='sign_request_id.state', string="Firma electrónica", store=True)

    @api.depends('line_ids.name', 'line_ids.quantity', 'line_ids.uom', 'line_ids.size')
    def _compute_items(self):
        for rec in self:
            if rec.line_ids:
                rec.items = "\n".join(
                    "%s %s %s%s" % (('%g' % l.quantity), l.uom, l.name, " talla %s" % l.size if l.size else "")
                    for l in rec.line_ids)
            elif not rec.items:
                rec.items = False

    def write(self, vals):
        if 'line_ids' in vals:
            signed = self.filtered(lambda r: r.state == 'firmada')
            if signed and not self.env.su:
                raise UserError("Una responsiva firmada no se modifica: haz una entrega nueva.")
        return super().write(vals)

    def _sgi_epp_sign_template(self):
        param = self.env['ir.config_parameter'].sudo().get_param('quimibond_sgi.epp_sign_template_id')
        template = self.env['sign.template'].sudo().browse(int(param)) if param and param.isdigit() else self.env['sign.template']
        return template.exists()

    def action_send_sign_request(self):
        """Crea la solicitud de firma para el empleado (una viva por responsiva)."""
        template = self._sgi_epp_sign_template()
        if not template:
            raise UserError("Liga primero la plantilla de Sign de la responsiva de EPP en Ajustes → SGI → EPP.")
        roles = template.sign_item_ids.mapped('responsible_id')
        if len(roles) != 1:
            raise UserError("La plantilla de firma debe tener campos de UN solo firmante (el empleado). Tiene %d roles." % len(roles))
        for rec in self:
            if rec.state == 'firmada':
                continue
            if rec.sign_request_id and rec.sign_request_id.state not in ('canceled', 'expired'):
                raise UserError("La responsiva %s ya tiene una solicitud de firma en curso." % rec.name)
            employee = rec.sudo().employee_id
            partner = employee.user_id.partner_id or employee.work_contact_id
            if not partner or not partner.email:
                raise UserError("%s no tiene contacto con correo: Sign no puede mandarle la solicitud." % employee.name)
            request = self.env['sign.request'].sudo().create({
                'template_id': template.id,
                'reference': "Responsiva EPP %s — %s" % (rec.name, employee.name),
                'subject': "Firma de responsiva de EPP %s" % rec.name,
                'reference_doc': '%s,%d' % (rec._name, rec.id),
                'request_item_ids': [(0, 0, {'partner_id': partner.id, 'role_id': roles.id})],
            })
            rec.sudo().sign_request_id = request
            rec.message_post(body="Solicitud de firma enviada por Sign a %s." % partner.display_name)
        return True

    def action_open_sign_request(self):
        self.ensure_one()
        if not self.sign_request_id:
            return False
        return {'type': 'ir.actions.act_window', 'res_model': 'sign.request', 'res_id': self.sign_request_id.id,
                'view_mode': 'form'}

    @api.model
    def _sgi_sync_from_sign(self):
        """Responsiva entregada cuya solicitud ya está firmada → firmada (con
        sudo: la firma electrónica es la evidencia de identidad)."""
        signed = self.sudo().search([('state', '=', 'entregada'), ('sign_state', '=', 'signed')])
        for rec in signed:
            completion = rec.sign_request_id.completion_date
            rec.write({'state': 'firmada',
                       'signed_date': fields.Datetime.to_datetime(completion) if completion else fields.Datetime.now()})
            rec.message_post(body="Responsiva firmada electrónicamente en Sign.")
        return len(signed)

    def sgi_format_info(self):
        self.ensure_one()
        code = 'F-P-S03-02'
        revision = self.env['sgi.format.map'].sudo()._revision_of(code)
        return "%s · Rev. %s" % (code, revision) if revision else code


class ResConfigSettingsEpp(models.TransientModel):
    _inherit = 'res.config.settings'

    sgi_epp_sign_template_id = fields.Many2one(
        'sign.template', string="Plantilla de Sign de la responsiva de EPP",
        config_parameter='quimibond_sgi.epp_sign_template_id',
        help="Plantilla de la app Firma hecha con el PDF «Responsiva de EPP» y un solo campo de firma (el empleado).")
