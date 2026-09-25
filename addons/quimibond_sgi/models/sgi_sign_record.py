# -*- coding: utf-8 -*-
"""REG-1 (53.0.0): firmas de Sign ligadas a su registro. Desde la orden de
compra, la entrega, el lote, el producto o el traslado, «Firmar» crea la
solicitud de firma con `reference_doc` apuntando al registro; las firmas
dejan de estar sueltas y un entregable puede exigir «firmado».
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

SGI_SIGN_MODELS = ('purchase.order', 'stock.picking', 'stock.lot', 'product.template')


class SgiSignRecordMixin(models.AbstractModel):
    _name = 'sgi.sign.record.mixin'
    _description = "Firmas de Sign ligadas al registro"

    sgi_sign_request_ids = fields.Many2many(
        'sign.request', compute='_compute_sgi_sign_requests', string="Solicitudes de firma")
    sgi_sign_count = fields.Integer(compute='_compute_sgi_sign_requests')
    sgi_signed = fields.Boolean(string="Firmado", compute='_compute_sgi_sign_requests',
                                search='_search_sgi_signed')

    @api.model
    def _sgi_sign_domain(self, records):
        return [('reference_doc', 'in', ['%s,%d' % (r._name, r.id) for r in records])]

    def _compute_sgi_sign_requests(self):
        Request = self.env['sign.request'].sudo()
        by_ref = {}
        if self.ids:
            for req in Request.search(self._sgi_sign_domain(self)):
                ref = req.reference_doc
                if ref:
                    by_ref[ref] = by_ref.get(ref, Request) | req
        for rec in self:
            requests = by_ref.get(rec, Request) if rec.id else Request
            rec.sgi_sign_request_ids = requests.ids
            rec.sgi_sign_count = len(requests)
            rec.sgi_signed = bool(requests.filtered(lambda r: r.state == 'signed'))

    @api.model
    def _search_sgi_signed(self, operator, value):
        signed = self.env['sign.request'].sudo().search(
            [('state', '=', 'signed'), ('reference_doc', 'like', '%s,%%' % self._name)])
        ids = [r.reference_doc.id for r in signed if r.reference_doc and r.reference_doc._name == self._name]
        # Odoo 19 normaliza '=' a 'in' con lista antes de llamar aquí.
        values = set(value) if isinstance(value, (list, tuple, set)) else {value}
        wants_signed = (operator in ('=', 'in')) == any(values)
        return [('id', 'in' if wants_signed else 'not in', ids)]

    def action_sgi_sign(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "Firmar desde el registro",
            'res_model': 'sgi.sign.request.wizard', 'view_mode': 'form', 'target': 'new',
            'context': {'default_res_model': self._name, 'default_res_id': self.id,
                        'default_partner_id': getattr(self, 'partner_id', self.env['res.partner']).id or False},
        }

    def action_sgi_view_sign_requests(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "Firmas — %s" % self.display_name,
            'res_model': 'sign.request', 'view_mode': 'list,form',
            'domain': self._sgi_sign_domain(self),
        }


class PurchaseOrderSign(models.Model):
    _name = 'purchase.order'
    _inherit = ['purchase.order', 'sgi.sign.record.mixin']


class StockPickingSign(models.Model):
    _name = 'stock.picking'
    _inherit = ['stock.picking', 'sgi.sign.record.mixin']


class StockLotSign(models.Model):
    _name = 'stock.lot'
    _inherit = ['stock.lot', 'sgi.sign.record.mixin']


class ProductTemplateSign(models.Model):
    _name = 'product.template'
    _inherit = ['product.template', 'sgi.sign.record.mixin']


class SgiSignRequestWizard(models.TransientModel):
    _name = 'sgi.sign.request.wizard'
    _description = "Crear solicitud de firma ligada a un registro"

    res_model = fields.Char(required=True)
    res_id = fields.Integer(required=True)
    template_id = fields.Many2one('sign.template', string="Plantilla de firma", required=True)
    partner_id = fields.Many2one('res.partner', string="Firmante", required=True)
    subject = fields.Char(string="Asunto")

    def action_confirm(self):
        self.ensure_one()
        record = self.env[self.res_model].browse(self.res_id).exists()
        if not record:
            raise UserError("El registro ya no existe.")
        template = self.template_id.sudo()
        roles = template.sign_item_ids.mapped('responsible_id')
        if len(roles) != 1:
            raise UserError("La plantilla debe tener campos de UN solo firmante; esta tiene %d." % len(roles))
        request = self.env['sign.request'].sudo().create({
            'template_id': template.id,
            'reference': "%s — %s" % (template.display_name, record.display_name),
            'subject': self.subject or "Firma: %s" % record.display_name,
            'reference_doc': '%s,%d' % (self.res_model, self.res_id),
            'request_item_ids': [(0, 0, {'partner_id': self.partner_id.id, 'role_id': roles.id})],
        })
        if hasattr(record, 'message_post'):
            record.message_post(body="Solicitud de firma creada: %s (firmante %s)." % (
                request.reference, self.partner_id.display_name))
        return {'type': 'ir.actions.act_window', 'res_model': 'sign.request',
                'res_id': request.id, 'view_mode': 'form'}


class SgiDeliverableSignSurvey(models.Model):
    """REG-1: el entregable puede exigir «firmado». REG-2: una respuesta de
    Encuestas puede ser el entregable (modelo survey.user_input, filtro por la
    encuesta y estado terminado)."""
    _inherit = 'sgi.deliverable'

    require_signed = fields.Boolean(
        string="Exige firmado", help="Cuenta como completo solo si el registro tiene una "
                                     "solicitud de Sign firmada ligada a él (REG-1).")
    survey_id = fields.Many2one(
        'survey.survey', string="Encuesta",
        help="El entregable es una respuesta terminada de esta encuesta (REG-2).")

    @api.onchange('survey_id')
    def _onchange_survey_id(self):
        if self.survey_id:
            model = self.env['ir.model']._get('survey.user_input')
            self.odoo_model_id = model.id
            self.measure_domain = "[('survey_id', '=', %d), ('state', '=', 'done')]" % self.survey_id.id
            self.measure_date_field = 'end_datetime'

    def _sgi_signed_ids(self, model_name, ids):
        """Ids de `ids` con una solicitud de firma firmada ligada."""
        if not ids or model_name not in SGI_SIGN_MODELS:
            return []
        signed = self.env['sign.request'].sudo().search([
            ('state', '=', 'signed'),
            ('reference_doc', 'in', ['%s,%d' % (model_name, i) for i in ids])])
        return [r.reference_doc.id for r in signed if r.reference_doc]
