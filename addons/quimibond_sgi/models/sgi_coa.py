# -*- coding: utf-8 -*-
"""COA (certificado de análisis) ligado a la entrega y al pedido — fase 1.

El laboratorio arma el COA fuera de Odoo (PDF) y hoy lo manda por correo sin
dejar rastro. Aquí se registra en la entrega de salida: quién lo adjuntó,
cuándo y si se mandó al cliente. El cliente dice si lo requiere
(``sgi_requires_coa``, por compañía); sus direcciones de entrega heredan el
requisito de la empresa comercial.

Validar la salida sin COA solo avisa. El bloqueo queda listo detrás del
parámetro ``quimibond_sgi.coa_block_validation``: con él, solo el Jefe de
Calidad (puesto ``quimibond_sgi.coa_exception_job_id``) valida sin COA y deja
el motivo.

Lo que se siga mandando por correo se liga solo: el buzón «COA» lee el nombre
del archivo (``<código de producto> <factura>.pdf``), sigue la factura al
pedido y al pedido a su salida con ese producto. Lo que no se liga queda en la
cola «COA sin ligar» para asignarlo a mano.
"""
import re

from odoo import api, fields, models
from odoo.exceptions import UserError

COA_STATUS = [
    ('no_aplica', "No aplica"),
    ('pendiente', "Pendiente"),
    ('adjunto', "Adjunto"),
    ('enviado', "Enviado"),
]
# El peor estado manda en el pedido.
_COA_RANK = {'no_aplica': 0, 'enviado': 1, 'adjunto': 2, 'pendiente': 3}

BLOCK_PARAM = 'quimibond_sgi.coa_block_validation'
EXCEPTION_JOB_PARAM = 'quimibond_sgi.coa_exception_job_id'

# «WJ032Q22JNT160 INV-2026-09-0145.pdf» → producto y factura. La factura es el
# nombre de Odoo con «-» en vez de «/» (INV-2026-09-0145 → INV/2026/09/0145).
_COA_NAME_RE = re.compile(
    r'^\s*(?P<product>\S+)\s+(?P<invoice>[A-Za-z]+(?:-\d+)+)\s*\.pdf\s*$',
    re.IGNORECASE)


def sgi_parse_coa_filename(filename):
    """(código de producto, nombre de factura en Odoo) o (None, None)."""
    match = _COA_NAME_RE.match(filename or '')
    if not match:
        return None, None
    return match.group('product'), match.group('invoice').upper().replace('-', '/')


class ResPartner(models.Model):
    _inherit = 'res.partner'

    sgi_requires_coa = fields.Boolean(
        string="Requiere COA en cada embarque", company_dependent=True,
        help="Cada salida a este cliente (o a sus direcciones de entrega) "
             "debe llevar el certificado de análisis adjunto.")
    sgi_coa_recipient_ids = fields.Many2many(
        'res.partner', 'sgi_partner_coa_recipient_rel', 'partner_id', 'recipient_id',
        string="Reciben el COA",
        help="Contactos a quienes se manda el COA. Vacío: el contacto de la entrega.")

    _SGI_COA_FIELDS = ('sgi_requires_coa', 'sgi_coa_recipient_ids')

    def write(self, vals):
        # Solo SGI o Calidad deciden qué cliente requiere COA (la vista lo
        # muestra de solo lectura a los demás; esta es la regla real).
        if (set(vals) & set(self._SGI_COA_FIELDS) and not self.env.su
                and not (self.env.user.has_group('quimibond_sgi.group_sgi_user')
                         or self.env.user.has_group('quality.group_quality_user'))):
            raise UserError("Solo SGI o Calidad pueden cambiar el requisito de COA "
                            "de un cliente.")
        res = super().write(vals)
        if 'sgi_requires_coa' in vals:
            # Las salidas abiertas del cliente toman el requisito nuevo; las ya
            # validadas no se tocan (no se marca retroactivamente).
            pickings = self.env['stock.picking'].search([
                ('partner_id', 'child_of', self.commercial_partner_id.ids),
                ('picking_type_code', '=', 'outgoing'),
                ('state', 'not in', ('done', 'cancel')),
            ])
            if pickings:
                # Se recalcula al siguiente acceso o flush.
                self.env.add_to_compute(pickings._fields['sgi_requires_coa'], pickings)
        return res


class StockPicking(models.Model):
    _inherit = 'stock.picking'

    sgi_requires_coa = fields.Boolean(
        string="Requiere COA", compute='_compute_sgi_requires_coa', store=True,
        help="Salida a un cliente que pide certificado de análisis en cada embarque.")
    sgi_coa_attachment_ids = fields.Many2many(
        'ir.attachment', 'sgi_picking_coa_attachment_rel', 'picking_id', 'attachment_id',
        string="COA", copy=False,
        help="Certificados de análisis del embarque (uno por producto).")
    sgi_coa_date = fields.Datetime(string="COA adjuntado", copy=False, readonly=True)
    sgi_coa_uid = fields.Many2one('res.users', string="COA adjuntado por",
                                  copy=False, readonly=True)
    sgi_coa_sent_date = fields.Datetime(string="COA enviado al cliente", copy=False,
                                        readonly=True)
    sgi_coa_status = fields.Selection(
        COA_STATUS, string="Estado del COA", compute='_compute_sgi_coa_status',
        store=True, index=True)

    @api.depends('partner_id', 'picking_type_id', 'company_id')
    def _compute_sgi_requires_coa(self):
        # El requisito es por compañía: se lee con la compañía de la salida.
        for picking in self:
            partner = picking.partner_id.commercial_partner_id
            picking.sgi_requires_coa = bool(
                picking.picking_type_code == 'outgoing' and partner
                and partner.with_company(picking.company_id).sgi_requires_coa)

    @api.depends('sgi_requires_coa', 'sgi_coa_attachment_ids', 'sgi_coa_sent_date')
    def _compute_sgi_coa_status(self):
        for picking in self:
            if not picking.sgi_requires_coa:
                picking.sgi_coa_status = 'no_aplica'
            elif picking.sgi_coa_sent_date:
                picking.sgi_coa_status = 'enviado'
            elif picking.sgi_coa_attachment_ids:
                picking.sgi_coa_status = 'adjunto'
            else:
                picking.sgi_coa_status = 'pendiente'

    # --- Adjuntar y enviar ----------------------------------------------
    def action_sgi_attach_coa(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Adjuntar COA",
            'res_model': 'sgi.coa.attach.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_picking_id': self.id},
        }

    def _sgi_coa_recipients(self):
        self.ensure_one()
        commercial = self.partner_id.commercial_partner_id.with_company(self.company_id)
        return commercial.sgi_coa_recipient_ids or self.partner_id

    def _sgi_coa_register(self, attachments, sent=False, when=None, user=None):
        """Liga los PDF a la entrega y deja fecha y usuario (y si ya se mandó)."""
        self.ensure_one()
        when = when or fields.Datetime.now()
        attachments.sudo().write({'res_model': 'stock.picking', 'res_id': self.id})
        vals = {'sgi_coa_attachment_ids': [(4, att.id) for att in attachments],
                'sgi_coa_date': when, 'sgi_coa_uid': (user or self.env.user).id}
        if sent:
            vals['sgi_coa_sent_date'] = when
        self.write(vals)

    def _sgi_coa_send(self, attachments, recipients):
        """Correo al cliente con la plantilla y los PDF; queda en el chatter."""
        self.ensure_one()
        template = self.env.ref('quimibond_sgi.mail_template_sgi_coa')
        self.message_post_with_source(
            template, message_type='comment', subtype_xmlid='mail.mt_comment',
            partner_ids=recipients.ids, attachment_ids=attachments.ids)
        self.sgi_coa_sent_date = fields.Datetime.now()

    # --- Validación: aviso hoy, bloqueo detrás de un parámetro -------------
    def _sgi_coa_block_enabled(self):
        value = self.env['ir.config_parameter'].sudo().get_param(BLOCK_PARAM, 'False')
        return str(value).strip().lower() in ('1', 'true', 'yes', 'si', 'sí')

    def _sgi_user_is_quality_head(self):
        raw = self.env['ir.config_parameter'].sudo().get_param(EXCEPTION_JOB_PARAM, '204')
        job_id = int(raw) if str(raw).strip().isdigit() else 0
        return bool(job_id and self.env.user.employee_ids.filtered(
            lambda emp: emp.job_id.id == job_id))

    def button_validate(self):
        if self._sgi_coa_block_enabled():
            missing = self.filtered(lambda p: p.sgi_coa_status == 'pendiente')
            # El motivo viaja en el contexto desde el asistente; el cliente RPC
            # lo puede forjar, así que solo cuenta si quien valida es el Jefe
            # de Calidad.
            reason = self.env.context.get('sgi_coa_exception_reason')
            if missing and not (reason and self._sgi_user_is_quality_head()):
                if self._sgi_user_is_quality_head():
                    return {
                        'type': 'ir.actions.act_window',
                        'name': "Validar sin COA",
                        'res_model': 'sgi.coa.exception.wizard',
                        'view_mode': 'form',
                        'target': 'new',
                        'context': {'default_picking_ids': [(6, 0, self.ids)]},
                    }
                raise UserError(
                    "Estas salidas requieren COA y no lo tienen adjunto: %s.\n"
                    "Adjunta el certificado (botón «Adjuntar COA») o pide al Jefe "
                    "de Calidad que valide la excepción." % ", ".join(missing.mapped('name')))
        return super().button_validate()


class SaleOrder(models.Model):
    _inherit = 'sale.order'

    sgi_coa_status = fields.Selection(
        COA_STATUS, string="COA", compute='_compute_sgi_coa_status', store=True,
        help="El peor estado del COA de sus salidas que lo requieren.")
    sgi_coa_attachment_count = fields.Integer(
        string="# COA", compute='_compute_sgi_coa_attachment_count')

    @api.depends('picking_ids.sgi_coa_status')
    def _compute_sgi_coa_status(self):
        for order in self:
            states = order.picking_ids.mapped('sgi_coa_status') or ['no_aplica']
            order.sgi_coa_status = max(states, key=lambda s: _COA_RANK.get(s, 0))

    def _compute_sgi_coa_attachment_count(self):
        for order in self:
            order.sgi_coa_attachment_count = len(order.picking_ids.sgi_coa_attachment_ids)

    def action_sgi_view_coa(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "COA de %s" % self.name,
            'res_model': 'ir.attachment',
            'view_mode': 'list,form',
            'domain': [('id', 'in', self.picking_ids.sgi_coa_attachment_ids.ids)],
            'context': {'create': False},
        }


class SgiCoaAttachWizard(models.TransientModel):
    _name = 'sgi.coa.attach.wizard'
    _description = "Adjuntar COA a la entrega"

    picking_id = fields.Many2one('stock.picking', string="Entrega", required=True)
    attachment_ids = fields.Many2many(
        'ir.attachment', 'sgi_coa_wizard_attachment_rel', 'wizard_id', 'attachment_id',
        string="COA (PDF)", help="Uno por producto. Se acepta el nombre que ya usa el laboratorio.")
    recipient_ids = fields.Many2many(
        'res.partner', 'sgi_coa_wizard_recipient_rel', 'wizard_id', 'partner_id',
        string="Destinatarios")
    send = fields.Boolean(string="Enviar al cliente", default=True)

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        picking = self.env['stock.picking'].browse(res.get('picking_id'))
        if picking and 'recipient_ids' in fields_list:
            res['recipient_ids'] = [(6, 0, picking._sgi_coa_recipients().ids)]
        return res

    def action_confirm(self):
        self.ensure_one()
        if not self.attachment_ids:
            raise UserError("Adjunta al menos un PDF del COA.")
        if self.send and not self.recipient_ids:
            raise UserError("Para enviarlo al cliente, indica al menos un destinatario.")
        picking = self.picking_id
        picking._sgi_coa_register(self.attachment_ids)
        if self.send:
            picking._sgi_coa_send(self.attachment_ids, self.recipient_ids)
        else:
            picking.message_post(
                body="COA adjunto (no enviado al cliente).",
                attachment_ids=self.attachment_ids.ids)
        return {'type': 'ir.actions.act_window_close'}


class SgiCoaExceptionWizard(models.TransientModel):
    _name = 'sgi.coa.exception.wizard'
    _description = "Validar salida sin COA (Jefe de Calidad)"

    picking_ids = fields.Many2many('stock.picking', string="Salidas")
    reason = fields.Text(string="Motivo", required=True)

    def action_confirm(self):
        self.ensure_one()
        if not (self.reason or '').strip():
            raise UserError("Escribe el motivo de la excepción.")
        if not self.picking_ids._sgi_user_is_quality_head():
            raise UserError("Solo el Jefe de Calidad puede validar una salida sin COA.")
        for picking in self.picking_ids:
            picking.message_post(body="Validada sin COA por %s. Motivo: %s" % (
                self.env.user.name, self.reason.strip()))
        return self.picking_ids.with_context(
            sgi_coa_exception_reason=self.reason.strip()).button_validate()


class SgiCoaInbox(models.Model):
    """Correos al buzón «COA»: cada PDF se liga a su salida por el nombre del
    archivo. Lo que no se liga queda aquí para que Calidad lo asigne."""
    _name = 'sgi.coa.inbox'
    _description = "COA recibido por correo"
    _inherit = ['mail.thread']
    _order = 'id desc'

    name = fields.Char(string="Asunto", required=True, default="COA")
    email_from = fields.Char(string="Remitente")
    state = fields.Selection([
        ('sin_ligar', "COA sin ligar"),
        ('ligado', "Ligado"),
    ], string="Estado", default='sin_ligar', required=True, tracking=True, index=True)
    picking_ids = fields.Many2many(
        'stock.picking', 'sgi_coa_inbox_picking_rel', 'inbox_id', 'picking_id',
        string="Salidas ligadas", readonly=True)
    picking_id = fields.Many2one(
        'stock.picking', string="Asignar a la salida",
        domain=[('picking_type_code', '=', 'outgoing')],
        help="Para los que no se ligaron solos: la salida a la que pertenecen.")
    result = fields.Text(string="Resultado", readonly=True)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company)

    @api.model
    def message_new(self, msg_dict, custom_values=None):
        values = dict(custom_values or {})
        values.setdefault('name', msg_dict.get('subject') or "COA")
        values.setdefault('email_from', msg_dict.get('email_from'))
        return super().message_new(msg_dict, custom_values=values)

    def _message_post_after_hook(self, message, msg_vals):
        res = super()._message_post_after_hook(message, msg_vals)
        # Los adjuntos del correo entrante llegan con el mensaje, no con
        # message_new: aquí ya existen.
        if message.message_type == 'email' and message.attachment_ids:
            self._sgi_link_attachments(message.attachment_ids, message.date, message.author_id)
        return res

    def _sgi_find_picking(self, filename):
        """Factura → pedido (invoice_origin) → salida hecha con ese producto,
        la más cercana a la fecha de la factura."""
        code, invoice_name = sgi_parse_coa_filename(filename)
        if not code:
            return None, "«%s»: el nombre no es «<producto> <factura>.pdf»." % filename
        invoice = self.env['account.move'].search([
            ('name', '=', invoice_name), ('move_type', '=', 'out_invoice')], limit=1)
        if not invoice:
            return None, "«%s»: no existe la factura %s." % (filename, invoice_name)
        origins = [o.strip() for o in (invoice.invoice_origin or '').split(',') if o.strip()]
        orders = self.env['sale.order'].search([('name', 'in', origins)])
        if not orders:
            return None, "«%s»: la factura %s no trae pedido de origen." % (
                filename, invoice_name)
        product = self.env['product.product'].with_context(active_test=False).search(
            [('default_code', '=', code)], limit=1)
        if not product:
            return None, "«%s»: no existe el producto %s." % (filename, code)
        pickings = orders.picking_ids.filtered(
            lambda p: p.state == 'done' and p.picking_type_code == 'outgoing'
            and product in p.move_ids.product_id)
        if not pickings:
            return None, "«%s»: el pedido %s no tiene salida hecha de %s." % (
                filename, ", ".join(orders.mapped('name')), code)
        ref = invoice.invoice_date or fields.Date.context_today(self)
        picking = min(pickings, key=lambda p: abs(
            (fields.Datetime.to_datetime(p.date_done).date() - ref).days))
        return picking, "«%s» → %s (%s)." % (filename, picking.name, invoice_name)

    def _sgi_link_attachments(self, attachments, when=None, author=None):
        for inbox in self:
            lines, all_linked = [], True
            for att in attachments.filtered(lambda a: (a.name or '').lower().endswith('.pdf')):
                picking, line = inbox._sgi_find_picking(att.name)
                lines.append(line)
                if not picking:
                    all_linked = False
                    continue
                copy = att.sudo().copy({'res_model': 'stock.picking', 'res_id': picking.id})
                user = author.user_ids[:1] if author else self.env['res.users']
                picking._sgi_coa_register(copy, sent=True, when=when, user=user or None)
                picking.message_post(body="COA recibido por correo: %s" % att.name,
                                     attachment_ids=copy.ids)
                inbox.picking_ids = [(4, picking.id)]
            if not lines:
                lines, all_linked = ["El correo no trae PDF."], False
            inbox.write({'result': "\n".join(lines),
                         'state': 'ligado' if all_linked else 'sin_ligar'})

    def action_link_manual(self):
        """Calidad asigna la salida: todos los PDF del correo van a ella."""
        for inbox in self:
            if not inbox.picking_id:
                raise UserError("Elige la salida a la que pertenece el COA.")
            pdfs = self.env['ir.attachment'].search([
                ('res_model', '=', self._name), ('res_id', '=', inbox.id)]).filtered(
                lambda a: (a.name or '').lower().endswith('.pdf'))
            if not pdfs:
                raise UserError("El correo no trae PDF que ligar.")
            copies = self.env['ir.attachment']
            for att in pdfs:
                copies |= att.sudo().copy({'res_model': 'stock.picking',
                                           'res_id': inbox.picking_id.id})
            inbox.picking_id._sgi_coa_register(copies, sent=True)
            inbox.picking_id.message_post(
                body="COA ligado a mano desde el buzón por %s." % self.env.user.name,
                attachment_ids=copies.ids)
            inbox.write({'picking_ids': [(4, inbox.picking_id.id)], 'state': 'ligado'})
        return True
