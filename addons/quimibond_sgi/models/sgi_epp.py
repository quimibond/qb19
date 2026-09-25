# -*- coding: utf-8 -*-
"""EPP del puesto con responsiva (PER-2, 19.0.48.0.0).

El EPP que exige cada puesto vive en `hr.job.sgi_epp_required` (sustituye
F-P-S03-01). La **responsiva de entrega** (F-P-S03-02) es un registro por
entrega, ligado al empleado: quién entregó qué y cuándo, y la firma del
propio empleado desde Odoo («Recibí y me comprometo a usarlo»). El candado
de identidad vive en write(), como en el acuse de lectura: solo el propio
empleado (o el Jefe MAST) firma.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError


class SgiEppDelivery(models.Model):
    _name = 'sgi.epp.delivery'
    _description = "Responsiva de entrega de EPP (S03-02)"
    _inherit = ['mail.thread']
    _order = 'date desc, id desc'

    name = fields.Char(string="Folio", readonly=True, copy=False, default="Nuevo")
    employee_id = fields.Many2one(
        'hr.employee', string="Empleado", required=True, index=True, ondelete='restrict')
    # Almacenado: hr.employee no es legible por cualquier usuario interno en
    # Odoo 19; el candado de firma y can_sign leen este campo, no al empleado.
    user_id = fields.Many2one(related='employee_id.user_id', string="Usuario", store=True)
    job_id = fields.Many2one(
        'hr.job', string="Puesto al entregar", compute='_compute_job_id', store=True, readonly=False)
    date = fields.Date(string="Fecha de entrega", default=fields.Date.context_today, required=True)
    items = fields.Text(
        string="EPP entregado", required=True,
        help="Se propone el EPP requerido del puesto; ajusta lo que realmente se entregó.")
    delivered_by_id = fields.Many2one(
        'res.users', string="Entregó", default=lambda self: self.env.user, required=True)
    note = fields.Text(string="Observaciones")
    state = fields.Selection([
        ('entregada', "Entregada, sin firmar"),
        ('firmada', "Firmada por el empleado"),
    ], string="Estado", default='entregada', required=True, tracking=True)
    signed_date = fields.Datetime(string="Firmada el", readonly=True)
    can_sign = fields.Boolean(compute='_compute_can_sign')

    _SGI_SIGN_FIELDS = {'state', 'signed_date'}

    @api.depends('employee_id')
    def _compute_job_id(self):
        for rec in self:
            if rec.employee_id and not rec.job_id:
                rec.job_id = rec.employee_id.job_id

    @api.depends('user_id', 'state')
    @api.depends_context('uid')
    def _compute_can_sign(self):
        for rec in self:
            rec.can_sign = bool(rec.state == 'entregada' and rec.user_id
                                and rec.user_id == self.env.user)

    @api.onchange('employee_id')
    def _onchange_employee_id(self):
        if self.employee_id and not self.items:
            self.items = self.employee_id.job_id.sgi_epp_required or False

    def _sgi_check_can_sign(self):
        if self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return
        for rec in self:
            if not rec.user_id or rec.user_id != self.env.user:
                raise UserError(
                    "Solo el propio empleado (o el Jefe de MAST) puede firmar la "
                    "responsiva de EPP de %s." % rec.sudo().employee_id.name)

    @api.model_create_multi
    def create(self, vals_list):
        Seq = self.env['ir.sequence']
        for vals in vals_list:
            if not vals.get('name') or vals['name'] == "Nuevo":
                vals['name'] = Seq.next_by_code('sgi.epp.delivery') or "Nuevo"
            if not vals.get('items') and vals.get('employee_id'):
                emp = self.env['hr.employee'].sudo().browse(vals['employee_id'])
                vals['items'] = emp.job_id.sgi_epp_required or False
        records = super().create(vals_list)
        records.filtered(lambda r: r.state != 'entregada' or r.signed_date)._sgi_check_can_sign()
        return records

    def write(self, vals):
        if self._SGI_SIGN_FIELDS & set(vals):
            self._sgi_check_can_sign()
        if 'items' in vals or 'employee_id' in vals:
            signed = self.filtered(lambda r: r.state == 'firmada')
            if signed and not self.env.su:
                raise UserError("Una responsiva firmada no se modifica: haz una entrega nueva.")
        return super().write(vals)

    def action_sign(self):
        """El empleado firma desde su ficha o desde Mi procedimiento."""
        for rec in self:
            if rec.state == 'firmada':
                continue
            rec.write({'state': 'firmada', 'signed_date': fields.Datetime.now()})
            rec.message_post(body="Responsiva firmada por %s: recibí el EPP y me comprometo a usarlo." % self.env.user.name)
        return True


class HrEmployeeEpp(models.Model):
    _inherit = 'hr.employee'

    sgi_epp_required = fields.Text(related='job_id.sgi_epp_required', string="EPP requerido por el puesto")
    sgi_epp_delivery_ids = fields.One2many('sgi.epp.delivery', 'employee_id', string="Responsivas de EPP")
    sgi_epp_delivery_count = fields.Integer(compute='_compute_sgi_epp_counts')
    sgi_epp_pending_count = fields.Integer(string="Responsivas sin firmar", compute='_compute_sgi_epp_counts')

    def _compute_sgi_epp_counts(self):
        Delivery = self.env['sgi.epp.delivery']
        for emp in self:
            emp.sgi_epp_delivery_count = Delivery.search_count([('employee_id', '=', emp.id)]) if emp.id else 0
            emp.sgi_epp_pending_count = Delivery.search_count(
                [('employee_id', '=', emp.id), ('state', '=', 'entregada')]) if emp.id else 0

    def action_sgi_epp_deliveries(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "Responsivas de EPP — %s" % self.name,
            'res_model': 'sgi.epp.delivery', 'view_mode': 'list,form',
            'domain': [('employee_id', '=', self.id)],
            'context': {'default_employee_id': self.id},
        }

    def action_sgi_deliver_epp(self):
        """«Entregar EPP»: responsiva nueva con el EPP del puesto propuesto."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "Entregar EPP",
            'res_model': 'sgi.epp.delivery', 'view_mode': 'form', 'target': 'current',
            'context': {'default_employee_id': self.id,
                        'default_items': self.job_id.sgi_epp_required or False},
        }
