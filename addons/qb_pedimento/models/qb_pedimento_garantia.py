# -*- coding: utf-8 -*-
"""Garantías navieras, depósitos y cuenta aduanera de garantía, hasta que se
devuelven. Al 2026-09-15 SISA debía $427,359 en garantías (hilo 73903) y los
depósitos se acumulaban en la cuenta 184.03.0004 sin recuperarse."""
from odoo import _, api, fields, models

TIPOS = [
    ('naviera', 'Garantía naviera (contenedor)'),
    ('cuenta_aduanera', 'Cuenta aduanera de garantía'),
    ('deposito', 'Depósito a la agencia'),
    ('otro', 'Otro'),
]


class QbPedimentoGarantia(models.Model):
    _name = 'qb.pedimento.garantia'
    _description = 'Garantía de comercio exterior'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'fecha_pago desc, id desc'

    name = fields.Char('Concepto', required=True)
    tipo = fields.Selection(TIPOS, required=True, default='naviera', tracking=True)
    pedimento_id = fields.Many2one('qb.pedimento', 'Pedimento', index=True, ondelete='set null')
    referencia = fields.Char('Referencia', help='Referencia de la agencia o contenedor, si no hay pedimento.')
    partner_id = fields.Many2one('res.partner', 'Quién la tiene', required=True, tracking=True,
                                 help='Naviera, agencia o banco que debe devolverla.')
    company_id = fields.Many2one('res.company', required=True, default=lambda s: s.env.company, index=True)
    currency_id = fields.Many2one('res.currency', 'Moneda', required=True, default=lambda s: s.env.company.currency_id)
    importe = fields.Monetary(required=True, tracking=True)
    fecha_pago = fields.Date('Fecha de pago', required=True, default=fields.Date.context_today, tracking=True)
    devuelto = fields.Monetary('Devuelto', tracking=True)
    fecha_devolucion = fields.Date('Fecha de devolución', tracking=True)
    perdida = fields.Boolean('Se da por perdida', tracking=True,
                             help='No se va a recuperar (aplicada a demoras, castigada).')
    pendiente = fields.Monetary(compute='_compute_state', store=True)
    pendiente_mxn = fields.Monetary(
        'Pendiente (MXN)', compute='_compute_state', store=True, currency_field='company_currency_id')
    company_currency_id = fields.Many2one(related='company_id.currency_id', string='Moneda de la compañía')
    state = fields.Selection([
        ('pendiente', 'Sin devolver'),
        ('parcial', 'Devuelta en parte'),
        ('devuelta', 'Devuelta'),
        ('perdida', 'Perdida'),
    ], compute='_compute_state', store=True, tracking=True)
    dias = fields.Integer('Días sin devolver', compute='_compute_dias')
    user_id = fields.Many2one(
        'res.users', 'Responsable', compute='_compute_user_id', store=True, readonly=False)

    @api.depends('importe', 'devuelto', 'perdida', 'currency_id', 'fecha_pago')
    def _compute_state(self):
        for rec in self:
            pendiente = max(rec.importe - rec.devuelto, 0.0)
            if rec.perdida:
                rec.state, pendiente = 'perdida', 0.0
            elif rec.currency_id.is_zero(pendiente):
                rec.state = 'devuelta'
            elif rec.devuelto:
                rec.state = 'parcial'
            else:
                rec.state = 'pendiente'
            rec.pendiente = pendiente
            company_currency = rec.company_id.currency_id
            rec.pendiente_mxn = rec.currency_id._convert(
                pendiente, company_currency, rec.company_id, rec.fecha_pago or fields.Date.today()
            ) if rec.currency_id and company_currency and rec.currency_id != company_currency else pendiente

    def _compute_dias(self):
        hoy = fields.Date.context_today(self)
        for rec in self:
            fin = rec.fecha_devolucion if rec.state == 'devuelta' else hoy
            rec.dias = (fin - rec.fecha_pago).days if rec.fecha_pago and fin else 0

    @api.depends('pedimento_id.user_id')
    def _compute_user_id(self):
        for rec in self:
            rec.user_id = rec.pedimento_id.user_id or rec.user_id or rec.company_id.qb_pedimento_responsable_id

    def _avisar(self, company):
        """Una actividad abierta por garantía mientras no se devuelva."""
        self.ensure_one()
        resumen = '[Pedimento] %s' % _('Garantía sin devolver')
        user = self.user_id or company.qb_pedimento_responsable_id
        if not user or self.activity_ids.filtered(lambda a: a.summary == resumen):
            return False
        self.activity_schedule('mail.mail_activity_data_todo', summary=resumen, user_id=user.id, note=_(
            '%(p)s debe %(m)s %(c)s desde hace %(d)s días. Pedir la devolución o marcarla perdida.',
            p=self.partner_id.display_name, m='{:,.2f}'.format(self.pendiente), c=self.currency_id.name,
            d=self.dias))
        return True

    def write(self, vals):
        res = super().write(vals)
        cerradas = self.filtered(lambda g: g.state in ('devuelta', 'perdida'))
        cerradas.activity_ids.filtered(lambda a: a.summary and a.summary.startswith('[Pedimento]')).action_feedback(
            feedback=_('Garantía cerrada.'))
        return res
