"""Solicitud de alta de proveedor que llega del sitio web.

Guarda los datos y los documentos (constancia fiscal, opinión de cumplimiento,
carátula bancaria) para que Compras los revise. Aprobarla crea o liga el
contacto del proveedor; los datos bancarios **no** se copian solos a la ficha:
los captura Cuentas por pagar después de validar la carátula.
"""
from odoo import _, api, fields, models
from odoo.exceptions import UserError

from .cfdi import limpiar_rfc

GIROS = [
    ('hilo', 'Hilos y fibras'),
    ('quimicos', 'Químicos y colorantes'),
    ('refacciones', 'Refacciones y mantenimiento'),
    ('empaque', 'Empaque'),
    ('transporte', 'Transporte y logística'),
    ('servicios', 'Servicios'),
    ('otro', 'Otro'),
]


class QbProveedorAlta(models.Model):
    _name = 'qb.proveedor.alta'
    _description = 'Solicitud de alta de proveedor'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'

    name = fields.Char(string='Razón social', required=True, tracking=True)
    rfc = fields.Char(string='RFC', required=True, index=True, tracking=True)
    state = fields.Selection([
        ('nueva', 'Nueva'),
        ('revision', 'En revisión'),
        ('aprobada', 'Aprobada'),
        ('rechazada', 'Rechazada'),
    ], string='Estado', default='nueva', required=True, tracking=True, index=True)
    company_id = fields.Many2one('res.company', string='Compañía', required=True,
                                 default=lambda self: self.env.company)
    giro = fields.Selection(GIROS, string='Qué ofrece', required=True)
    descripcion = fields.Text(string='Productos o servicios')
    contacto = fields.Char(string='Contacto', required=True)
    puesto = fields.Char(string='Puesto')
    email = fields.Char(string='Correo', required=True)
    telefono = fields.Char(string='Teléfono')
    sitio_web = fields.Char(string='Sitio web')
    ciudad = fields.Char(string='Ciudad y estado')
    banco = fields.Char(string='Banco')
    clabe = fields.Char(string='CLABE')
    titular = fields.Char(string='Titular de la cuenta')
    moneda_cuenta = fields.Selection([('MXN', 'Pesos'), ('USD', 'Dólares')], string='Moneda de la cuenta', default='MXN')
    constancia = fields.Binary(string='Constancia de situación fiscal', attachment=True)
    constancia_nombre = fields.Char()
    opinion = fields.Binary(string='Opinión de cumplimiento', attachment=True)
    opinion_nombre = fields.Char()
    caratula = fields.Binary(string='Carátula bancaria', attachment=True)
    caratula_nombre = fields.Char()
    certificados = fields.Binary(string='Certificados', attachment=True,
                                 help='Certificados de calidad, ambientales o de producto (opcional).')
    certificados_nombre = fields.Char()
    acepta_codigo = fields.Boolean(string='Aceptó el código de conducta', readonly=True)
    acepta_fecha = fields.Datetime(string='Aceptado el', readonly=True)
    acepta_ip = fields.Char(string='Aceptado desde (IP)', readonly=True)
    partner_id = fields.Many2one('res.partner', string='Contacto del proveedor', copy=False, tracking=True)
    partner_existente_id = fields.Many2one('res.partner', string='Ya existe con ese RFC',
                                           compute='_compute_partner_existente')
    motivo_rechazo = fields.Char(string='Motivo del rechazo', tracking=True)

    @api.depends('rfc')
    def _compute_partner_existente(self):
        for rec in self:
            rfc = limpiar_rfc(rec.rfc)
            rec.partner_existente_id = rfc and self.env['res.partner'].search(
                [('vat', 'in', (rfc, 'MX' + rfc)), ('parent_id', '=', False)], limit=1)

    def _avisar_compras(self):
        for rec in self:
            sitio = self.env['website'].sudo().search(
                [('qb_prov_alta_user_id', '!=', False), ('company_id', '=', rec.company_id.id)], limit=1)
            usuario = sitio.qb_prov_alta_user_id
            if usuario and not usuario.share:
                rec.activity_schedule(
                    'mail.mail_activity_data_todo', user_id=usuario.id,
                    summary=_('Alta de proveedor por revisar'),
                    note=_('%(empresa)s (%(rfc)s) solicitó darse de alta como proveedor.', empresa=rec.name, rfc=rec.rfc))

    def action_revisar(self):
        self.filtered(lambda r: r.state == 'nueva').write({'state': 'revision'})
        return True

    def action_aprobar(self):
        """Crea el contacto del proveedor (o liga el que ya existe con ese RFC)."""
        for rec in self:
            if rec.state == 'aprobada':
                continue
            socio = rec.partner_id or rec.partner_existente_id
            if not socio:
                mexico = self.env.ref('base.mx', raise_if_not_found=False)
                socio = self.env['res.partner'].create({
                    'name': rec.name, 'is_company': True, 'vat': limpiar_rfc(rec.rfc),
                    'email': rec.email, 'phone': rec.telefono, 'website': rec.sitio_web,
                    'country_id': mexico.id if mexico else False,
                    'supplier_rank': 1, 'company_id': False,
                    'comment': rec.descripcion,
                })
                self.env['res.partner'].create({
                    'name': rec.contacto, 'parent_id': socio.id, 'type': 'contact',
                    'email': rec.email, 'phone': rec.telefono, 'function': rec.puesto,
                })
            elif not socio.supplier_rank:
                socio.supplier_rank = 1
            rec.write({'partner_id': socio.id, 'state': 'aprobada'})
            rec.activity_ids.action_done()
        return True

    def action_rechazar(self):
        for rec in self:
            if not rec.motivo_rechazo:
                raise UserError(_('Escriba el motivo del rechazo.'))
            rec.state = 'rechazada'
            rec.activity_ids.action_done()
        return True

    def action_ver_contacto(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'res_model': 'res.partner',
            'res_id': (self.partner_id or self.partner_existente_id).id, 'view_mode': 'form',
        }
