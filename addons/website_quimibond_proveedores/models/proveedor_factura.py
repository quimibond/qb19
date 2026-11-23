"""Factura que un proveedor sube en el portal contra una orden de compra.

Es la **recepción** del PDF y el XML, no la factura contable: no crea asientos.
Cuentas por pagar la revisa y con un botón crea (o liga) la factura de
proveedor. El portal le muestra al proveedor en qué va.
"""
import base64

from odoo import _, api, fields, models
from odoo.exceptions import UserError

from .cfdi import CfdiInvalido, leer_cfdi, limpiar_rfc

MAX_PDF = 5 * 1024 * 1024
TOLERANCIA = 1.0


class QbProveedorFactura(models.Model):
    _name = 'qb.proveedor.factura'
    _description = 'Factura recibida en el portal de proveedores'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'

    name = fields.Char(string='Factura', required=True, readonly=True)
    purchase_id = fields.Many2one('purchase.order', string='Orden de compra', required=True, readonly=True,
                                  index=True, ondelete='restrict')
    partner_id = fields.Many2one('res.partner', string='Proveedor', required=True, readonly=True, index=True)
    company_id = fields.Many2one('res.company', string='Compañía', required=True, readonly=True)
    usuario_id = fields.Many2one('res.users', string='La subió', readonly=True)
    state = fields.Selection([
        ('recibida', 'Por revisar'),
        ('aceptada', 'Registrada'),
        ('rechazada', 'Rechazada'),
    ], string='Estado', default='recibida', required=True, tracking=True, index=True)
    uuid = fields.Char(string='Folio fiscal', required=True, readonly=True, index=True)
    emisor_rfc = fields.Char(string='RFC del emisor', readonly=True)
    emisor_nombre = fields.Char(string='Emisor', readonly=True)
    receptor_rfc = fields.Char(string='RFC del receptor', readonly=True)
    fecha = fields.Date(string='Fecha de la factura', readonly=True)
    moneda = fields.Char(string='Moneda del CFDI', readonly=True)
    metodo_pago = fields.Char(string='Método de pago', readonly=True)
    subtotal = fields.Float(string='Subtotal del CFDI', digits=(16, 2), readonly=True)
    total = fields.Float(string='Total del CFDI', digits=(16, 2), readonly=True)
    por_facturar = fields.Float(string='Recibido sin facturar', digits=(16, 2), readonly=True,
                                help='Importe sin impuestos de lo recibido y aún no facturado en la orden, '
                                     'al momento en que el proveedor subió la factura.')
    cuadra = fields.Boolean(string='Coincide con lo recibido', readonly=True)
    avisos = fields.Text(string='Avisos al proveedor', readonly=True,
                         help='Lo que el portal le dijo al proveedor al recibir la factura.')
    notas = fields.Text(string='Avisos internos', readonly=True,
                        help='Lo que conviene revisar antes de registrar la factura. El proveedor no lo ve.')
    sat_estado = fields.Selection([
        ('vigente', 'Vigente en el SAT'),
        ('cancelado', 'Cancelada en el SAT'),
        ('pendiente', 'Aún no aparece en el SAT'),
        ('sin_dato', 'Sin verificar'),
    ], string='Estado en el SAT', compute='_compute_sat_estado')
    xml_file = fields.Binary(string='XML', attachment=True, readonly=True)
    xml_nombre = fields.Char(string='Nombre del XML', readonly=True)
    pdf_file = fields.Binary(string='PDF', attachment=True, readonly=True)
    pdf_nombre = fields.Char(string='Nombre del PDF', readonly=True)
    move_id = fields.Many2one('account.move', string='Factura de proveedor', readonly=True, copy=False, index=True)
    motivo_rechazo = fields.Char(string='Motivo del rechazo', tracking=True,
                                 help='Se le muestra al proveedor en el portal.')

    # -- SAT -------------------------------------------------------------------

    @api.model
    def _estado_sat(self, uuid):
        """Estado del folio en los CFDI del SAT que ya están en la base, si el módulo del SAT está instalado."""
        if 'sat.cfdi' not in self.env or not uuid:
            return 'sin_dato'
        cfdi = self.env['sat.cfdi'].sudo().search([('uuid', 'in', (uuid.lower(), uuid.upper()))], limit=1)
        if not cfdi:
            return 'pendiente'
        return 'cancelado' if cfdi.estado_sat == 'cancelado' else 'vigente'

    def _compute_sat_estado(self):
        for rec in self:
            rec.sat_estado = self._estado_sat(rec.uuid)

    # -- recepción desde el portal ------------------------------------------

    @api.model
    def _por_facturar(self, order):
        """Importe sin impuestos de lo recibido y no facturado de la orden."""
        total = 0.0
        for linea in order.order_line:
            if linea.display_type:
                continue
            cantidad = max(linea.qty_received - linea.qty_invoiced, 0.0)
            total += cantidad * linea.price_unit * (1 - (linea.discount or 0.0) / 100.0)
        return order.currency_id.round(total)

    @api.model
    def _recibir(self, order, xml, xml_nombre, pdf, pdf_nombre, usuario):
        """Valida y guarda la factura que sube un proveedor. Devuelve el registro.

        Lanza `CfdiInvalido` (mensaje para el proveedor) si no se puede recibir.
        Se llama con sudo desde el controlador, que ya comprobó que la orden es
        de la empresa del usuario.
        """
        order = order.sudo()
        if not pdf or pdf[:5] != b'%PDF-':
            raise CfdiInvalido('Suba también la factura en PDF.')
        if len(pdf) > MAX_PDF:
            raise CfdiInvalido('El PDF pesa más de 5 MB.')
        datos = leer_cfdi(xml)
        if datos['tipo'] != 'I':
            raise CfdiInvalido('El XML no es una factura (es un comprobante de tipo %s). '
                               'Las notas de crédito y los complementos de pago se envían a Cuentas por pagar.'
                               % (datos['tipo'] or 'desconocido'))
        proveedor = order.partner_id.commercial_partner_id
        rfc_proveedor = limpiar_rfc(proveedor.vat)
        rfc_empresa = limpiar_rfc(order.company_id.vat)
        if rfc_empresa and datos['receptor_rfc'] != rfc_empresa:
            raise CfdiInvalido('La factura no está a nombre de %s (RFC %s): el receptor es %s.'
                               % (order.company_id.name, rfc_empresa, datos['receptor_rfc'] or 'otro'))
        if rfc_proveedor and datos['emisor_rfc'] != rfc_proveedor:
            raise CfdiInvalido('El RFC que emite la factura (%s) no es el de su empresa (%s).'
                               % (datos['emisor_rfc'], rfc_proveedor))
        if self.sudo().search_count([('uuid', '=', datos['uuid']), ('state', '!=', 'rechazada')]):
            raise CfdiInvalido('Esta factura ya se había subido (folio fiscal %s).' % datos['uuid'])
        sat = self._estado_sat(datos['uuid'])
        if sat == 'cancelado':
            raise CfdiInvalido('Esta factura aparece cancelada en el SAT. Suba una factura vigente.')

        avisos, notas = [], []
        registrada = self._buscar_factura_contable(datos['uuid'], proveedor)
        por_facturar = self._por_facturar(order)
        neto = datos['subtotal'] - datos['descuento']
        cuadra = False
        if datos['moneda'] != (order.currency_id.name or '').upper():
            avisos.append('La factura está en %s y la orden en %s.' % (datos['moneda'], order.currency_id.name))
        elif not por_facturar and not registrada:
            avisos.append('Todavía no registramos la recepción de esta orden. '
                          'La factura queda en espera hasta que se reciba la mercancía o el servicio.')
        elif abs(neto - por_facturar) <= max(TOLERANCIA, 0.001 * por_facturar):
            cuadra = True
        elif not registrada:
            avisos.append('El importe de la factura (%s) no coincide con lo recibido pendiente de facturar (%s). '
                          'Cuentas por pagar la revisará.' % (self._importe(neto, order), self._importe(por_facturar, order)))
        if not rfc_proveedor:
            notas.append('El proveedor no tiene RFC en su ficha: no se pudo validar el emisor (%s).' % datos['emisor_rfc'])
        if sat == 'pendiente':
            notas.append('El folio fiscal todavía no aparece entre los CFDI descargados del SAT.')
        if 'sat_69b_status' in proveedor._fields and proveedor.sat_69b_status in ('presumed', 'definitive'):
            notas.append('El proveedor está en la lista 69-B del SAT (%s).' % proveedor.sat_69b_status)

        nombre = '-'.join(p for p in (datos['serie'], datos['folio']) if p) or datos['uuid'][:8]
        factura = self.sudo().create({
            'name': nombre,
            'purchase_id': order.id,
            'partner_id': proveedor.id,
            'company_id': order.company_id.id,
            'usuario_id': usuario.id,
            'uuid': datos['uuid'],
            'emisor_rfc': datos['emisor_rfc'],
            'emisor_nombre': datos['emisor_nombre'],
            'receptor_rfc': datos['receptor_rfc'],
            'fecha': datos['fecha'] or False,
            'moneda': datos['moneda'],
            'metodo_pago': datos['metodo_pago'],
            'subtotal': datos['subtotal'],
            'total': datos['total'],
            'por_facturar': por_facturar,
            'cuadra': cuadra,
            'avisos': '\n'.join(avisos) or False,
            'notas': '\n'.join(notas) or False,
            'xml_file': base64.b64encode(xml),
            'xml_nombre': (xml_nombre or '%s.xml' % datos['uuid'])[:120],
            'pdf_file': base64.b64encode(pdf),
            'pdf_nombre': (pdf_nombre or '%s.pdf' % datos['uuid'])[:120],
        })
        if registrada:
            factura.write({'move_id': registrada.id, 'state': 'aceptada'})
        else:
            factura._avisar_cxp()
        return factura

    @api.model
    def _importe(self, monto, order):
        return '%s %s' % ('{:,.2f}'.format(monto), order.currency_id.name)

    def _avisar_cxp(self):
        """Actividad para Cuentas por pagar (o el comprador de la orden)."""
        for rec in self:
            sitio = self.env['website'].sudo().search(
                [('qb_prov_cxp_user_id', '!=', False), ('company_id', '=', rec.company_id.id)], limit=1)
            usuario = sitio.qb_prov_cxp_user_id or rec.purchase_id.user_id
            if not usuario or usuario.share:
                continue
            rec.activity_schedule(
                'mail.mail_activity_data_todo', user_id=usuario.id,
                summary=_('Factura de proveedor por revisar'),
                note=_('%(proveedor)s subió la factura %(factura)s contra %(orden)s.',
                       proveedor=rec.partner_id.name, factura=rec.name, orden=rec.purchase_id.name))

    # -- liga con la factura contable ------------------------------------------

    @api.model
    def _buscar_factura_contable(self, uuid, proveedor):
        """Factura de proveedor ya registrada con ese folio fiscal, o vacío."""
        Move = self.env['account.move'].sudo()
        if not uuid:
            return Move
        dominio = [('move_type', '=', 'in_invoice'), ('state', '!=', 'cancel'),
                   ('partner_id', 'child_of', proveedor.id)]
        if 'sat.cfdi' in self.env:
            cfdi = self.env['sat.cfdi'].sudo().search(
                [('uuid', 'in', (uuid.lower(), uuid.upper())), ('move_id', '!=', False)], limit=1)
            if cfdi and cfdi.move_id.filtered_domain(dominio):
                return cfdi.move_id
        if 'l10n_mx_edi_cfdi_uuid' in Move._fields:
            return Move.search(dominio + [('l10n_mx_edi_cfdi_uuid', '=ilike', uuid)], limit=1)
        return Move

    def _ligar(self):
        """Liga las recibidas con la factura contable que ya tenga su folio fiscal. Devuelve las ligadas."""
        ligadas = self.browse()
        for rec in self.filtered(lambda r: r.state == 'recibida' and not r.move_id):
            move = self._buscar_factura_contable(rec.uuid, rec.partner_id)
            if move:
                rec.write({'move_id': move.id, 'state': 'aceptada'})
                rec.activity_ids.action_done()
                ligadas |= rec
        return ligadas

    @api.model
    def _cron_ligar(self):
        self.search([('state', '=', 'recibida'), ('move_id', '=', False)])._ligar()

    def action_ligar(self):
        ligadas = self._ligar()
        if not ligadas:
            raise UserError(_('No hay una factura de proveedor registrada con ese folio fiscal.'))
        return True

    def action_crear_factura(self):
        """Crea la factura de proveedor en borrador desde la orden y le adjunta el PDF y el XML."""
        self.ensure_one()
        if self.state == 'rechazada':
            raise UserError(_('La factura está rechazada.'))
        if not self.move_id and not self._ligar():
            orden = self.purchase_id
            antes = orden.invoice_ids
            orden.action_create_invoice()
            nueva = (orden.invoice_ids - antes)[:1]
            if not nueva:
                raise UserError(_('La orden no tiene nada por facturar.'))
            nueva.write({'ref': self.name, 'invoice_date': self.fecha or fields.Date.context_today(self)})
            for archivo, nombre in ((self.xml_file, self.xml_nombre), (self.pdf_file, self.pdf_nombre)):
                if archivo:
                    self.env['ir.attachment'].create({
                        'name': nombre, 'datas': archivo, 'res_model': 'account.move', 'res_id': nueva.id,
                    })
            nueva.message_post(body=_('Creada desde la factura %s que el proveedor subió en el portal (folio fiscal %s).',
                                      self.name, self.uuid))
            self.write({'move_id': nueva.id, 'state': 'aceptada'})
            self.activity_ids.action_done()
        return {
            'type': 'ir.actions.act_window', 'res_model': 'account.move', 'res_id': self.move_id.id,
            'view_mode': 'form',
        }

    def action_rechazar(self):
        for rec in self:
            if not rec.motivo_rechazo:
                raise UserError(_('Escriba el motivo del rechazo: el proveedor lo ve en el portal.'))
            if rec.move_id:
                raise UserError(_('Ya tiene una factura de proveedor ligada.'))
            rec.state = 'rechazada'
            rec.activity_ids.action_done()
            if rec.usuario_id.partner_id.email:
                rec.message_notify(
                    partner_ids=rec.usuario_id.partner_id.ids,
                    subject=_('Factura %s rechazada', rec.name),
                    body=_('La factura %(factura)s de la orden %(orden)s fue rechazada: %(motivo)s. '
                           'Puede subir la factura corregida en el portal de proveedores.',
                           factura=rec.name, orden=rec.purchase_id.name, motivo=rec.motivo_rechazo),
                    email_layout_xmlid='mail.mail_notification_light')
        return True

    def action_reabrir(self):
        self.filtered(lambda r: r.state == 'rechazada').write({'state': 'recibida'})
        return True
