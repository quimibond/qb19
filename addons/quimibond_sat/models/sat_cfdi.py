# -*- coding: utf-8 -*-
"""CFDI tal como lo tiene el SAT (vía Syntage), ligado a su factura de Odoo.

El cruce es por folio fiscal (UUID): primero contra l10n_mx_edi.document
(el registro que Odoo crea al timbrar o al recibir un XML), después contra
el campo l10n_mx_edi_cfdi_uuid del asiento. Si hay varios asientos con el
mismo UUID (XML capturado dos veces) gana el publicado más reciente.
"""
import logging

from odoo import _, api, fields, models

_logger = logging.getLogger(__name__)

INVOICE_TYPES = ('out_invoice', 'out_refund', 'in_invoice', 'in_refund')
GENERIC_RFCS = ('XAXX010101000', 'XEXX010101000')


def _parse_dt(value):
    """Syntage manda '2026-04-29 22:42:37' o ISO con 'T'; a datetime de Odoo."""
    if not value:
        return False
    text = str(value).strip().replace('T', ' ')[:19]
    try:
        return fields.Datetime.to_datetime(text)
    except ValueError:
        return False


def _num(value):
    try:
        return float(value) if value not in (None, '') else 0.0
    except (TypeError, ValueError):
        return 0.0


class SatCfdi(models.Model):
    _name = 'sat.cfdi'
    _description = 'CFDI en el SAT (Syntage)'
    _order = 'fecha_emision desc, id desc'

    name = fields.Char(compute='_compute_name', store=True)
    uuid = fields.Char(string='Folio fiscal (UUID)', required=True, index=True, copy=False)
    syntage_id = fields.Char(string='ID en Syntage', index=True)
    company_id = fields.Many2one('res.company', string='Compañía', required=True, index=True,
                                 default=lambda self: self.env.company)
    tipo = fields.Selection([
        ('I', 'Ingreso'), ('E', 'Egreso'), ('P', 'Pago'), ('N', 'Nómina'), ('T', 'Traslado'),
    ], string='Tipo', index=True)
    direction = fields.Selection([('issued', 'Emitido'), ('received', 'Recibido')],
                                 string='Sentido', required=True, index=True)
    estado_sat = fields.Selection([
        ('vigente', 'Vigente'), ('cancelado', 'Cancelado'), ('cancelacion_pendiente', 'Cancelación pendiente'),
    ], string='Estado en el SAT', default='vigente', required=True, index=True)
    fecha_emision = fields.Datetime(string='Emisión', index=True)
    fecha_timbrado = fields.Datetime(string='Timbrado')
    fecha_cancelacion = fields.Datetime(string='Cancelación')
    emisor_rfc = fields.Char(string='RFC emisor')
    emisor_nombre = fields.Char(string='Emisor')
    receptor_rfc = fields.Char(string='RFC receptor')
    receptor_nombre = fields.Char(string='Receptor')
    serie = fields.Char()
    folio = fields.Char()
    subtotal = fields.Float(digits=(16, 2))
    descuento = fields.Float(digits=(16, 2))
    total = fields.Float(digits=(16, 2))
    impuestos_trasladados = fields.Float(digits=(16, 2))
    impuestos_retenidos = fields.Float(digits=(16, 2))
    moneda = fields.Char(default='MXN')
    tipo_cambio = fields.Float(digits=(16, 6), default=1.0)
    metodo_pago = fields.Char(string='Método de pago')
    forma_pago = fields.Char(string='Forma de pago')
    uso_cfdi = fields.Char(string='Uso CFDI')
    emisor_blacklist = fields.Char(string='Lista 69-B emisor')
    receptor_blacklist = fields.Char(string='Lista 69-B receptor')
    raw_json = fields.Json(string='Payload Syntage')
    last_event_type = fields.Char(string='Último evento')
    last_event_at = fields.Datetime(string='Último evento el')

    # Contraparte (la otra parte del CFDI) y su partner en Odoo
    counterparty_rfc = fields.Char(string='RFC contraparte', compute='_compute_counterparty', store=True, index=True)
    counterparty_name = fields.Char(string='Contraparte', compute='_compute_counterparty', store=True)
    partner_id = fields.Many2one('res.partner', string='Contacto', compute='_compute_counterparty', store=True)

    # Cruce con Odoo
    move_id = fields.Many2one('account.move', string='Factura en Odoo', index=True, copy=False,
                              domain="[('move_type', 'in', ('out_invoice', 'out_refund', 'in_invoice', 'in_refund'))]")
    match_method = fields.Selection([
        ('uuid_document', 'UUID (documento CFDI)'),
        ('uuid_move', 'UUID (factura)'),
        ('manual', 'Manual'),
    ], string='Ligado por', copy=False)
    match_status = fields.Selection([
        ('matched', 'En Odoo'), ('solo_sat', 'Solo en el SAT'), ('ignorado', 'Ignorado'),
    ], string='Cruce', default='solo_sat', required=True, index=True, copy=False)
    ignore_reason = fields.Char(string='Motivo para ignorar')
    move_state = fields.Selection(related='move_id.state', string='Estado en Odoo')
    move_amount_total = fields.Monetary(related='move_id.amount_total', string='Total en Odoo',
                                        currency_field='move_currency_id')
    move_currency_id = fields.Many2one(related='move_id.currency_id')
    amount_diff = fields.Float(string='Diferencia', compute='_compute_issue', store=True, digits=(16, 2))
    issue = fields.Selection([
        ('ok', 'Cuadra'),
        ('solo_sat', 'Solo en el SAT'),
        ('monto', 'Monto distinto'),
        ('cancelado_sat', 'Cancelado en el SAT, publicado en Odoo'),
        ('cancelado_odoo', 'Cancelado en Odoo, vigente en el SAT'),
        ('ignorado', 'Ignorado'),
    ], string='Hallazgo', compute='_compute_issue', store=True, index=True)

    _uuid_uniq = models.Constraint('unique(uuid)', 'Ya existe un CFDI con ese folio fiscal.')

    # ── computes ───────────────────────────────────────────────────────

    @api.depends('serie', 'folio', 'uuid', 'tipo')
    def _compute_name(self):
        for rec in self:
            ref = ' '.join(p for p in (rec.serie, rec.folio) if p) or (rec.uuid or '')[:8].upper()
            rec.name = '%s %s' % (rec.tipo or 'CFDI', ref)

    @api.depends('direction', 'emisor_rfc', 'receptor_rfc', 'emisor_nombre', 'receptor_nombre', 'company_id')
    def _compute_counterparty(self):
        for rec in self:
            if rec.direction == 'issued':
                rfc, name = rec.receptor_rfc, rec.receptor_nombre
            else:
                rfc, name = rec.emisor_rfc, rec.emisor_nombre
            rec.counterparty_rfc = (rfc or '').strip().upper() or False
            rec.counterparty_name = name or False
            rec.partner_id = rec._find_partner(rec.counterparty_rfc)

    def _find_partner(self, rfc):
        Partner = self.env['res.partner'].sudo()
        if not rfc or rfc in GENERIC_RFCS:
            return Partner
        partners = Partner.search([
            ('vat', '=ilike', rfc),
            ('company_id', 'in', [False, self.company_id.id]),
        ])
        if not partners:
            return Partner
        top = partners.filtered(lambda p: not p.parent_id) or partners
        return top[0].commercial_partner_id

    @api.depends('move_id', 'move_id.state', 'move_id.amount_total', 'estado_sat', 'total', 'match_status')
    def _compute_issue(self):
        for rec in self:
            move = rec.move_id
            rec.amount_diff = (rec.total - move.amount_total) if move else 0.0
            if rec.match_status == 'ignorado':
                rec.issue = 'ignorado'
            elif not move:
                rec.issue = 'solo_sat'
            elif rec.estado_sat == 'cancelado' and move.state == 'posted':
                rec.issue = 'cancelado_sat'
            elif rec.estado_sat != 'cancelado' and move.state == 'cancel':
                rec.issue = 'cancelado_odoo'
            elif abs(rec.amount_diff) > max(1.0, 0.005 * abs(rec.total)):
                rec.issue = 'monto'
            else:
                rec.issue = 'ok'

    # ── ingesta ────────────────────────────────────────────────────────

    @api.model
    def _vals_from_syntage(self, obj, company, deleted=False):
        issuer = obj.get('issuer') or {}
        receiver = obj.get('receiver') or {}
        status = str(obj.get('status') or '').lower()
        if deleted or obj.get('canceledAt') or status in ('cancelado', 'canceled'):
            estado = 'cancelado'
        elif 'pendiente' in status:
            estado = 'cancelacion_pendiente'
        else:
            estado = 'vigente'
        rfc = (company.vat or '').strip().upper()
        if obj.get('isIssuer') is True:
            direction = 'issued'
        elif obj.get('isReceiver') is True:
            direction = 'received'
        else:
            direction = 'issued' if (issuer.get('rfc') or '').upper() == rfc else 'received'
        transferred = obj.get('transferredTaxes') or {}
        retained = obj.get('retainedTaxes') or {}
        return {
            'uuid': (obj.get('uuid') or '').strip().lower(),
            'syntage_id': obj.get('id') or obj.get('@id') or False,
            'company_id': company.id,
            'tipo': obj.get('type') if obj.get('type') in ('I', 'E', 'P', 'N', 'T') else False,
            'direction': direction,
            'estado_sat': estado,
            'fecha_emision': _parse_dt(obj.get('issuedAt')),
            'fecha_timbrado': _parse_dt(obj.get('certifiedAt')),
            'fecha_cancelacion': _parse_dt(obj.get('canceledAt')) or (fields.Datetime.now() if deleted else False),
            'emisor_rfc': (issuer.get('rfc') or '').upper() or False,
            'emisor_nombre': issuer.get('name') or False,
            'receptor_rfc': (receiver.get('rfc') or '').upper() or False,
            'receptor_nombre': receiver.get('name') or False,
            'serie': obj.get('serie') or False,
            'folio': obj.get('folio') or obj.get('internalIdentifier') or False,
            'subtotal': _num(obj.get('subtotal')),
            'descuento': _num(obj.get('discount')),
            'total': _num(obj.get('total')),
            'impuestos_trasladados': _num(transferred.get('total') if isinstance(transferred, dict) else 0),
            'impuestos_retenidos': _num(retained.get('total') if isinstance(retained, dict) else 0),
            'moneda': obj.get('currency') or 'MXN',
            'tipo_cambio': _num(obj.get('exchangeRate')) or 1.0,
            'metodo_pago': obj.get('paymentType') or False,
            'forma_pago': obj.get('paymentMethod') or False,
            'uso_cfdi': obj.get('usage') or False,
            'emisor_blacklist': issuer.get('blacklistStatus') or False,
            'receptor_blacklist': receiver.get('blacklistStatus') or False,
            'raw_json': obj,
        }

    @api.model
    def _upsert_from_syntage(self, obj, company, event_type=None):
        """Crea o actualiza el CFDI (clave: UUID) y lo cruza con Odoo."""
        vals = self._vals_from_syntage(obj, company, deleted=(event_type == 'invoice.deleted'))
        if not vals['uuid']:
            raise ValueError('CFDI sin uuid (id=%s)' % (obj.get('id') or obj.get('@id')))
        vals.update({'last_event_type': event_type or 'pull', 'last_event_at': fields.Datetime.now()})
        rec = self.sudo().search([('uuid', '=', vals['uuid'])], limit=1)
        if rec:
            rec.write(vals)
        else:
            rec = self.sudo().create(vals)
        rec._match_move()
        return rec

    # ── cruce ──────────────────────────────────────────────────────────

    @api.model
    def _uuid_sources_available(self):
        """(hay l10n_mx_edi.document, account.move tiene l10n_mx_edi_cfdi_uuid)."""
        return ('l10n_mx_edi.document' in self.env,
                'l10n_mx_edi_cfdi_uuid' in self.env['account.move']._fields)

    def _candidate_moves(self):
        """Facturas de Odoo con este UUID y por qué fuente se encontraron."""
        self.ensure_one()
        Move = self.env['account.move'].sudo()
        has_doc, has_field = self._uuid_sources_available()
        if has_doc:
            docs = self.env['l10n_mx_edi.document'].sudo().search([
                ('attachment_uuid', '=ilike', self.uuid), ('move_id', '!=', False),
            ])
            moves = docs.mapped('move_id').filtered(
                lambda m: m.move_type in INVOICE_TYPES and m.company_id == self.company_id)
            if moves:
                return moves, 'uuid_document'
        if has_field:
            moves = Move.search([
                ('l10n_mx_edi_cfdi_uuid', '=ilike', self.uuid),
                ('move_type', 'in', INVOICE_TYPES),
                ('company_id', '=', self.company_id.id),
            ])
            if moves:
                return moves, 'uuid_move'
        return Move, False

    @api.model
    def _pick_move(self, moves):
        """Entre varias facturas con el mismo UUID (XML capturado dos veces)
        gana la publicada, y entre publicadas la más reciente."""
        if not moves:
            return moves
        rank = {'posted': 0, 'draft': 1, 'cancel': 2}
        return moves.sorted(key=lambda m: (rank.get(m.state, 3), -m.id))[0]

    def _find_move(self):
        self.ensure_one()
        moves, method = self._candidate_moves()
        return self._pick_move(moves), method

    def _match_move(self):
        for rec in self:
            if rec.match_status == 'ignorado' or rec.match_method == 'manual':
                continue
            move, method = rec._find_move()
            if move:
                rec.write({'move_id': move.id, 'match_method': method, 'match_status': 'matched'})
            elif not rec.move_id:
                rec.write({'match_status': 'solo_sat', 'match_method': False})

    def write(self, vals):
        # Ligar o desligar a mano desde el formulario.
        if 'move_id' in vals and 'match_method' not in vals:
            vals = dict(vals, match_method='manual' if vals['move_id'] else False,
                        match_status='matched' if vals['move_id'] else 'solo_sat')
        return super().write(vals)

    @api.model
    def _cron_match_unmatched(self, limit=2000):
        cfdis = self.sudo().search([('match_status', '=', 'solo_sat')], limit=limit)
        cfdis._match_move()
        matched = len(cfdis.filtered(lambda c: c.match_status == 'matched'))
        _logger.info('sat.cfdi: cruce de %s pendientes, %s ligados', len(cfdis), matched)
        return matched

    @api.model
    def _cron_daily_extraction(self):
        self.env['sat.syntage.client']._run_daily_extraction()

    @api.model
    def _cron_pull_recent(self):
        self.env['sat.syntage.client']._run_pull_recent()

    # ── entrada pública para MCP / acciones de servidor ────────────────

    @api.model
    def action_pull_period(self, date_from, date_to, company_id=None, mode='pull',
                           include_retentions=False):
        """Mismo trabajo que el asistente "Traer CFDI del SAT", pero como método
        público de un modelo regular: los asistentes (transitorios) no se pueden
        exponer por MCP y así se puede lanzar una descarga o extracción desde
        fuera. Devuelve un resumen del registro de bitácora."""
        company = self.env['res.company'].browse(company_id) if company_id else self.env.company
        date_from = fields.Date.to_date(date_from)
        date_to = fields.Date.to_date(date_to)
        client = self.env['sat.syntage.client']
        if mode == 'extraction':
            log = client.request_extraction(company, date_from, date_to, include_retentions)
        else:
            log = client.pull_invoices(company, date_from, date_to, commit=False)
        return {
            'log_id': log.id,
            'status': log.status,
            'summary': log.summary,
            'items_fetched': log.items_fetched,
            'items_upserted': log.items_upserted,
            'items_errored': log.items_errored,
        }

    # ── acciones ───────────────────────────────────────────────────────

    def action_open_move(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'account.move',
            'res_id': self.move_id.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def action_rematch(self):
        for rec in self:
            if rec.match_status == 'ignorado':
                rec.write({'match_status': 'solo_sat', 'ignore_reason': False})
            rec.write({'match_method': False})
        self._match_move()

    def action_unlink_move(self):
        self.write({'move_id': False, 'match_method': False, 'match_status': 'solo_sat'})

    def action_ignore(self):
        for rec in self:
            rec.write({'match_status': 'ignorado',
                       'ignore_reason': rec.ignore_reason or _('Se registra por póliza, no como factura')})
