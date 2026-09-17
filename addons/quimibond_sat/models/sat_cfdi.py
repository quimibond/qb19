# -*- coding: utf-8 -*-
"""CFDI tal como lo tiene el SAT (vía Syntage), ligado a su factura de Odoo.

El cruce es por folio fiscal (UUID): primero contra l10n_mx_edi.document
(el registro que Odoo crea al timbrar o al recibir un XML), después contra
el campo l10n_mx_edi_cfdi_uuid del asiento. Si hay varios asientos con el
mismo UUID (XML capturado dos veces) gana el publicado más reciente.
"""
import base64
import json
import logging
from datetime import timedelta

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

INVOICE_TYPES = ('out_invoice', 'out_refund', 'in_invoice', 'in_refund')
GENERIC_RFCS = ('XAXX010101000', 'XEXX010101000')
# Ligados por una persona: el cruce automático por UUID no los toca.
MANUAL_METHODS = ('manual', 'sugerido', 'conciliado')


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
        ('sugerido', 'Sugerencia aceptada'),
        ('conciliado', 'Conciliado (asistente)'),
    ], string='Ligado por', copy=False)
    match_status = fields.Selection([
        ('matched', 'En Odoo'), ('solo_sat', 'Solo en el SAT'), ('ignorado', 'Ignorado'),
    ], string='Cruce', default='solo_sat', required=True, index=True, copy=False)
    ignore_reason = fields.Char(string='Motivo para ignorar')
    note = fields.Char(string='Nota del cruce', copy=False)
    # Segunda pasada: candidata por RFC + monto + fecha cuando no hay UUID que cruce
    suggested_move_id = fields.Many2one('account.move', string='Factura sugerida', copy=False, index=True)
    suggestion_reason = fields.Selection([
        ('sin_uuid', 'Factura sin XML en Odoo, mismo RFC, monto y fecha'),
        ('rfc_monto_fecha', 'Mismo RFC, monto y fecha (UUID distinto en Odoo)'),
        ('uuid_cruzado', 'La factura trae el XML de otro CFDI del mismo proveedor'),
    ], string='Por qué', copy=False)
    suggestion_rejected = fields.Boolean(string='Sugerencia rechazada', default=False, copy=False)
    move_state = fields.Selection(related='move_id.state', string='Estado en Odoo')
    move_amount_total = fields.Monetary(related='move_id.amount_total', string='Total en Odoo',
                                        currency_field='move_currency_id')
    move_currency_id = fields.Many2one(related='move_id.currency_id')
    amount_diff = fields.Float(string='Diferencia', compute='_compute_issue', store=True, digits=(16, 2))
    issue = fields.Selection([
        ('ok', 'Cuadra'),
        ('solo_sat', 'Solo en el SAT'),
        ('monto', 'Monto distinto'),
        ('moneda', 'Moneda distinta'),
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

    # Al centavo: se tolera un centavo (redondeo de impuestos por línea); a partir
    # de dos centavos es 'monto'. Si la factura de Odoo está en otra moneda que
    # el CFDI, los totales no son comparables: 'moneda'.
    AMOUNT_TOLERANCE = 0.01

    @api.depends('move_id', 'move_id.state', 'move_id.amount_total', 'move_id.currency_id',
                 'estado_sat', 'total', 'moneda', 'match_status')
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
            elif move.currency_id.name != (rec.moneda or 'MXN'):
                rec.issue = 'moneda'
            elif abs(rec.amount_diff) > self.AMOUNT_TOLERANCE + 1e-6:
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
            if rec.tipo == 'I':
                # Pagos que llegaron antes que la factura (webhook, 2024...).
                self.env['sat.cfdi.pago'].sudo().search([
                    ('invoice_uuid', '=ilike', rec.uuid), ('invoice_cfdi_id', '=', False)])._relink()
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

    def _taken_by(self, move):
        """CFDI ligado a mano (o por sugerencia/conciliación) a esa factura, si
        hay: no se le roba por UUID (XML cruzado)."""
        return self.sudo().search([
            ('move_id', '=', move.id), ('id', '!=', self.id), ('match_method', 'in', MANUAL_METHODS),
        ], limit=1)

    def _pick_free_move(self, moves):
        """Como _pick_move, pero salta las facturas que otra persona ya ligó a
        otro CFDI. Con el mismo UUID en dos facturas (doble registro), antes se
        elegía la más reciente, se veía tomada y se rendía; ahora prueba las
        demás. Devuelve (factura o vacío, factura tomada o vacío)."""
        self.ensure_one()
        rank = {'posted': 0, 'draft': 1, 'cancel': 2}
        taken = self.browse()
        for move in moves.sorted(key=lambda m: (rank.get(m.state, 3), -m.id)):
            other = self._taken_by(move)
            if other:
                taken = taken or other
                continue
            return move, taken
        return moves.browse(), taken

    def _match_move(self):
        for rec in self:
            if rec.match_status == 'ignorado' or rec.match_method in MANUAL_METHODS:
                continue
            moves, method = rec._candidate_moves()
            move, taken = rec._pick_free_move(moves)
            if move:
                rec.write({'move_id': move.id, 'match_method': method, 'match_status': 'matched'})
            elif taken:
                rec.write({'match_status': 'solo_sat', 'match_method': False, 'move_id': False,
                           'note': _('La factura %s con este UUID quedó ligada al CFDI %s')
                           % (taken.move_id.name, taken.uuid)})
            elif not rec.move_id:
                if rec.partner_id.commercial_partner_id.sat_cfdi_policy == 'poliza':
                    # Banco / impuestos: Odoo lo registra por póliza, no como factura.
                    rec.write({'match_status': 'ignorado', 'match_method': False,
                               'ignore_reason': _('Política del contacto: se registra por póliza')})
                else:
                    rec.write({'match_status': 'solo_sat', 'match_method': False})

    # ── segunda pasada: sugerencias por RFC + monto + fecha ────────────

    def _amount_candidates(self, days=45):
        """Facturas publicadas de la misma compañía y contraparte (RFC), del
        tipo que corresponde al CFDI, con el mismo total (±0.5%, mín. $1) y
        fecha a ±days."""
        self.ensure_one()
        Move = self.env['account.move'].sudo()
        rfc = self.counterparty_rfc
        if not rfc or rfc in GENERIC_RFCS or not self.total or self.tipo not in ('I', 'E'):
            return Move
        if self.direction == 'issued':
            types = ['out_refund'] if self.tipo == 'E' else ['out_invoice']
        else:
            types = ['in_refund'] if self.tipo == 'E' else ['in_invoice']
        tol = max(1.0, 0.005 * abs(self.total))
        domain = [
            ('company_id', '=', self.company_id.id),
            ('move_type', 'in', types),
            ('state', '=', 'posted'),
            ('commercial_partner_id.vat', '=ilike', rfc),
            ('amount_total', '>=', self.total - tol),
            ('amount_total', '<=', self.total + tol),
        ]
        date = fields.Date.to_date(self.fecha_emision) if self.fecha_emision else None
        if date:
            domain += [('invoice_date', '>=', date - timedelta(days=days)),
                       ('invoice_date', '<=', date + timedelta(days=days))]
        return Move.search(domain)

    def _suggest_move(self):
        """(factura sugerida, motivo) o (vacío, False)."""
        self.ensure_one()
        Move = self.env['account.move'].sudo()
        cands = self._amount_candidates()
        if not cands:
            return Move, False
        date = fields.Date.to_date(self.fecha_emision) if self.fecha_emision else None

        def closeness(m):
            return abs((m.invoice_date - date).days) if (date and m.invoice_date) else 999

        linked = {c.move_id.id: c for c in self.sudo().search([('move_id', 'in', cands.ids)])}
        free = cands.filtered(lambda m: m.id not in linked)
        has_uuid_field = 'l10n_mx_edi_cfdi_uuid' in Move._fields
        if free:
            best = min(free, key=closeness)
            has_uuid = bool(has_uuid_field and best.l10n_mx_edi_cfdi_uuid)
            return best, ('rfc_monto_fecha' if has_uuid else 'sin_uuid')
        # Todas las candidatas ya tienen CFDI: si el suyo no cuadra en monto,
        # la factura trae el XML equivocado y este CFDI es el bueno.
        crossed = cands.filtered(lambda m: linked[m.id].issue in ('monto', 'moneda'))
        if crossed:
            return min(crossed, key=closeness), 'uuid_cruzado'
        return Move, False

    def action_suggest(self):
        """Calcula la sugerencia de los CFDI 'solo en el SAT' seleccionados."""
        found = 0
        for rec in self:
            if rec.match_status != 'solo_sat' or rec.suggestion_rejected or rec.move_id:
                continue
            move, reason = rec._suggest_move()
            vals = {'suggested_move_id': move.id or False, 'suggestion_reason': reason or False}
            if move:
                found += 1
            rec.write(vals)
        return found

    def action_accept_suggestion(self):
        for rec in self:
            move = rec.suggested_move_id
            if not move:
                continue
            others = self.sudo().search([('move_id', '=', move.id), ('id', '!=', rec.id)])
            others.write({
                'move_id': False, 'match_method': False, 'match_status': 'solo_sat',
                'note': _('XML cruzado: la factura %(move)s se ligó al CFDI %(uuid)s') % {
                    'move': move.name, 'uuid': rec.uuid},
            })
            rec.write({
                'move_id': move.id, 'match_method': 'sugerido', 'match_status': 'matched',
                'suggested_move_id': False, 'suggestion_reason': False, 'ignore_reason': False,
            })
            others.action_suggest()

    def action_reject_suggestion(self):
        self.write({'suggestion_rejected': True, 'suggested_move_id': False, 'suggestion_reason': False})

    # ── conciliación asistida ──────────────────────────────────────────
    #
    # El asistente muestra TODAS las facturas del mismo RFC que cuadran en
    # monto (publicadas o en borrador, ventana de fecha amplia) y la persona
    # elige. Al conciliar se liga el CFDI, se deja constancia en el chatter de
    # la factura y, si Syntage entrega el XML, se adjunta a la factura para
    # que la localización mexicana (l10n_mx_edi) registre el folio fiscal
    # como si el XML se hubiera subido a mano.

    def action_reconcile(self):
        self.ensure_one()
        return self.env['sat.reconcile.wizard'].open_for(self)

    def _reconcile_candidates(self, days=180, tol_pct=0.005, states=('posted', 'draft')):
        """Facturas de Odoo de la misma compañía y contraparte, del tipo del
        CFDI, con el mismo total (±tol_pct, mín. $1) y fecha a ±days (0 = sin
        límite). Ordenadas: primero la que menos difiere en monto, luego en
        fecha. Con RFC genérico se busca por el contacto ligado."""
        self.ensure_one()
        Move = self.env['account.move']
        if self.tipo not in ('I', 'E') or not self.total:
            return Move
        rfc = self.counterparty_rfc
        if self.direction == 'issued':
            types = ['out_refund'] if self.tipo == 'E' else ['out_invoice']
        else:
            types = ['in_refund'] if self.tipo == 'E' else ['in_invoice']
        domain = [('company_id', '=', self.company_id.id), ('move_type', 'in', types),
                  ('state', 'in', list(states))]
        if rfc and rfc not in GENERIC_RFCS:
            domain.append(('commercial_partner_id.vat', '=ilike', rfc))
        elif self.partner_id:
            domain.append(('commercial_partner_id', '=', self.partner_id.commercial_partner_id.id))
        else:
            return Move
        tol = max(1.0, tol_pct * abs(self.total)) if tol_pct else self.AMOUNT_TOLERANCE
        domain += [('amount_total', '>=', abs(self.total) - tol), ('amount_total', '<=', abs(self.total) + tol)]
        date = fields.Date.to_date(self.fecha_emision) if self.fecha_emision else None
        if date and days:
            domain += [('invoice_date', '>=', date - timedelta(days=days)),
                       ('invoice_date', '<=', date + timedelta(days=days))]

        def key(move):
            dd = abs((move.invoice_date - date).days) if (date and move.invoice_date) else 999
            return (round(abs(move.amount_total - abs(self.total)), 2), dd, -move.id)
        return Move.search(domain).sorted(key=key)

    def _reconcile_with(self, move, attach_xml=True):
        """Liga este CFDI a `move` (una persona lo decidió). Si la factura
        estaba ligada a otro CFDI, ese queda 'solo en el SAT' con nota (XML
        cruzado). Devuelve el texto que quedó en el chatter."""
        self.ensure_one()
        if not move or move.company_id != self.company_id:
            raise UserError(_('La factura debe ser de la misma compañía que el CFDI.'))
        if move.move_type not in INVOICE_TYPES:
            raise UserError(_('Solo se concilia contra facturas o notas de crédito.'))
        others = self.sudo().search([('move_id', '=', move.id), ('id', '!=', self.id)])
        if others:
            others.write({
                'move_id': False, 'match_method': False, 'match_status': 'solo_sat',
                'note': _('XML cruzado: la factura %(move)s se concilió con el CFDI %(uuid)s') % {
                    'move': move.name, 'uuid': self.uuid},
            })
        self.write({
            'move_id': move.id, 'match_method': 'conciliado', 'match_status': 'matched',
            'suggested_move_id': False, 'suggestion_reason': False, 'suggestion_rejected': False,
            'ignore_reason': False,
            'note': _('Conciliado por %s') % self.env.user.name,
        })
        xml_note = self._attach_xml_to_move(move) if attach_xml else _('sin adjuntar el XML')
        body = _('CFDI del SAT conciliado: %(uuid)s (%(name)s), total %(total)s %(cur)s, emitido el %(date)s; %(xml)s.') % {
            'uuid': (self.uuid or '').upper(), 'name': self.name, 'total': '{:,.2f}'.format(self.total or 0.0),
            'cur': self.moneda or 'MXN', 'date': fields.Date.to_date(self.fecha_emision) if self.fecha_emision else '—',
            'xml': xml_note}
        move.with_context(disable_attachment_import=True).message_post(
            body=body, message_type='comment', subtype_xmlid='mail.mt_note')
        if others:
            others.action_suggest()
        return body

    # Rutas donde Syntage puede servir el XML de una factura. La API no está
    # documentada de forma accesible desde aquí; se prueban en orden y la que
    # funcione queda guardada en `quimibond_sat.syntage_xml_path`.
    XML_PATH_CANDIDATES = (
        '/invoices/{id}/xml',
        '/invoices/{id}/files',
        '/invoices/{id}/files/xml',
        '/invoices/{id}/download/xml',
        '/invoices/{id}/file/xml',
    )

    @api.model
    def _xml_from_payload(self, client, content):
        """Bytes de XML a partir de lo que respondió una ruta: el archivo tal
        cual, o metadatos JSON-LD (objeto o colección hydra) con URL o
        contenido en base64. Vacío si no hay XML ahí."""
        head = content.lstrip()[:1]
        if head == b'<':
            # Tiene que ser el CFDI, no cualquier XML (p.ej. el recurso
            # serializado por la API).
            return content if b'Comprobante' in content[:4000] else b''
        if head not in (b'{', b'['):
            return b''
        try:
            data = json.loads(content)
        except ValueError:
            return b''
        items = data if isinstance(data, list) else data.get('hydra:member') or data.get('member') or [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            kind = ' '.join(str(item.get(k) or '') for k in ('type', 'format', 'mimeType', 'name', '@id', 'url')).lower()
            if len(items) > 1 and 'xml' not in kind:
                continue
            if item.get('content'):
                try:
                    blob = base64.b64decode(item['content'])
                except (ValueError, TypeError):
                    blob = b''
                if blob.lstrip()[:1] == b'<':
                    return blob
            url = item.get('url') or item.get('contentUrl') or item.get('downloadUrl') or item.get('@id')
            if url and url != item.get('@id') or (url and 'xml' in kind):
                try:
                    blob = client._request_raw(url, accept='application/xml')
                except UserError:
                    continue
                if blob.lstrip()[:1] == b'<':
                    return blob
        return b''

    def _fetch_xml(self):
        """XML del CFDI desde Syntage (bytes). Prueba la ruta configurada y
        luego las candidatas; la primera que sirve se guarda para las demás."""
        self.ensure_one()
        if not self.syntage_id:
            raise UserError(_('El CFDI %s no tiene id de Syntage.') % self.uuid)
        icp = self.env['ir.config_parameter'].sudo()
        client = self.env['sat.syntage.client']
        errors = []
        # Ruta documentada: el archivo del webhook file.created.
        sfile = self.env['sat.syntage.file']._xml_for_invoice(self.syntage_id)
        if sfile:
            path = sfile.download_path()
            try:
                xml = self._xml_from_payload(client, client._request_raw(path, accept='application/xml'))
                if xml:
                    return xml
                errors.append('%s: sin XML en la respuesta' % path)
            except UserError as exc:
                errors.append('%s: %s' % (path, str(exc)[:80]))
        else:
            errors.append(_('sin archivo registrado por webhook (file.created) para %s') % self.syntage_id)
        configured = icp.get_param('quimibond_sat.syntage_xml_path') or ''
        candidates = [configured] if configured else []
        candidates += [c for c in self.XML_PATH_CANDIDATES if c != configured]
        for template in candidates:
            path = template.format(id=self.syntage_id, uuid=self.uuid)
            try:
                content = client._request_raw(path, accept='application/xml')
            except UserError as exc:
                errors.append('%s: %s' % (path, str(exc)[:80]))
                continue
            xml = self._xml_from_payload(client, content)
            if xml:
                if template != configured:
                    icp.set_param('quimibond_sat.syntage_xml_path', template)
                    _logger.info('sat.cfdi: ruta del XML en Syntage fijada a %s', template)
                return xml
            errors.append('%s: sin XML en la respuesta' % path)
        raise UserError(_('Syntage no devolvió el XML del CFDI %(uuid)s. Rutas probadas: %(errors)s') % {
            'uuid': self.uuid, 'errors': '; '.join(errors)})

    def _attach_xml_to_move(self, move):
        """Adjunta el XML del SAT a la factura. En una factura publicada se
        sube por el chatter como lo haría una persona, para que l10n_mx_edi
        lo lea y registre el folio fiscal; si eso falla, queda adjunto sin
        más. Nunca revienta la conciliación: devuelve un texto con lo que pasó."""
        self.ensure_one()
        try:
            content = self._fetch_xml()
        except Exception as exc:  # noqa: BLE001 — la liga vale aunque no haya XML
            _logger.warning('sat.cfdi %s: XML no adjuntado: %s', self.uuid, exc)
            return _('XML no adjuntado (%s)') % exc
        name = '%s.xml' % (self.uuid or '').upper()
        Attachment = self.env['ir.attachment'].sudo()
        if Attachment.search_count([('res_model', '=', 'account.move'), ('res_id', '=', move.id), ('name', '=', name)]):
            return _('el XML ya estaba adjunto')
        att = Attachment.create({'name': name, 'raw': content, 'mimetype': 'application/xml',
                                 'res_model': 'account.move', 'res_id': move.id})
        has_doc, has_field = self._uuid_sources_available()
        if move.state == 'posted' and has_doc:
            try:
                with self.env.cr.savepoint():
                    move.message_post(body=_('XML del CFDI %s (Syntage)') % self.uuid.upper(),
                                      attachment_ids=[att.id], message_type='comment', subtype_xmlid='mail.mt_note')
                move.invalidate_recordset()
                if has_field and (move.l10n_mx_edi_cfdi_uuid or '').lower() == (self.uuid or '').lower():
                    return _('XML adjunto y folio fiscal registrado por Odoo')
                return _('XML adjunto (Odoo no registró el folio fiscal)')
            except Exception:  # noqa: BLE001 — el importador de Odoo no debe tumbar la liga
                _logger.exception('sat.cfdi %s: Odoo no pudo importar el XML en %s', self.uuid, move.name)
        move.with_context(disable_attachment_import=True).message_post(
            body=_('XML del CFDI %s (Syntage)') % self.uuid.upper(),
            attachment_ids=[att.id], message_type='comment', subtype_xmlid='mail.mt_note')
        return _('XML adjunto a la factura')

    # Aceptación automática: factura sin XML, mismo RFC, total exacto (al
    # centavo), misma moneda, fecha a ±AUTO_ACCEPT_DAYS y sin otra candidata.
    AUTO_ACCEPT_DAYS = 10

    def _auto_accept_suggestions(self):
        """Liga solo las sugerencias que no dejan lugar a duda. Devuelve cuántas."""
        accepted = 0
        for rec in self:
            move = rec.suggested_move_id
            if not move or rec.suggestion_reason != 'sin_uuid' or rec.match_status != 'solo_sat':
                continue
            if rec.partner_id.commercial_partner_id.sat_cfdi_policy != 'factura':
                continue
            if abs(rec.total - move.amount_total) > self.AMOUNT_TOLERANCE + 1e-6:
                continue
            if move.currency_id.name != (rec.moneda or 'MXN'):
                continue
            date = fields.Date.to_date(rec.fecha_emision) if rec.fecha_emision else None
            if not date or not move.invoice_date or abs((move.invoice_date - date).days) > self.AUTO_ACCEPT_DAYS:
                continue
            # Otra sugerencia o CFDI apuntando a la misma factura: que decida una persona.
            rivals = self.sudo().search_count([
                ('id', '!=', rec.id), '|', ('suggested_move_id', '=', move.id), ('move_id', '=', move.id)])
            if rivals:
                continue
            rec.action_accept_suggestion()
            rec.write({'note': _('Ligado automáticamente: factura sin XML, mismo RFC, total exacto '
                                 'y fecha a ±%s días') % self.AUTO_ACCEPT_DAYS})
            accepted += 1
        return accepted

    @api.model
    def _cron_suggest_matches(self, limit=3000):
        pending = self.sudo().search([
            ('match_status', '=', 'solo_sat'), ('suggestion_rejected', '=', False),
            ('tipo', 'in', ('I', 'E')), ('estado_sat', '=', 'vigente'),
        ], order='fecha_emision desc, id desc', limit=limit)
        found = pending.action_suggest()
        accepted = pending._auto_accept_suggestions()
        _logger.info('sat.cfdi: %s sugerencias para %s CFDI solo en el SAT, %s ligadas automáticamente',
                     found, len(pending), accepted)
        return found

    # ── alerta diaria ──────────────────────────────────────────────────

    ALERT_ISSUES = ('cancelado_odoo', 'cancelado_sat', 'monto', 'moneda')
    # Días sin timbrados nuevos del SAT a partir de los cuales la alerta avisa
    # que la extracción se estancó.
    STALE_DAYS = 3

    @api.model
    def _alert_recipients(self):
        raw = self.env['ir.config_parameter'].sudo().get_param('quimibond_sat.alert_email') or ''
        return [e.strip() for e in raw.replace(';', ',').split(',') if e.strip()]

    @api.model
    def _cron_daily_alert(self, new_days=7):
        """Correo con los hallazgos abiertos de la comparación (cancelados, monto,
        moneda) y los que aparecieron en los últimos ``new_days`` días. Sin
        destinatarios o sin hallazgos no manda nada."""
        recipients = self._alert_recipients()
        if not recipients:
            _logger.info('sat.cfdi: sin quimibond_sat.alert_email, no se manda alerta')
            return False
        Line = self.env['sat.compare.line'].sudo()
        companies = self.env['res.company'].sudo().search([('sat_sync_enabled', '=', True)])
        lines = Line.search([('company_id', 'in', companies.ids), ('issue', 'in', self.ALERT_ISSUES)],
                            order='fecha desc')
        # Hasta cuándo hay datos del SAT: si se estanca, la comparación deja de
        # valer y hay que decirlo aunque no haya hallazgos.
        today = fields.Date.today()
        coverage, stale = [], []
        for company in companies:
            for label, until in (('emitidos', company.sat_data_until_issued),
                                 ('recibidos', company.sat_data_until_received)):
                coverage.append('%s %s: %s' % (company.name, label, until or '—'))
                if not until or (today - until).days > self.STALE_DAYS:
                    stale.append('%s %s (%s)' % (company.name, label, until or 'sin datos'))
        if not lines and not stale:
            return False
        since = fields.Date.today() - timedelta(days=new_days)
        new = lines.filtered(lambda l: l.fecha and l.fecha >= since)
        labels = dict(Line._fields['issue']._description_selection(self.env))
        summary = {}
        for line in lines:
            item = summary.setdefault(line.issue, [0, 0.0])
            item[0] += 1
            item[1] += line.delta
        rows = ''.join(
            '<tr><td>%s</td><td style="text-align:right">%s</td><td style="text-align:right">%s</td></tr>'
            % (labels.get(issue, issue), n, '{:,.2f}'.format(d))
            for issue, (n, d) in sorted(summary.items()))
        detail = ''.join(
            '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>'
            '<td style="text-align:right">%s</td><td style="text-align:right">%s</td></tr>' % (
                line.fecha or '', labels.get(line.issue, line.issue),
                'Emitido' if line.direction == 'issued' else 'Recibido',
                line.counterparty_name or '', line.move_name or '',
                '{:,.2f}'.format(line.total_sat or 0.0), '{:,.2f}'.format(line.delta or 0.0))
            for line in new[:200])
        body = (
            '<p>Datos del SAT hasta: %s.</p>%s'
            '<p>Hallazgos abiertos en la comparación SAT vs Odoo (Δ en MXN):</p>'
            '<table border="1" cellpadding="4" cellspacing="0"><tr><th>Hallazgo</th><th>Docs</th><th>Δ</th></tr>'
            '%s</table>'
            '<p>Nuevos en los últimos %s días: <b>%s</b></p>'
            '<table border="1" cellpadding="4" cellspacing="0"><tr><th>Fecha</th><th>Hallazgo</th><th>Sentido</th>'
            '<th>Contraparte</th><th>Odoo</th><th>Total SAT</th><th>Δ</th></tr>%s</table>'
            '<p>Detalle en Odoo: Contabilidad → SAT (Syntage) → Conciliar, filtro "Con hallazgo".</p>'
        ) % ('; '.join(coverage),
             ('<p><b>Sin datos nuevos del SAT desde hace más de %s días: %s.</b> Revisa la extracción '
              'diaria (Datos → Bitácora Syntage).</p>' % (self.STALE_DAYS, ', '.join(stale))) if stale else '',
             rows, new_days, len(new), detail or '<tr><td colspan="7">Ninguno</td></tr>')
        subject = _('SAT vs Odoo: %(open)s hallazgos abiertos, %(new)s nuevos') % {
            'open': len(lines), 'new': len(new)}
        if stale:
            subject = _('SAT sin datos nuevos: %s') % ', '.join(stale) + ' · ' + subject
        mail = self.env['mail.mail'].sudo().create({
            'subject': subject,
            'email_to': ', '.join(recipients),
            'body_html': body,
            'auto_delete': False,
        })
        mail.send()
        return mail

    def write(self, vals):
        # Ligar o desligar a mano desde el formulario.
        if 'move_id' in vals and 'match_method' not in vals:
            vals = dict(vals, match_method='manual' if vals['move_id'] else False,
                        match_status='matched' if vals['move_id'] else 'solo_sat')
        return super().write(vals)

    @api.model
    def _cron_match_unmatched(self, limit=2000):
        # Solo facturas y notas de crédito (los P y N no se cruzan aquí), y los
        # más recientes primero: con el límite, los CFDI nuevos no pueden
        # quedarse detrás de miles de nóminas viejas.
        cfdis = self.sudo().search([('match_status', '=', 'solo_sat'), ('tipo', 'in', ('I', 'E'))],
                                   order='fecha_emision desc, id desc', limit=limit)
        cfdis._match_move()
        matched = len(cfdis.filtered(lambda c: c.match_status == 'matched'))
        _logger.info('sat.cfdi: cruce de %s pendientes, %s ligados', len(cfdis), matched)
        self._cron_suggest_matches()
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
                           include_retentions=False, background=False):
        """Mismo trabajo que el asistente "Traer CFDI del SAT", pero como método
        público de un modelo regular: los asistentes (transitorios) no se pueden
        exponer por MCP y así se puede lanzar una descarga o extracción desde
        fuera. Devuelve un resumen del registro de bitácora.

        ``background=True`` encola el trabajo y lo corre un cron enseguida, con
        commit por página: es la forma correcta para meses completos (una
        llamada síncrona por MCP se corta a los 60 s y un mes trae ~1,000 CFDI).
        El avance se sigue en sat.sync.log (status queued → running → OK)."""
        company = self.env['res.company'].browse(company_id) if company_id else self.env.company
        date_from = fields.Date.to_date(date_from)
        date_to = fields.Date.to_date(date_to)
        client = self.env['sat.syntage.client']
        labels = {'extraction': _('Extracción'), 'payments': _('Pagos')}
        if background:
            log = self.env['sat.sync.log'].sudo().create({
                'name': _('%(kind)s %(rfc)s %(from)s..%(to)s') % {
                    'kind': labels.get(mode, _('Descarga')),
                    'rfc': company.vat or company.display_name, 'from': date_from, 'to': date_to},
                'kind': 'extraction' if mode == 'extraction' else 'pull',
                'mode': mode if mode in ('extraction', 'payments') else 'pull',
                'include_retentions': include_retentions,
                'company_id': company.id, 'status': 'queued',
                'date_from': date_from, 'date_to': date_to,
            })
            self.env.ref('quimibond_sat.cron_sat_run_queued').sudo()._trigger()
        elif mode == 'extraction':
            log = client.request_extraction(company, date_from, date_to, include_retentions)
        elif mode == 'payments':
            log = client.pull_payments(company, date_from, date_to, commit=False)
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

    @api.model
    def _cron_run_queued(self):
        self.env['sat.syntage.client']._run_queued()

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
        self.write({'move_id': False, 'match_method': False, 'match_status': 'solo_sat', 'note': False})

    def action_ignore(self):
        for rec in self:
            rec.write({'match_status': 'ignorado',
                       'ignore_reason': rec.ignore_reason or _('Se registra por póliza, no como factura')})
