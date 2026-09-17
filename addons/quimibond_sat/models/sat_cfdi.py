# -*- coding: utf-8 -*-
"""CFDI tal como lo tiene el SAT (vía Syntage), ligado a su factura de Odoo.

El cruce es por folio fiscal (UUID): primero contra l10n_mx_edi.document
(el registro que Odoo crea al timbrar o al recibir un XML), después contra
el campo l10n_mx_edi_cfdi_uuid del asiento. Si hay varios asientos con el
mismo UUID (XML capturado dos veces) gana el publicado más reciente.
"""
import logging
from datetime import timedelta

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
        ('sugerido', 'Sugerencia aceptada'),
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
            if rec.match_status == 'ignorado' or rec.match_method in ('manual', 'sugerido'):
                continue
            move, method = rec._find_move()
            if move:
                # Una factura ligada a mano o por sugerencia a OTRO CFDI no se
                # roba por UUID: es el caso del XML cruzado, donde el UUID de
                # Odoo apunta al CFDI equivocado.
                taken = self.sudo().search([
                    ('move_id', '=', move.id), ('id', '!=', rec.id),
                    ('match_method', 'in', ('manual', 'sugerido')),
                ], limit=1)
                if taken:
                    rec.write({'match_status': 'solo_sat', 'match_method': False, 'move_id': False,
                               'note': _('La factura %s con este UUID quedó ligada al CFDI %s')
                               % (move.name, taken.uuid)})
                    continue
                rec.write({'move_id': move.id, 'match_method': method, 'match_status': 'matched'})
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
        ], limit=limit)
        found = pending.action_suggest()
        accepted = pending._auto_accept_suggestions()
        _logger.info('sat.cfdi: %s sugerencias para %s CFDI solo en el SAT, %s ligadas automáticamente',
                     found, len(pending), accepted)
        return found

    # ── alerta diaria ──────────────────────────────────────────────────

    ALERT_ISSUES = ('cancelado_odoo', 'cancelado_sat', 'monto', 'moneda')

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
        if not lines:
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
            '<p>Hallazgos abiertos en la comparación SAT vs Odoo (Δ en MXN):</p>'
            '<table border="1" cellpadding="4" cellspacing="0"><tr><th>Hallazgo</th><th>Docs</th><th>Δ</th></tr>'
            '%s</table>'
            '<p>Nuevos en los últimos %s días: <b>%s</b></p>'
            '<table border="1" cellpadding="4" cellspacing="0"><tr><th>Fecha</th><th>Hallazgo</th><th>Sentido</th>'
            '<th>Contraparte</th><th>Odoo</th><th>Total SAT</th><th>Δ</th></tr>%s</table>'
            '<p>Detalle en Odoo: Contabilidad → SAT (Syntage) → Comparación SAT vs Odoo, filtro "Con hallazgo".</p>'
        ) % (rows, new_days, len(new), detail or '<tr><td colspan="7">Ninguno</td></tr>')
        mail = self.env['mail.mail'].sudo().create({
            'subject': _('SAT vs Odoo: %(open)s hallazgos abiertos, %(new)s nuevos') % {
                'open': len(lines), 'new': len(new)},
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
        cfdis = self.sudo().search([('match_status', '=', 'solo_sat')], limit=limit)
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
        if background:
            log = self.env['sat.sync.log'].sudo().create({
                'name': _('%(kind)s %(rfc)s %(from)s..%(to)s') % {
                    'kind': _('Extracción') if mode == 'extraction' else _('Descarga'),
                    'rfc': company.vat or company.display_name, 'from': date_from, 'to': date_to},
                'kind': 'extraction' if mode == 'extraction' else 'pull',
                'mode': 'extraction' if mode == 'extraction' else 'pull',
                'include_retentions': include_retentions,
                'company_id': company.id, 'status': 'queued',
                'date_from': date_from, 'date_to': date_to,
            })
            self.env.ref('quimibond_sat.cron_sat_run_queued').sudo()._trigger()
        elif mode == 'extraction':
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
