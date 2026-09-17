# -*- coding: utf-8 -*-
"""Obligaciones vivas. Ver README. Piloto: cobranza.

Reglas que no se negocian: el dueño es un res.users; el cierre por evidencia es
una consulta, nunca un juicio de un modelo; lo que nace de Odoo nace confirmado
y lo que nace del correo espera la confirmación del dueño.
"""
import logging
from datetime import timedelta

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

OPEN_STATES = ('candidate', 'confirmed')
PAID_STATES = ('paid', 'in_payment', 'reversed')


class QbObligation(models.Model):
    _name = 'qb.obligation'
    _description = 'Obligación'
    _inherit = ['mail.thread']
    _order = 'date_deadline asc, id asc'

    # ── identidad ────────────────────────────────────────────────────
    name = fields.Char(required=True)
    obligation_type = fields.Selection([
        ('collection.overdue_invoice', 'Cobrar factura vencida'),
        ('collection.apply_payment', 'Aplicar pago que el SAT ya ve'),
        ('collection.payment_promise', 'Promesa de pago (correo)'),
    ], required=True, index=True)
    state = fields.Selection([
        ('candidate', 'Por confirmar'),
        ('confirmed', 'Confirmada'),
        ('done', 'Cumplida'),
        ('discarded', 'Descartada'),
        ('cancelled', 'Cancelada'),
    ], default='candidate', required=True, index=True, tracking=True)
    company_id = fields.Many2one('res.company', required=True, default=lambda self: self.env.company, index=True)
    partner_id = fields.Many2one('res.partner', string='Cliente', index=True)

    # ── los cinco datos ──────────────────────────────────────────────
    description = fields.Text(string='Qué hay que hacer', required=True)
    user_id = fields.Many2one('res.users', string='Dueño', required=True, index=True, tracking=True)
    res_model = fields.Char(string='Documento (modelo)', index=True)
    res_id = fields.Many2oneReference(string='Documento', model_field='res_model', index=True)
    evidence_rule_key = fields.Selection([
        ('invoice_paid_or_credited', 'Saldo de la factura en cero (pago o nota de crédito)'),
        ('owner_ack', 'Acuse del dueño'),
    ], string='Cómo se prueba', required=True, default='owner_ack')
    date_deadline = fields.Date(string='Vence', required=True, index=True)

    # ── origen ───────────────────────────────────────────────────────
    source = fields.Selection([
        ('odoo', 'Odoo'), ('email', 'Correo'), ('digest', 'Resumen'), ('manual', 'Manual'), ('gabinete', 'Gabinete'),
    ], required=True, default='manual')
    source_ref = fields.Char(string='Referencia externa', index=True)
    source_thread_key = fields.Char(string='Hilo de correo')
    detection_payload = fields.Json(string='Detección')
    weak_key = fields.Boolean(string='Sin fecha firme', help='La fecha se supuso porque el origen no la traía.')

    # ── confirmación ─────────────────────────────────────────────────
    confirmed_by = fields.Many2one('res.users')
    confirmed_at = fields.Datetime()
    discarded_by = fields.Many2one('res.users')
    discarded_at = fields.Datetime()
    discard_reason = fields.Char(string='Motivo de descarte')

    # ── cierre ───────────────────────────────────────────────────────
    completed_at = fields.Datetime()
    close_method = fields.Selection([
        ('evidence', 'Evidencia'), ('owner_ack', 'Acuse del dueño'), ('manual', 'Manual'),
    ])
    evidence_model = fields.Char()
    evidence_res_id = fields.Many2oneReference(model_field='evidence_model', string='Evidencia')
    evidence_summary = fields.Char()
    closed_by_cron = fields.Boolean()

    # ── escalación ───────────────────────────────────────────────────
    escalated_at = fields.Datetime()
    escalate_after_days = fields.Integer(default=3)
    escalated_to = fields.Many2one('res.users')

    # ── métricas ─────────────────────────────────────────────────────
    currency_id = fields.Many2one('res.currency')
    amount_at_creation = fields.Monetary(string='Saldo al crear', currency_field='currency_id')
    amount_residual = fields.Monetary(string='Saldo', currency_field='currency_id',
                                      help='Último saldo conocido del documento (se refresca en cada corrida).')
    amount_collected = fields.Monetary(string='Cobrado', currency_field='currency_id',
                                       help='Saldo al crear menos saldo al cerrar.')
    amount_residual_company = fields.Monetary(string='Saldo (moneda compañía)',
                                              currency_field='company_currency_id')
    company_currency_id = fields.Many2one(related='company_id.currency_id')
    days_overdue = fields.Integer(string='Días vencida', compute='_compute_days', store=True)
    days_to_close = fields.Integer(string='Días para cerrar', compute='_compute_days', store=True)
    times_notified = fields.Integer(default=0)
    last_notified_at = fields.Datetime()

    def init(self):
        # Una sola obligación abierta por documento y tipo, y una por referencia externa.
        self.env.cr.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS qb_obligation_open_anchor_uniq
                ON qb_obligation (obligation_type, res_model, res_id)
                WHERE state IN ('candidate', 'confirmed') AND res_model IS NOT NULL AND res_id IS NOT NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS qb_obligation_open_source_ref_uniq
                ON qb_obligation (source_ref)
                WHERE state IN ('candidate', 'confirmed') AND source_ref IS NOT NULL;
        """)

    @api.depends('date_deadline', 'state', 'completed_at', 'create_date')
    def _compute_days(self):
        today = fields.Date.context_today(self)
        for rec in self:
            if rec.state in OPEN_STATES and rec.date_deadline:
                rec.days_overdue = max((today - rec.date_deadline).days, 0)
            elif rec.state == 'done' and rec.date_deadline and rec.completed_at:
                rec.days_overdue = max((rec.completed_at.date() - rec.date_deadline).days, 0)
            else:
                rec.days_overdue = 0
            if rec.state == 'done' and rec.completed_at and rec.create_date:
                rec.days_to_close = (rec.completed_at - rec.create_date).days
            else:
                rec.days_to_close = 0

    # ── documento ancla ──────────────────────────────────────────────

    def _anchor(self):
        self.ensure_one()
        if self.res_model and self.res_id and self.res_model in self.env:
            rec = self.env[self.res_model].browse(self.res_id).exists()
            return rec
        return None

    def action_open_anchor(self):
        self.ensure_one()
        if not (self.res_model and self.res_id):
            raise UserError(_('Esta obligación no tiene documento ancla.'))
        return {'type': 'ir.actions.act_window', 'res_model': self.res_model, 'res_id': self.res_id,
                'view_mode': 'form'}

    # ── dueños ───────────────────────────────────────────────────────

    @api.model
    def _collection_owner(self, partner, company):
        partner = partner.commercial_partner_id if partner else partner
        owner = partner.with_company(company).collection_user_id if partner else self.env['res.users']
        return owner or company.obligation_collection_user_id

    # ── flujo del dueño ──────────────────────────────────────────────

    def action_confirm(self):
        for rec in self:
            if rec.state != 'candidate':
                raise UserError(_('Solo se confirma una obligación por confirmar.'))
            rec.write({'state': 'confirmed', 'confirmed_by': self.env.user.id,
                       'confirmed_at': fields.Datetime.now()})
        return True

    def action_discard(self, reason=None):
        for rec in self:
            if rec.state not in OPEN_STATES:
                raise UserError(_('Solo se descarta una obligación abierta.'))
            rec.write({'state': 'discarded', 'discarded_by': self.env.user.id,
                       'discarded_at': fields.Datetime.now(),
                       'discard_reason': reason or rec.discard_reason})
        return True

    def action_ack_done(self):
        """El dueño da por cumplida la obligación. Vale siempre; queda registrado
        como acuse, no como evidencia."""
        for rec in self:
            if rec.state not in OPEN_STATES:
                raise UserError(_('Solo se cumple una obligación abierta.'))
            rec._close('owner_ack', summary=_('Acuse de %s') % self.env.user.name)
        return True

    def action_cancel(self):
        for rec in self:
            if rec.state not in OPEN_STATES:
                raise UserError(_('Solo se cancela una obligación abierta.'))
            rec.write({'state': 'cancelled'})
        return True

    def _close(self, method, summary=None, evidence=None, by_cron=False):
        for rec in self:
            residual = rec.amount_residual if rec.currency_id else 0.0
            vals = {
                'state': 'done', 'close_method': method, 'completed_at': fields.Datetime.now(),
                'evidence_summary': summary, 'closed_by_cron': by_cron,
                'amount_collected': (rec.amount_at_creation or 0.0) - residual,
            }
            if evidence is not None:
                vals.update(evidence_model=evidence._name, evidence_res_id=evidence.id)
            rec.write(vals)

    # ── API para orígenes externos (correo, gabinete, MCP) ────────────

    @api.model
    def create_candidate(self, vals):
        """Crea (o reutiliza) una obligación candidata desde un origen externo.
        Idempotente por ``source_ref``: si ya hay una abierta con esa referencia
        se actualiza descripción y fecha y se devuelve la misma. Sin dueño
        resoluble no crea nada y devuelve un recordset vacío."""
        vals = dict(vals)
        source_ref = vals.get('source_ref')
        if source_ref:
            existing = self.search([('source_ref', '=', source_ref), ('state', 'in', OPEN_STATES)], limit=1)
            if existing:
                existing.write({k: v for k, v in vals.items()
                                if k in ('description', 'date_deadline', 'detection_payload', 'weak_key')})
                return existing
        company = self.env['res.company'].browse(vals.get('company_id')) if vals.get('company_id') else self.env.company
        partner = self.env['res.partner'].browse(vals['partner_id']) if vals.get('partner_id') else None
        if not vals.get('user_id'):
            owner = self._collection_owner(partner, company)
            if not owner:
                _logger.info('qb.obligation: sin dueño para %s en %s, no se crea candidata', partner and partner.name, company.name)
                return self.browse()
            vals['user_id'] = owner.id
        if not vals.get('date_deadline'):
            vals['date_deadline'] = fields.Date.add(fields.Date.context_today(self), days=3)
            vals['weak_key'] = True
        vals.setdefault('company_id', company.id)
        vals.setdefault('source', 'email')
        vals.setdefault('state', 'candidate')
        vals.setdefault('evidence_rule_key', 'invoice_paid_or_credited' if vals.get('res_model') == 'account.move' else 'owner_ack')
        vals.setdefault('name', (vals.get('description') or '')[:80] or 'Obligación')
        if partner:
            vals['partner_id'] = partner.commercial_partner_id.id
        rec = self.create(vals)
        rec._refresh_amounts()
        return rec

    # ── cobranza: generación desde Odoo ──────────────────────────────

    @api.model
    def _generate_overdue_invoices(self, company=None):
        """Una obligación confirmada por cada factura de cliente vencida con
        saldo. Sin dueño configurado no crea nada."""
        companies = company or self.env['res.company'].search([])
        today = fields.Date.context_today(self)
        created = self.browse()
        Move = self.env['account.move']
        for comp in companies:
            if not comp.obligation_collection_user_id and not self._any_partner_owner(comp):
                _logger.info('qb.obligation: %s sin dueño de cobranza, no se generan obligaciones', comp.name)
                continue
            moves = Move.search(Move._obligation_overdue_domain(comp, today))
            if not moves:
                continue
            open_ids = {r.res_id for r in self.search([
                ('obligation_type', 'in', ('collection.overdue_invoice', 'collection.apply_payment')),
                ('res_model', '=', 'account.move'), ('res_id', 'in', moves.ids), ('state', 'in', OPEN_STATES)])}
            for move in moves:
                if move.id in open_ids:
                    continue
                owner = self._collection_owner(move.partner_id, comp)
                if not owner:
                    continue
                rec = self.create({
                    'name': _('Cobrar %(inv)s a %(partner)s') % {
                        'inv': move.name, 'partner': move.partner_id.commercial_partner_id.name},
                    'obligation_type': 'collection.overdue_invoice',
                    'state': 'confirmed', 'confirmed_at': fields.Datetime.now(),
                    'company_id': comp.id, 'partner_id': move.partner_id.commercial_partner_id.id,
                    'description': _('Cobrar la factura %(inv)s, vencida el %(due)s, saldo %(res)s %(cur)s.') % {
                        'inv': move.name, 'due': move.invoice_date_due, 'res': '{:,.2f}'.format(move.amount_residual),
                        'cur': move.currency_id.name},
                    'user_id': owner.id, 'res_model': 'account.move', 'res_id': move.id,
                    'evidence_rule_key': 'invoice_paid_or_credited',
                    'date_deadline': move.invoice_date_due or today,
                    'source': 'odoo', 'source_ref': 'account.move:%s' % move.id,
                    'escalate_after_days': comp.obligation_escalate_days,
                    'currency_id': move.currency_id.id,
                    'amount_at_creation': move.amount_residual,
                })
                rec._refresh_amounts()
                created |= rec
        if created:
            _logger.info('qb.obligation: %s obligaciones de cobranza creadas', len(created))
        return created

    @api.model
    def _any_partner_owner(self, company):
        return bool(self.env['res.partner'].with_company(company).search_count([('collection_user_id', '!=', False)]))

    # ── cobranza: cierre por evidencia ───────────────────────────────

    def _refresh_amounts(self):
        for rec in self:
            move = rec._anchor() if rec.res_model == 'account.move' else None
            if move is None or not move:
                continue
            residual = abs(move.amount_residual)
            vals = {'amount_residual': residual, 'currency_id': move.currency_id.id}
            if not rec.amount_at_creation and rec.state in OPEN_STATES:
                vals['amount_at_creation'] = residual
            vals['amount_residual_company'] = move.currency_id._convert(
                residual, rec.company_id.currency_id, rec.company_id, fields.Date.context_today(rec))
            rec.write(vals)

    @api.model
    def _close_by_evidence(self):
        """Regla única de cobranza: saldo dentro de tolerancia o estado de pago
        pagado / en proceso / revertido → cumplida. Factura cancelada o bloqueada
        → cancelada."""
        closed = self.browse()
        cancelled = self.browse()
        for rec in self.search([('evidence_rule_key', '=', 'invoice_paid_or_credited'), ('state', 'in', OPEN_STATES),
                                ('res_model', '=', 'account.move')]):
            move = rec._anchor()
            if move is None or not move:
                rec.write({'state': 'cancelled'})
                cancelled |= rec
                continue
            rec._refresh_amounts()
            tol = rec.company_id.obligation_residual_tolerance or 0.0
            if move.state == 'cancel' or move.payment_state == 'blocked':
                rec.message_post(body=_('Factura %s cancelada o bloqueada en Odoo.') % move.name)
                rec.write({'state': 'cancelled'})
                cancelled |= rec
                continue
            if move.state != 'posted':
                continue
            if abs(move.amount_residual) <= tol or move.payment_state in PAID_STATES:
                labels = dict(move._fields['payment_state']._description_selection(self.env))
                rec._close('evidence', by_cron=True, evidence=move,
                           summary=_('%(inv)s: saldo %(res)s %(cur)s, estado %(st)s') % {
                               'inv': move.name, 'res': '{:,.2f}'.format(move.amount_residual),
                               'cur': move.currency_id.name, 'st': labels.get(move.payment_state, move.payment_state)})
                closed |= rec
        if closed or cancelled:
            _logger.info('qb.obligation: %s cerradas por evidencia, %s canceladas', len(closed), len(cancelled))
        return closed

    # ── cobranza: complemento SAT sin pago aplicado ──────────────────

    @api.model
    def _reassign_sat_paid(self):
        """Si el SAT tiene complementos de pago vigentes por más de lo que Odoo
        registra cobrado, el cliente ya pagó y falta aplicarlo: la obligación
        deja de ser de cobranza y pasa a ser de contabilidad. Requiere
        quimibond_sat."""
        if 'sat.cfdi.pago' not in self.env:
            return self.browse()
        Pago = self.env['sat.cfdi.pago'].sudo()
        changed = self.browse()
        for rec in self.search([('obligation_type', '=', 'collection.overdue_invoice'), ('state', 'in', OPEN_STATES),
                                ('res_model', '=', 'account.move')]):
            move = rec._anchor()
            if move is None or not move or move.state != 'posted':
                continue
            pagos = Pago.search([('move_id', '=', move.id), ('estado_sat', '=', 'vigente')])
            if not pagos:
                continue
            same_currency = pagos.filtered(lambda p: (p.moneda or 'MXN') == move.currency_id.name)
            pagado_sat = sum(same_currency.mapped('monto'))
            pagado_odoo = abs(move.amount_total) - abs(move.amount_residual)
            tol = rec.company_id.obligation_residual_tolerance or 0.0
            if pagado_sat <= pagado_odoo + tol:
                continue
            owner = rec.company_id.obligation_accounting_user_id or rec.user_id
            rec.write({
                'obligation_type': 'collection.apply_payment', 'user_id': owner.id,
                'name': _('Aplicar pago de %(partner)s a %(inv)s') % {'partner': rec.partner_id.name, 'inv': move.name},
                'description': _('El SAT tiene complementos de pago vigentes por %(sat)s %(cur)s sobre %(inv)s y Odoo '
                                 'registra cobrado %(odoo)s. Aplicar el pago en Odoo o cancelar el REP de más.') % {
                    'sat': '{:,.2f}'.format(pagado_sat), 'cur': move.currency_id.name, 'inv': move.name,
                    'odoo': '{:,.2f}'.format(pagado_odoo)},
                'evidence_model': 'sat.cfdi.pago', 'evidence_res_id': same_currency[:1].id,
            })
            rec.message_post(body=_('Reasignada: el SAT ya ve el pago (%s complementos).') % len(same_currency))
            changed |= rec
        return changed

    # ── escalación ───────────────────────────────────────────────────

    @api.model
    def _escalate(self):
        now = fields.Datetime.now()
        today = fields.Date.context_today(self)
        escalated = self.browse()
        for rec in self.search([('state', '=', 'confirmed'), ('escalated_at', '=', False)]):
            comp = rec.company_id
            to = comp.obligation_escalation_user_id
            if not to:
                continue
            since = rec.confirmed_at or rec.create_date
            days = max(rec.escalate_after_days if rec.escalate_after_days is not None else comp.obligation_escalate_days, 0)
            if (now - since) < timedelta(days=days):
                continue
            overdue = (today - rec.date_deadline).days if rec.date_deadline else 0
            big = comp.obligation_escalate_amount and rec.amount_residual_company >= comp.obligation_escalate_amount
            old = comp.obligation_escalate_overdue_days and overdue > comp.obligation_escalate_overdue_days
            if not (big or old):
                continue
            rec.write({'escalated_at': now, 'escalated_to': to.id})
            rec.message_post(body=_('Escalada a %s.') % to.name)
            escalated |= rec
        return escalated

    # ── crons ────────────────────────────────────────────────────────

    @api.model
    def _cron_collection(self):
        self._generate_overdue_invoices()
        self._close_by_evidence()
        self._reassign_sat_paid()
        self._escalate()
        return True

    # ── recordatorio diario ──────────────────────────────────────────

    @api.model
    def _digest_rows(self, records):
        """Filas agrupadas por cliente: n, saldo en moneda compañía, vencida más antigua."""
        groups = {}
        for rec in records:
            key = rec.partner_id.id
            g = groups.setdefault(key, {'partner': rec.partner_id.name or '—', 'n': 0, 'amount': 0.0,
                                        'oldest': rec.date_deadline, 'currency': rec.company_currency_id.name})
            g['n'] += 1
            g['amount'] += rec.amount_residual_company or 0.0
            if rec.date_deadline and (not g['oldest'] or rec.date_deadline < g['oldest']):
                g['oldest'] = rec.date_deadline
        return sorted(groups.values(), key=lambda g: -g['amount'])

    @api.model
    def _digest_table(self, rows, today):
        body = ''.join(
            '<tr><td>%s</td><td style="text-align:right">%s</td><td style="text-align:right">%s %s</td>'
            '<td>%s</td><td style="text-align:right">%s</td></tr>' % (
                g['partner'], g['n'], '{:,.0f}'.format(g['amount']), g['currency'], g['oldest'] or '',
                (today - g['oldest']).days if g['oldest'] else '')
            for g in rows) or '<tr><td colspan="5">Ninguna</td></tr>'
        return ('<table border="1" cellpadding="4" cellspacing="0"><tr><th>Cliente</th><th>Obligaciones</th>'
                '<th>Saldo</th><th>Vence desde</th><th>Días</th></tr>%s</table>' % body)

    @api.model
    def _cron_digest(self):
        """Un correo por dueño con lo abierto agrupado por cliente y las candidatas
        por confirmar; otro a Dirección solo con lo escalado."""
        today = fields.Date.context_today(self)
        now = fields.Datetime.now()
        mails = self.env['mail.mail']
        open_recs = self.search([('state', 'in', OPEN_STATES)])
        open_recs._refresh_amounts()
        for owner in open_recs.mapped('user_id'):
            mine = open_recs.filtered(lambda r: r.user_id == owner)
            confirmed = mine.filtered(lambda r: r.state == 'confirmed')
            candidates = mine.filtered(lambda r: r.state == 'candidate')
            due = confirmed.filtered(lambda r: r.date_deadline and r.date_deadline <= fields.Date.add(today, days=2))
            if not (due or candidates):
                continue
            total = sum(due.mapped('amount_residual_company'))
            cand_rows = ''.join(
                '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>' % (
                    r.partner_id.name or '—', r.name, r.date_deadline or '', dict(
                        r._fields['obligation_type']._description_selection(self.env)).get(r.obligation_type))
                for r in candidates) or '<tr><td colspan="4">Ninguna</td></tr>'
            body = (
                '<p>Obligaciones vencidas o por vencer en 48 h: <b>%s</b> por <b>%s %s</b>.</p>%s'
                '<p>Por confirmar (vienen del correo; confirma o descarta en Odoo): <b>%s</b></p>'
                '<table border="1" cellpadding="4" cellspacing="0"><tr><th>Cliente</th><th>Obligación</th>'
                '<th>Vence</th><th>Tipo</th></tr>%s</table>'
                '<p>Detalle en Odoo: Contabilidad → Obligaciones → Mis obligaciones.</p>'
            ) % (len(due), '{:,.0f}'.format(total), self.env.company.currency_id.name,
                 self._digest_table(self._digest_rows(due), today), len(candidates), cand_rows)
            if not owner.email:
                _logger.warning('qb.obligation: %s sin correo, no se manda recordatorio', owner.name)
                continue
            mail = self.env['mail.mail'].sudo().create({
                'subject': _('Cobranza: %(n)s obligaciones abiertas, %(c)s por confirmar') % {
                    'n': len(due), 'c': len(candidates)},
                'email_to': owner.email, 'body_html': body, 'auto_delete': False,
            })
            mail.send()
            mails |= mail
            for rec in (due | candidates):
                rec.write({'times_notified': (rec.times_notified or 0) + 1, 'last_notified_at': now})
        # Escaladas → Dirección
        escalated = open_recs.filtered(lambda r: r.escalated_at and r.escalated_to)
        for to in escalated.mapped('escalated_to'):
            mine = escalated.filtered(lambda r: r.escalated_to == to)
            if not to.email:
                continue
            detail = ''.join(
                '<tr><td>%s</td><td>%s</td><td>%s</td><td style="text-align:right">%s %s</td><td>%s</td><td>%s</td></tr>' % (
                    r.partner_id.name or '—', r.name, r.user_id.name, '{:,.0f}'.format(r.amount_residual_company),
                    r.company_currency_id.name, r.date_deadline or '', (today - r.date_deadline).days if r.date_deadline else '')
                for r in mine.sorted(lambda r: -r.amount_residual_company))
            body = (
                '<p>Obligaciones escaladas (abiertas más de %s días tras confirmarse y por encima del umbral):</p>'
                '<table border="1" cellpadding="4" cellspacing="0"><tr><th>Cliente</th><th>Obligación</th><th>Dueño</th>'
                '<th>Saldo</th><th>Vence</th><th>Días</th></tr>%s</table>'
            ) % (self.env.company.obligation_escalate_days, detail)
            mail = self.env['mail.mail'].sudo().create({
                'subject': _('Dirección: %s obligaciones escaladas') % len(mine),
                'email_to': to.email, 'body_html': body, 'auto_delete': False,
            })
            mail.send()
            mails |= mail
        return mails
