# -*- coding: utf-8 -*-
"""Obligaciones vivas. Ver README.

Una obligación es un compromiso con cinco datos (qué, quién, sobre qué
documento, cómo se prueba, cuándo vence) que puede nacer de Odoo, del correo
(memoria en Supabase), de un resumen, del gabinete o a mano, en cualquier área.
Reglas que no se negocian: el dueño es un res.users; el cierre por evidencia es
una consulta, nunca un juicio de un modelo; lo que nace del correo espera la
confirmación del dueño. La cobranza NO vive aquí: las facturas vencidas ya
están en Contabilidad.

La obligación no es una app aparte: cada obligación abierta es una actividad
nativa de Odoo (mail.activity) sobre su documento ancla o su contacto, con el
dueño y la fecha. Marcarla hecha cierra la obligación por acuse; cancelarla la
descarta; y cuando la evidencia la cierra, la actividad se marca hecha sola.
"""
import logging
from datetime import timedelta

from markupsafe import escape

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

OPEN_STATES = ('candidate', 'confirmed')
PAID_STATES = ('paid', 'in_payment', 'reversed')

AREAS = [
    ('comercial', 'Comercial'), ('operaciones', 'Operaciones'), ('compras', 'Compras'),
    ('finanzas', 'Finanzas'), ('sgi', 'SGI'), ('rh', 'RH'), ('otro', 'Otro'),
]
# Prefijo del tipo → área. Los tipos de cobranza conservan su clave histórica.
AREA_BY_PREFIX = {'collection': 'finanzas'}
# Tipo detectado en el correo (email_pending_actions.tipo) → tipo de obligación.
EMAIL_TYPE_MAP = {
    'promesa_pago': 'collection.payment_promise',
    'compromiso_entrega': 'comercial.delivery_commitment',
    'cotizacion': 'comercial.quote',
    'solicitud_documento': 'comercial.document_request',
    'rfq': 'compras.rfq',
}
EMAIL_SOURCE_PREFIX = 'supabase:email_pending_actions:'


class QbObligation(models.Model):
    _name = 'qb.obligation'
    _description = 'Obligación'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'date_deadline asc, id asc'

    # ── identidad ────────────────────────────────────────────────────
    name = fields.Char(required=True)
    obligation_type = fields.Selection([
        # Comercial
        ('comercial.delivery_commitment', 'Cumplir compromiso de entrega'),
        ('comercial.quote', 'Enviar cotización pendiente'),
        ('comercial.document_request', 'Enviar documento solicitado'),
        ('comercial.reply', 'Responder al cliente'),
        # Operaciones
        ('operaciones.delivery', 'Entregar pedido'),
        ('operaciones.complaint', 'Atender reclamación'),
        # Compras
        ('compras.rfq', 'Cotizar con proveedor'),
        ('compras.receipt', 'Recibir compra pendiente'),
        # Finanzas (clave histórica 'collection')
        ('collection.payment_promise', 'Promesa de pago (correo)'),
        # SGI / RH / otros
        ('sgi.record', 'Registro o acuse SGI'),
        ('rh.file', 'Expediente o acuse RH'),
        ('otro.generic', 'Otro compromiso'),
    ], required=True, index=True)
    area = fields.Selection(AREAS, compute='_compute_area', store=True, index=True)
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
        ('so_delivered', 'Pedido de venta entregado por completo'),
        ('po_received', 'Orden de compra recibida por completo'),
        ('email_resolved', 'El pendiente quedó resuelto en el correo (memoria)'),
        ('owner_ack', 'Acuse del dueño'),
    ], string='Cómo se prueba', required=True, default='owner_ack')
    date_deadline = fields.Date(string='Vence', required=True, index=True)

    # ── espejo en actividades de Odoo ────────────────────────────────
    activity_id = fields.Many2one('mail.activity', string='Actividad', ondelete='set null', copy=False,
                                  help='Actividad nativa que representa esta obligación mientras está abierta.')

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

    @api.depends('obligation_type')
    def _compute_area(self):
        areas = dict(AREAS)
        for rec in self:
            prefix = (rec.obligation_type or '').split('.')[0]
            prefix = AREA_BY_PREFIX.get(prefix, prefix)
            rec.area = prefix if prefix in areas else 'otro'

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
        # sudo: el campo es company-dependent y el cron o el usuario pueden no tener
        # acceso a todas las compañías; leer el dueño no expone nada más.
        partner = partner.commercial_partner_id.sudo() if partner else partner
        owner = partner.with_company(company).collection_user_id if partner else self.env['res.users']
        return owner or company.sudo().obligation_collection_user_id

    @api.model
    def _area_of_type(self, obligation_type):
        prefix = (obligation_type or '').split('.')[0]
        prefix = AREA_BY_PREFIX.get(prefix, prefix)
        return prefix if prefix in dict(AREAS) else 'otro'

    @api.model
    def _default_owner(self, partner, company, obligation_type=None):
        """Dueño por defecto, en orden: lo que la memoria aprendió del correo
        para ese contacto y área (qb_memoria), el mapa de cobranza si es
        finanzas, y el dueño del área configurado en la compañía."""
        area = self._area_of_type(obligation_type)
        if partner and hasattr(partner, 'memoria_owner_for'):
            learned = partner.sudo().memoria_owner_for(area)
            if learned:
                return learned
        if area == 'finanzas':
            return self._collection_owner(partner, company)
        field = 'obligation_owner_%s_id' % area
        company = company.sudo()
        return company[field] if field in company._fields else self.env['res.users']

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

    # ── espejo en actividades nativas ────────────────────────────────

    ACTIVITY_XMLID = 'qb_obligation.mail_activity_type_obligation'
    ACTIVITY_FIELDS = ('state', 'user_id', 'date_deadline', 'name', 'description', 'evidence_rule_key')

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        records._activity_sync()
        return records

    def write(self, vals):
        res = super().write(vals)
        if any(k in vals for k in self.ACTIVITY_FIELDS):
            self._activity_sync()
        return res

    def _activity_targets(self):
        """Dónde puede vivir la actividad, en orden: el documento ancla (si
        tiene actividades), el contacto, la propia obligación."""
        self.ensure_one()
        targets = []
        anchor = self._anchor()
        if anchor and 'activity_ids' in anchor._fields and self._owner_can_read(anchor):
            targets.append(anchor)
        if self.partner_id:
            targets.append(self.partner_id)
        targets.append(self)
        return targets

    def _owner_can_read(self, record):
        """El dueño tiene que poder abrir el documento donde vive su actividad;
        si no (p.ej. ventas sin acceso a facturas), la actividad va al contacto."""
        try:
            return record.with_user(self.user_id).has_access('read')
        except Exception:  # noqa: BLE001 — ante la duda, al contacto
            return False

    def _activity_summary(self):
        self.ensure_one()
        return ('%s%s' % (_('Por confirmar: ') if self.state == 'candidate' else '', self.name))[:200]

    def _activity_note(self):
        self.ensure_one()
        types = dict(self._fields['obligation_type']._description_selection(self.env))
        rules = dict(self._fields['evidence_rule_key']._description_selection(self.env))
        lines = [escape(self.description or ''),
                 _('Tipo: %s') % escape(types.get(self.obligation_type, self.obligation_type or '')),
                 _('Se cierra sola cuando: %s') % escape(rules.get(self.evidence_rule_key, ''))]
        if self.state == 'candidate':
            lines.append(_('Detectada en el correo, falta que la confirmes: si no aplica, cancela esta actividad; '
                           'si ya se hizo, márcala hecha.'))
        else:
            lines.append(_('Si ya se hizo y Odoo no lo puede comprobar solo, marca esta actividad hecha.'))
        lines.append('<a href="/web#id=%s&amp;model=qb.obligation&amp;view_type=form">%s</a>' % (
            self.id, _('Ver la obligación')))
        return '<p>%s</p>' % '</p><p>'.join(str(x) for x in lines)

    def _activity_sync(self):
        """Una actividad por obligación abierta, en su documento o contacto,
        con el dueño y la fecha. Cerrada por evidencia o acuse → la actividad
        se marca hecha; descartada o cancelada → se quita. Nunca tumba la
        operación que la disparó."""
        if self.env.context.get('qb_obligation_skip_activity'):
            return
        act_type = self.env.ref(self.ACTIVITY_XMLID, raise_if_not_found=False)
        for rec in self.with_context(qb_obligation_skip_activity=True):
            try:
                with self.env.cr.savepoint():
                    rec._activity_sync_one(act_type)
            except Exception:  # noqa: BLE001 — el espejo no puede romper la obligación
                _logger.exception('qb.obligation %s: no se pudo sincronizar la actividad', rec.id)

    def _activity_sync_one(self, act_type):
        self.ensure_one()
        activity = self.activity_id.exists() if self.activity_id else self.env['mail.activity']
        if self.state not in OPEN_STATES:
            if activity:
                if self.state == 'done':
                    activity.sudo()._action_done(feedback=self.evidence_summary or _('Cumplida'))
                else:
                    activity.sudo().unlink()
            return
        vals = {'user_id': self.user_id.id, 'date_deadline': self.date_deadline,
                'summary': self._activity_summary(), 'note': self._activity_note()}
        if activity:
            if (activity.user_id.id != vals['user_id'] or activity.date_deadline != vals['date_deadline']
                    or (activity.summary or '') != vals['summary']):
                activity.sudo().write(vals)
            return
        if not act_type:
            return
        for target in self._activity_targets():
            try:
                with self.env.cr.savepoint():
                    activity = target.sudo().activity_schedule(act_type_xmlid=self.ACTIVITY_XMLID, **vals)
                break
            except Exception:  # noqa: BLE001 — p.ej. el dueño no tiene acceso al documento
                _logger.info('qb.obligation %s: sin actividad en %s, se prueba el siguiente destino',
                             self.id, target._name)
                activity = self.env['mail.activity']
        if activity:
            super(QbObligation, self).write({'activity_id': activity.id})

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
                                if k in ('description', 'date_deadline', 'detection_payload', 'weak_key') and (v or k != 'date_deadline')})
                return existing
        company = self.env['res.company'].browse(vals.get('company_id')) if vals.get('company_id') else self.env.company
        partner = self.env['res.partner'].browse(vals['partner_id']) if vals.get('partner_id') else None
        if not vals.get('user_id'):
            owner = self._default_owner(partner, company, vals.get('obligation_type'))
            if not owner:
                _logger.info('qb.obligation: sin dueño para %s en %s, no se crea candidata', partner and partner.name, company.name)
                return self.browse()
            vals['user_id'] = owner.id
        if not vals.get('date_deadline'):
            vals['date_deadline'] = fields.Date.add(fields.Date.context_today(self), days=3)
            vals['weak_key'] = True
        vals.setdefault('company_id', company.id)
        vals.setdefault('escalate_after_days', company.sudo().obligation_escalate_days)
        vals.setdefault('source', 'email')
        vals.setdefault('state', 'candidate')
        vals.setdefault('evidence_rule_key', 'invoice_paid_or_credited' if vals.get('res_model') == 'account.move'
                        else 'email_resolved' if (vals.get('source_ref') or '').startswith(EMAIL_SOURCE_PREFIX)
                        else 'owner_ack')
        vals.setdefault('name', (vals.get('description') or '')[:80] or 'Obligación')
        if partner:
            vals['partner_id'] = partner.commercial_partner_id.id
        rec = self.create(vals)
        rec._refresh_amounts()
        return rec

    # ── cierre por evidencia ─────────────────────────────────────────

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
        """Factura ancla con saldo dentro de tolerancia o estado de pago pagado /
        en proceso / revertido → cumplida. Factura cancelada o bloqueada →
        cancelada. Luego los documentos (pedido entregado, compra recibida)."""
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
        closed |= self._close_documents_done()
        if closed or cancelled:
            _logger.info('qb.obligation: %s cerradas por evidencia, %s canceladas', len(closed), len(cancelled))
        return closed

    @api.model
    def _close_documents_done(self):
        """Pedido entregado por completo / compra recibida por completo: la
        evidencia es el estado del documento en Odoo (sale y purchase son
        opcionales: si no están instalados la regla no corre)."""
        closed = self.browse()
        rules = (
            ('so_delivered', 'sale.order', 'delivery_status', 'full', _('Pedido %s entregado por completo')),
            ('po_received', 'purchase.order', 'receipt_status', 'full', _('Compra %s recibida por completo')),
        )
        for key, model, field, value, text in rules:
            if model not in self.env or field not in self.env[model]._fields:
                continue
            for rec in self.search([('evidence_rule_key', '=', key), ('state', 'in', OPEN_STATES),
                                    ('res_model', '=', model)]):
                doc = rec._anchor()
                if doc is None or not doc:
                    rec.write({'state': 'cancelled'})
                    continue
                if doc.state == 'cancel':
                    rec.message_post(body=_('Documento %s cancelado en Odoo.') % doc.display_name)
                    rec.write({'state': 'cancelled'})
                elif doc[field] == value:
                    rec._close('evidence', by_cron=True, evidence=doc, summary=text % doc.display_name)
                    closed |= rec
        return closed

    # ── correo: candidatas desde la memoria (Supabase) ───────────────

    @api.model
    def _email_partner(self, client, company_ids):
        """Empresa de la memoria → partner de Odoo (por odoo_partner_id, RFC o nombre)."""
        result = {}
        ids = sorted({int(c) for c in company_ids if c})
        if not ids:
            return result
        Partner = self.env['res.partner'].sudo()
        for i in range(0, len(ids), 200):
            chunk = ids[i:i + 200]
            rows = client.get('companies', {'select': 'id,odoo_partner_id,rfc,name',
                                            'id': 'in.(%s)' % ','.join(str(x) for x in chunk)})
            for row in rows:
                partner = Partner
                if row.get('odoo_partner_id'):
                    partner = Partner.browse(int(row['odoo_partner_id'])).exists()
                if not partner and row.get('rfc'):
                    partner = Partner.search([('vat', '=ilike', row['rfc'])], limit=1)
                if not partner and row.get('name'):
                    partner = Partner.search([('is_company', '=', True), ('name', '=ilike', row['name'])], limit=1)
                if partner:
                    result[int(row['id'])] = partner.commercial_partner_id
        return result

    @api.model
    def _email_owner(self, account, partner, company, obligation_type):
        """El buzón que recibió el correo es el dueño natural; si no es un
        usuario de Odoo, el dueño del área."""
        Users = self.env['res.users'].sudo()
        account = (account or '').strip().lower()
        owner = Users
        if account:
            owner = Users.search(['|', ('login', '=ilike', account), ('email', '=ilike', account),
                                  ('share', '=', False), ('active', '=', True)], limit=1)
        return owner or self._default_owner(partner, company, obligation_type)

    @api.model
    def _sync_email_pending(self, limit=500):
        """Trae los pendientes abiertos detectados en el correo como candidatas
        (idempotente por id) y cierra o cancela los que la memoria ya marcó
        resueltos o expirados. Devuelve (creadas, cerradas, canceladas)."""
        if 'qb.memoria.client' not in self.env:
            return self.browse(), self.browse(), self.browse()
        client = self.env['qb.memoria.client']
        company = self.env.company
        rows = client.get('email_pending_actions', {
            'select': 'id,thread_id,tipo,descripcion,deadline,company_id,company_name,account,detected_at,status',
            'status': 'eq.open', 'order': 'id.asc', 'limit': limit})
        partners = self._email_partner(client, [r.get('company_id') for r in rows])
        refs = ['%s%s' % (EMAIL_SOURCE_PREFIX, r['id']) for r in rows]
        known = set(self.search([('source_ref', 'in', refs)]).mapped('source_ref'))
        created = self.browse()
        for row in rows:
            otype = EMAIL_TYPE_MAP.get(row.get('tipo') or '', 'otro.generic')
            partner = partners.get(int(row['company_id'])) if row.get('company_id') else None
            owner = self._email_owner(row.get('account'), partner, company, otype)
            if not owner:
                _logger.info('qb.obligation: pendiente de correo %s sin dueño (buzón %s), se omite',
                             row.get('id'), row.get('account'))
                continue
            desc = (row.get('descripcion') or '').strip()
            vals = {
                'obligation_type': otype, 'description': desc or _('Pendiente detectado en el correo'),
                'name': (desc[:80] or _('Pendiente de correo')),
                'partner_id': partner.id if partner else False, 'user_id': owner.id,
                'company_id': company.id, 'source': 'email',
                'source_ref': '%s%s' % (EMAIL_SOURCE_PREFIX, row['id']),
                'source_thread_key': str(row.get('thread_id') or ''),
                'detection_payload': row, 'date_deadline': row.get('deadline') or False,
                'evidence_rule_key': 'email_resolved',
            }
            rec = self.create_candidate(vals)
            if rec and vals['source_ref'] not in known:
                created |= rec
        # Cierre: lo que la memoria ya dio por resuelto o expirado
        closed = self.browse()
        cancelled = self.browse()
        opened = self.search([('source_ref', '=like', EMAIL_SOURCE_PREFIX + '%'), ('state', 'in', OPEN_STATES),
                              ('evidence_rule_key', '=', 'email_resolved')])
        by_ref = {int(r.source_ref[len(EMAIL_SOURCE_PREFIX):]): r for r in opened
                  if r.source_ref[len(EMAIL_SOURCE_PREFIX):].isdigit()}
        ids = sorted(by_ref)
        for i in range(0, len(ids), 200):
            chunk = ids[i:i + 200]
            for row in client.get('email_pending_actions', {
                    'select': 'id,status,resolved_at', 'id': 'in.(%s)' % ','.join(str(x) for x in chunk),
                    'status': 'in.(resolved,expired)'}):
                rec = by_ref.get(int(row['id']))
                if not rec:
                    continue
                if row.get('status') == 'resolved':
                    rec._close('evidence', by_cron=True,
                               summary=_('Resuelto en el correo el %s') % (row.get('resolved_at') or '')[:10])
                    closed |= rec
                else:
                    rec.message_post(body=_('El pendiente expiró en la memoria sin resolverse.'))
                    rec.write({'state': 'cancelled'})
                    cancelled |= rec
        _logger.info('qb.obligation: correo: %s candidatas nuevas, %s cerradas, %s canceladas',
                     len(created), len(closed), len(cancelled))
        return created, closed, cancelled

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
    def _cron_obligations(self):
        """Corrida horaria de todas las áreas: el correo propone, la evidencia
        cierra, el tiempo escala. La red no tumba la corrida."""
        try:
            with self.env.cr.savepoint():
                self._sync_email_pending()
        except Exception:  # noqa: BLE001 — la memoria puede no responder
            _logger.exception('qb.obligation: no se pudieron traer los pendientes del correo')
        self._close_by_evidence()
        self._escalate()
        self.search([('state', 'in', OPEN_STATES), ('activity_id', '=', False)])._activity_sync()
        return True

    # El cron de producción (noupdate) sigue llamando al nombre del piloto.
    _cron_collection = _cron_obligations

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
                '<p>Cada obligación es una actividad en Odoo (reloj arriba a la derecha); el detalle está en Contactos → Obligaciones.</p>'
            ) % (len(due), '{:,.0f}'.format(total), self.env.company.currency_id.name,
                 self._digest_table(self._digest_rows(due), today), len(candidates), cand_rows)
            if not owner.email:
                _logger.warning('qb.obligation: %s sin correo, no se manda recordatorio', owner.name)
                continue
            mail = self.env['mail.mail'].sudo().create({
                'subject': _('Obligaciones: %(n)s vencidas o por vencer, %(c)s por confirmar') % {
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


class MailActivity(models.Model):
    _inherit = 'mail.activity'

    def _obligations_open(self):
        if self.env.context.get('qb_obligation_skip_activity'):
            return self.env['qb.obligation']
        # sudo: cualquier usuario cierra sus actividades; la obligación es
        # contabilidad interna del sistema, no un acceso que pida el usuario.
        return self.env['qb.obligation'].sudo().search([('activity_id', 'in', self.ids), ('state', 'in', OPEN_STATES)])

    def _action_done(self, feedback=False, attachment_ids=None):
        """Marcar hecha la actividad = acuse del dueño sobre la obligación."""
        obligations = self._obligations_open()
        if obligations:
            summary = _('Acuse de %s desde la actividad') % self.env.user.name
            if feedback:
                summary = '%s: %s' % (summary, feedback)
            obligations.with_context(qb_obligation_skip_activity=True)._close('owner_ack', summary=summary[:500])
        return super()._action_done(feedback=feedback, attachment_ids=attachment_ids)

    def unlink(self):
        """Cancelar la actividad = descartar la obligación (pegajoso)."""
        obligations = self._obligations_open()
        if obligations:
            obligations.with_context(qb_obligation_skip_activity=True).write({
                'state': 'discarded', 'discarded_by': self.env.user.id, 'discarded_at': fields.Datetime.now(),
                'discard_reason': _('Actividad cancelada por %s') % self.env.user.name})
        return super().unlink()
