import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class IntelligenceActionItem(models.Model):
    _name = 'intelligence.action.item'
    _description = 'Intelligence Action Item'
    _order = 'priority_seq, due_date, create_date desc'
    _inherit = ['mail.thread']

    name = fields.Char(string='Descripcion', required=True)
    action_type = fields.Selection([
        ('call', 'Llamar'),
        ('email', 'Enviar email'),
        ('meeting', 'Reunion'),
        ('follow_up', 'Seguimiento'),
        ('send_quote', 'Enviar cotizacion'),
        ('send_invoice', 'Enviar factura'),
        ('review', 'Revisar'),
        ('approve', 'Aprobar'),
        ('deliver', 'Entregar'),
        ('pay', 'Pagar'),
        ('investigate', 'Investigar'),
        ('other', 'Otro'),
    ], string='Tipo', default='other')
    priority = fields.Selection([
        ('low', 'Baja'),
        ('medium', 'Media'),
        ('high', 'Alta'),
        ('critical', 'Critica'),
    ], string='Prioridad', default='medium', tracking=True)
    priority_seq = fields.Integer(
        compute='_compute_priority_seq', store=True)
    state = fields.Selection([
        ('open', 'Abierto'),
        ('in_progress', 'En progreso'),
        ('done', 'Completado'),
        ('cancelled', 'Cancelado'),
    ], string='Estado', default='open', tracking=True)
    due_date = fields.Date(string='Fecha limite')
    is_overdue = fields.Boolean(
        compute='_compute_is_overdue', store=True)
    partner_id = fields.Many2one(
        'res.partner', string='Contacto relacionado', index=True)
    assignee_id = fields.Many2one(
        'res.users', string='Asignado a',
        default=lambda self: self.env.user)
    source_date = fields.Date(string='Detectado en')
    source_account = fields.Char(string='Cuenta origen')
    supabase_id = fields.Integer(
        string='Supabase ID', index=True, copy=False)
    notes = fields.Text(string='Notas')

    @api.depends('priority')
    def _compute_priority_seq(self):
        mapping = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        for rec in self:
            rec.priority_seq = mapping.get(rec.priority, 2)

    @api.depends('due_date', 'state')
    def _compute_is_overdue(self):
        today = fields.Date.today()
        for rec in self:
            rec.is_overdue = (
                rec.state in ('open', 'in_progress')
                and rec.due_date
                and rec.due_date < today
            )

    def action_start(self):
        self.write({'state': 'in_progress'})

    def action_done(self):
        self.write({'state': 'done'})
        self._sync_to_supabase()

    def action_cancel(self):
        self.write({'state': 'cancelled'})
        self._sync_to_supabase()

    def action_reopen(self):
        self.write({'state': 'open'})

    def _sync_to_supabase(self):
        """Sync action completion/cancellation to Supabase."""
        get = lambda k, d='': (
            self.env['ir.config_parameter'].sudo()
            .get_param('quimibond_intelligence.%s' % k, d)
        )
        url = get('supabase_url')
        key = get('supabase_service_role_key') or get('supabase_key')
        if not url or not key:
            return
        try:
            from ..services.supabase_service import SupabaseService
            supa = SupabaseService(url, key)
            for rec in self:
                if rec.supabase_id:
                    supa.complete_action_item(rec.supabase_id)
        except Exception as exc:
            _logger.debug('Action sync to Supabase: %s', exc)
