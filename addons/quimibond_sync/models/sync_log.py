"""
Sync log model — records each push/pull execution for visibility in Odoo UI.
"""
from odoo import api, fields, models


class SyncLog(models.Model):
    _name = 'quimibond.sync.log'
    _description = 'Quimibond Sync Log'
    _order = 'create_date desc'

    name = fields.Char('Tipo', required=True)
    direction = fields.Selection([
        ('push', 'Push (Odoo → Supabase)'),
        ('pull', 'Pull (Supabase → Odoo)'),
    ], string='Direccion', required=True)
    status = fields.Selection([
        ('success', 'Exitoso'),
        ('error', 'Error'),
    ], string='Estado', required=True)
    summary = fields.Text('Resumen')
    duration_seconds = fields.Float('Duracion (s)')
    create_date = fields.Datetime('Fecha', readonly=True)
