# -*- coding: utf-8 -*-
from odoo import fields, models


class SatSyncLog(models.Model):
    _name = 'sat.sync.log'
    _description = 'Bitácora de sincronización con Syntage'
    _order = 'id desc'

    name = fields.Char(required=True)
    kind = fields.Selection([
        ('pull', 'Descarga por API'),
        ('extraction', 'Extracción solicitada'),
        ('match', 'Cruce con Odoo'),
    ], required=True)
    company_id = fields.Many2one('res.company', string='Compañía', index=True)
    status = fields.Selection([
        ('queued', 'En cola'),
        ('running', 'Corriendo'),
        ('success', 'OK'),
        ('partial', 'Parcial'),
        ('error', 'Error'),
    ], default='success', required=True, index=True)
    mode = fields.Selection([('pull', 'Descarga por API'), ('extraction', 'Extracción')], default='pull')
    include_retentions = fields.Boolean(default=False)
    summary = fields.Text()
    date_from = fields.Date(string='Desde')
    date_to = fields.Date(string='Hasta')
    items_fetched = fields.Integer(string='Recibidos')
    items_upserted = fields.Integer(string='Guardados')
    items_errored = fields.Integer(string='Con error')
    duration_seconds = fields.Float(string='Duración (s)')
