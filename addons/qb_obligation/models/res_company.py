# -*- coding: utf-8 -*-
from odoo import fields, models


class ResCompany(models.Model):
    _inherit = 'res.company'

    obligation_collection_user_id = fields.Many2one(
        'res.users', string='Dueño Finanzas / cobranza (default)',
        help='Dueño de las obligaciones de finanzas (p.ej. promesas de pago del correo) cuando la memoria '
             'no aprendió uno y el contacto no tiene dueño de cobranza propio.')
    obligation_owner_comercial_id = fields.Many2one(
        'res.users', string='Dueño Comercial (default)',
        help='Dueño de las obligaciones comerciales que llegan sin buzón reconocible.')
    obligation_owner_operaciones_id = fields.Many2one('res.users', string='Dueño Operaciones (default)')
    obligation_owner_compras_id = fields.Many2one('res.users', string='Dueño Compras (default)')
    obligation_owner_sgi_id = fields.Many2one('res.users', string='Dueño SGI (default)')
    obligation_owner_rh_id = fields.Many2one('res.users', string='Dueño RH (default)')
    obligation_owner_otro_id = fields.Many2one('res.users', string='Dueño otros (default)')
    obligation_escalation_user_id = fields.Many2one(
        'res.users', string='Escalar a (Dirección)',
        help='Recibe el correo de obligaciones escaladas. Vacío: no se escala.')
    obligation_residual_tolerance = fields.Float(
        string='Tolerancia de saldo', default=1.0, digits=(16, 2),
        help='Saldo (en la moneda de la factura) por debajo del cual la factura se considera cobrada.')
    obligation_escalate_days = fields.Integer(
        string='Escalar después de (días)', default=3,
        help='Días abiertos desde la confirmación antes de escalar. El reloj no corre desde el vencimiento.')
    obligation_escalate_amount = fields.Monetary(
        string='Escalar si el saldo es mayor a', default=100000.0, currency_field='currency_id')
    obligation_escalate_overdue_days = fields.Integer(
        string='Escalar si lleva vencida más de (días)', default=30)
