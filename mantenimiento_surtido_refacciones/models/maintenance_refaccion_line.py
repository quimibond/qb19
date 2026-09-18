# -*- coding: utf-8 -*-
from odoo import api, fields, models
from odoo.exceptions import ValidationError


class MaintenanceRefaccionLine(models.Model):
    """Regla de Negocio 3:
    Cada línea representa una refacción y cantidad requerida dentro de
    una solicitud de mantenimiento. Calcula, sin necesidad de guardar
    la solicitud, la disponibilidad en la ubicación MANTTO y el costo
    (unitario y total) de la refacción.
    """
    _name = 'maintenance.refaccion.line'
    _description = 'Línea de Refacción Requerida en Solicitud de Mantenimiento'

    request_id = fields.Many2one(
        'maintenance.request', string='Solicitud de Mantenimiento',
        required=True, ondelete='cascade')
    product_id = fields.Many2one(
        'product.product', string='Refacción', required=True,
        domain="[('type', 'in', ('product', 'consu'))]")
    product_uom_id = fields.Many2one(
        'uom.uom', string='UdM', related='product_id.uom_id',
        store=True, readonly=True)
    quantity = fields.Float(string='Cantidad Requerida', default=1.0, required=True)

    location_mantto_id = fields.Many2one(
        'stock.location', string='Ubicación Origen',
        related='request_id.location_mantto_id', store=False)
    qty_available_mantto = fields.Float(
        string='Disponible en MANTTO', compute='_compute_qty_available_mantto',
        help='Cantidad a la mano de esta refacción en la ubicación de refacciones '
             '(MANTTO) al momento de consultar la solicitud.')
    disponible = fields.Boolean(
        string='¿Alcanza?', compute='_compute_qty_available_mantto',
        help='Indica si la cantidad disponible en MANTTO cubre la cantidad requerida.')

    unit_cost = fields.Float(string='Costo Unitario', compute='_compute_costos')
    subtotal_cost = fields.Monetary(
        string='Costo Total', compute='_compute_costos',
        currency_field='company_currency_id')
    company_currency_id = fields.Many2one(
        related='request_id.company_id.currency_id', string='Moneda')

    move_id = fields.Many2one(
        'stock.move', string='Movimiento Generado', readonly=True, copy=False,
        help='Movimiento de inventario generado al presionar '
             '"Generar Requerimiento de Surtido".')

    @api.depends('product_id', 'quantity', 'request_id.location_mantto_id')
    def _compute_qty_available_mantto(self):
        for line in self:
            if line.product_id and line.request_id.location_mantto_id:
                qty = line.product_id.with_context(
                    location=line.request_id.location_mantto_id.id
                ).qty_available
            else:
                qty = 0.0
            line.qty_available_mantto = qty
            line.disponible = qty >= line.quantity

    @api.depends('product_id', 'quantity')
    def _compute_costos(self):
        for line in self:
            cost = line.product_id.standard_price if line.product_id else 0.0
            line.unit_cost = cost
            line.subtotal_cost = cost * line.quantity

    @api.constrains('quantity')
    def _check_quantity_positive(self):
        for line in self:
            if line.quantity <= 0:
                raise ValidationError(
                    'La cantidad requerida de "%s" debe ser mayor a cero.'
                    % line.product_id.display_name)
