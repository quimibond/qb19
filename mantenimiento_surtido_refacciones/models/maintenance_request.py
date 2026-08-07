# -*- coding: utf-8 -*-
from odoo import api, fields, models
from odoo.exceptions import UserError


class MaintenanceRequest(models.Model):
    """Extiende maintenance.request para permitir dar de alta las
    refacciones requeridas y generar, con un botón, el traslado interno
    (Regla de Negocio 2 y 4) que el almacén debe surtir.
    """
    _inherit = 'maintenance.request'

    refaccion_line_ids = fields.One2many(
        'maintenance.refaccion.line', 'request_id',
        string='Refacciones Requeridas')

    location_mantto_id = fields.Many2one(
        'stock.location', string='Ubicación de Refacciones (Origen)',
        default=lambda self: self._default_location_mantto_id(),
        help='Ubicación de almacén donde se resguardan las refacciones '
             '(p. ej. Toluca Varios TVAR/MTTO).')
    location_consumo_id = fields.Many2one(
        'stock.location', string='Ubicación de Consumo Mantenimiento (Destino)',
        default=lambda self: self._default_location_consumo_id(),
        help='Ubicación virtual donde se registra el consumo de '
             'refacciones para efectos de costeo del equipo.')

    picking_refaccion_ids = fields.One2many(
        'stock.picking', 'maintenance_request_id', string='Traslados de Refacciones')
    picking_refaccion_count = fields.Integer(
        string='# Traslados', compute='_compute_picking_refaccion_count')

    costo_total_refacciones = fields.Monetary(
        string='Costo Total de Refacciones', compute='_compute_costo_total_refacciones',
        currency_field='company_currency_id', store=True)
    company_currency_id = fields.Many2one(
        related='company_id.currency_id', string='Moneda')

    @api.model
    def _default_location_mantto_id(self):
        param = self.env['ir.config_parameter'].sudo().get_param(
            'mantenimiento_surtido_refacciones.location_mantto_id')
        return int(param) if param else False

    @api.model
    def _default_location_consumo_id(self):
        param = self.env['ir.config_parameter'].sudo().get_param(
            'mantenimiento_surtido_refacciones.location_consumo_id')
        return int(param) if param else False

    @api.depends('picking_refaccion_ids')
    def _compute_picking_refaccion_count(self):
        for rec in self:
            rec.picking_refaccion_count = len(rec.picking_refaccion_ids)

    @api.depends('refaccion_line_ids.subtotal_cost')
    def _compute_costo_total_refacciones(self):
        for rec in self:
            rec.costo_total_refacciones = sum(rec.refaccion_line_ids.mapped('subtotal_cost'))

    def _get_picking_type_refacciones(self):
        """Regla de Negocio 2: localiza el tipo de operación de traslado
        interno dedicado al surtido de refacciones. Primero busca uno
        cuya ubicación origen por defecto sea la de MANTTO; si no existe,
        recurre al tipo de operación de datos del módulo.
        """
        self.ensure_one()
        picking_type = self.env['stock.picking.type'].search([
            ('code', '=', 'internal'),
            ('warehouse_id.company_id', '=', self.company_id.id),
            ('default_location_src_id', '=', self.location_mantto_id.id),
        ], limit=1)
        if not picking_type:
            picking_type = self.env.ref(
                'mantenimiento_surtido_refacciones.picking_type_refacciones_mantto',
                raise_if_not_found=False)
        return picking_type

    def action_generar_requerimiento_refacciones(self):
        """Regla de Negocio 4: genera el traslado interno (requisición)
        de refacciones desde MANTTO hacia Consumo Mantenimiento, con una
        línea de movimiento por cada refacción dada de alta.
        """
        self.ensure_one()
        if not self.refaccion_line_ids:
            raise UserError('Agregue al menos una refacción antes de generar el requerimiento.')
        if not self.location_mantto_id or not self.location_consumo_id:
            raise UserError(
                'Configure la ubicación de origen (MANTTO) y la ubicación de '
                'destino (Consumo Mantenimiento) en Ajustes > Inventario, o '
                'directamente en la solicitud.')

        picking_type = self._get_picking_type_refacciones()
        if not picking_type:
            raise UserError(
                'No se encontró un tipo de operación de traslado interno '
                'configurado para la ubicación de refacciones.')

        move_vals = [(0, 0, {
            'name': line.product_id.display_name,
            'product_id': line.product_id.id,
            'product_uom_qty': line.quantity,
            'product_uom': line.product_uom_id.id,
            'location_id': self.location_mantto_id.id,
            'location_dest_id': self.location_consumo_id.id,
        }) for line in self.refaccion_line_ids]

        picking = self.env['stock.picking'].create({
            'picking_type_id': picking_type.id,
            'location_id': self.location_mantto_id.id,
            'location_dest_id': self.location_consumo_id.id,
            'origin': self.name,
            'maintenance_request_id': self.id,
            'move_ids': move_vals,
        })
        picking.action_confirm()
        picking.action_assign()

        # Enlaza cada línea con su movimiento generado, en el mismo orden
        # en que fueron creados (no se reordena en action_confirm).
        for line, move in zip(self.refaccion_line_ids, picking.move_ids):
            line.move_id = move.id

        return {
            'type': 'ir.actions.act_window',
            'name': 'Traslado de Refacciones',
            'res_model': 'stock.picking',
            'view_mode': 'form',
            'res_id': picking.id,
        }

    def action_ver_pickings_refacciones(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Traslados de Refacciones',
            'res_model': 'stock.picking',
            'view_mode': 'list,form',
            'domain': [('maintenance_request_id', '=', self.id)],
        }
