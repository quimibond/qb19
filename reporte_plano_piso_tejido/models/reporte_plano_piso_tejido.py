# -*- coding: utf-8 -*-
from odoo import api, fields, models


class ReportePlanoPisoTejido(models.TransientModel):
    """Reporte tipo Vista: Plano de Piso Tejido.

    Modelo transitorio que toma una "fotografía" (snapshot) del estado
    actual de las órdenes de trabajo de la operación Tejido Circular
    cada vez que se genera el reporte, evitando así reconstruir un
    reporte PDF y permitiendo usar filtros/agrupaciones nativas de la
    vista lista de Odoo.
    """

    _name = 'reporte.plano.piso.tejido'
    _description = 'Plano de Piso Tejido'
    _order = 'product_id, workcenter_id'

    fecha_hora = fields.Datetime(
        string='Fecha y Hora del Reporte', readonly=True)
    production_id = fields.Many2one(
        'mrp.production', string='Orden de Fabricación', readonly=True)
    product_id = fields.Many2one(
        'product.product', string='Producto', readonly=True)
    name = fields.Char(string='Operación', readonly=True)
    workcenter_id = fields.Many2one(
        'mrp.workcenter', string='Centro de Trabajo', readonly=True)
    product_qty = fields.Float(
        string='Cantidad a Producir (OF)', readonly=True,
        digits='Product Unit of Measure')
    qty_produced = fields.Float(
        string='Cantidad Producida', readonly=True,
        digits='Product Unit of Measure')
    qty_remaining = fields.Float(
        string='Cantidad Restante', readonly=True,
        digits='Product Unit of Measure')
    product_uom_id = fields.Many2one(
        'uom.uom', string='Unidad', readonly=True)
    duration_expected = fields.Float(
        string='Duración Esperada (min)', readonly=True)
    duration = fields.Float(
        string='Duración Real (min)', readonly=True)
    state = fields.Char(string='Estado', readonly=True)

    @api.model
    def _get_workorder_domain(self):
        """Dominio de órdenes de trabajo de la operación Tejido Circular
        cuya orden de fabricación está en estatus 'En progreso'."""
        return [
            ('production_id.state', '=', 'progress'),
            ('workcenter_id.name', '=like', 'CIRCULAR%'),
            ('name', '=like', 'TEJIDO%'),
        ]

    @api.model
    def action_generar_reporte(self):
        """Regenera el snapshot del usuario actual y abre la vista lista."""
        # Limpieza del snapshot anterior de este usuario
        self.sudo().search([('create_uid', '=', self.env.uid)]).unlink()

        now = fields.Datetime.now()
        workorders = self.env['mrp.workorder'].search(
            self._get_workorder_domain())

        vals_list = [{
            'fecha_hora': now,
            'production_id': wo.production_id.id,
            'product_id': wo.product_id.id,
            'name': wo.name,
            'workcenter_id': wo.workcenter_id.id,
            'product_qty': wo.production_id.product_qty,
            'qty_produced': wo.qty_produced,
            'qty_remaining': wo.qty_remaining,
            'product_uom_id': wo.production_id.product_uom_id.id,
            'duration_expected': wo.duration_expected,
            'duration': wo.duration,
            'state': wo.state,
        } for wo in workorders]

        records = self.create(vals_list)

        return {
            'type': 'ir.actions.act_window',
            'name': 'Plano de Piso Tejido',
            'res_model': 'reporte.plano.piso.tejido',
            'view_mode': 'list',
            'target': 'current',
            'domain': [('id', 'in', records.ids)],
            'context': {'create': False, 'edit': False},
        }
