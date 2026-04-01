# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError

class MrpRevisadoWizard(models.TransientModel):
    _name = 'mrp.revisado.wizard'
    _description = 'Wizard de Calidad'

    production_id = fields.Many2one('mrp.production', string='Orden de Fabricación', readonly=True)
    workcenter_id = fields.Many2one(related='production_id.workcenter_id', string="Centro de Trabajo", readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string="Artículo", readonly=True)
    
    rollos_pendientes_text = fields.Text(string="Pendientes", compute="_compute_rollos_pendientes")
    barcode_scan = fields.Char(string='Escanear Rollo')
    lot_id = fields.Many2one('stock.lot', string='ID Rollo', readonly=True)
    
    peso_original = fields.Float(string="Peso Original (Kg)", readonly=True)
    peso_actual = fields.Float(string='Nuevo Peso (Kg)', digits=(12, 4))

    @api.depends('production_id')
    def _compute_rollos_pendientes(self):
        for wizard in self:
            lotes = self.env['stock.lot'].search([
                ('production_id', '=', wizard.production_id.id),
                ('needs_review', '=', True),
                ('is_reviewed', '=', False)
            ])
            wizard.rollos_pendientes_text = ", ".join(lotes.mapped('name')) if lotes else "Ninguno"

    @api.onchange('barcode_scan')
    def _onchange_barcode_scan(self):
        if self.barcode_scan:
            lot = self.env['stock.lot'].search([
                ('name', '=', self.barcode_scan),
                ('production_id', '=', self.production_id.id)
            ], limit=1)
            if lot:
                if not lot.needs_review:
                    raise UserError(_("Este rollo NO requiere revisión."))
                if lot.is_reviewed:
                    raise UserError(_("Este rollo YA fue revisado."))
                self.lot_id = lot
                self.peso_original = lot.product_qty
            else:
                raise UserError(_("Rollo no encontrado."))

    def confirmar_revisado(self):
        self.ensure_one()
        # Aseguramos que el peso actual sea válido
        if self.peso_actual <= 0:
            raise UserError(_("Capture un peso válido mayor a cero. Valor: %s") % self.peso_actual)
        
        # Guardar historial en el Log
        self.env['mrp.revision.log'].create({
            'production_id': self.production_id.id,
            'lot_id': self.lot_id.id,
            'peso_original': self.peso_original,
            'peso_final': self.peso_actual,
        })

        # Actualizar lote y marcar como revisado
        self.lot_id.write({
            'product_qty': self.peso_actual,
            'is_reviewed': True
        })
        return {'type': 'ir.actions.act_window_close'}