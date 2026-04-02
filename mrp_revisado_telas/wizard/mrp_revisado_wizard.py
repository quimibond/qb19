# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError

class MrpRevisadoWizard(models.TransientModel):
    _name = 'mrp.revisado.wizard'
    _description = 'Wizard de Revisado de Calidad'

    # Campos de contexto y datos de la MO
    production_id = fields.Many2one('mrp.production', string='Orden de Fabricación', readonly=True)
    workcenter_id = fields.Many2one(related='production_id.workcenter_id', string="Centro de Trabajo", readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string="Artículo", readonly=True)
    
    # Información visual para el usuario
    rollos_pendientes_text = fields.Text(string="Pendientes", compute="_compute_rollos_pendientes")
    barcode_scan = fields.Char(string='Escanear Rollo')
    
    # Campos de datos del rollo (Clave para la persistencia)
    lot_id = fields.Many2one('stock.lot', string='ID Rollo')
    peso_original = fields.Float(string="Peso Original (Kg)", readonly=True)
    peso_actual = fields.Float(string='Nuevo Peso (Kg)', digits=(12, 4))

    @api.depends('production_id')
    def _compute_rollos_pendientes(self):
        """ Calcula qué rollos sorteados faltan por revisar para mostrar en el banner amarillo """
        for wizard in self:
            lotes = self.env['stock.lot'].search([
                ('production_id', '=', wizard.production_id.id),
                ('needs_review', '=', True),
                ('is_reviewed', '=', False)
            ])
            wizard.rollos_pendientes_text = ", ".join(lotes.mapped('name')) if lotes else "Ninguno pendiente"

    @api.onchange('barcode_scan')
    def _onchange_barcode_scan(self):
        """ Busca el lote y recupera su peso actual del movimiento de inventario """
        if self.barcode_scan:
            lot = self.env['stock.lot'].search([
                ('name', '=', self.barcode_scan),
                ('production_id', '=', self.production_id.id)
            ], limit=1)
            
            if lot:
                if not lot.needs_review:
                    raise UserError(_("Este rollo NO fue seleccionado para revisión aleatoria."))
                if lot.is_reviewed:
                    raise UserError(_("Este rollo YA fue revisado y procesado."))
                
                self.lot_id = lot
                
                # Buscamos el peso real en el movimiento de inventario (stock.move.line)
                # Esto es más preciso que leer el lote directamente si hay retraso en el cálculo de Odoo
                move_line = self.env['stock.move.line'].search([
                    ('lot_id', '=', lot.id),
                    ('production_id', '=', self.production_id.id)
                ], limit=1)
                
                self.peso_original = move_line.quantity if move_line else lot.product_qty
            else:
                self.lot_id = False
                self.peso_original = 0.0
                raise UserError(_("El código de rollo no pertenece a esta Orden de Fabricación."))

    def confirmar_revisado(self):
        """ 
        PROCESO CRÍTICO: 
        1. Valida persistencia del lote.
        2. Crea registro en el Log de Historial.
        3. Actualiza el movimiento de inventario (stock.move.line).
        4. Ajusta los totales de la MO y la WO por la diferencia de peso.
        """
        self.ensure_one()

        # SEGURIDAD: Si por error de sesión el lot_id llega vacío, intentamos recuperarlo por el texto del escáner
        if not self.lot_id and self.barcode_scan:
            self.lot_id = self.env['stock.lot'].search([
                ('name', '=', self.barcode_scan),
                ('production_id', '=', self.production_id.id)
            ], limit=1)

        if not self.lot_id:
            raise UserError(_("Error de lectura: El sistema perdió la referencia del rollo. Por favor, escanee de nuevo."))

        if self.peso_actual <= 0:
            raise UserError(_("Debe capturar un peso mayor a cero para confirmar la revisión."))

        # 1. Crear el registro en el historial (Log de Calidad)
        self.env['mrp.revision.log'].create({
            'production_id': self.production_id.id,
            'lot_id': self.lot_id.id,
            'peso_original': self.peso_original,
            'peso_final': self.peso_actual,
        })

        # 2. Buscar el movimiento contable de inventario original para corregirlo
        move_line = self.env['stock.move.line'].search([
            ('lot_id', '=', self.lot_id.id),
            ('production_id', '=', self.production_id.id)
        ], limit=1)

        if move_line:
            # Calculamos la diferencia (p.e. si pesaba 35 y ahora 32, diff = -3)
            diferencia = self.peso_actual - move_line.quantity
            
            # Actualizamos la cantidad del movimiento real (Esto afecta el Stock Físico)
            move_line.write({'quantity': self.peso_actual})
            
            # 3. Ajustar el avance de la Orden de Fabricación (qty_producing)
            self.production_id.qty_producing += diferencia
            
            # 4. Ajustar el avance de la Orden de Trabajo (qty_produced)
            if move_line.workorder_id:
                move_line.workorder_id.qty_produced += diferencia

        # 5. Actualizar el Lote y marcarlo como revisado para liberar el candado de cierre
        self.lot_id.write({
            'product_qty': self.peso_actual,
            'is_reviewed': True
        })

        return {'type': 'ir.actions.act_window_close'}