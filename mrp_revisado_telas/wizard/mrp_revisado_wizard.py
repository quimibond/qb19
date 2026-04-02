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
    
    # Campos de datos del rollo
    lot_id = fields.Many2one('stock.lot', string='ID Rollo')
    peso_original = fields.Float(string="Peso Original (Kg)", readonly=True)
    peso_actual = fields.Float(string='Nuevo Peso (Kg)', digits=(12, 4))

    # Punto 6: Causa de la desviación (Etiquetas de Calidad filtradas por TEJIDO)
    causa_id = fields.Many2one(
        'quality.tag', 
        string="Causa de Desviación", 
        domain="[('name', '=like', 'TEJIDO%')]",
        help="Seleccione la causa del ajuste de peso"
    )

    @api.depends('production_id')
    def _compute_rollos_pendientes(self):
       for wizard in self:
            # Usamos el nuevo contador que definimos en la OF
            total_revisados = wizard.production_id.rollos_revisados_count
            wizard.rollos_pendientes_text = f"Control de Calidad: Se han revisado {total_revisados} rollos en esta orden."

    @api.onchange('barcode_scan')
    def _onchange_barcode_scan(self):
        """ 
        Busca el lote y recupera su peso original.
        Eliminada la restricción de 'needs_review' para permitir revisión manual.
        """
        if self.barcode_scan:
            # Buscamos el lote por nombre y vinculación a la OF
            lot = self.env['stock.lot'].search([
                ('name', '=', self.barcode_scan.strip()),
                ('production_id', '=', self.production_id.id)
            ], limit=1)
            
            if lot:
                # 1. Validación de ya revisado (esta sí se queda)
                if lot.is_reviewed:
                    self.barcode_scan = False
                    raise UserError(_("Este rollo YA fue revisado y procesado."))
                
                self.lot_id = lot.id
                
                # 2. Recuperar el peso. Intentamos primero por la línea de movimiento 
                # (que es el peso real en la MO) y si no, directamente del lote.
                move_line = self.env['stock.move.line'].search([
                    ('lot_id', '=', lot.id),
                    ('production_id', '=', self.production_id.id)
                ], limit=1)
                
                # ASIGNACIÓN: Aquí es donde se soluciona el "blanco"
                peso_detectado = move_line.quantity if move_line else lot.product_qty
                
                self.peso_original = peso_detectado
                self.peso_actual = peso_detectado
                
                # Limpiar escáner para el siguiente
                self.barcode_scan = False 
            else:
                barcode_err = self.barcode_scan
                self.barcode_scan = False
                self.lot_id = False
                self.peso_original = 0.0
                raise UserError(_("El código de rollo '%s' no pertenece a esta Orden de Fabricación.") % barcode_err)

    def confirmar_revisado(self):
        """ 
        Procesa el cambio de peso, genera el historial y valida estados.
        """
        self.ensure_one()
        
        if not self.lot_id:
            raise UserError(_("Debe escanear o seleccionar un rollo válido."))

        # Punto 5 y 7: Validar que la OF y la OT estén abiertas (En Proceso)
        if self.production_id.state != 'progress':
            raise UserError(_("La Orden de Fabricación debe estar 'En Proceso' para registrar cambios."))

        # Buscamos si hay una orden de trabajo activa
        wo_activa = self.production_id.workorder_ids.filtered(lambda w: w.state == 'progress')
        if not wo_activa:
            raise UserError(_("No hay una Orden de Trabajo en proceso. Inicie la operación antes de revisar."))

        # Punto 6: Validar que se haya seleccionado una causa
        if not self.causa_id:
            raise UserError(_("Debe seleccionar una Causa de Desviación para continuar."))

        # 1. Crear el registro en el historial (Log de Calidad) incluyendo la Causa
        self.env['mrp.revision.log'].create({
            'production_id': self.production_id.id,
            'lot_id': self.lot_id.id,
            'peso_original': self.peso_original,
            'peso_final': self.peso_actual,
            'causa_id': self.causa_id.id, # Punto 6 y 8
        })

        # 2. Buscar el movimiento de inventario para corregir cantidades (Lógica original)
        move_line = self.env['stock.move.line'].search([
            ('lot_id', '=', self.lot_id.id),
            ('production_id', '=', self.production_id.id)
        ], limit=1)

        if move_line:
            diferencia = self.peso_actual - move_line.quantity
            
            # Actualizamos la cantidad del movimiento real
            move_line.write({'quantity': self.peso_actual})
            
            # 3. Ajustar el avance de la Orden de Fabricación (qty_producing)
            self.production_id.qty_producing += diferencia
            
            # 4. Ajustar el avance de la Orden de Trabajo (qty_produced)
            if move_line.workorder_id:
                move_line.workorder_id.qty_produced += diferencia

        # 5. Actualizar el Lote y marcarlo como revisado (Lógica original)
        self.lot_id.write({
            'is_reviewed': True,
            'product_qty': self.peso_actual # Sincronizar peso en el lote
        })

        # Punto 2: Impresión de etiqueta tras cambio de peso (Mismo formato que pesaje)
        # Se asume que el método _print_zpl_label está en mrp.production como lo definimos antes
        self.production_id._print_zpl_label(self.lot_id.name, self.peso_actual, self.lot_id.name)

        return {'type': 'ir.actions.client', 'tag': 'reload'}