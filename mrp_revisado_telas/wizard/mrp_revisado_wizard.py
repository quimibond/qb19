# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError, ValidationError

class MrpRevisadoWizard(models.TransientModel):
    _name = 'mrp.revisado.wizard'
    _description = 'Wizard de Revisado de Calidad'

    # 1. Nuevos campos para identificación
    employee_number = fields.Char(string="Número de Empleado", required=True)
    employee_name = fields.Char(string="Nombre del Revisor", compute="_compute_employee_name", store=True)

    @api.depends('employee_number')
    def _compute_employee_name(self):
        for reg in self:
            if reg.employee_number:
                # Validamos que el puesto comience con OPERADOR INSPEC TEJ
                emp = self.env['hr.employee'].search([
                    ('x_studio_nmero_de_trabajador', '=', reg.employee_number),
                    ('job_title', 'ilike', 'OPERADOR INSPEC TEJ')
                ], limit=1)
                reg.employee_name = emp.name if emp else "NO AUTORIZADO / NO EXISTE"
            else:
                reg.employee_name = False

    @api.onchange('employee_number')
    def _onchange_employee_number(self):
        if self.employee_number:
            emp = self.env['hr.employee'].search([
                ('x_studio_nmero_de_trabajador', '=', self.employee_number),
                ('job_title', 'ilike', 'OPERADOR INSPEC TEJ')
            ], limit=1)
            
            if not emp:
                val_erroneo = self.employee_number
                self.employee_number = False # Limpia para ocultar la vista
                self.employee_name = False
                
                check_exists = self.env['hr.employee'].search([('x_studio_nmero_de_trabajador', '=', val_erroneo)], limit=1)
                if check_exists:
                    puesto = check_exists.job_title or "SIN PUESTO"
                    raise ValidationError(_("El trabajador %s tiene el puesto '%s'. Solo los de INSPECCIÓN pueden revisar.") % (check_exists.name, puesto))
                else:
                    raise ValidationError(_("Acceso Denegado: El número de empleado '%s' no existe.") % val_erroneo)

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
        for reg in self:
            if reg.production_id:
                revisados = reg.production_id.rollos_revisados_count
                requeridos = reg.production_id.rollos_requeridos_count
                
                if revisados >= requeridos:
                    reg.rollos_pendientes_text = f"¡META CUMPLIDA! ({revisados} revisados)"
                else:
                    faltantes = requeridos - revisados
                    reg.rollos_pendientes_text = f"Faltan {faltantes} rollos por revisar para cumplir el requisito del Centro de Trabajo ({revisados}/{requeridos})."
            else:
                reg.rollos_pendientes_text = ""

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
        self.ensure_one()

        # NUEVO CANDADO ESTRICTO: (Se mantiene igual)
        revisados_actuales = len(self.production_id.revision_log_ids)
        meta_requerida = self.production_id.rollos_requeridos_count
    
        if revisados_actuales >= meta_requerida:
            raise UserError(_(
                 "Meta de revisión completada.\n"
                 "Ya se han revisado %s rollos de una meta de %s. "
                 "No se permiten revisiones adicionales."
            ) % (revisados_actuales, meta_requerida))

        # Validamos que el peso actual no sea superior al original (Se mantiene igual)
        if self.peso_actual > self.peso_original:
            raise UserError(_(
                "Error de Validación:\n"
                "El nuevo peso (%.3f kg) no puede ser mayor al peso original "
                "del rollo (%.3f kg)."
            ) % (self.peso_actual, self.peso_original))

        if not self.lot_id:
            raise UserError(_("Debe escanear un rollo válido antes de confirmar."))

        # VALIDACIÓN QUIRÚRGICA: Redondeo a 2 decimales (Se mantiene igual)
        peso_orig_rd = round(self.peso_original, 2)
        peso_act_rd = round(self.peso_actual, 2)
        hubo_desviacion_actual = peso_orig_rd != peso_act_rd

        # Si el peso CAMBIÓ y no seleccionó causa (Se mantiene igual)
        if hubo_desviacion_actual and not self.causa_id:
            raise UserError(_("El peso ha cambiado (De %.2f a %.2f). Debe seleccionar una Causa de Desviación.") % (peso_orig_rd, peso_act_rd))

        # 1. Crear el log de revisión (Se mantiene igual)
        self.env['mrp.revision.log'].create({
            'production_id': self.production_id.id,
            'lot_id': self.lot_id.id,
            'peso_original': self.peso_original,
            'peso_final': self.peso_actual,
            'causa_id': self.causa_id.id if hubo_desviacion_actual else False,
            'inspector': self.employee_name,
        })

        # 2. Buscar el movimiento de inventario para corregir cantidades (Se mantiene igual)
        move_line = self.env['stock.move.line'].search([
            ('lot_id', '=', self.lot_id.id),
            ('production_id', '=', self.production_id.id)
        ], limit=1)

        if move_line:
            diferencia = self.peso_actual - move_line.quantity
            move_line.write({'quantity': self.peso_actual})
            self.production_id.qty_producing += diferencia
            if move_line.workorder_id:
                move_line.workorder_id.qty_produced += diferencia

        # 3. Actualizar el Lote y marcarlo como revisado (Se mantiene igual)
        self.lot_id.write({
            'is_reviewed': True,
            'product_qty': self.peso_actual
        })

        # --- REVISIÓN DE CONTROL DE CALIDAD (Solo se corrigió el campo y el método) ---
        self.production_id.invalidate_recordset(['rollos_revisados_count', 'revision_log_ids'])
        
        revisados_reales = len(self.production_id.revision_log_ids)
        meta_reales = self.production_id.rollos_requeridos_count
        
        # Eliminamos la evaluación de desviaciones y subproductos para que cierre solo con la meta
        if revisados_reales >= meta_reales:
            checks = self.env['quality.check'].search([
                ('production_id', '=', self.production_id.id),
                ('quality_state', '=', 'none') # Nombre de campo correcto Odoo 19
            ])
            for check in checks:
                # Verificación de método para evitar el AttributeError
                if hasattr(check, 'do_pass'):
                    check.do_pass()
                elif hasattr(check, 'action_pass'):
                    check.action_pass()
                else:
                    check.write({
                        'quality_state': 'pass',
                        'user_id': self.env.user.id,
                        'control_date': fields.Datetime.now()
                    })
        # --- FIN LÓGICA CALIDAD ---

        # Punto 2: Impresión de etiqueta (Se mantiene igual)
        if hubo_desviacion_actual:
            self.production_id._print_zpl_label(self.lot_id.name, self.peso_actual, self.lot_id.name,pesador=self.employee_name )
            
            report_action = self.env.ref('mrp_revisado_telas.action_report_revisado_label').report_action(self.production_id)
            report_action.update({'close_on_report_download': True})
            return report_action

        return {'type': 'ir.actions.client', 'tag': 'reload'}