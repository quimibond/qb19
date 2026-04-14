# -*- coding: utf-8 -*-
from odoo import models, fields, api

class MrpWeighRollWizard(models.TransientModel):
    _name = 'mrp.weigh.roll.wizard'
    _description = 'Asistente de Pesado de Rollos'

    workorder_id = fields.Many2one('mrp.workorder', string="Orden de Trabajo")
    production_id = fields.Many2one('mrp.production', related="workorder_id.production_id", string="Orden de Fabricación")
    workcenter_id = fields.Many2one('mrp.workcenter', related="workorder_id.workcenter_id", string="Centro de Trabajo")
    product_id = fields.Many2one('product.product', related="production_id.product_id", string="Artículo")
    
    qty_to_produce = fields.Float(related="production_id.product_qty", string="Total a Producir")
    qty_produced = fields.Float(related="production_id.qty_producing", string="Producido al Momento")
    roll_count = fields.Integer(related="production_id.roll_count", string="Rollos Registrados")

    production_percentage = fields.Float(
        string="Porcentaje de Producción", 
        compute="_compute_production_percentage"
    )
    
    next_lot_name = fields.Char(string="Número de Lote a Generar", compute="_compute_next_lot_name")
    weight = fields.Float(string="Peso del Rollo Actual (kg)", digits=(12, 3), required=True)

    @api.depends('workorder_id', 'production_id.roll_count')
    def _compute_next_lot_name(self):
        for reg in self:
            if reg.production_id:
                mo_identifier = reg.production_id.name.split('/')[-1]
                reg.next_lot_name = f"{mo_identifier}-{(reg.production_id.roll_count + 1):04d}"
            else:
                reg.next_lot_name = False

    @api.depends('weight', 'qty_produced', 'qty_to_produce')
    def _compute_production_percentage(self):
        for reg in self:
            # Sumamos lo ya producido en la OF + el peso que está en la báscula ahorita
            total_con_este_rollo = reg.qty_produced + reg.weight
            
            if reg.qty_to_produce > 0:
                # Calculamos el avance total real incluyendo el rollo actual
                reg.production_percentage = total_con_este_rollo / reg.qty_to_produce
            else:
                reg.production_percentage = 0.0

    def confirm_weighing(self):
        """ Registro de pesaje, impresión y cierre automático """
        self.ensure_one()
        # 1. Registra el peso en la MO
        self.production_id.action_register_roll_with_weight(self.weight)
        
        # 2. Generar datos para la nueva función de impresión
        mo_identifier = self.production_id.name.split('/')[-1]
        lot_name = f"{mo_identifier}-{self.production_id.roll_count:04d}"
        
        # 3. LLAMADA CRÍTICA: Forzamos el uso de la función de pesaje original
        # Esto corrige el Centro de Trabajo (evita N/A) y el diseño del código de barras
        self.production_id._print_zpl_pesaje_original(lot_name, self.weight, lot_name)
        
        # 4. Disparo del reporte
        report_action = self.env.ref('pesaje_rollos_tejido.action_report_weigh_roll').report_action(self.production_id)
        
        # 5. Cerramos la ventana automáticamente
        report_action.update({'close_on_report_download': True})
        
        return report_action

class MrpSubproductWizard(models.TransientModel):
    _name = 'mrp.subproduct.wizard'
    _description = 'Asistente de Pesado de Subproducto'

    workorder_id = fields.Many2one('mrp.workorder', string="Orden de Trabajo")
    production_id = fields.Many2one('mrp.production', related="workorder_id.production_id", string="Orden de Fabricación")
    workcenter_id = fields.Many2one('mrp.workcenter', related="workorder_id.workcenter_id", string="Centro de Trabajo")
    
    product_id = fields.Many2one('product.product', string="Subproducto", compute="_compute_subproduct")
    next_lot_name = fields.Char(string="Lote a Generar", compute="_compute_next_lot_name")
    weight = fields.Float(string="Peso Subproducto (kg)", digits=(12, 3), required=True)
    
    @api.depends('production_id')
    def _compute_subproduct(self):
        for reg in self:
            move = reg.production_id.move_byproduct_ids[:1]
            reg.product_id = move.product_id if move else False

    @api.depends('production_id')
    def _compute_next_lot_name(self):
        for reg in self:
            if reg.production_id:
                mo_identifier = reg.production_id.name.split('/')[-1]
                reg.next_lot_name = f"SUB-{mo_identifier}-{fields.Date.today()}"
            else:
                reg.next_lot_name = False

    def confirm_subproduct(self):
        self.ensure_one()
        # 1. Registramos y capturamos el objeto del lote creado
        # IMPORTANTE: Tu función en mrp_production debe retornar el lote creado
        lot = self.production_id.action_register_subproduct_manual(self.weight, self.product_id)
        
        # 2. Si la función no retorna el lote, lo buscamos por el nombre que calculó el wizard
        lot_to_print = lot.name if lot else self.next_lot_name

        # 3. Disparamos la impresión pasando el nombre del lote correcto
        self.production_id._print_subproduct_zpl(self.product_id, self.weight, lot_to_print)
        
        res = self.env.ref('pesaje_rollos_tejido.action_report_subproduct_weigh').report_action(self.production_id)
        res.update({'close_on_report_download': True})
        return res