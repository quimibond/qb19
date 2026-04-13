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

    def confirm_weighing(self):  # IMPRESION DE LA ETIQEUTA
        self.ensure_one()
        self.production_id.action_register_roll_with_weight(self.weight)
        # OBTENER EL REPORTE Y EJECUTARLO
        # Esto disparará el envío al IoT Box si la impresora está vinculada
        report = self.env.ref('pesaje_rollos_tejido.action_report_weigh_roll')
        return report.report_action(self.production_id)

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
        # Llamamos al nombre exacto definido en mrp_production.py
        self.production_id.action_register_subproduct_manual(self.weight, self.next_lot_name)
        # DISPARAR ETIQUETA DE SUBPRODUCTO
        report = self.env.ref('pesaje_rollos_tejido.action_report_subproduct_weigh')
        return report.report_action(self.production_id)